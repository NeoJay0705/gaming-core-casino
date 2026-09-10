# Actionable Metrics staged changes 必要修正方法

## 1. 目的與範圍

本文件記錄依據 `ACTIONABLE_METRICS_DESIGN.md` review 目前 staged changes 後，仍需完成的最小修正。
只處理會影響壓測邊界、metrics 正確性、wire contract 邊界或設計明列測試契約的項目；不新增新的
metrics、config、middleware、dashboard、alert、tracing 或通用壓測能力。

目前 production 架構與模組邊界不需重構。必要調整集中在：

1. 修正 load client 的終止判定、連線關閉、write deadline 與 login identity。
2. 修正 shutdown 競速時的 terminal reason。
3. 將只服務壓測的 EnterRoom／Echo API 移到 example-local package，避免形成正式相容性承諾。
4. 讓 example command ID contract test 真正鎖定可重現的 wire values。
5. 補齊會直接保護 metrics 正確性的最小 contract tests。
6. 刪除一個已無呼叫端的 private compatibility method。

---

## 2. Load client：只忽略由全域 deadline 引起的預期錯誤

### 問題

`examples/metrics/load/main.go` 的 `isExpectedLoadTermination` 目前只要觀察到 `ctx.Err() != nil`，就把
任意 connection error 當成正常結束。若 server error、protocol error 或 unexpected EOF 發生在 deadline
附近，caller 實際檢查 error 時 context 已到期，壓測便會錯誤增加 `success`。

### 最小修改

保留現有函式，不新增 error type。判斷順序改為：

```go
func isExpectedLoadTermination(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Now().Before(deadline) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var timeoutErr net.Error
	return errors.As(err, &timeoutErr) && timeoutErr.Timeout()
}
```

不得因 `ctx.Err() != nil` 就接受 EOF、protobuf decode error、WebSocket close error 或其他任意錯誤。

### 必要測試

新增 `examples/metrics/load/main_test.go`，以 table-driven tests 驗證：

- 已過 deadline 的 `context.DeadlineExceeded`：`true`。
- 已過 deadline 的 wrapped timeout `net.Error`：`true`。
- 已過 deadline 的一般 error：`false`。
- 尚未到 deadline 的 timeout：`false`。
- `nil` error：`false`。

使用已在過去的 deadline 與尚未到期的 deadline，不使用 `time.Sleep`。

預估：production 修改約 5–10 行，測試約 25–35 行。

---

## 3. Load client：確保 duration 能限制 write，並正常完成 WebSocket close

### 問題

目前只有 read 使用 context deadline；`WriteMessage` 沒有 write deadline，對端停止讀取時可能超過
指定 duration。另一方面，`defer conn.Close()` 直接關閉 TCP，不會送出 WebSocket normal-close frame，
Gate 可能把每次正常壓測結束記為 `read_error`，污染本次最重要的 error metrics。

### 最小修改

在 `runConnection` 完成 dial 後，將同一個全域 deadline 設為 write deadline：

```go
if deadline, ok := ctx.Deadline(); ok {
	if err := conn.SetWriteDeadline(deadline); err != nil {
		_ = conn.Close()
		return 0, fmt.Errorf("set write deadline: %w", err)
	}
}
```

將 `defer conn.Close()` 改為：

```go
defer closeLoadConnection(conn)
```

並新增單一 helper：

```go
func closeLoadConnection(conn *websocket.Conn) {
	if conn == nil {
		return
	}
	deadline := time.Now().Add(time.Second)
	_ = conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		deadline,
	)
	_ = conn.Close()
}
```

Close control frame 是 best effort；不得因關閉失敗覆蓋原本的壓測結果，也不需要 retry 或額外
goroutine。Gate 不應把 `CloseAbnormalClosure` 一律改成正常關閉，否則會隱藏真正的斷線。

### 必要測試

使用本機 `httptest` WebSocket server 做一個小型 contract test：client 結束時 server 收到 normal-close
code。write deadline 只需驗證已設定的 deadline 能終止 blocked/expired write，不建立通用 network
fault framework。

預估：production 修改約 10–18 行，測試約 25–40 行。

---

## 4. Load client：LoginName 必須由程式保證唯一

### 問題

每條 connection 目前使用 `time.Now().UnixNano()` 建立 LoginName。Go API 不保證同時呼叫一定得到不同
值；若碰撞，`SessionRegistry` 會把其中一條連線當成舊 session replacement 並關閉，形成假的壓測錯誤。

### 最小修改

使用本次 run 的固定 ID 加上 loop index，不新增 UUID dependency：

```go
runID := startedAt.UnixNano()
for i := 0; i < *connections; i++ {
	loginName := fmt.Sprintf("load-%d-%d", runID, i)
	wg.Add(1)
	go func(loginName string) {
		defer wg.Done()
		requests, err := runConnection(ctx, *gateURL, payload, loginName)
		// 保留既有統計流程。
	}(loginName)
}
```

`runConnection` 增加 `loginName string` 參數，LoginRequest 直接使用該值。不要用 random、global counter
或把 identity 放進 metric label。

預估：約修改 4–7 行；這個簡單映射不需要獨立 unit test，會由 full-flow/load smoke test 覆蓋。

---

## 5. Gate：shutdown 競速必須記錄 `shutdown`

### 問題

`products/gateproduct/websocket.go` 在 HTTP upgrade 已成功、但 server 同時進入 stopping 的分支，把
terminal reason 設為 `server_closed`。依設計，App shutdown 必須與主動 kick/replacement 的
`server_closed` 分開。

### 最小修改

只修改既有一行：

```go
_ = session.closeWithReason(closeReasonShutdown)
```

不要新增新的 reason，也不改 public `Close()`；public `Close()` 仍維持 `server_closed`。

測試併入 connection close reason contract，驗證 shutdown 與 public close 使用不同的固定 label；
不建立 nondeterministic 的大量 dial/stop race test。

預估：production 1 行，測試約 10–20 行。

---

## 6. Example API：移出正式 `pkg/gateproto`

### 6.1 決策與理由

本次 player-facing 流程中，真正穩定且既有的正式 API 只有 Login。EnterRoom 與 Echo 是為本次
login → enter-room → forward →
server-send 壓測路徑建立的可執行範本，而實際業務 request、response 與資料表尚未定義。若繼續放在
`pkg/gateproto`，就會讓外部 repository 合理地把它們視為 framework 公開 API，產生 protobuf field 與
command ID 的長期 backward-compatibility 負擔。

因此調整為：

- Login proto 與 command IDs 留在正式 `pkg/gateproto`。
- EnterRoom／Echo schema、generated code、IDs 與 contract tests 移到
  `examples/metrics/internal/protocol`。
- Gate/Game product 不依賴 example package；只有 `examples/metrics` 內的 workflow、load client 與
  full-flow test 使用它。
- 保留目前數值與 wire shape，讓範例壓測可重現，但不宣告為 production API reservation。

使用 Go `internal` package 是刻意限制：repository 外部與 `examples/metrics` 之外的 production package
不能引用這組測試協定。這不影響 dispatcher registration 或 metrics labels；合法 command 仍由 runtime
registration 判斷。

### 6.2 檔案搬移

| 目前檔案 | 修正後 |
|---|---|
| `pkg/gateproto/room.proto`、`room.pb.go` | `examples/metrics/internal/protocol/room.proto`、`room.pb.go` |
| `pkg/gateproto/echo.proto`、`echo.pb.go` | `examples/metrics/internal/protocol/echo.proto`、`echo.pb.go` |
| `pkg/gateproto/command_contract_test.go` 中的 EnterRoom/Echo assertions | `examples/metrics/internal/protocol/protocol_contract_test.go` |
| `pkg/gateproto/command.go` 中的 EnterRoom/Echo IDs | `examples/metrics/internal/protocol/command.go` |

`pkg/gateproto/login.go` 恢復原本的 Login request/response constants；完成搬移後刪除只為集中 sample IDs
而新增的 `pkg/gateproto/command.go`。既有 `pkg/gateproto/login_contract_test.go` 已鎖定 Login IDs，不需
複製測試。

不要只是複製後保留兩份 schema 或 constants；那會形成兩個 truth sources。

### 6.3 Example protocol 定義

新增 `examples/metrics/internal/protocol/command.go`：

```go
package protocol

const (
	EnterRoomRequestCommandID  uint32 = 0xF1000001
	EnterRoomResponseCommandID uint32 = 0xF1000002
	EchoRequestCommandID       uint32 = 0xF1000011
	EchoResponseCommandID      uint32 = 0xF1000012
)
```

兩個 `.proto` 使用 example-specific protobuf namespace 與 internal Go package：

```proto
package metrics.example.v1;

option go_package = "github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/protocol;protocol";
```

Message fields 維持不變：

```proto
message EnterRoomRequest { string room_id = 1; }
message EnterRoomResponse { uint32 code = 1; }
message EchoRequest { bytes payload = 1; }
message EchoResponse { bytes payload = 1; }
```

`.pb.go` 必須由 `.proto` 重新生成，不手改 generated code。沿用 staged generated header 所記錄的工具版本，
可使用相同版本執行：

```text
protoc --go_out=. --go_opt=paths=source_relative \
  examples/metrics/internal/protocol/room.proto \
  examples/metrics/internal/protocol/echo.proto
```

本次不為此搬移新增 Buf、Make target 或 codegen framework。

### 6.4 更新 example imports 與註冊

下列檔案同時 import 正式 Login package 與 example protocol：

```go
import (
	exampleproto "github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/protocol"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gateproto"
)
```

需更新：

- `examples/metrics/internal/workflow/gate.go`
  - Login 仍使用 `gateproto.LoginRequest`、`LoginResponse` 與 Login command IDs。
  - EnterRoom handler 改用 `exampleproto.EnterRoomRequest`、`EnterRoomResponse` 與 sample IDs。
- `examples/metrics/internal/workflow/game.go`
  - Echo dispatcher registration、request、response 與 IDs 全部改用 `exampleproto`。
- `examples/metrics/load/main.go`
  - Login 使用 `gateproto`；EnterRoom/Echo 使用 `exampleproto`。
- `examples/metrics/flow_contract_test.go`
  - 與 load client 使用相同分工，不再從正式 package 引用 sample messages。
- `examples/metrics/README.md`
  - 將「正式 workflow/API」改為「可執行 example workflow」。
  - 明確說明 EnterRoom/Echo 只屬於範例，不是 production authentication 或 business API。

Registration 已經位於 `examples/metrics/internal/workflow`，不需要搬動 Gate/Game dispatcher，也不把
handler 加入 product default modules。

### 6.5 Example protocol contract test

在 `examples/metrics/internal/protocol/protocol_contract_test.go` 逐項鎖定 sample IDs：

```go
tests := []struct {
	name string
	got  uint32
	want uint32
}{
	{"EnterRoomRequest", EnterRoomRequestCommandID, 0xF1000001},
	{"EnterRoomResponse", EnterRoomResponseCommandID, 0xF1000002},
	{"EchoRequest", EchoRequestCommandID, 0xF1000011},
	{"EchoResponse", EchoResponseCommandID, 0xF1000012},
}
```

逐項驗證 `got == want`，並保留 uniqueness、protobuf full name、field number、field name 與 kind
assertions。這些 IDs 雖不是 production contract，仍須固定，否則 README、load client、Gate workflow
與 Game workflow 可能漂移。不要建立額外 command catalog 或 proto enum。

### 6.6 Dispatcher nil contract

在 `pkg/dispatcher/dispatcher_contract_test.go` 增加：

```go
var nilDispatcher *Dispatcher
if nilDispatcher.IsRegistered("gate-request", 7) {
	t.Fatal("nil Dispatcher reported a registered command")
}
```

### 6.7 同步修正設計文件

這項搬移改變原設計「正式 Echo API」的決策，必須先同步修改 `ACTIONABLE_METRICS_DESIGN.md`，不能只改
程式。只調整與 API ownership 直接相關的段落：

- 第 1、2、3 節：把 EnterRoom／Echo 從正式 API 改為 example-local test protocol。
- 第 4.6、7 節：owner 改為 `examples/metrics/internal/protocol`，Login 仍屬於 `pkg/gateproto`；移除
  `0xF1000000`–`0xF10000FF` 是 production API reservation 的宣告，改為 example-local 固定值。
- 第 10.2、10.3 節：更新實際檔案清單與 imports。
- 第 12.4、13、14 節：移除 production compatibility 宣告，改記錄 internal example 的取捨與限制。
- 第 15 節：調整實作順序，先產生 example proto，再組裝 workflow。

其他 metrics、lifecycle、state、pool 與測試設計不變，不需要重寫整份設計文件。

預估：搬移／重新生成約 420–460 行（主要是 generated code 與 contract test 的位置變更）；實際 handwritten code、imports
與 tests 約修改 35–55 行，設計文件約修改 25–45 行。

---

## 7. Gate/Game metrics：補齊 terminal-state contract tests

### 問題

目前測試大多只確認 metric family 存在。這無法發現 double count、錯誤 label、in-flight 未歸零、
queue Gauge 漂移，或 server-send envelope 沒有 terminal result。這些都是本次 instrumentation 的核心
正確性，而不是額外 coverage。

### 最小測試集合

優先擴充現有測試與 helper，不新增 mock framework。

#### 7.1 `products/gateproduct/websocket_metrics_contract_test.go`

- Queue full：保留現有案例，增加以下 assertions：
  - `write_queue_full_total{source="handler"} == 1`。
  - `connection_closes_total{reason="write_queue_full"} == 1`。
  - `websocket_connections`、`write_queue_messages`、`write_queue_capacity` 最終皆為 `0`。
- Close discard：enqueue 一個 `source=server_send` envelope，不啟動 writer；close 後呼叫 `finish()`：
  - delivery Histogram `{target="connection",result="dropped"}` count 為 `1`。
  - queue Gauge 回到 `0`。
- Write error：使用 nil/已關閉 connection 讓 writer 執行一次確定失敗的 write：
  - `writes_total{source="server_send",result="error"} == 1`。
  - delivery Histogram `{target="connection",result="error"}` count 為 `1`。
  - writes in-flight 最終為 `0`，close reason 為 `write_error`。

#### 7.2 `examples/metrics/flow_contract_test.go`

在既有 login → enter-room → echo full flow 增加精確 assertions：

- Gate local Login success 為兩筆（主流程與 login-only），EnterRoom success 為一筆。
- Gate `route="game",command="forward",result="success"` Echo 為一筆。
- Gate-to-Game `code="OK"` 為一筆。
- handler write 與 server-send write 的 success counters 存在且數量正確。
- server-send delivery `{target="connection",result="success"}` Histogram count 為一筆。
- 未登入進房與未進房 forward 分別只產生 `login_required`、`room_required` close reason。
- command/RPC/write in-flight 與 queue messages 最終為零。

Histogram 只斷言 sample count 與 bounded labels，不斷言實際 duration 或 bucket 分布。需要等待 async writer
完成時，使用有 deadline 的 polling helper，不使用固定 `time.Sleep`。

#### 7.3 `products/gameproduct/metrics_contract_test.go`

- registered 與 unknown command 分別驗證 exact label/count，且 raw unknown ID 不出現在 labels。
- 用小型 fake `RequestPlayerSender` 驗證 success/error counters、Histogram count 與 in-flight 歸零。
- fake 只需回傳固定 receipt/error；若要觀察 in-flight，使用一個受 test channel 控制的 blocked call。

這些測試沿用既有 `prometheus.Gatherer`，不把 Gatherer 擴大成 production DI contract。

預估：約增加 100–150 行。

---

## 8. DB/Redis pool：驗證 started/stopped samples 與 lazy registration

### 問題

Redis test 目前只 scrape 未啟動的空 client；Database test 只檢查一個 Gauge。尚未鎖住六個 stats mappings、
Stop 後 sample 消失，以及 resource 未 resolve 時不註冊 collector 的設計契約。

### 最小測試集合

#### 8.1 Database

擴充 `pkg/infra/database/metrics_contract_test.go`：

- 使用既有 `testConnector` 啟動 client。
- Gather 後驗證六個固定 family name 與正確 metric type。
- 至少驗證 `max_open_connections` 的設定值，以及 open/in-use/idle 間的一致關係。
- 呼叫 `Stop` 後再次 Gather，確認六個 pool families 都沒有 samples。

不需要為了製造 wait contention 新增 worker pool；wait counters 的欄位名稱、Counter type 與零值映射即可
保護本次 collector contract。

#### 8.2 Redis

擴充 `pkg/infra/redis/metrics_contract_test.go`：

- 使用既有 `miniredis` dependency 與真實 `redis.Client.Start`。
- 驗證六個固定 family name、metric type，以及 total/idle connections 為非負且關係合理。
- Stop 後 Gather，確認 pool families 不再輸出 samples。

不建立 Redis Cluster 或 per-node case，因本次 contract 明確採用 aggregate `PoolStats()`。

#### 8.3 Infra lazy registration

擴充 `pkg/infra/module_contract_test.go`，透過 framework DI 驗證：

- 無 hook/component 依賴 Redis/Database 時，registry 沒有 pool metric families，也不會嘗試連線。
- hook 依賴 Redis client 時才 resolve/start client，並出現 Redis pool metrics。

現有直接呼叫 `newRedis`／`newDatabase` 的 duplicate registration test 保留。無需為 Database 啟動
外部 MySQL；Database lifecycle mapping 已由 package test 覆蓋。

預估：約增加 60–80 行。

---

## 9. 刪除唯一確認的冗餘程式

`products/gateproduct/websocket.go` 的以下 private method 已無呼叫端：

```go
func (s *webSocketConnection) close() error {
	return s.closeWithReason(closeReasonServerClosed)
}
```

直接刪除即可。保留 public `Close()` 與 private `closeWithReason()`；不需要另外抽象 terminal reason manager。

預估：刪除 1 行（目前為單行格式）。

---

## 10. 不需要修改的部分

Review 後以下設計與實作維持原狀：

- App-local Prometheus registry 與 Go/process collectors。
- `Dispatcher.IsRegistered` 作為 command label trust boundary。
- Gate 對未註冊 command 使用 `forward`，Game 使用 registered numeric ID 或 `unknown`。
- `SessionState` 與 login/room enforcement。
- asynchronous WebSocket writer 與 private outbound envelope。
- server-send RPC 的 enqueue/acceptance 語意。
- DB/Redis collector 隨 lazy resource resolve 才註冊。
- 正式 Login proto、四個 runnable mains 與既有 endpoint/lifecycle。
- EnterRoom/Echo 的 message fields、numeric IDs 與 workflow 行為；只改 ownership 與 import path。

不新增 bytes metrics、command name registry、per-user/room labels、distributed tracing、alerts、dashboard、
Docker Compose、retry、rate limit 或新的 runtime queue。

---

## 11. 修改規模與完成條件

預估必要修改：

| 類別 | 預估行數 |
|---|---:|
| Production/load 修正 | 20–30 |
| Example protocol handwritten code/imports/tests | 35–55 |
| 設計文件 API ownership 修訂 | 25–45 |
| Gate/Game terminal metrics tests | 100–150 |
| DB/Redis/infra lifecycle tests | 60–80 |
| 刪除冗餘 | 1 |
| **實質 handwritten 修改合計** | **約 241–361** |

此外約有 420–460 行 proto/generated code 與既有 contract test 搬移或重新生成；這是 ownership
relocation，不是新增相同規模
的 runtime 邏輯。實際 `git diff --stat` 會受 Git rename detection 影響，不適合直接當成設計複雜度。

完成後執行：

```text
gofmt
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
git diff --check
```

並人工確認：

1. load duration 到期不會增加 failure，也不會讓 Gate 正常結束被記成 `read_error`。
2. 任意非 timeout error 即使發生在 deadline 附近仍會增加 failure。
3. 所有新增 label value 都來自文件定義的 bounded set。
4. 所有 Gauge 在 request/connection terminal state 後回到零。
5. `pkg/gateproto` 不再包含 EnterRoom/Echo 或其 IDs，避免正式與 example 同時存在兩份 contract。
6. staged diff 仍只包含修訂後 `ACTIONABLE_METRICS_DESIGN.md` 第 10 節允許的檔案與本修正文件。

`ACTIONABLE_METRICS_DESIGN.md` 目前尚未被 tracked；完成第 6.7 節的必要 ownership 修訂後，若設計與
實作要在同一 commit 發布，應將設計文件與本修正文件一併加入 staging。
