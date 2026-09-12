# GateLink hash routing review 必要修正

## 1. 結論與範圍

`GATELINK_HASH_ROUTING_DESIGN.md` 對 Gate → Game player affinity、DNS refresh 與 per-endpoint connection pool
的方向正確；目前 staged implementation 也沒有加入 retry、額外 metrics、custom balancer 或其他非需求功能。

在進一步 review 後，需要修正以下四項，才能完整符合設計與長時間運行的 correctness：

1. picker 必須喚醒進入 `Idle` 的 gRPC connections，並且不能推進未選中 endpoint 的 round-robin counter；
2. 必須用 race detector 實際覆蓋 concurrent `Forward`／refresh／`Stop`；
3. 必須驗證 WebSocket 注入的 affinity key 確實是 authenticated `login_name`；
4. product composition 回報 client config 錯誤時，必須包含 `grpc.clients.game` config path。

這四項都直接對應既有需求或設計契約。本次不調整 hash 演算法、不新增 public API、不修改 protobuf、
dispatcher、Game server、server send、broadcast、metrics 或 load CLI。

---

## 2. 修正一：分離 endpoint readiness probe 與 connection selection

### 2.1 問題

目前 `pickEndpoint` 在掃描每個 endpoint 時都呼叫 `endpoint.pick()`。`pick()` 會推進該 pool 的 atomic
round-robin counter，因此即使 endpoint 最後沒有被選中，其 counter 仍會改變。

這會使 connection 分配取決於其他 endpoint 的 request 順序。例如兩個 endpoint 各有兩條 connection，而 request
依序交替選中兩個 endpoint 時，每個 pool 的 counter 都會在每筆 request 前進，可能讓實際被選中的 request
重複落到同一條 connection，無法穩定利用 `connections_per_host` 提供的容量。

另一個必要問題是 grpc-go v1.72.0 的 `ClientConn` 預設閒置一段時間後會進入 `Idle`。建立 pool 時只呼叫一次
`Connect()` 不足以處理之後重新進入 `Idle` 的情況：

- picker 只把 `Ready` connection 視為可用；
- 只要另有 Ready endpoint／connection，Idle connection 就不會收到 RPC；
- 沒有 RPC 或新的 `Connect()`，該 connection 不會離開 Idle；
- 長時間運行後，affinity endpoint 或 per-host pool 容量可能無法自行恢復。

### 2.2 必要修改

修改 `pkg/gatelink/routing.go`，新增 package-private、只負責觀察狀態的 method：

```go
func (p *endpointPool) hasReady() bool {
	if p == nil {
		return false
	}
	ready := false
	for _, connection := range p.conns {
		if connection == nil || connection.conn == nil {
			continue
		}
		switch connection.conn.GetState() {
		case connectivity.Ready:
			ready = true
		case connectivity.Idle:
			// Connect 對非 Idle state 是 no-op；此處只喚醒已閒置的 channel。
			connection.conn.Connect()
		}
	}
	return ready
}
```

`hasReady` 必須符合以下限制：

- 不讀寫 `endpointPool.next`；
- 掃描完整個 pool，使同一 pool 中「一條 Ready、另一條 Idle」時也會喚醒 Idle connection；
- 不等待 connection 變成 Ready；本次 request 仍依當下 snapshot 與 state 選擇；
- 不加入 keepalive、idle-timeout config 或 background connection monitor。

接著修改 `pickEndpoint`：

1. rendezvous scan 只選出 `preferred` 與最高分的 `readyPool`；
2. readiness 判斷改用 `endpoint.hasReady()`；
3. scan 期間不保存 connection，也不呼叫 `endpoint.pick()`；
4. endpoint 決定後，才對選中的 pool 呼叫一次 `pick()`。

核心形狀如下：

```go
for _, endpoint := range snapshot.endpoints {
	if endpoint == nil || len(endpoint.conns) == 0 {
		continue
	}
	score := rendezvousScore(affinityKey, endpoint.address)
	if !preferredSet || score > preferredScore {
		preferred, preferredScore, preferredSet = endpoint, score, true
	}
	if endpoint.hasReady() && (!readySet || score > readyScore) {
		readyPool, readyScore, readySet = endpoint, score, true
	}
}

selected := preferred
if readySet {
	selected = readyPool
}
if selected == nil {
	return nil, nil
}
connection, _ := selected.pick()
return selected, connection
```

`hasReady()` 與後續 `pick()` 之間的 state 可能改變，這是 gRPC connectivity state 的正常競態。`pick()` 在沒有
Ready connection 時仍回傳 deterministic fallback，讓 grpc-go／request deadline 回報最終結果；不得因此加入
第二個 endpoint retry。

### 2.3 必要測試

在 `pkg/gatelink/routing_contract_test.go` 增加：

- `TestContractEndpointScanDoesNotAdvanceUnselectedPool`
  - 建立兩個 Ready pools；
  - 找出特定 affinity key 會選中的 pool；
  - 呼叫 `pickEndpoint` 後，斷言選中 pool 的 `next` 增加一次、另一個 pool 的 `next` 保持不變。
- `TestContractReadinessProbeReconnectsIdleConnections`
  - 同一 pool 放入一條 Ready 與一條 Idle fake connection；
  - 呼叫 `hasReady()`；
  - 斷言結果為 true，Idle connection 的 `Connect()` 恰好被呼叫一次，且 `next` 沒有改變。
- 保留既有 `TestContractEndpointConnectionsRoundRobinWithinAffinity`，用來證明真正選中 pool 後仍會在 Ready
  connections 間 round-robin。

為測試 Connect 次數，只需在既有 `pickerConn` 增加 `connect atomic.Int32`，並讓其 `Connect()` 累加；不新增
production factory seam。

---

## 3. 修正二：補 concurrent Forward／refresh／Stop race contract

### 3.1 問題

目前執行 `go test -race` 會通過，但現有測試沒有讓 `Forward`、refresh 與 `Stop` 同時操作同一個 `Client`。
因此 race detector 尚未真正覆蓋設計最重要的 snapshot publish／connection close 邊界。

### 3.2 必要修改

只修改 `pkg/gatelink/routing_contract_test.go`，沿用既有 package-private fake，不新增 production clock、resolver
或 connection factory abstraction。

新增 `TestContractConcurrentForwardRefreshAndStop`：

1. 建立一個 `started=true` 的 test client、fake resolver 及包含 fake connection／fake RPC client 的 snapshot；
2. fake RPC client 在 `Forward` 進入後透過 channel 通知測試，並阻塞到測試釋放；
3. RPC 已取得 snapshot 後，同時啟動一次 `refreshDNS` 與 `Stop`；
4. 釋放 RPC，等待三條 goroutine 全部結束；
5. 斷言沒有 panic、RPC 呼叫最多一次、connection 最終只被關閉一次，且 `snapshot.Load()` 為 nil；
6. test 必須能在 `go test -race` 下穩定執行。

測試同步使用 channel／`sync.WaitGroup`，不以固定 `time.Sleep` 判斷 goroutine 是否進入臨界區。可以保留一個短
test deadline 防止測試本身永久等待，但 deadline 不作為流程排序依據。

這個測試接受設計已明定的行為：已取得舊 snapshot 的 in-flight Forward 可能因 Stop／endpoint removal 關閉
connection 而失敗；必要契約是沒有 data race、panic、重送或資源重複關閉，而不是保證該 RPC 成功。

---

## 4. 修正三：明確驗證 WebSocket 使用 authenticated login name

### 4.1 問題

既有 WebSocket integration test 可證明 Forward 前存在 affinity key，因為缺少 key 時 `gatelink.Client.Forward`
會直接失敗；但它無法確認 key 的內容是 `SessionRegistry` 中的 authenticated `login_name`。若日後誤改為
connection ID，既有測試仍會通過，卻會破壞跨 reconnect／跨 Gate 的 player affinity。

affinity key 刻意不傳入 gRPC metadata，因此不能在 Game test server 端檢查；應在 Gate 本機 transport boundary
驗證。

### 4.2 必要修改

修改 `products/gateproduct/websocket.go`，新增最小 package-private consumer interface：

```go
type gameForwarder interface {
	Forward(context.Context, gatelink.Request) (*gatelink.Reply, error)
}
```

只把 `WebSocketServer.gameClient` 欄位型別由 `*gatelink.Client` 改成 `gameForwarder`。`webSocketServerInputs.GameClient`
仍維持 `*gatelink.Client`，因此不修改 dig registration、public constructor 或 product composition。

constructor 必須先檢查 `inputs.GameClient == nil`，再賦值給 interface，避免 typed nil pointer 被包入 interface 後
變成非 nil：

```go
if inputs.GameClient == nil {
	return nil, errors.New("gate websocket: Game client is nil")
}

server := &WebSocketServer{
	// 其他欄位維持原樣。
	gameClient: inputs.GameClient,
}
```

不得把這個 interface 移到 public package，也不修改 `gatelink.Client` public API。

### 4.3 必要測試

在現有 `products/gateproduct/websocket_state_contract_test.go` 增加一個 fake `gameForwarder`，其 `Forward`：

- 從收到的 context 呼叫 `gatelink.AffinityKeyFromContext`；
- 記錄 key 與 request；
- 回傳 nil reply、nil error。

新增 `TestWebSocketForwardUsesAuthenticatedLoginNameAffinity`：

1. 建立 SessionRegistry、fake session 與未註冊任何目標 command 的 dispatcher；
2. 將 session 註冊為 login name `alice` 並加入 room；
3. server 注入 fake `gameForwarder`；
4. 呼叫 `dispatchPacket`；
5. 斷言 Forward 收到的 affinity key 是完整的 `alice`，且 command ID／payload 未改變；
6. 不從 payload 或 connection ID 推導預期值。

這是 package-private 的單方法 seam，只為驗證既有 transport boundary，不擴張為通用 client abstraction。

---

## 5. 修正四：保留完整 config path

### 5.1 問題

`gateGameGRPCConfig` 已為負值等 binding error 提供完整 key，但 unsupported target scheme、target 格式錯誤及
static target 明確設定 `dns_refresh_interval` 等錯誤，是在 `gatelink.NewClientWithLogger` 才發生。目前
`newGateGameGRPCClient` 直接回傳底層錯誤，operator 無法從錯誤訊息直接定位 `grpc.clients.game`。

### 5.2 必要修改

修改 `products/gateproduct/grpc.go` 的 `newGateGameGRPCClient`，只在 product composition boundary 包裝一次：

```go
client, err := gatelink.NewClientWithLogger(cfg, logger)
if err != nil {
	return nil, fmt.Errorf("gate gRPC: validate grpc.clients.game: %w", err)
}
return client, nil
```

不在 product 層重寫 target parser，也不新增 error type。底層錯誤已包含具體欄位或 target，外層只補 product
config path。

### 5.3 必要測試

在 `products/gateproduct/grpc_contract_test.go` 增加 composition-level invalid target test，至少覆蓋一種只有
`NewClientWithLogger` 才能發現的錯誤，例如 unsupported scheme，並斷言錯誤同時包含：

- `grpc.clients.game`；
- `unsupported target scheme`。

不必為每種底層 target parse error 重複 table test；`pkg/gatelink` 已負責 parser contract。

---

## 6. 具體檔案修改清單

| 檔案 | 必要修改 |
|---|---|
| `pkg/gatelink/routing.go` | 新增無 counter side effect 的 readiness probe、喚醒 Idle connection、選定 endpoint 後才 pick connection |
| `pkg/gatelink/routing_contract_test.go` | 增加 unselected counter、Idle recovery 及 concurrent Forward／refresh／Stop tests |
| `products/gateproduct/websocket.go` | 增加 package-private `gameForwarder`，保留 production DI 使用 concrete `*gatelink.Client` |
| `products/gateproduct/websocket_state_contract_test.go` | 驗證 authenticated `login_name` 是實際 affinity key |
| `products/gateproduct/grpc.go` | 包裝 GateLink client 建立錯誤並補上 `grpc.clients.game` path |
| `products/gateproduct/grpc_contract_test.go` | 驗證 composition error 包含 config path 與底層原因 |

不需要修改其他檔案。

---

## 7. 驗證順序與完成條件

先完成實作與測試檔案，再執行：

```text
gofmt -w <本次修改的 Go 檔案>
go test -count=1 ./pkg/gatelink ./products/gateproduct
go test -race -count=1 ./pkg/gatelink ./products/gateproduct
go test -count=1 ./...
go vet ./...
git diff --check
```

完成條件：

- Idle connections 會被重新觸發連線，且不需要新增 config；
- endpoint scan 不改變未選中 pool 的 round-robin state；
- 同一筆 Forward 發出後失敗時仍不 retry；
- concurrent Forward／refresh／Stop 沒有 race、panic 或重複 close；
- WebSocket affinity key 精確等於 SessionRegistry 的 login name；
- invalid Gate → Game config error 可直接定位 `grpc.clients.game`；
- 全 repository test／vet／diff check 通過。

實際 DNS scale、affinity 分布與壓測仍依原設計留到實作者完成且 user review 後再執行。

---

## 8. 修改量與 self-review

實際修改約 279 行新增、23 行刪除（約 302 行 touched）：

- production code 約 47 行新增、15 行刪除；
- routing、Idle 與 concurrent lifecycle tests 約 169 行新增、8 行刪除；
- WebSocket affinity test 約 44 行新增；
- config error test 約 19 行新增。

這個數字以本次實作相對於原 staged snapshot 的 `git diff --stat` 為準；增加的行數主要是 deterministic
contract test 的同步與斷言，不代表增加了額外 runtime 功能。

必要性複核：

| 調整 | 必要原因 | 是否擴張需求 |
|---|---|---|
| 喚醒 Idle connection | 避免 affinity endpoint／configured pool 容量長時間無法恢復 | 否；修正現有 pool lifecycle |
| scan 與 pick 分離 | 確保只有真正使用的 pool 推進 round-robin | 否；修正現有 picker correctness |
| concurrent lifecycle test | 驗證 immutable snapshot 與 close 邊界的核心安全性 | 否；只補設計明定的 contract |
| login-name affinity test | 防止誤用 connection ID 等非業務 identity | 否；直接驗證既有需求 |
| config path wrapping | 讓 operator 能定位設定錯誤 | 否；不增加 config 或 parser |

明確不加入 connection idle-timeout knob、keepalive、retry、jitter、health RPC、custom balancer、額外 metrics、
public resolver／forwarder interface 或新的 background monitor。上述修正剛好處理 review 發現的 correctness 與
contract 缺口，沒有冗餘，也沒有超出 `GATELINK_HASH_ROUTING_DESIGN.md` 的需求。
