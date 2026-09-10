# Gate-to-Game Unary 原路回覆設計

## 1. 目的

將 `RequestPlayerSender` 對「目前這筆 Gate request 的原始 WebSocket connection」回覆，從目前額外發起的
Game → Gate unary gRPC request，改為放入既有 Gate → Game `Forward` unary RPC 的 response。

修改後的 Echo 路徑為：

```text
WebSocket request
  -> Gate gatelink.Client.Forward
  -> Game gRPC handler
  -> Game dispatcher / business handler
  -> RequestPlayerSender 寫入本次 Forward 的單一 reply slot
  -> business handler 結束
  -> ForwardResponse 沿同一個 unary RPC 返回 Gate
  -> Gate enqueue 原 WebSocket connection
  -> 既有 writer goroutine 寫回 player
```

這項修改的目的只有：

- request-correlated reply 不再建立第二筆反向 gRPC request；
- `RequestPlayerSender` 的業務介面維持不變；
- 每筆 forwarded request 最多產生一筆直接回覆；
- 保留既有 WebSocket queue、write、錯誤與 metrics 觀測流程。

## 2. 需求理解與合理假設

### 2.1 必要行為

1. Game business handler 仍只呼叫：

   ```go
   SendToRequestPlayer(context.Context, serversend.RequestPlayerMessage) (serversend.Receipt, error)
   ```

2. 同一筆 `Forward` request 最多只能成功呼叫一次 `SendToRequestPlayer`。
3. handler 沒有呼叫 sender 時，允許回傳沒有 reply 的成功 response。Framework 不強制所有 command 都要回覆；
   Echo 是否必須回覆由 Echo contract test 保證。
4. unary response 必須等 Game handler 返回後才送出。`SendToRequestPlayer` 只表示 reply 已被本次 request
   接受，不表示 Gate 已 enqueue，也不表示 WebSocket client 已收到。
5. handler 設定 reply 後若最終返回 error，reply 必須丟棄，只回傳 gRPC error。
6. Gate 收到 reply 後，必須 enqueue 到發起該 request 的同一個 WebSocket session；不得依 player presence、
   Redis directory 或 Gate endpoint 重新尋址。
7. `ExpectedLoginName` 非空時，Gate 在 enqueue 前仍須驗證該 connection 的目前登入身分。

### 2.2 範圍外

- `PlayerSender`、`BroadcastSender` 與非 request-correlated server push 的 transport。
- handler 返回後才產生的非同步回覆。
- 一筆 request 多筆 reply、server streaming 或 bidirectional streaming。
- application queue、retry、delivery acknowledgment、gRPC connection pool或訊息持久化。
- WebSocket packet header、Login／EnterRoom state policy及 dispatcher registration 規則。
- Redis／Database pool、observability HTTP server 與 deployment 設定。

## 3. 現況與問題

目前 `pkg/gatelink/gatelink.proto` 的 `Forward` 回傳 `google.protobuf.Empty`。Game 的
`DirectRequestPlayerSender` 從 request metadata 取得 Gate reply endpoint，再呼叫
`GRPCTransport.SendToConnection` 建立另一筆 Game → Gate unary RPC。Gate receiver enqueue 成功後，第二筆 RPC
才返回 Game，接著第一筆 Gate → Game RPC 才能完成。

因此一筆 Echo 形成兩個巢狀的同步 RPC：

```text
Gate --Forward--> Game --SendToConnection--> Gate
Gate <--return--- Game <--enqueue result---- Gate
```

既有壓測已顯示 business handler 與 WebSocket write 本身不是主要延遲，延遲集中於這兩段同步 gRPC
transport。對只需要回覆原請求 connection 的 `RequestPlayerSender`，第二筆 RPC 並非必要。

## 4. 架構與模組邊界

### 4.1 `pkg/gatelink`

負責：

- `ForwardRequest`／`ForwardResponse` wire contract；
- 每筆 inbound `Forward` 專屬、最多一筆的 reply slot；
- 在 handler 成功結束後將 reply 編入 unary response；
- client 將 protobuf response 轉成 transport-neutral Go reply。

`gatelink` 不依賴 `serversend`、Gate session registry 或 WebSocket framing。

### 4.2 `pkg/serversend`

負責：

- 保留公開的 `RequestPlayerSender` interface 與 `RequestPlayerMessage`；
- 驗證 message／payload bound；
- 將 `RequestPlayerMessage` 寫入 `gatelink` 在 context 中提供的 reply slot；
- 將 missing slot、duplicate call 等錯誤轉為明確的 server-send error。

`RequestPlayerSender` 不再解析 `ReplyEndpoint`，也不再呼叫 `GRPCTransport`。

### 4.3 `products/gameproduct`

負責照常執行 dispatcher 與 metrics decorator。Game product 不自行組裝 protobuf response；reply slot 的建立與
收尾由 `gatelink.Server` 統一管理，避免每個 product handler 重複處理 transport lifecycle。

### 4.4 `products/gateproduct`

負責：

- 接收 `gatelink.Client.Forward` 的零或一筆 reply；
- 驗證 optional expected login identity；
- 使用目前 `dispatchPacket` 已持有的 WebSocket session enqueue reply；
- 沿用既有 `outboundSourceServerSend`、write queue、writer 與 Gate metrics。

Gate 不以 response 中的資料尋找其他 connection；原始 session 是唯一目的地。

## 5. 核心資料模型與介面

### 5.1 Protobuf contract

修改 `pkg/gatelink/gatelink.proto`：

```proto
syntax = "proto3";

package gatelink.v1;

option go_package = "github.com/NeoJay0705/gaming-core-casino/pkg/gatelink";

message GateRequest {
  uint32 command_id = 1;
  bytes payload = 2;
}

message ForwardReply {
  uint32 command_id = 1;
  bytes payload = 2;
  // 非空時，Gate 必須確認原 connection 仍是此登入身分。
  string expected_login_name = 3;
}

message ForwardResponse {
  // message field 具 presence；未設定表示 handler 沒有立即回覆。
  ForwardReply reply = 1;
}

service GateRequestService {
  rpc Forward(GateRequest) returns (ForwardResponse);
}
```

不使用 `repeated`，因為 contract 明確限制每筆 request 最多一筆 reply。移除不再使用的
`google/protobuf/empty.proto` import，並以 repository 現有版本重新產生 `gatelink.pb.go` 與
`gatelink_grpc.pb.go`；不得手改 generated code。

### 5.2 Transport-neutral reply

在新的 `pkg/gatelink/reply.go` 增加：

```go
type Reply struct {
	CommandID         uint32
	Payload           []byte
	ExpectedLoginName string
}
```

`Payload` 在寫入 slot 與 client response mapping 時都必須複製，避免 caller 修改 slice 或 protobuf buffer
重用造成資料競爭。

`Client.Forward` 改為明確回傳 optional reply：

```go
func (c *Client) Forward(ctx context.Context, request Request) (*Reply, error)
```

- `nil, nil`：Game handler 成功，但沒有 immediate reply。
- non-nil reply, `nil`：收到一筆合法 reply。
- non-nil error：gRPC、handler 或 response validation 失敗；不得同時交付 reply。

這是刻意的 source-level contract change。若保留只回傳 `error` 的舊 method，新的 reply 可能被外部 caller
靜默丟棄，因此不增加會忽略 response 的 compatibility wrapper。

### 5.3 單一 reply slot

`gatelink.Server.Forward` 在呼叫既有 `RequestHandler` 前，於 request context 放入一個 package-private
reply slot。`RequestHandler` 的 `HandleGateRequest(context.Context, Request) error` signature 不變，dispatcher
與所有 business handler 也不需改為回傳 transport type。

`pkg/gatelink` 提供一個只供 transport adapter 使用的小型 bridge：

```go
func SetForwardReply(ctx context.Context, reply Reply) error
```

業務 module 不直接呼叫此函式；`RequestPlayerSender` 是唯一正常入口。slot 必須以 mutex 保證 concurrent call
下最多一個 caller 成功，並遵守：

1. 沒有 slot或 handler 已結束：回 `ErrForwardReplyUnavailable`；
2. slot 已有 reply：回 `ErrForwardReplyAlreadySet`，保留第一筆，不覆寫；
3. context 已取消：回 `ctx.Err()`；
4. `CommandID == 0`：拒絕；
5. handler 返回時原子地關閉 slot 並取出 reply，之後不得再寫入；
6. handler error 或 panic 時關閉並丟棄 slot 內容。

不建立 channel或 goroutine。reply 只在同步 handler lifecycle 內寫一次，以 mutex 保護一個 pointer 已足夠。

### 5.4 `RequestPlayerSender`

`serversend.RequestPlayerSender` interface 不變。修改 `DirectRequestPlayerSender`：

```go
type DirectRequestPlayerSender struct {
	maxPayloadBytes int
}

func NewDirectRequestPlayerSender(maxPayloadBytes int) (*DirectRequestPlayerSender, error)
```

`maxPayloadBytes == 0` 沿用 `DefaultMaxPayloadBytes`，負值拒絕。`SendToRequestPlayer` 固定執行：

1. 驗證 `RequestPlayerMessage` 與 payload size；
2. 將 `CommandID`、複製後的 `Payload`、`ExpectedLoginName` 映射為 `gatelink.Reply`；
3. 呼叫 `gatelink.SetForwardReply`；
4. 成功時回傳既有 `Receipt{AcceptedAt: ...}`。

新增 `serversend.ErrRequestReplyAlreadySet`。沒有 active reply slot 時沿用
`ErrRequestRouteUnavailable`；不再要求 `GateID`、`ConnectionID` 或 `ReplyEndpoint`，因為目的地由原 unary caller
持有，不再進行 routing。

constructor signature 的改變是必要的 compile-time migration：不再接收 `*GRPCTransport`，避免留下虛假的
network dependency。不要為舊行為保留第二個 `RequestPlayerSender` implementation，避免同一 interface
在 framework product 中出現兩種互斥 delivery semantics。

### 5.5 Game product composition

`products/gameproduct/server_send.go` 的 `newGameRequestPlayerSender` 改由 `serversend.Config.MaxPayloadBytes`
建立 direct sender，再套用既有 `measuredRequestPlayerSender`。它不再依賴 `*serversend.GRPCTransport`。

第一版仍在既有 `server_send` config 啟用時提供 `RequestPlayerSender`，避免趁本次 transport 修改改動 product DI
啟用條件。`GRPCTransport`、Redis presence/directory 與 Gate receiver lifecycle 仍由 `PlayerSender`、broadcast
及混合版本 rollout 使用，本次不刪除。

### 5.6 Gate enqueue

`products/gateproduct/websocket.go` 在 `Client.Forward` 成功後：

1. reply 為 `nil`：command 成功完成，不 enqueue frame；
2. reply 非 nil：以 `time.Now()` 記錄 Gate 收到 unary reply 的時間；
3. `ExpectedLoginName` 非空時，以 `SessionRegistry.State(session.ID())` 驗證目前身分；
4. 以既有 `encodeWebSocketPacket` 建立 `CommandID`／`Payload` frame；
5. 透過 `sendOutbound` enqueue 到目前 session，source 使用既有 `outboundSourceServerSend`、target 使用
   `serverSendTargetConnection`；
6. enqueue 成功後才把整體 Gate command 記為 success。

Gate 沿用既有 bounded result：身分不符記 `ignored`、enqueue 失敗記 `error`、成功記 `queued`；不增加新的
result label value。

身分不符或 enqueue 失敗時，Gate command 為 error並終止該 connection。write queue full 已由
`sendOutbound` 設定 `write_queue_full`，caller 不得用 `forward_error` 覆蓋先發生的 close reason；其他 reply
delivery error 沿用 `forward_error`。

`ExpectedLoginName` 為空時可直接使用目前 session，因此不要求 session 已登入；Gate 現有「forward 前必須進房」
政策仍會阻止目前 example 的 pre-login forward，這項設計不改該政策。

## 6. 資料流與 completion 語意

### 6.1 成功且有 reply

```text
Gate Forward starts
  Game Server creates reply slot
    dispatcher invokes handler
      RequestPlayerSender validates and stores one reply
    handler returns nil
  Game Server closes slot and serializes ForwardResponse
Gate receives response
Gate validates identity and enqueues frame
Gate Forward command completes
writer asynchronously writes frame
```

`SendToRequestPlayer` 成功只代表「reply 已放入本次 unary response slot」。Gate enqueue 會發生在 Game handler
結束之後，因此 Game 無法透過 unary response取得 enqueue acknowledgment；若未來要求 client delivery ack，必須
另案設計 streaming 或 application-level ack，不能假裝 `Receipt` 已涵蓋。

### 6.2 成功但無 reply

handler 返回 `nil` 且 slot 為空時，Game 回傳 `ForwardResponse{reply:nil}`。Gate 不產生 WebSocket frame，連線
保持開啟。這保留 command 可為 fire-and-forget 的 framework 彈性。

### 6.3 handler error

無論 slot 是否已有 reply，只回 gRPC error。Gate 不 enqueue reply，並沿用既有 `forward_error` 關閉連線策略。
這避免 client 同時收到成功 payload 與連線錯誤。

### 6.4 重複呼叫

第二次及後續 `SendToRequestPlayer` 回 `ErrRequestReplyAlreadySet`，不發出第二筆訊息且不覆蓋第一筆。正常 handler
應向上回傳此 error；即使錯誤被忽略，wire response 仍最多只有第一筆，維持 bounded contract。

## 7. Metrics 調整

不新增 metric family，也不增加 label；因 request-player 已改為本機 slot acceptance，移除無法提供獨立
actionability 的 request-player duration／in-flight metrics，保留其 bounded request counter。

既有 metrics 的記錄點調整如下：

| Metric | 修改後語意 |
|---|---|
| `gaming_core_gate_game_grpc_duration_seconds` | Gate 發出 request 到收到同一 unary response；包含 Game handler 與 response transport |
| `gaming_core_game_gate_command_duration_seconds` | Game handler 全程；包含本機 reply validation／slot acceptance，不再包含反向 gRPC |
| `gaming_core_game_server_send_requests_total{operation="request_player",...}` | `RequestPlayerSender` 的本機 acceptance success/error；不代表 Gate enqueue 或 WebSocket delivery |
| `gaming_core_gate_server_send_requests_total{target="connection",...}` | Gate 收到 logical request-player reply後的 enqueue 結果；記錄點由 reverse receiver 移到 Forward response handling |
| `gaming_core_gate_server_send_delivery_duration_seconds{target="connection",...}` | Gate 收到 unary reply 到 WebSocket write terminal；既有 writer 記錄點不變 |
| `gaming_core_gate_websocket_writes_total{source="server_send",...}` | request-player reply 的實際 WebSocket write；source 表示 logical message source，不表示一定使用 reverse RPC |

如此可保持 bounded labels，同時移除錯誤的「Game request-player acceptance 等於 Game → Gate dependency
latency」解讀。平均 latency 可比較：

```text
Gate gRPC average - Game handler average
  ~= Gate request transport + Game response transport + runtime scheduling residual
```

Histogram quantile 不可相減；只有相同測量窗口且 sample 一對一時才能以 `_sum`／`_count` 的平均值估算 residual。

`PlayerSender`／broadcast 仍走 reverse transport，它們在 Gate receiver 的既有 server-send metrics 記錄點不變。

## 8. 錯誤處理

| 情況 | 處理 |
|---|---|
| request／reply `CommandID == 0` | `InvalidArgument` 或 sender validation error |
| reply payload 超過設定上限 | `ErrPayloadTooLarge`，handler 應回 error，reply 不送出 |
| context 沒有 active Forward slot | `ErrRequestRouteUnavailable` |
| 同一 request 第二次設定 reply | `ErrRequestReplyAlreadySet`，保留第一筆 |
| handler cancellation／deadline | 丟棄 reply，沿用 `Canceled`／`DeadlineExceeded` |
| handler 其他 error 或 panic | 丟棄 reply，沿用既有 gRPC `Internal` mapping／recovery |
| client 收到 malformed reply | `Client.Forward` 回 error，Gate 不 enqueue |
| expected login identity 不符 | Gate 記錄 ignored/error並依既有 forward failure policy 關閉 connection |
| WebSocket queue full | 沿用 `write_queue_full` metrics 與 close reason |
| asynchronous socket write error | writer 記錄 error並關閉 connection；不可能回報給已結束的 Game handler |

錯誤不得包含 payload、login name或任意 client 輸入的 Prometheus label。

## 9. 必要修改檔案

### 9.1 Production code

- `pkg/gatelink/gatelink.proto`
  - 新增單一 optional reply response並修改 RPC return type。
- `pkg/gatelink/gatelink.pb.go`、`pkg/gatelink/gatelink_grpc.pb.go`
  - 由既有 protoc toolchain 重新產生。
- `pkg/gatelink/reply.go`
  - `Reply`、reply slot、context bridge 與 bounded lifecycle errors。
- `pkg/gatelink/server.go`
  - 建立／關閉 slot，handler 成功後組裝 `ForwardResponse`。
- `pkg/gatelink/client.go`
  - 回傳並驗證 optional reply。
- `pkg/serversend/message.go`
  - 補 duplicate reply error及更新 `Receipt`／`RequestPlayerSender` contract 註解。
- `pkg/serversend/sender.go`
  - direct request-player sender 改寫入 reply slot，移除 transport／route dependency。
- `products/gameproduct/server_send.go`
  - constructor dependency 改為 payload bound。
- `products/gateproduct/websocket.go`
  - 接收 reply、identity check、enqueue 與既有 metrics 記錄。

不修改 `serversend.proto`、`GRPCTransport`、receiver、Player/Broadcast sender、dispatcher、framework lifecycle或
observability HTTP module；既有 metric descriptors 與 label schema 也不修改。

### 9.2 文件

實作時同步更新下列既有敘述，避免文件仍宣稱 Echo 使用 reverse gRPC／`ReplyEndpoint`：

- `ACTIONABLE_METRICS_DESIGN.md`
- `ACTIONABLE_METRICS_EXAMPLE_VALIDATION.md`
- `ACTIONABLE_METRICS_NEXT_CHANGES.md`
- `examples/metrics/README.md`

這些文件只改 request-player 資料流與 metrics 解讀，不重寫其他章節。

## 10. 測試策略

### 10.1 `pkg/gatelink` contract tests

必要覆蓋：

1. handler 未設定 reply時，client 得到 `nil, nil`；
2. 一筆 reply 的 command、payload、expected login 可完整 round trip；
3. input/output payload 都有 defensive copy；
4. 第二次及 concurrent 設定只有一個成功，第一筆不被覆蓋；
5. handler 返回後再設定會失敗；
6. handler 設定 reply 後返回 error／panic，client 只得到 error；
7. malformed reply（nil response、command zero）被 client 拒絕；
8. 既有 metadata、round-robin、timeout、unknown command與 lifecycle contracts 仍成立。

### 10.2 `pkg/serversend` contract tests

必要覆蓋：

1. `RequestPlayerSender` 將 message 寫入 active Forward slot；
2. 不需要 GateID、ReplyEndpoint、Redis或 `GRPCTransport`；
3. missing slot、duplicate call、invalid message與 oversized payload回正確 typed error；
4. `ExpectedLoginName` 與 payload defensive copy 被保留。

原本以 `DirectRequestPlayerSender` 測 reverse gRPC route 的案例應移除或改由 Player/Broadcast transport tests
承擔，不保留已廢止的 request-player transport 行為。

### 10.3 Product/full-flow contract tests

必要覆蓋：

1. Game product 仍注入同一個 `serversend.RequestPlayerSender` interface；
2. Gate → Game handler 設定一筆 reply後，原 WebSocket connection 收到正確 frame；
3. 測試不啟動 Gate server-send gRPC listener，Echo 仍成功，以此證明沒有額外 Game → Gate RPC；
4. 另一條 WebSocket connection 不會收到 reply；
5. no-reply handler 不送 frame且 connection 保持可用；
6. expected login mismatch、queue full與 handler error沿用預期 close／metrics 行為；
7. Echo 每個成功 request各產生一筆 Game command、一筆 logical request-player與一筆 WebSocket write sample。

### 10.4 驗證順序

依 repository package 邊界執行：

```sh
go test ./pkg/gatelink ./pkg/serversend
go test ./products/gameproduct ./products/gateproduct
go test ./examples/metrics/...
go test ./...
go vet ./...
```

壓測不屬於 compile／contract test 的必要條件；功能測試通過並經人工 review 後，再以相同
`GOMAXPROCS`、warm-up、duration、scrape interval及 200／400 connections重跑基準。

## 11. 相容性與 rollout

protobuf response 從 Empty 改為帶 optional field 的 message。舊 client會忽略未知 response fields，新 client
可把舊 Game 的空 response解讀為 no reply，但混合版本仍有功能差異：新 Game 不再發 reverse request，舊 Gate
會忽略 response 中的 reply。

必要 rollout 順序：

1. 先部署能解析 `ForwardResponse`、且仍保留 reverse receiver 的 Gate；
2. 再部署改用 unary reply slot 的 Game；
3. 確認沒有舊 Game 後，`ReplyEndpoint` metadata及相容性清理可另案處理。

本次保留 `RequestSource.ReplyEndpoint`、metadata propagation、Gate reverse listener及 Redis endpoint
registration。立即刪除它們會擴大修改面，也會破壞 Gate-first rollout：舊 Game仍需要 `ReplyEndpoint`，而
PlayerSender／broadcast等能力仍需要 reverse listener或既有 server-send topology。

`Client.Forward` 與 `NewDirectRequestPlayerSender` 是 Go source-level breaking changes，外部 module必須在同一
版本升級時處理新的 return value／constructor argument。這比保留會靜默丟 reply或虛假依賴 transport 的舊
signature 更安全。

## 12. 關鍵決策與取捨

### 12.1 使用 unary response，不使用 streaming

目前只需要零或一筆同步 reply。Unary 已提供 deadline、cancellation、status及 backpressure；streaming
只會增加 connection lifecycle、ordering與測試成本。

### 12.2 使用單一 slot，不修改 dispatcher handler signature

dispatcher 是 transport-neutral 且目前只回 `error`。為一個 gRPC response 改動所有 handler signature會讓
transport concern進入業務介面。request-scoped slot只影響 `gatelink` 與 sender adapter，修改面較小。

### 12.3 不等待 Gate enqueue acknowledgment

Unary server送出 response 後無法再等待 client acknowledgment。`Receipt` 明確表示本機 response acceptance；
Gate enqueue與 WebSocket terminal結果由 Gate metrics負責。若要求同步 delivery acknowledgment，必須改協定，
不應在目前 interface內隱藏第二筆 RPC。

### 12.4 不刪除一般 server-send transport

原路 response只能處理目前 request 的同步回覆。`PlayerSender`、broadcast及 handler結束後的 push仍需要
Game → Gate transport，因此保留既有 receiver、directory與 fan-out。

## 13. 已知限制與後續擴充方向

- 每筆 request最多一筆 immediate reply；多筆事件應使用明確的 push／streaming contract另案設計。
- Game只知道 reply被本機 slot接受，不知道 Gate enqueue或 browser接收結果。
- mixed-version deployment必須 Gate-first。
- `ReplyEndpoint` metadata在 rollout期間保留；移除是後續 compatibility cleanup，不納入本次。
- Histogram buckets、dashboard、alert與 gRPC runtime profiling不因 transport修改而擴充。

## 14. Self-review：需求符合性與必要性

| 檢查項目 | 結論 |
|---|---|
| 是否沿原 Gate → Game unary RPC返回 | 是；reply是 `ForwardResponse` 的單一 optional message，不建立第二筆 request |
| 業務介面是否維持 | 是；handler仍呼叫 `RequestPlayerSender.SendToRequestPlayer` |
| 是否限制只能呼叫一次 | 是；每個 Forward slot只有第一次設定成功，wire最多一筆 reply |
| 是否等 handler結束才返回 | 是；server只在 handler成功返回後取出 slot並建立 response |
| 未呼叫 sender是否有定義 | 是；成功返回 no-reply，Gate不送 frame |
| handler失敗是否可能誤送 reply | 否；error／panic一律丟棄 slot |
| 是否仍回原 connection | 是；Gate使用呼叫 `Forward` 時持有的 session，不重新 routing |
| 是否保留必要身分檢查 | 是；非空 `ExpectedLoginName` 在 Gate enqueue前驗證 |
| 是否影響其他 server push | 否；PlayerSender、BroadcastSender與 reverse transport不變 |
| metrics是否仍可觀測 latency/error/saturation | 是；保留必要 stage metrics與 request-player counter，移除無獨立 actionability 的本機 duration／in-flight |
| 是否引入不必要元件 | 否；沒有 queue、goroutine、stream、retry、ack、connection pool或新設定 |
| 是否有為清潔而擴大刪除 | 否；ReplyEndpoint與 reverse lifecycle保留供 rollout及其他能力，後續另案清理 |

### Review 後保留的必要調整

1. `ForwardResponse` 與 `Client.Forward` return value：沒有它，Gate無法取得原路 reply。
2. request-scoped single slot：維持既有 business／dispatcher interface並落實最多一次。
3. sender從 reverse transport改為 slot：這是消除額外 RPC 的核心修改。
4. Gate enqueue response：完成 unary reply到既有 WebSocket writer的最後一段。
5. payload、identity、handler-error與duplicate contracts：避免修改後降低既有安全與錯誤語意。
6. metrics記錄點與文件校正：避免仍把本機 slot latency誤判為 Game → Gate network latency。
7. contract/full-flow tests：證明沒有 reverse listener時 Echo仍能完成，才能確認修改真的達成需求。

其餘可能方案——多 reply、streaming、async queue、delivery ack、自動 retry、gRPC pool、移除全部 server-send／
Redis topology或新增 dashboard——均未納入，因為不是完成本次 request-correlated unary reply所必需。
