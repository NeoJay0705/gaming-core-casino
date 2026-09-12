# Batch Player Send 設計

> 本文件是目前 Game → Gate player-send 的最新 contract；其中涉及
> `PlayerSender`、`GateDelivery` 專用 player RPC 與 player fallback 的部分，
> 取代早期文件對單筆 player route 的定義。request-player 與 room broadcast
> 的既有 contract 不受此文件影響。

## 1. 目的與結論

本次將 Game → Gate 的指定玩家推送由逐玩家 routing／逐玩家 gRPC，改為批次 routing 與依 Gate endpoint 分組傳送。設計只處理指定玩家推送，不修改 request-player 原 unary reply，也不改 room broadcast 的 Redis Pub/Sub chain。

定案如下：

- `PlayerSender` 僅提供批次 `SendToPlayers`；單一玩家以長度一的 slice 呼叫，不保留重複的單筆介面；
- Redis presence 與 Gate endpoint 使用 pipeline 批次查詢，不為每個玩家建立 goroutine；
- 正常路徑依 endpoint 分組，同一 endpoint 的一個 chunk 只發一個 gRPC；
- 不新增專用 RPC，使用既有 `GateDelivery.Forward(gatelink.GateRequest)` 與 `RemoteCommandChannel` dispatcher；
- 只有 Redis client 操作失敗且 context 仍有效時，才放棄 exact routing 並將整批訊息透過 DNS endpoint fan-out 至所有 Gate；
- presence 不存在、endpoint key 不存在、route 資料 malformed、gRPC／Gate handler error 都不觸發 fallback；
- 每個批次 command 的 protobuf encoded payload 上限沿用 `serversend.DefaultMaxPayloadBytes`，目前為 1 MiB；
- 不同 endpoint 使用既有上限 16 的 bounded parallel；同一 endpoint 的 chunks 依輸入順序同步傳送；
- sender 不新增背景 queue 或常駐 worker；方法等待 routing 與 Gate `Forward` 完成。Gate 接受後仍由既有 WebSocket writer goroutine 非同步 write。

這些選擇讓 RPC 數量由最壞的 `players × Gates` 降為正常路徑約 `endpoints × chunks`，同時保留明確 backpressure、既有 lifecycle 與通用 command contract。

## 2. 需求理解與合理假設

### 2.1 必要行為

呼叫者一次提供多筆 `PlayerMessage`。每筆包含：

- framework 已驗證的 canonical `login_name`；
- client-facing `command_id`；
- opaque protobuf `payload` bytes。

Game 批次取得玩家目前的 Gate ownership 與 Gate endpoint，依 endpoint 分組後透過共用 `Forward` 傳送。Gate 只解開 batch envelope，依 `login_name` 找本機 session，將 `command_id + payload` 編成既有 16-byte WebSocket header 加 payload，再 enqueue 至該 session。

Gate 不解析 client payload 的 protobuf schema。多筆相同 `login_name` 是合法的，輸入順序即為該玩家的 enqueue 順序。

### 2.2 「非同步」的邊界

批次、並行 I/O 與背景非同步 queue 是不同能力。本次只需要批次與 bounded parallel，不建立 Game-side queue／worker：

1. `SendToPlayers` 在呼叫 goroutine 內完成 Redis routing；
2. sender 使用暫時性 bounded workers 對不同 endpoint 並行呼叫 `Forward`；
3. 方法等待所有已排程 endpoint plan 結束後返回；
4. Gate handler 成功只代表訊息已進入本機 WebSocket delivery path，不代表 socket 已 write；
5. 實際 WebSocket write 繼續由每條 connection 的既有 writer goroutine 執行。

不新增常駐 queue，可以避免額外定義容量、滿載策略、shutdown drain、持久化、重試與訊息所有權。

### 2.3 Delivery 保證

本功能維持 best-effort、非 durable delivery：

- Redis route lookup 與 gRPC 都不重試；
- 不提供 exactly-once；
- gRPC request 已被 Gate 處理但 response 遺失時，呼叫者只能看到不確定結果；
- 本次不以該結果觸發 fallback，因此 framework 不會因 response error 主動重送；
- Gate 本機找不到 login session 是正常 ownership race，記錄為 ignored 後繼續其他項目。

## 3. 不在本次範圍

- `RequestPlayerSender` 與 Gate → Game 原 unary response；
- room broadcast 的 Redis Publish／PSubscribe 與 gRPC fallback；
- Gate → Gate kick command；
- Game-side durable queue、常駐 worker、retry、ack storage 或 deduplication；
- 根據 gRPC status、Gate handler 結果或 WebSocket write 結果啟動 fallback；
- 新增 gRPC service、listener 或專用 `SendToPlayers` RPC；
- 解析任一 client business protobuf payload；
- 動態 batch size 或 concurrency 設定；
- 修改目前 1 MiB logical payload contract。

## 4. 架構與模組邊界

```text
Game business module
    │ []serversend.PlayerMessage
    ▼
PlayerSender.SendToPlayers
    │
    ├─ Redis pipeline: presence by unique login_name
    ├─ Redis pipeline: endpoint by unique GateID
    │
    ├─ Redis operation success
    │      └─ group by exact endpoint
    │
    └─ Redis operation failure and ctx active
           └─ DNS list all Gate endpoints; every endpoint receives all input
                  messages
    │
    ▼
split each endpoint group into <= 1 MiB SendPlayersCommand chunks
    │ endpoint plans bounded-parallel; chunks within one endpoint sequential
    ▼
GRPCTransport.Forward
    │ GateDelivery.Forward(gatelink.GateRequest)
    ▼
RemoteCommandChannel dispatcher
    │ PlayerDeliveryCommandID
    ▼
Gate core player-delivery handler
    ├─ unmarshal SendPlayersCommand envelope
    ├─ keep each client_payload opaque
    ├─ lookup local session by login_name
    └─ enqueue existing WebSocket packet
```

### 4.1 `pkg/serversend`

此 package 擁有跨 product 的 delivery contract 與 routing：

- batch input model 與 `PlayerSender` interface；
- Redis batch resolver；
- exact route grouping、chunking 與 fallback decision；
- formal batch command protobuf 與 framework-reserved command ID；
- generic `GRPCTransport.Forward`；
- bounded endpoint execution。

它不依賴 `gateproduct.SessionRegistry`，也不解析 client payload。

### 4.2 `products/gameproduct`

Game product 只負責 DI composition：

- 提供 Redis batch resolvers；
- 提供共用 `GRPCTransport`；
- 將既有 DNS Gate directory 同時提供給 broadcast fan-out 與 player fallback；
- 建立 batch `PlayerSender`。

不新增 lifecycle resource。Redis、gRPC transport 與 DNS resolver 仍使用既有 owner。

### 4.3 `products/gateproduct`

Gate product 擁有 local session delivery：

- 在既有 dispatcher 的 `RemoteCommandChannel` 註冊 framework-reserved player-delivery command；
- 解碼 batch routing envelope；
- 依 login name 使用既有 `SessionRegistry`；
- 使用既有 WebSocket packet encoder、write queue 與 metrics。

`GateDeliveryService` 只轉接 generic `Forward` 至 dispatcher，不保留 dedicated player RPC branch。

## 5. Public API 與資料模型

### 5.1 Go API

保留既有單筆 domain type：

```go
type PlayerMessage struct {
    LoginName LoginName
    Message
}

type PlayerSender interface {
    SendToPlayers(context.Context, []PlayerMessage) (Receipt, error)
}
```

不增加 `PlayerMessages` wrapper type；slice 已足以表達批次，額外 wrapper 沒有 invariant 可維護。單一玩家使用 `[]PlayerMessage{message}`。

輸入 contract：

- slice 不可為空；
- 每筆 `login_name` 與 `command_id` 必填；
- 每筆 client payload 不得超過現有 logical limit；
- sender 在 routing／並行工作前 clone payload；呼叫者不得在方法執行期間無同步地修改輸入
  buffer，方法返回後可安全重用原 buffer；
- 可出現重複 login name；routing lookup 對 login name 去重，但傳送與 Gate enqueue 保持原輸入順序。

### 5.2 Formal command protobuf

在 `pkg/serversend/serversend.proto` 增加 message，不增加 RPC：

```protobuf
message SendPlayersCommand {
  repeated PlayerDelivery messages = 1;
}

message PlayerDelivery {
  string login_name = 1;
  uint32 client_command_id = 2;
  bytes client_payload = 3;
}
```

`GateDelivery` 最終只保留：

```protobuf
service GateDelivery {
  rpc Forward(gatelink.v1.GateRequest) returns (google.protobuf.Empty);
}
```

移除 dedicated `SendToPlayerRequest`、`DeliveryResponse` 與 `DeliveryStatus` proto；它們只服務被取代的單筆 RPC，繼續保留會形成第二套 contract。

傳送時：

```text
GateRequest.command_id = PlayerDeliveryCommandID
GateRequest.payload    = marshal(SendPlayersCommand)
```

`PlayerDeliveryCommandID` 是 `serversend` 擁有的固定常數，屬於 `RemoteCommandChannel` 的 framework-reserved command。實作時選定數值並以 contract test 鎖定，不由 application config 提供，避免不同 process 設定不一致。

### 5.3 Receipt 與 error

不新增逐玩家 response model。`Forward` 保持 `google.protobuf.Empty`，`Receipt` 語意為至少一個 endpoint chunk 已被 Gate `Forward` 成功接受。

- 全部可傳送 chunk 成功：non-zero `Receipt`, `nil` error；
- 部分 endpoint／chunk 成功：non-zero `Receipt`, joined error；
- 沒有 chunk 成功：zero `Receipt`, error；
- 正常查不到 presence／endpoint 的玩家以包含 login name 的 typed error 回報，其餘可路由玩家仍繼續；
- Redis infrastructure error 後 fallback 全部成功：non-zero `Receipt`, `nil` error，與既有 broadcast fallback semantics 一致；
- fallback 失敗：依是否至少一個 fallback chunk 成功回傳 receipt，並 join 原 Redis error 與 fallback errors。

Receipt 不代表某個 login name 一定仍在線，也不代表 WebSocket write 成功。

## 6. Redis Batch Routing

### 6.1 Presence pipeline

`RedisPresenceResolver` 增加 batch method，輸入先依 login name 去重，再在一個 pipeline 中對每個 key 執行 `HGETALL`。結果分類為：

- valid presence：加入 `login_name → GateID`；
- empty hash：`ErrPresenceNotFound`，不 fallback；
- malformed hash：route data invalid，使用 `ErrDestinationInvalid` 類別，不 fallback；
- Redis client／network／auth failure：`ErrRouteStoreUnavailable`；只要 pipeline 有此類錯誤且 context 仍有效，整次 exact route 尚未發送任何 gRPC，直接切換為全輸入 fallback；
- `context.Canceled`／`context.DeadlineExceeded`：直接返回，不 fallback。

### 6.2 Endpoint pipeline

對 valid presence 的 GateID 去重，使用第二個 pipeline `GET` endpoint key：

- valid endpoint：加入 route plan；
- `redis.Nil`：`ErrGateEndpointNotFound`，不 fallback；
- malformed address：`ErrDestinationInvalid`，不 fallback；
- Redis client operation failure：若 context 仍有效，放棄尚未發送的 exact route plan，將完整原輸入切換至 DNS all-Gate fallback；
- context error：直接返回，不 fallback。

兩個 routing phase 都在任何 gRPC 前完成，所以 Redis operation failure 改走整批 fallback 不會和 exact route 形成 framework 主動造成的 duplicate delivery。

### 6.3 必要的錯誤分類修正

目前部分 malformed presence／invalid endpoint 也會包裝為 `ErrRouteStoreUnavailable`。實作必須收窄：

- `ErrRouteStoreUnavailable` 只表示 Redis operation 無法完成；
- missing presence 保持 `ErrPresenceNotFound`；
- missing endpoint 保持 `ErrGateEndpointNotFound`；
- malformed presence 或 endpoint 使用 `ErrDestinationInvalid`（或既有等價 validation error），不得觸發 fallback。

不新增 retryable-error framework；sender 只以既有 typed errors 與 context state 做這一個必要判斷。

## 7. Grouping、Chunking 與 Concurrency

### 7.1 Exact route grouping

依 validated endpoint address 建立 endpoint plan。每個 plan 中的 messages 保持 caller input order。相同 GateID／address 只建立一個 plan，避免同 Gate 重複 RPC。

presence 不存在或 endpoint 不合法的項目不加入 plan，但不阻止其他玩家傳送。

### 7.2 Fallback grouping

fallback 使用既有 `grpc.clients.gate.fanout.target` 的 DNS directory 與 `max_endpoints` 保護：

- DNS endpoints 去重並排序；
- 每個 endpoint 都取得完整原輸入 messages；
- Gate handler只 enqueue 本機存在的 login name，其他項目 ignored；
- fallback directory 不可使用 Redis。

### 7.3 Encoded bytes 上限

每個 `SendPlayersCommand` marshal 後的 bytes 必須：

```text
len(encoded SendPlayersCommand) <= DefaultMaxPayloadBytes
```

目前為 1 MiB。這正是 `GRPCTransport.Forward` 與 Gate `dispatchRemoteCommand` 已驗證的 `Message.Payload` 上限；不新增另一個 batch config。

不能只加總 `client_payload` 長度，因為 protobuf 還包含 login name、command ID、field tag、length varint 與 repeated item envelope。切割器採 greedy、保序策略：

1. 以 protobuf wire-size 計算下一個 `PlayerDelivery` 加入後的 batch size；
2. 若超過 1 MiB，先結束目前非空 chunk，再由該 item 開始下一個 chunk；
3. marshal chunk 後以 `len(encoded)`／`proto.Size(command)` 做最終 invariant check；
4. 將 encoded bytes 放入 `gatelink.GateRequest.payload` 前，再驗證外層 request 的 command ID 與 logical payload；
5. 單一 item 加 envelope 已超過 1 MiB 時，於任何 network I/O 前回傳 `ErrPayloadTooLarge`。

外層 `GateRequest` 只有少量 protobuf overhead。目前 grpc-go server 未另外設定 message size，使用預設約 4 MiB receive limit；1 MiB payload 加外層 envelope 有充分空間。本次不依賴接近 4 MiB 的邊界，也不修改 gRPC server config。

已知限制：單筆 client payload 即使自身未超過 1 MiB，也可能因 batch envelope 而超過完整 command 的 1 MiB 上限。該項必須失敗；為此放大所有 generic `Forward` payload 上限不屬於本次必要修改。

### 7.4 發送並行模型

使用既有 `defaultFanoutConcurrency = 16`，不新增設定：

- endpoint plan 之間最多 16 個並行；
- 每個 endpoint plan 由一個 worker 依序送出自己的 chunks；
- 同一 endpoint 不並行送 chunks，以維持同一玩家的輸入順序並避免瞬間建立過多 HTTP/2 streams；
- context cancellation 停止排入新 endpoint plan；已進行的 RPC 使用相同 context 結束；
- 所有 worker 結束後才聚合 receipt 與 errors。

```text
Gate A: chunk 1 → chunk 2 → chunk 3
Gate B: chunk 1 → chunk 2
Gate C: chunk 1

Gate A/B/C 可並行；每一列內依序執行。
```

`GRPCTransport` 繼續重用每個 endpoint 的 cached `grpc.ClientConn`，不得 per-chunk dial。

## 8. Gate Handler

Gate product 在 composition 時註冊內建 handler：

```text
channel = serversend.RemoteCommandChannel
command = serversend.PlayerDeliveryCommandID
```

handler：

1. unmarshal `SendPlayersCommand`；
2. 驗證 batch 非空，並逐項驗證 login name、client command ID、單項 payload 與完整 batch limit；
3. 不 unmarshal `client_payload`；
4. 以既有 `encodeWebSocketPacket` 建立 `16-byte header + client_payload`；
5. 依輸入順序呼叫既有 session registry local send；
6. login session 不存在時記錄 `ignored` 並繼續；
7. enqueue 失敗時記錄 `error`，繼續其他項目，最後以 `errors.Join` 回傳；
8. 至少 enqueue 成功的項目沿用既有 `queued` metrics；實際 write terminal metrics 維持在 writer loop。

handler 不回傳逐玩家 status，也不因一名玩家不存在而使 `Forward` 失敗。若 queue full 等本機錯誤造成 RPC error，Game 將錯誤返回呼叫者，但不得 fallback。

## 9. 具體檔案修改

### 9.1 `pkg/serversend/serversend.proto` 與 generated files

- `GateDelivery` 移除 `SendToPlayer`，只保留 `Forward`；
- 移除 `SendToPlayerRequest`、`DeliveryResponse`、`DeliveryStatus`；
- 增加 `SendPlayersCommand`、`PlayerDelivery`；
- 以既有 protobuf generation workflow 更新 `*.pb.go`／`*_grpc.pb.go`；
- descriptor contract test 鎖定 service method 與新 message field numbers。

### 9.2 `pkg/serversend/message.go`

- `PlayerSender.SendToPlayer` 改為 `SendToPlayers(ctx, []PlayerMessage)`；
- 更新 `Receipt` 與 interface contract；
- 增加固定 `PlayerDeliveryCommandID`；
- 不新增 batch wrapper 或 per-player result type。

### 9.3 `pkg/serversend/presence.go`、`gate_registry.go`、`redisstore/store.go`

- 為 Redis resolver 增加 batch pipeline 能力；
- lookup key 去重並保持可測試的 narrow store boundary；
- 修正 malformed route 不再偽裝成 `ErrRouteStoreUnavailable`；
- 不修改 claim／renew／release 或 Gate endpoint registration lifecycle。

### 9.4 `pkg/serversend/sender.go`

- 以 batch implementation 取代 `RoutedPlayerSender` 單筆流程；
- 先完整 validation／clone，再 routing，最後 grouping／chunking／send；
- Redis operation failure 才進入 DNS fallback；
- endpoint 間 bounded parallel、endpoint 內 sequential；
- 移除 `FanoutSender.SendToPlayer`；`FanoutSender.Broadcast` 保持既有 broadcast 行為；
- 可共用小型 endpoint-plan executor，但不建立通用 job framework。

### 9.5 `pkg/serversend/grpc_transport.go`、`receiver.go`

- 移除 dedicated `GRPCTransport.SendToPlayer` 與 response mapping；
- `GRPCTransport.Forward` 保持唯一 Game/Gate generic outbound path；
- `GateDeliveryService` 移除 `LocalReceiver` dependency 與 dedicated handler；
- constructor 只需要 dispatcher；
- 不修改 listener、timeout、connection cache 或 lifecycle。

### 9.6 `products/gameproduct`

- 更新 `newGamePlayerSender` DI，注入 batch resolvers、transport 與 DNS fallback directory；
- DNS directory 應提供為可共用 dependency，避免每個 chunk 重做 DNS list；
- 維持 `grpc.clients.gate.timeout`、`fanout.target`、`fanout.max_endpoints`；
- 不新增 `server_send.player` config。

### 9.7 `products/gateproduct`

- 以內建 batch command handler 取代 `gateDeliveryReceiver`；
- 在既有 dispatcher 註冊固定 command；
- handler 使用既有 `SessionRegistry.sendToLoginNameAt`、packet encoder 與 metrics；
- 更新 `newGateDeliveryService` 只注入 dispatcher；
- 不修改 WebSocket queue、writer、session ownership 或 endpoint registration。

### 9.8 Examples 與文件

- example 中若有單筆 `PlayerSender` 呼叫，改為長度一 slice；
- Game 的 `grpc.clients.gate.fanout` 註解說明 player fallback 僅在 Redis routing operation
  failure 時發生；`server_send.broadcast` 註解只描述 broadcast fallback；
- 不為 batch bytes 或 concurrency 新增 YAML key。

## 10. 錯誤處理

### 10.1 不可 fallback

以下錯誤直接回報或與其他結果聚合：

- invalid／empty input；
- oversized item 或 chunk；
- `ErrPresenceNotFound`；
- `ErrGateEndpointNotFound`；
- malformed presence／endpoint；
- context cancellation／deadline；
- DNS fallback directory 自身錯誤；
- exact 或 fallback gRPC error；
- Gate dispatcher／handler／WebSocket enqueue error。

### 10.2 唯一 fallback trigger

只有在任何 exact gRPC 尚未開始前，Redis presence 或 endpoint pipeline 因 Redis client operation error 回傳 `ErrRouteStoreUnavailable`，且 `ctx.Err() == nil` 時，才執行一次 DNS all-Gate fallback。

不根據 error message string 判斷；使用 `errors.Is(err, ErrRouteStoreUnavailable)`，並由 resolver 保證該 error 類別不包含 missing／malformed data。

## 11. 測試策略

先完成可編譯的 unit／contract tests；實際整合與壓測留待使用者確認實作後執行。

### 11.1 API 與 proto contract

- `PlayerSender` 只有批次 method；
- `GateDelivery` descriptor 只有 `Forward`；
- new messages 的名稱、field number 與 field kind 固定；
- dedicated single-player RPC/messages 不存在；
- framework command ID 固定且 Gate composition 會註冊。

### 11.2 Routing tests

- presence lookup 對重複 login name 去重；
- endpoint lookup 對重複 GateID 去重；
- 多個玩家同 endpoint 形成一個未超限 RPC；
- 多 endpoint 各自收到正確且保序的 messages；
- missing presence／endpoint 不 fallback；
- malformed route 不 fallback；
- Redis operation failure 對完整原輸入只啟動一次 all-Gate fallback；
- canceled context 不 fallback；
- exact gRPC／handler error 不 fallback。

### 11.3 Chunking 與 concurrency tests

- protobuf envelope bytes 納入 1 MiB 計算；
- 剛好等於上限可傳，超過一 byte 會切 chunk；
- 單一 item 無法容納時，在 network I/O 前失敗；
- 同 endpoint chunks 依序呼叫；
- 不同 endpoints 可並行但 active calls 不超過 16；
- payload 在 caller 修改後不影響已建立的 plan。

### 11.4 Gate tests

- handler 不解析任意／非 protobuf 的 client payload bytes；
- login name 能映射正確 session；
- WebSocket frame 仍是既有 16-byte header 加原 payload；
- missing local session 被 ignored 且不阻止其他玩家；
- queue failure 不阻止其他項目，最後回傳 aggregated error；
- queued／ignored／error 與 write terminal metrics 沒有回歸。

### 11.5 Integration tests

- 兩個 Gate、不同 ownership，單次 batch 各只呼叫對應 endpoint；
- Redis operation failure 時每個 DNS Gate 收到 chunks，各自只送本機玩家；
- Redis 正常但 endpoint key 缺失時不呼叫 DNS fallback；
- gRPC failure 時不呼叫 fallback；
- `go test ./...` 可編譯並通過；並行測試以 `go test -race` 驗證共享結果收集。

## 12. 關鍵決策與取捨

### 12.1 共用 `Forward`，不新增 RPC

指定玩家批次只是另一個 remote command。使用 command ID + protobuf payload 可直接共用 Gate gRPC service、Redis ingress dispatcher model、listener、interceptor 與 lifecycle。專用 RPC 只會重複 transport contract。

### 12.2 Pipeline，而非每玩家 goroutine

pipeline 降低 Redis round trips，並避免玩家數直接轉成 goroutine 與 connection-pool 競爭。它不是跨 key atomic snapshot；ownership 在 routing 與 enqueue 間仍可能改變，Gate local lookup 負責安全忽略 stale route。

### 12.3 endpoint 間 parallel、endpoint 內 sequential

全同步會讓 latency 隨 endpoint 數線性增加；所有 chunks 無界並行則會製造 stream 與 queue 突波。混合模型只需要既有 concurrency constant，又能保持單 endpoint ordering。

### 12.4 fallback 不修復 delivery failure

fallback 只維持 Redis routing infrastructure 故障時的可達性。若 gRPC 結果也觸發 fan-out，在 ambiguous response 下可能重複推送，因此本次明確禁止。

### 12.5 1 MiB 是完整 command payload 上限

沿用現有 logical contract 可以避免同時修改 gRPC send/receive limits 與設定面。代價是極接近 1 MiB 的單筆 client payload 加 envelope 後無法送出；此限制明確且可測試。

## 13. 已知限制與未來擴充

- Redis pipeline 不是 ownership snapshot，玩家切換 Gate 時可能被舊 Gate ignored；
- fallback 無逐玩家 delivery confirmation，只表示 Gate command RPC 結果；
- batch 沒有 durable retry 或 deduplication；
- 大量單筆 payload 會產生多個 sequential chunks；
- 若未來實測證明 1 MiB batch 不足，可另案同時設計 application batch limit 與明確的 gRPC max send/receive config；
- 若未來需要 fire-and-forget，必須另案定義 bounded queue、滿載策略、shutdown 與 delivery semantics，不應藏在此 sender 中。

## 14. Self Review

| 檢查項目 | 結論 |
|---|---|
| 批次多玩家 | `SendToPlayers([]PlayerMessage)`，單筆亦走相同介面 |
| Redis 查詢效率 | unique keys + 兩階段 pipeline，沒有 per-player goroutine |
| 依 endpoint 合併 RPC | 是；每 endpoint／chunk 一次 `Forward` |
| 共用 Gate service／RPC | 是；只使用既有 `GateDelivery.Forward` |
| command contract | 固定 command ID + formal protobuf payload |
| Gate 是否解析 client payload | 否；只解析 routing envelope |
| fallback trigger | 僅 Redis operation failure 且 context 有效 |
| gRPC／handler error 是否 fallback | 否 |
| fallback 是否依賴 Redis | 否；使用既有 DNS directory |
| bytes 是否安全 | 完整 batch protobuf 最大 1 MiB，marshal 後再驗證 |
| concurrency 是否 bounded | endpoint 間最多 16，同 endpoint chunks sequential |
| 是否新增 background worker | 否 |
| 是否新增 RPC／listener／config | 否 |
| 是否影響 request-player／broadcast | 否 |
| 是否保留重複 single-player RPC | 否 |

Review 後確認：文件中的新增 protobuf batch envelope、Redis pipeline、route error 分類、chunker、bounded endpoint executor 與 Gate handler，都是完成已確認需求不可缺少的部分。未加入 gRPC response-based fallback、per-player result、背景 queue、retry、deduplication、動態 tuning config 或新的 transport abstraction；沒有超出需求。
