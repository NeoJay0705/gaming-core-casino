# 通用 Broadcast Chain 設計

## 1. 文件目的

本文件定義對目前 staged／unstaged 實作的下一輪必要修正，使 broadcast chain 符合以下已確認需求：

- Game 與 Gate 都可注入同一個 `serversend.BroadcastSender` 作為 producer；
- primary 使用 Redis Pub/Sub `Publish`／`PSubscribe`；
- Redis publish 失敗或沒有 subscriber 時，fallback 為逐一呼叫所有 Gate endpoint 的 gRPC；
- gRPC 與 Redis 傳輸都只攜帶 `command_id + protobuf payload`；
- Gate 的 gRPC ingress 與 Redis subscriber 使用同一個 `dispatcher.Dispatcher` instance 與同一個 channel；
- 不把 room、kick、server-send 等業務語意寫入通用 transport API；
- 本次只實作 Game → Gate room broadcast 範例；Gate → Gate kick 僅保留可擴充能力，不實作 command、protobuf 或 handler。

本文件是目前 broadcast chain 的唯一有效設計；Player-targeted server send 與 request-player unary reply 不在本次重構範圍。

## 2. 需求理解與合理假設

### 2.1 已確認需求

1. Broadcast 是一種傳輸策略，不是 transport RPC 的業務模型。
2. Broadcast producer 送出的唯一通用資料為：
   - `command_id`：決定 Gate product handler；
   - `payload`：該 command 的 protobuf bytes，transport 不解碼。
3. Redis Pub/Sub 與 gRPC fallback 必須具有相同的 command contract，避免兩條路徑各自維護 room-specific request。
4. Gate 收到 command 後，由 dispatcher registration 判斷是否合法並選擇 handler；不另維護 allowlist。
5. Room routing 是範例業務 handler 的責任。Framework transport 不知道 `room_id`。
6. Room command handler 可解碼一層 routing protobuf 取得 `room_id` 與 client-facing command ID，但不解析要送給 WebSocket client 的內層 protobuf payload。
7. WebSocket outbound message 使用現有 16-byte packet header；broadcast 沒有原始 client request correlation，因此 `sequence`、`session`、`version` 固定為零。

### 2.2 必要假設

- `dispatcher.Dispatcher` 已支援 concurrent dispatch 與 registration，無需新增 dispatcher abstraction。
- 所有 Gate instance 都訂閱同一個 application-level broadcast channel；room filtering 在各 Gate 的 local handler 執行。
- Redis Pub/Sub 維持 best-effort。publish 成功只表示 Redis 接受且當時至少有一個 subscription，不表示所有 Gate handler 執行成功。
- gRPC fallback 使用既有 `DNSGateDirectory` 列舉 Gate endpoints；單次 round-robin RPC 不視為 broadcast。
- 無外部 repo 相容性要求，因此可直接修改尚未發布的 protobuf contract。

## 3. 本次範圍

### 3.1 必須完成

- 將 room-specific broadcast transport contract 改為 generic command contract。
- Redis 與 gRPC 共用相同 protobuf request shape。
- 兩個 Gate ingress 共用同一 dispatcher channel。
- 移除只為舊 room delivery adapter 存在的 result slot、delivery kind 與 command-ID-only registration helper。
- Gate product 保留 `BroadcastSender` DI wiring。
- 在 metrics example 定義並註冊正式可執行的 room broadcast command。
- 驗證 room handler 產生正確的 16-byte WebSocket packet。
- 保留現有 Redis primary → gRPC fanout fallback policy 與 lifecycle。

### 3.2 明確不做

- Gate → Gate kick command、skip-self 或 `source_gate_id`。
- transport 層解析 room command 或 client protobuf payload。
- Redis Streams、ack、replay、retry queue、deduplication 或 exactly-once。
- per-room Redis channel／subscription。
- broadcast recipient count 的跨 Gate 聚合。
- 新增另一個 gRPC listener、另一套 generic gRPC framework 或重構 Gate → Game `gatelink`。
- 修改 `RequestPlayerSender` 的原 unary reply 路徑。
- 修改 `PlayerSender` 的 Redis presence lookup、精準 endpoint 路由或 all-Gate fallback。
- 為本需求新增 metrics family、trace system、authorization framework 或 command registry abstraction。

## 4. 架構與模組邊界

```text
Game product handler / Gate product handler
              │
              │ Broadcast(ctx, Message{CommandID, Payload})
              ▼
      serversend.BroadcastSender
              │
       primary=redis ?
       ┌──────┴────────┐
       │               │
       ▼               ▼
Redis Publish      gRPC FanoutSender
       │               │ one Forward RPC / unique Gate endpoint
       │ publish error │
       │ or count=0 ───┘
       ▼                               ▼
Gate Redis PSubscribe          GateDelivery.Forward
       │                               │
       └──────────────┬────────────────┘
                      ▼
          same dispatcher instance
          RemoteCommandChannel
                      │ command_id
                      ▼
            product-registered handler
                      │
        example: decode BroadcastRoomCommand
                      │ room_id + opaque client payload
                      ▼
       SessionRegistry.BroadcastRoom + 16-byte packet
```

### 4.1 `pkg/serversend`

負責：

- 通用 broadcast sender interface；
- Redis command encode／publish 與 subscribe／decode；
- DNS endpoint enumeration 與 bounded gRPC fanout；
- gRPC `Forward` adapter；
- 共用 payload size validation、timeout、fallback 與 lifecycle。

不負責：

- room、kick 等 command schema；
- command authorization；
- local session 查詢或 WebSocket framing；
- 判斷任一 command 是否為「合法業務 command」。

### 4.2 `products/gateproduct`

負責：

- 在既有 product-level gRPC server 註冊 `GateDelivery` service；
- 將 gRPC `Forward` 與 Redis subscriber 注入同一個 dispatcher；
- 提供 `SessionRegistry` 與 WebSocket packet encoder 給 product handler；
- 在 config 啟用時建立 Redis subscriber lifecycle；
- 提供 Gate 作為 `BroadcastSender` producer 的 DI wiring。

不負責：

- 預先列舉所有可廣播 command；
- 用一個通用 handler 將所有 command 強制解讀為 room broadcast。

### 4.3 `products/gameproduct`

負責：

- 提供與 Gate 相同的 `BroadcastSender` DI contract；
- 按 config 組合 Redis primary 與 gRPC fallback。

Game handler 自行 marshal product command protobuf，再呼叫 `BroadcastSender`。

### 4.4 `examples/metrics`

負責示範一個真正可啟動、可測試的 room broadcast command。範例 protobuf 與 handler 留在 example，避免把測試業務 API 放進 framework package。

## 5. 核心資料模型與介面

### 5.1 Transport-neutral message

沿用 `serversend.Message`：

```go
type Message struct {
    CommandID uint32
    Payload   []byte
}

type BroadcastSender interface {
    Broadcast(context.Context, Message) (Receipt, error)
}
```

必要變更：

- `BroadcastSender.Broadcast` 不再接收 `BroadcastMessage`；
- 移除 framework 層的 `BroadcastMessage` 與其 `RoomID`；
- `Message.Validate` 仍要求非零 `CommandID`；
- Redis 與 gRPC sender 在跨 async／RPC boundary 前複製 payload，避免 caller mutation race；
- 沿用 `DefaultMaxPayloadBytes` 驗證完整 command payload。

`PlayerMessage` 與 `RequestPlayerMessage` 保持不變。

### 5.2 通用 transport protobuf

修改 `pkg/serversend/serversend.proto`：

```proto
import "google/protobuf/empty.proto";
import "pkg/gatelink/gatelink.proto";

service GateDelivery {
  rpc SendToPlayer(SendToPlayerRequest) returns (DeliveryResponse);
  rpc Forward(gatelink.v1.GateRequest) returns (google.protobuf.Empty);
}
```

設計決策：

- 保留現有 `GateDelivery` service，避免僅為命名再增加 service、registration 與 generated API；
- 將 `BroadcastRoom` 改成 `Forward`，其 request 不包含 room 或 broadcast 語意；
- request 直接重用 Gate → Game 的 `gatelink.v1.GateRequest{command_id,payload}`，不重複宣告等價 message；
- 不直接重用 `gatelink.GateRequestService` server implementation，因現有 `gatelink` ingress 必須有 source connection metadata，並管理 request-player unary reply slot；broadcast command 沒有這兩種語意；
- `Forward` 回傳 `google.protobuf.Empty`，因 Redis Pub/Sub 無法提供對等的 delivered count，且 generic dispatcher handler 沒有跨 Gate recipient result contract；
- `SendToPlayer` 與 `DeliveryResponse` 保留，因它們屬於既有精準 player delivery contract。

從 proto 移除：

- `BroadcastRoomRequest`；
- `RedisBroadcastEnvelope`；
- `GateDelivery.BroadcastRoom`。

generated `.pb.go` 與 `_grpc.pb.go` 必須由既有 proto generation command 重建，不手動修改。

### 5.3 Redis wire payload

Redis `Publish` 的 value 直接使用 protobuf marshal 後的 `gatelink.GateRequest`。Redis 與 gRPC 因此共用同一個 wire message，沒有第二層 transport-specific envelope。

Redis channel 是固定的 `<infra-prefix>:server-send:broadcast`，不含 `room_id`。`Keyspace` 提供 `broadcastChannel()`；`PSubscribe` 使用同一個完整值作 pattern，移除 per-room channel builder。保留 `PSubscribe` 是為了符合既定 adapter dependency，不藉此加入未使用的 wildcard routing。

本次不在 Redis payload 加入 `trace_id`。gRPC metadata 可繼續傳遞既有 trace context，但跨 Redis 的 tracing 若未來需要，應獨立設計 versioned envelope，不應偷偷擴張本次只有 `command_id + payload` 的 contract。

### 5.4 Example room command

在 `examples/metrics/internal/protocol` 定義：

```proto
message BroadcastRoomCommand {
  string room_id = 1;
  uint32 client_command_id = 2;
  bytes client_payload = 3;
}
```

欄位責任：

- `room_id`：Gate handler 做 local room lookup；
- `client_command_id`：要寫入 WebSocket 16-byte header 的 command ID；
- `client_payload`：已 marshal 的 client protobuf bytes，Gate handler只複製、不 unmarshal。

此 command 自身有一個固定且 bounded 的 transport `command_id`，定義於 example `command.go`，例如 `BroadcastRoomCommandID`。它是 dispatcher route，不是 client-facing response command ID。

這個 outer protobuf 是必要的，因 transport 必須保持 opaque，但最終 Gate 仍需知道 room target 與 client packet command。把 `room_id` 放回 transport request，或要求 framework 從任意 protobuf payload 局部解析 field，都會破壞 schema ownership，因此不採用。

## 6. Dispatcher 與資料流

### 6.1 共用 channel

將舊 `GateDeliveryChannel` 改為中性的：

```go
const RemoteCommandChannel dispatcher.Channel = "gate-remote-command"
```

同一個 Gate process 內：

- `GateDeliveryService.Forward` 使用 DI 注入的 `*dispatcher.Dispatcher`；
- `RedisBroadcastSubscriber` 使用同一個 DI instance；
- 兩者呼叫同一個窄型 helper：

```go
func dispatchRemoteCommand(
    ctx context.Context,
    d *dispatcher.Dispatcher,
    message Message,
) (handled bool, err error)
```

helper 只做：

1. validate `command_id` 與 payload size；
2. defensive copy payload；
3. 呼叫 `d.Dispatch(ctx, RemoteCommandChannel, commandID, payload)`。

它不建立 context result slot、不推測 delivery kind，也不要求 handler 回報 recipient count。

### 6.2 Registration

移除 `gateproduct.RegisterDeliveryCommands(commandIDs ...uint32)`。它只接受 ID，卻把所有 command 綁到同一個 room/player delivery handler，無法表達不同 protobuf 與不同業務行為。

產品或 example module 直接使用既有 dispatcher API：

```go
dispatcher.Register(
    serversend.RemoteCommandChannel,
    dispatcher.CommandID(protocol.BroadcastRoomCommandID),
    broadcastRoomHandler,
)
```

這一個 registration 同時是合法 command 判斷與 handler mapping，不增加第二份 allowlist。

### 6.3 Game → Gate room broadcast

1. Game example 建立 client payload protobuf bytes。
2. Game marshal `BroadcastRoomCommand{room_id, client_command_id, client_payload}`。
3. Game 呼叫 `BroadcastSender.Broadcast`，外層 `Message.CommandID` 為 `BroadcastRoomCommandID`。
4. Redis primary publish `gatelink.GateRequest`。
5. 每個 Gate subscriber decode `gatelink.GateRequest`，把 payload 原樣交給 dispatcher。
6. `broadcastRoomHandler` decode `BroadcastRoomCommand`，但不 decode `client_payload`。
7. handler 建立：

```go
gateproduct.WebSocketPacket{
    CommandID: command.ClientCommandId,
    Payload:   append([]byte(nil), command.ClientPayload...),
}
```

8. `EncodeWebSocketPacket` 產生：
   - bytes 0..3：`client_command_id`；
   - bytes 4..7：包含 header 的總 packet size；
   - bytes 8..11：sequence = 0；
   - bytes 12..13：session = 0；
   - bytes 14..15：version = 0；
   - bytes 16..：opaque `client_payload`。
9. handler 呼叫 `SessionRegistry.BroadcastRoom(roomID, packet)`。

### 6.4 Redis fallback

`FallbackBroadcastSender` 僅在下列情況執行一次 gRPC fanout：

- `PUBLISH` 回傳 error；
- `PUBLISH` subscriber count 為零。

gRPC fanout：

- 從既有 `DNSGateDirectory` 取得 endpoints；
- 依 address 去重；
- 維持既有 `max_endpoints` 與 bounded concurrency；
- 對每個 endpoint 呼叫一次 `GateDelivery.Forward(gatelink.GateRequest)`；
- 任一 endpoint error 仍回報 partial error，不做 background retry。

Redis publish 結果不明時 fallback 可能造成重複 delivery；本次接受此 best-effort 取捨，不加入 dedupe ID。

## 7. gRPC 與 local delivery 責任調整

### 7.1 `SendToPlayer`

`GateDeliveryService` 同時依賴：

- `LocalReceiver`：只供現有 `SendToPlayer` 精準 local delivery；
- `*dispatcher.Dispatcher`：只供通用 `Forward`。

`SendToPlayer` 直接 validate request 後呼叫 `LocalReceiver.SendToPlayer`，沿用 `DELIVERED`／`IGNORED` response。不再為它建立 `DeliveryRequest` context slot。這是移除舊共用 delivery machinery 後維持既有行為的最小作法。

### 7.2 `Forward`

`Forward` 不呼叫 `LocalReceiver`，只 dispatch：

- malformed request、zero command ID、payload too large → `InvalidArgument`；
- command 未註冊 → `Unimplemented`；
- context canceled／deadline → 對應 gRPC code；
- handler error → `Internal`，除非原 error 已是明確 gRPC status；
- handler success → empty response。

Redis subscriber 對相同問題無 response channel：記錄 bounded error log、丟棄該筆訊息並繼續 consume。既有 panic containment 與 reconnect lifecycle 保留。

### 7.3 應刪除的舊抽象

下列型別只服務 room-specific generic handler workaround，修正後沒有必要：

- `DeliveryKind`／`DeliveryPlayer`／`DeliveryRoom`；
- `DeliveryRequest`；
- `DeliveryResult`；
- `deliveryState` 與 context key；
- `NewDeliveryHandler`；
- `ErrDeliveryContextUnavailable`／`ErrDeliveryResultInvalid`；
- `gateproduct.RegisterDeliveryCommands`。

`ErrDeliveryCommandNotRegistered` 可改名為中性的 `ErrCommandNotRegistered`，供 gRPC status mapping 與 Redis log classification 使用。

## 8. Config 與 lifecycle

### 8.1 保留的 config

Game 與 Gate 都沿用：

```yaml
server_send:
  broadcast:
    primary: redis # default: redis；可選 redis、grpc

grpc:
  clients:
    gate:
      timeout: 3s # default 由既有 TransportConfig 決定
      fanout:
        target: "dns:///gate-headless:9091" # BroadcastSender 啟用時必要
        max_endpoints: 256                  # 0 使用 framework default
```

Redis connection 與 key prefix 沿用既有 infra config，不新增 room-specific config。舊 `room_broadcast` 必須繼續被拒絕。

`server_send.broadcast` 的 root binding 應採 strict config struct，使未知 sibling key 不會因只 bind nested path 而被忽略。Game 與 Gate 的 example config 必須列出所有支援欄位、必要性與 default 註解。

### 8.2 Lifecycle

- `primary=redis`：Game/Gate sender 組合 Redis publisher + gRPC fallback；Gate 額外啟動 Redis subscriber。
- `primary=grpc`：sender 直接使用 gRPC fanout；Gate 不啟動 Redis subscriber。
- Gate product-level gRPC server 始終只使用既有 listener；`GateDelivery` service 不擁有 listener。
- Gate endpoint registration 與 session ownership lifecycle 保持不變。
- 啟動 Redis subscriber 失敗時 Gate startup 失敗，避免 ready 但收不到 primary broadcast。
- shutdown 先停止 subscriber 接收，再由既有 product lifecycle 停止 gRPC server與 infra。

## 9. 具體檔案修改

### 9.1 `pkg/serversend`

- `serversend.proto`
  - `BroadcastRoom` → `Forward`；
  - `Forward` request 直接重用 `gatelink.GateRequest{command_id,payload}`；
  - 移除 `RedisBroadcastEnvelope`；
  - 保留 `SendToPlayer` 與 `DeliveryResponse`。
- generated protobuf files
  - 用既有 generation workflow 重建。
- `message.go`
  - `BroadcastSender` 改收 `Message`；
  - 移除 `BroadcastMessage`／room validation。
- `delivery.go`
  - 改為窄型 `RemoteCommandChannel` 與 `dispatchRemoteCommand`；
  - 移除 delivery kind／result-slot machinery。
- `receiver.go`
  - `LocalReceiver` 只保留 `SendToPlayer`；
  - `GateDeliveryService` 注入 receiver + dispatcher；
  - `SendToPlayer` 直接 local delivery；
  - 新增 generic `Forward` dispatch。
- `grpc_transport.go`
  - `BroadcastRoom(endpoint, BroadcastMessage)` 改為 `Forward(endpoint, Message)`。
- `sender.go`
  - `FanoutSender.Broadcast` 與 `FallbackBroadcastSender.Broadcast` 改收 `Message`；
  - gRPC fanout 呼叫 `Forward`；
  - 保留既有 endpoint bound、concurrency 與 error aggregation。
- `redis_broadcast.go`
  - publish／subscribe `gatelink.GateRequest`；
  - 改用固定 broadcast channel；
  - subscriber 呼叫 `dispatchRemoteCommand`。
- `keyspace` 所在檔案
  - per-room channel helper 改為固定 broadcast channel／pattern。

### 9.2 `products/gateproduct`

- `server_send.go`
  - `gateDeliveryReceiver` 移除 `BroadcastRoom`；
  - 保留 player delivery 與 `encodeServerSendPacket`。
- `delivery_registration.go`
  - 刪除整個 framework helper；業務 module 改註冊真正 handler。
- `grpc.go`
  - 建構 `GateDeliveryService(receiver, dispatcher)`；
  - registration 仍掛在同一個 product gRPC server。
- `server_send_runtime.go`
  - Redis subscriber 繼續取得 product dispatcher；
  - Gate 的 `BroadcastSender` provider 保留。
- config 與 app composition
  - strict bind `server_send` root；
  - 只調整受 interface／constructor 變更影響的 wiring。

### 9.3 `products/gameproduct`

- `BroadcastSender` provider 保留；
- 只調整 `Broadcast(ctx, Message)` 型別變更與 strict config binding；
- 不修改 Gate → Game request handler、RequestPlayerSender 或 PlayerSender。

### 9.4 `examples/metrics`

- protocol
  - 新增 `BroadcastRoomCommand` 與固定 dispatcher command ID；
  - 若需可觀察 client payload，可重用既有 `EchoResponse` 作 `client_payload`，不新增另一個等價 message。
- Gate workflow
  - 在 `RemoteCommandChannel` 註冊 room handler；
  - 只解碼 outer room command；
  - 使用現有 packet encoder 與 SessionRegistry broadcast。
- Game workflow／validation trigger
  - 注入 `BroadcastSender` 並產生一筆可重現 room broadcast；
  - 不新增與 broadcast 無關的 HTTP API 或 background scheduler。
- config
  - 顯示 Redis primary、gRPC fallback target、timeout、max endpoints 的必要性與 default。

## 10. 錯誤處理

| 情況 | Producer 結果 | Gate ingress 行為 |
|---|---|---|
| command ID 為 0 | 立即回傳 validation error | 不送出 |
| payload 超過上限 | 立即回傳 `ErrPayloadTooLarge` | 不送出／拒絕 |
| Redis publish error | 執行一次 gRPC fanout | 無 Redis delivery 保證 |
| Redis subscriber count = 0 | 執行一次 gRPC fanout | 無 Redis handler result |
| Redis publish count > 0 | 回傳 accepted receipt | 不宣稱所有 Gate 已處理 |
| gRPC command 未註冊 | endpoint 回 `Unimplemented` | fanout 聚合為 partial/full error |
| Redis command 未註冊 | producer 不會同步得知 | Gate log 後丟棄並繼續 consume |
| handler error | gRPC 回明確 status | Redis log 後繼續 consume |
| 部分 gRPC endpoint 失敗 | receipt + joined partial error | 不自動 retry |
| context canceled/deadline | 停止尚未派送工作 | 回傳 context 對應錯誤 |

## 11. 測試策略

### 11.1 Contract tests

1. Proto descriptor 只保留 `SendToPlayer` 與 `Forward`，且 `Forward` input 是既有 `gatelink.GateRequest`；其 field 1 為 `command_id`、field 2 為 `payload`。
2. `Forward` 與 Redis subscriber 對相同 command 命中同一 dispatcher instance、channel、handler。
3. 未註冊 command：gRPC 為 `Unimplemented`；Redis 不終止 consume loop。
4. Handler 收到的 payload 與 producer 傳入 bytes 完全一致，且 caller 後續 mutation 不影響已接受訊息。
5. Redis publisher 使用固定 channel，不從 payload 或 room ID 建立 channel。
6. Redis publish error／zero subscribers 各只觸發一次 gRPC fallback；成功 publish 不觸發 fallback。
7. gRPC fanout 對 unique endpoints 各呼叫一次，維持 max endpoint 與 concurrency bound。
8. `SendToPlayer` 的 `DELIVERED`／`IGNORED` 行為未回歸。
9. Gate 與 Game app 都能 resolve `serversend.BroadcastSender`。
10. Strict config 拒絕 `room_broadcast`、未知 `server_send` sibling 與未知 broadcast field。

### 11.2 Example tests

1. `BroadcastRoomCommand` 能取得 room ID，但 `client_payload` 不被 handler 解碼或改寫。
2. 只有目標 room 的本機 sessions 收到一筆 packet。
3. packet 長度至少 16 bytes，header command／size 正確，sequence／session／version 為零，payload bytes 原樣一致。
4. 兩個 Gate instance 都收到 Redis broadcast；Redis 不可用時兩個 Gate 都可經 gRPC fallback 收到。
5. Redis reconnect 測試發送的每個 command ID 都有明確 registration，避免用未註冊 command 誤判 reconnect failure。

### 11.3 驗證順序

依使用者既定流程，實作階段先完成 source 與 tests 並只做 compile validation；待使用者 review 後才執行實際 unit、integration 與 live Redis/gRPC tests。

## 12. 關鍵決策與取捨

### 12.1 保留 `GateDelivery` service，不新增第二個 service

新增 `CommandService` 雖然命名更純粹，但會增加 generated API、service registration 與 lifecycle contract；現有 `GateDelivery` 已掛在通用 Gate gRPC server。將 room-specific method 改為 `Forward` 已能移除 API 的 broadcast／room 耦合，因此新增 service 不具必要性。

### 12.2 不直接重用 `gatelink.GateRequestService`

其 request metadata 與 reply slot 是 WebSocket request forwarding 的必要語意。硬重用會讓 broadcast producer偽造 connection metadata，或迫使 server 增加模式分支。維持相同的 generic method/request shape，但使用獨立 ingress adapter，是較小且清楚的修改。

### 12.3 Room envelope 放在 example

Framework 只看到 opaque bytes，product handler才理解 room。這避免 framework 納入尚未穩定的業務 schema，也讓未來 kick command 可使用另一個 protobuf 而不修改 transport。

### 12.4 不回傳 delivered count

Redis Pub/Sub 無法同步取得各 Gate handler結果。若 gRPC path 回傳 recipient count，兩條 transport 的 success semantics 將不一致，且 producer 可能錯把 Redis subscriber count 當成 delivery count。因此 generic broadcast 只承諾 accepted/best-effort，實際 WebSocket delivery 由既有 Gate metrics 觀察。

### 12.5 固定 broadcast channel

per-room channels 會讓 transport 需要知道 room，並增加 subscription cardinality。所有 Gate 接收後由 local handler篩選，符合目前規模與 command-oriented API；未來若流量證明需要 partitioning，再以獨立設計增加 shard key。

## 13. 已知限制與可擴充方向

- Redis Pub/Sub 中斷期間會遺失訊息，且 fallback 結果不明時可能重複；這是已接受的 best-effort contract。
- 所有 Gate 都會接收每筆 broadcast command；大規模部署若出現 Redis／CPU fanout 壓力，可再設計固定低基數 shard，但目前無數據支持。
- 未來 Gate → Gate kick 只需新增自己的 protobuf 與 dispatcher registration，並沿用 `BroadcastSender`；是否略過來源 Gate、如何驗證 ownership 必須由該需求另行定義。
- 若 production 需要跨 Redis tracing，應新增具版本的通用 envelope；本次不預留未使用欄位。
- Broadcast authorization 由 producer 與 Gate product handler 負責；framework 不提供 policy engine。

## 14. Self-review：必要性與範圍檢查

### 14.1 符合需求

- [x] Redis Pub/Sub primary 與 gRPC all-Gate fallback 都保留。
- [x] 兩種 transport 都使用 `command_id + opaque protobuf payload`。
- [x] gRPC method 不再具有 room／broadcast 語意。
- [x] gRPC ingress 與 Redis subscriber 共用 dispatcher instance與 channel。
- [x] 合法 command 由實際 handler registration判斷，沒有第二份 allowlist。
- [x] Gate 作為 producer 的 `BroadcastSender` DI wiring 保留。
- [x] Room ID 只在 example-owned outer protobuf 中解碼，client payload不解析。
- [x] WebSocket outbound 明確使用既有 16-byte header。

### 14.2 沒有少做

- 若只將 RPC rename 而保留 `room_id`、per-room Redis channel 或共用 delivery handler，仍不符合通用 command需求；本設計已納入必要移除。
- 若只新增 dispatcher registration 而未提供 example handler，鏈路無法實際驗證；本設計已納入最小正式 example command。
- 若 Gate 沒有 producer DI，未來 Gate command無法重用相同鏈路；本設計保留既有 wiring，但沒有提前實作 kick。

### 14.3 沒有超出需求

- 未新增 listener、generic RPC framework、message bus abstraction、retry queue、ack、dedupe、sharding、authorization或 tracing。
- 未實作 Gate-to-Gate kick、skip-self與任何未定義 command。
- 未修改 request-player、player presence與精準 routing semantics。
- 未新增 framework 內建業務 protobuf；room schema僅存在可執行 example。
- 未為了觀察而新增 metrics；沿用現有 Gate WebSocket delivery與 transport metrics。

### 14.4 最終判定

本設計符合目前明確需求。列出的修改都是為了移除現有 room-specific transport耦合、建立可實際運作的 generic broadcast chain，或提供最低限度的 regression證明；其餘可能的可靠性、擴充性與 Gate-to-Gate能力均刻意留在本次範圍之外。
