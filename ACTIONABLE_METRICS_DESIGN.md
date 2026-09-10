# Actionable Product Metrics 與可執行範本設計

## 1. 結論

本次在既有 `pkg/observability` App-local Prometheus registry 上，加入能直接回答 Rate、Error、
Duration 與 Saturation 的最小 metrics，並提供 `gameproduct`、`gateproduct`、`apiproduct`、
`gmsproduct` 四個可實際啟動的範本程式。

壓測主流程使用既有正式 Login proto，加上範例專用且穩定可重現的 player-facing proto：進房、Echo
command。Echo 代表目前尚未定義的 Game business command，會走完整的 WebSocket read、Gate-to-Game
gRPC、Game handler、server-send、Gate receive 與 WebSocket write 路徑；它是可啟動範例的完整 transport
workflow，不是 framework product 對外承諾的業務 API。

Command label 不維護第二份 allowlist。`dispatcher` 的可信任註冊資料是合法 command 的唯一來源：
在能判定合法性的 process 內，已註冊 ID 才能成為 label value，未註冊輸入聚合成 `unknown`；Gate
尚無法判斷 Game command 是否註冊時則使用 `forward`。因此不會讓 client 任意提供的 `uint32` 擴張
Prometheus cardinality。

本文件是 `OBSERVABILITY_HTTP_DESIGN.md` 完成後的下一階段設計。前一文件刻意延後的 built-in
metrics 與 proto 修改，只有本文件明確列出的部分在本次解除限制；Health、Readiness、Metrics
共用 listener、App-local registry 與 lifecycle 契約不變。兩份文件不合併，也不回頭改寫已完成
階段的設計紀錄。

---

## 2. 需求理解與合理假設

### 2.1 本次要完成

- 四個 product 都能實際啟動，並透過既有 `/metrics` 暴露本 App 的 metrics。
- Gate/Game 可啟動範本支援以下狀態流程：
  1. WebSocket connection 建立。
  2. Login command 在 Gate 處理。
  3. Enter-room command 在 Gate 處理。
  4. Echo command 由 Gate forward 到 Game gRPC。
  5. Game handler 透過 server-send 回到原 Gate connection。
  6. Gate writer 實際完成 WebSocket binary write。
- 未登入執行 enter-room，或未進房執行 forward，Gate 都記錄固定原因並關閉該 connection。
- 能從 metrics 判斷各階段的 latency、RPS、error 與現有資源 saturation。
- 暴露實際被 product 使用的 Redis 與 Database connection pool 狀態。
- 範本與 tests 不依賴尚未定義的資料表或正式 authentication backend。

### 2.2 觀測時間點的明確定義

需求中的時間點定義如下：

| 原始描述 | 本設計的實際邊界 |
|---|---|
| read 後 | Gate 完成一個 WebSocket binary packet 解析後，開始 command timer |
| gRPC 前 | Gate 呼叫 `gatelink.Client.Forward` 前，開始 Gate-to-Game timer |
| gRPC handler 前 | Game 進入 product request handler 後，開始 Game command timer |
| server-send 前 | Game handler 呼叫 direct request-player sender 前，開始 server-send timer |
| Gate 收到後 | Gate 的 `LocalReceiver` 收到並完成基本 request validation 後，保存本機 `receivedAt` |
| write 成功 | writer goroutine 的 `WriteMessage` 返回 nil 時結束 write 與 server-send delivery timer |

每個 duration 都由同一 process 的 `time.Now` 差值產生，不輸出每筆 timestamp，也不跨主機相減。
Prometheus Histograms 用於比較各階段的分布；真正 client 端 round-trip latency 由壓測 client 測量，
不假裝能由多個 Histogram quantile 相減重建。

### 2.3 Actionable 標準

每個新增 metric 必須至少支持一個立即判斷：

- error 是否發生、發生在哪個固定類別；
- latency 上升位於 Gate dispatch、gRPC、Game handler、server-send 或 WebSocket write；
- 壓力是否卡在 active/in-flight work、write queue、Redis pool、Database pool、Go runtime 或 process；
- RPS 是否已上升到與上述資源飽和同時發生。

目前沒有 SLO、CPU quota 或部署容量資料，因此本次不提交 alert thresholds、recording rules 或
dashboard。先提供語意穩定的原始 metrics；閾值必須由第一次基準壓測與正式 SLO 決定。

### 2.4 不在本次範圍

- 正式帳號驗證、authorization、資料表與 persistence schema。
- Redis command、SQL statement 或 RocketMQ operation 的個別 latency/error metrics。
- 任意 command ID、user、room、connection、trace 或 error text labels。
- Distributed tracing、跨 process timestamp correlation 或 request-level event export。
- Prometheus alert rules、Grafana dashboard、remote write 與通用 load-testing framework。
- 修改 `/health`、`/ready`、`/metrics` endpoint 或 framework lifecycle。
- 為 API/GMS 虛構沒有實際 handler 的 request traffic。
- 為觀測而新增 worker pool、queue、retry、rate limit 或其他 runtime 行為。

---

## 3. 現況與必要缺口

1. `pkg/observability` 已提供 App-local `prometheus.Registerer`，但 registry 刻意不含任何 collector。
2. `dispatcher.Dispatch` 已回傳 `handled`，`RegisteredCommandIDs` 也只列出可信任程式註冊的 ID；
   不需要 metrics 自行列舉 proto command。
3. Gate 對本地 dispatcher 未處理的 command 會 forward 到 Game；Gate 當下無法知道 Game 是否註冊，
   因此 Gate 不能安全地用該 raw ID 當 label。
4. Game dispatcher 是判斷 forwarded command 是否由目前 process 正式註冊的權威位置。
5. `WebSocketSession.SendBinary` 現在只表示 enqueue 成功；server-send gRPC 的 `DELIVERED` 也同樣只
   表示 Gate 接受 enqueue，不能代表 socket write 成功。
6. `webSocketConnection.writeCh` 只保存 `[]byte`，writer 不知道來源與 Gate receive 時間，因此目前
   無法計算「Gate 收到 server-send message 到 write 完成」。
7. Database wrapper 持有 `*sql.DB`，Redis wrapper 持有 `redis.UniversalClient`；兩者已有官方 pool
   stats，但 wrapper 尚未提供 Prometheus collector。
8. Infrastructure resource 是 lazy。加入 pool metrics 不可讓原本未使用的 Redis/Database 因 scrape
   而被迫建立連線。
9. Login proto 已存在；enter-room 與 Echo 只需要供本次 runnable example 使用的 proto/command 尚未
   存在。四個 product 也沒有 runnable `main` 範本。

---

## 4. 架構與模組邊界

```text
player
  | WebSocket packet
  v
Gate WebSocketServer
  |-- Gate dispatcher ----> login / enter-room handler
  |       registered ID          |
  |                              +--> SessionRegistry state
  |
  |-- unhandled + room entered
  |       |
  |       v
  |   gatelink.Client.Forward
  |       |
  v       v
Game gRPC handler --> Game dispatcher --> Echo handler
                                           |
                                           v
                              RequestPlayerSender / server-send
                                           |
                                           v
Gate LocalReceiver -- receivedAt --> outbound envelope --> writer --> player

prometheus.Registerer
  |-- Go/process collectors
  |-- Gate/Game product metrics
  `-- lazy Redis/Database pool collectors
```

### 4.1 `pkg/observability`

只增加所有 process 都需要的 Go runtime 與 process collectors。它仍只擁有 registry、HTTP
exposition 與 readiness，不知道 Gate、Game、Redis 或 Database 的業務型別。

### 4.2 `pkg/dispatcher`

維持 transport-neutral，不依賴 Prometheus。新增只讀的 `IsRegistered(channel, commandID) bool`，
讓 ingress 在執行前安全判斷 raw command 是否來自可信任 registration。它不判斷 login/room state，
也不負責 metrics recording。

`RegisteredCommandIDs` 保留給管理與 contract inspection；request hot path 不複製或遍歷整個 ID
slice。`IsRegistered` 使用既有 `RWMutex` 與 map lookup。

### 4.3 `products/gateproduct`

擁有 Gate transport、connection state、forward 與 writer queue 的 metrics。新增 package-private
`gateMetrics` holder，constructor 注入標準 `prometheus.Registerer`，再注入 `WebSocketServer` 與
server-send receiver。外部 product 不需要直接操作此 holder。

Gate 的 login/room 業務 handler 仍由外部 module 註冊；example workflow module 是第一個實作者。

### 4.4 `products/gameproduct`

擁有 Game gRPC ingress、dispatcher handler 與 direct server-send call 的 metrics。新增
package-private `gameMetrics`，注入 Game gRPC server，並以一個小型 decorator 包住既有
`serversend.RequestPlayerSender`。本次不包裝未出現在壓測主流程的 `PlayerSender` 與
`BroadcastSender`。

### 4.5 `pkg/infra/database` 與 `pkg/infra/redis`

各自擁有只讀 pool collector，因為只有各 package 能正確處理 client lifecycle 與 stats 語意。
Collector 不執行 query、PING 或其他 I/O；scrape 時只讀本機 stats snapshot。

`pkg/infra` 負責在 resource 確實被 DI resolve 時，若容器中存在 `prometheus.Registerer`，自動註冊
對應 pool collector。這保留 resource lazy 語意，也讓未使用 observability module 的既有引用端
仍能建立 infra graph。

### 4.6 `pkg/gateproto` 與 runnable examples

`pkg/gateproto` 只擁有既有正式 Login player-facing wire schema 和 Login command IDs。EnterRoom／Echo
是本次壓測 workflow 的 example-local protocol，放在 `examples/metrics/internal/protocol`，由範例
workflow 註冊與使用；它們不成為 framework product 的公開業務 API。Runnable 範本只負責組裝正式
product App 與範例 handler，不把假資料邏輯加入四個 product 的預設 module。

### 4.7 `pkg/gatelink` 與 direct request route

`gatelink.RequestSource` 保留 `GateID`、`ConnectionID`，並增加由 Gate server-send listener 產生的
`ReplyEndpoint`。既有 gRPC metadata interceptor 以固定 `x-gate-request-reply-endpoint` key 傳遞它；一般
Gate request 可不帶此欄位，只有 `RequestPlayerSender` 要求三者完整。Gate 不接受 player payload 提供 endpoint。

`serversend.DirectRequestPlayerSender` 只依賴既有 `GRPCTransport`，從 request context 組出已驗證的
`GateEndpoint` 後直接呼叫 `SendToConnection`，不再以 GateID 呼叫 Redis `GateResolver`。`RoutedPlayerSender`
與 broadcast sender 的 presence/directory route 不變。

---

## 5. Metrics 契約

### 5.1 共通規則

- Prefix 固定為 `gaming_core_`，接著是 owner（`gate`、`game`、`database`、`redis`）。
- Duration 一律以 seconds 為單位並使用 Histogram；第一版使用 `prometheus.DefBuckets`，不增加
  config surface。取得正式 SLO 後可另案調整 buckets。
- RPS 由 `_total` Counter 的 `rate()` 計算，不建立會自行取樣的 Gauge。
- In-flight、active、queue depth/capacity 使用 Gauge。
- Error 不使用原始 error string；只使用本節定義的固定結果或 gRPC code。
- Ping/Pong 等 WebSocket control frames 不計入 application write rate/latency。
- Metrics 更新不得阻塞 network path，不得因 metrics 問題改變 request result。

### 5.2 Go runtime 與 process saturation

`pkg/observability` 使用目前已鎖定的 `client_golang`：

- `collectors.NewGoCollector()`
- `collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})`

這些標準 collectors 提供 goroutines、heap/GC、process CPU、resident memory、file descriptors 等
壓測必要資訊。不啟用額外的 advanced runtime metric set，也不重新命名標準 metrics。

`newRegistryOwner` 改為可回傳 error 的 constructor，逐一使用 `Register`；不使用 `MustRegister`。

### 5.3 Gate metrics

| Metric | Type / labels | 用途與記錄點 |
|---|---|---|
| `gaming_core_gate_websocket_connections` | Gauge | WebSocket upgrade 成功後加一；connection 完整清理後減一 |
| `gaming_core_gate_websocket_connections_total` | Counter | 計算 connection 建立速率 |
| `gaming_core_gate_websocket_connection_closes_total` | Counter `{reason}` | 每條 connection 恰好一次 terminal reason |
| `gaming_core_gate_websocket_commands_total` | Counter `{route,command,result}` | packet 解析完成後的 command RPS/error |
| `gaming_core_gate_websocket_command_duration_seconds` | Histogram `{route,command,result}` | packet 解析完成至 local handler 或 forward 返回 |
| `gaming_core_gate_websocket_commands_in_flight` | Gauge | 正在 dispatch/forward 的 packets |
| `gaming_core_gate_game_grpc_requests_total` | Counter `{code}` | Gate-to-Game RPC rate/error；`code` 為 bounded gRPC status code |
| `gaming_core_gate_game_grpc_duration_seconds` | Histogram `{code}` | `gameClient.Forward` 呼叫時間 |
| `gaming_core_gate_game_grpc_in_flight` | Gauge | Gate-to-Game concurrent RPC |
| `gaming_core_gate_websocket_writes_total` | Counter `{source,result}` | application binary writes；不含 Ping/Pong |
| `gaming_core_gate_websocket_write_duration_seconds` | Histogram `{source,result}` | `WriteMessage` 自呼叫至返回 |
| `gaming_core_gate_websocket_writes_in_flight` | Gauge | 正在執行 socket write 的數量 |
| `gaming_core_gate_websocket_write_queue_messages` | Gauge | 所有 active connections 尚未開始 write 的 application messages |
| `gaming_core_gate_websocket_write_queue_capacity` | Gauge | 所有 active connection queues 的總 capacity |
| `gaming_core_gate_websocket_write_queue_full_total` | Counter `{source}` | non-blocking enqueue 因 queue full 失敗 |
| `gaming_core_gate_server_send_requests_total` | Counter `{target,result}` | Gate LocalReceiver 收到的 direct/player/room request；結果為 `queued`、`ignored` 或 `error` |
| `gaming_core_gate_server_send_delivery_duration_seconds` | Histogram `{target,result}` | Gate receive 到每個目標 write terminal state |

固定 label 值：

- `route`：`local`、`game`。
- Gate `command`：local dispatcher 已註冊 ID 的十進位字串或 `forward`。
- 一般 `result`：`success`、`error`；server-send delivery 額外使用 `dropped`。
- `source`：`handler`、`server_send`。
- `target`：`connection`、`player`、`room`。
- Connection close 的 `reason`：`client_closed`、`read_error`、`invalid_frame`、`login_required`、
  `room_required`、`handler_error`、`forward_error`、`write_error`、`write_queue_full`、
  `server_closed`、`shutdown`、`panic`。

若同一 connection 同時出現多個錯誤，close counter 只使用最先決定終止流程的原因。詳細錯誤仍由
既有 log 保留，不能放進 label。

### 5.4 Game metrics

| Metric | Type / labels | 用途與記錄點 |
|---|---|---|
| `gaming_core_game_gate_commands_total` | Counter `{command,result}` | Game gRPC product handler 收到的 commands |
| `gaming_core_game_gate_command_duration_seconds` | Histogram `{command,result}` | 進入 Game product handler 到 dispatcher 返回 |
| `gaming_core_game_gate_commands_in_flight` | Gauge | Game 正在處理的 Gate commands |
| `gaming_core_game_server_send_requests_total` | Counter `{operation,result}` | direct request-player sender 呼叫 rate/error |
| `gaming_core_game_server_send_duration_seconds` | Histogram `{operation,result}` | Game 呼叫 sender 到 Gate 回覆 enqueue 結果 |
| `gaming_core_game_server_send_in_flight` | Gauge `{operation}` | concurrent server-send calls |

`command` 只可能是 Game dispatcher 已註冊 ID 的十進位字串或 `unknown`；`operation` 本次只有
`request_player`。未知 command 回 `Unimplemented`，不把 raw ID 暴露為 label。

### 5.5 Database pool metrics

| Metric | Type | `database/sql.DBStats` source |
|---|---|---|
| `gaming_core_database_pool_max_open_connections` | Gauge | `MaxOpenConnections` |
| `gaming_core_database_pool_open_connections` | Gauge | `OpenConnections` |
| `gaming_core_database_pool_in_use_connections` | Gauge | `InUse` |
| `gaming_core_database_pool_idle_connections` | Gauge | `Idle` |
| `gaming_core_database_pool_wait_total` | Counter | `WaitCount` |
| `gaming_core_database_pool_wait_seconds_total` | Counter | `WaitDuration.Seconds()` |

本 repository 每個 App 目前只有一個 Database pool，因此不增加 `db_name` label。Connection close
原因 counters 對本次 saturation 判斷非必要，先不暴露。

### 5.6 Redis pool metrics

| Metric | Type | `redis.UniversalClient.PoolStats()` source |
|---|---|---|
| `gaming_core_redis_pool_total_connections` | Gauge | `TotalConns` |
| `gaming_core_redis_pool_idle_connections` | Gauge | `IdleConns` |
| `gaming_core_redis_pool_pending_requests` | Gauge | `PendingRequests` |
| `gaming_core_redis_pool_wait_total` | Counter | `WaitCount` |
| `gaming_core_redis_pool_wait_seconds_total` | Counter | `WaitDurationNs` |
| `gaming_core_redis_pool_timeouts_total` | Counter | `Timeouts` |

不暴露 Redis address/node label。Cluster 的 `PoolStats` 是 client 提供的 aggregate，且 `PoolSize` 是
per-node 設定，因此不建立語意可能錯誤的 max-capacity Gauge；pending、wait 與 timeout 已能直接
判斷 pool saturation。

---

## 6. Command 合法性與狀態資料流

### 6.1 Dispatcher 是合法 command 的唯一來源

新增：

```go
func (d *Dispatcher) IsRegistered(channel Channel, commandID CommandID) bool
```

規則如下：

1. Gate 收到 packet 後先查 Gate `WebSocketChannel`。
2. 已註冊表示 local command，metric 使用可信任的 numeric ID。
3. 未註冊表示 candidate Game command；Gate metric 使用 `forward`，不使用 raw ID。
4. Game 收到 gRPC request 後查 Game `GateRequestChannel`。
5. 已註冊才使用 numeric ID；未註冊一律記為 `unknown` 並回 `Unimplemented`。

Registration 只能由可信任的 application composition code 呼叫。即使未來新增 runtime
registration，cardinality 仍由程式註冊數量決定，而不是 client 輸入空間決定。本次不增加 dispatcher
freeze、command name registry 或 Prometheus dependency。

### 6.2 Connection state

`SessionRegistry` 新增只讀 snapshot：

```go
type SessionState struct {
	LoginName LoginName
	RoomID    RoomID
}

func (r *SessionRegistry) State(connectionID WebSocketConnectionID) (SessionState, bool)
```

它只複製既有 map 中的 canonical state，不暴露 session pointer，也不執行 authentication。

- Enter-room handler 查不到 state：回傳 typed `login required` error。
- Gate 準備 forward 時查不到 state 或 `RoomID == ""`：記錄對應 close reason 並關閉 connection，
  不呼叫 gRPC。
- Dispatcher 只判斷 command registration；login/room policy 仍由 Gate handler/ingress 負責。

### 6.3 Outbound write completion

保留公開 `WebSocketSession.SendBinary([]byte) error` 的 enqueue 契約。為讓外部 product module 能以
同一份 wire contract 回覆封包，新增 `gateproduct.EncodeWebSocketPacket(WebSocketPacket) []byte`；
它只編碼既有 16-byte header，不改變 session 或送出語意。`webSocketConnection` 內部把
`writeCh` element 由 `[]byte` 改為 package-private envelope：

```go
type outboundMessage struct {
	data       []byte
	source     outboundSource
	receivedAt time.Time
	target     serverSendTarget
}
```

- Handler 直接呼叫 `SendBinary` 時，source 為 `handler`，enqueue 當下設定時間。
- Gate server-send receiver 在 method entry 保存同一個 `receivedAt`，透過 package-private registry
  helper 傳給每一個目標 connection；room broadcast 的所有 envelopes 共用這個 receive time。
- 真實 `webSocketConnection` 走 timestamp-aware private enqueue；既有 test fake 仍可 fallback 到公開
  `SendBinary`，因此不擴大 public interface。
- Writer dequeue 時更新 queue Gauge；`WriteMessage` 返回後記錄 write result 與 delivery duration。
- Write error 先記錄當前 envelope 為 `error`，再 close connection。
- Server-send enqueue 立即失敗時，delivery duration 以 `error` 結束，不建立等待 writer 的 observation。
- Connection 關閉時 writer 將尚未處理的 envelopes 記為 `dropped` 並修正 queue Gauge，避免 queue
  metric 漂移或 server-send delivery 永遠沒有 terminal result。
- Metrics callback 是同步、固定成本的 Counter/Gauge/Histogram update，不另建 goroutine/channel。

Concrete connection 以 package-private `closeWithReason` 保存 first terminal reason；公開 `Close()` 對
既有 server-side kick/replacement 使用 `server_closed`，App shutdown 則由 owner 明確使用 `shutdown`。
不修改 `ClosableWebSocketSession` public contract。

Server-send RPC 的既有 `DELIVERED` 語意維持「成功 enqueue」。本次只增加非同步 write outcome
metrics，不讓 gRPC 等待 client socket write，避免改變 timeout 與 backpressure 行為。

---

## 7. Example protocol 與範本行為

### 7.1 Proto

保留現有 `pkg/gateproto` 的 `LoginRequest`／`LoginResponse`。EnterRoom／Echo 僅供本次範例 workflow
使用，schema 與 generated code 放在 `examples/metrics/internal/protocol`，protobuf namespace 使用
`metrics.example.v1`，不形成 production API 相容性承諾。

```proto
message EnterRoomRequest {
  string room_id = 1;
}

message EnterRoomResponse {
  uint32 code = 1;
}

message EchoRequest {
  bytes payload = 1;
}

message EchoResponse {
  bytes payload = 1;
}
```

範例保留以下固定 command IDs，讓 README、workflow 與 load client 可重現；這些數值只屬於 example-local
contract，不宣告為 project-owned production range：

- EnterRoom request：`0xF1000001`；response：`0xF1000002`。
- Echo request：`0xF1000011`；response：`0xF1000012`。

現有 Login request/response 維持 `0xC00002`／`0xC00003`，由 `pkg/gateproto/login.go` 與既有 contract
test 鎖定。Example IDs 由 `examples/metrics/internal/protocol/command.go` 集中定義，並由同 package
contract test 鎖定；不得散落在 handlers 或 load client。Example protocol 未來可隨範例重寫，不對正式
業務 client 提供 backward-compatibility 保證。

Echo payload 使用既有 server-send payload limit，不增加可由 client 指定 response size、delay 或錯誤
模式的欄位；這避免範例 endpoint 成為資源放大或故障注入入口。錯誤/延遲情境由 contract test 注入
fake dependency，不放進 wire API。

### 7.2 Gate 範本 module

- Login：unmarshal `LoginRequest`，要求非空 `login_name`；範本不驗證 token，呼叫
  `SessionRegistry.Register`，enqueue `LoginResponse`。
- Enter-room：unmarshal example-local `EnterRoomRequest`，用 connection ID 查 `SessionState`；未登入回 typed state
  error，成功則呼叫 `EnterRoom` 並 enqueue response。
- Echo 不在 Gate dispatcher 註冊，因此走 Game forward path。
- 任意未在 Gate 註冊的 command 同樣受「必須已進房」限制；Game 再決定是否已註冊。

範本的 login 行為只是一個清楚標示的 in-memory example policy，不宣稱 production authentication。
Login proto 與既有 transport path 是 public contract；EnterRoom/Echo transport path 僅用於可啟動範例，
範例 authentication implementation 也不是正式 authentication policy。

### 7.3 Game 範本 module

- 在 `GateRequestChannel` 註冊 example-local Echo command。
- Unmarshal example-local `EchoRequest`，建立 `EchoResponse`。
- 使用 `RequestPlayerSender.SendToRequestPlayer` 回到原始 Gate connection。
- direct request-player reply 直接使用 Gate request context 的 `ReplyEndpoint`；不為每筆 Echo reply 查 Redis。
- 不使用 Database；`PlayerSender`／broadcast 仍依既有設定使用 Redis presence/directory route。

### 7.4 四個 runnable programs

新增：

```text
examples/metrics/
  README.md
  gate/main.go
  game/main.go
  api/main.go
  gms/main.go
  load/main.go
  load/metrics.go
  load/metrics_test.go
  internal/protocol/
    command.go
    room.proto
    room.pb.go
    echo.proto
    echo.pb.go
    protocol_contract_test.go
  internal/workflow/gate.go
  internal/workflow/game.go
  configs/*.yaml
```

每個 service `main` 只做 config path flag、`signal.NotifyContext`、對應 `NewApp` 與 `Run`。不建立共用
CLI framework。Gate/Game 注入 workflow module；API/GMS 因尚無 handler，只啟動正式 App lifecycle
與 Go/process metrics，不產生假的 request counter。

`README.md` 說明四個 process 的啟動順序、固定範例 ports、Redis dependency、WebSocket binary frame
格式、example command IDs、Prometheus scrape URLs 與完整 login → enter-room → echo 壓測順序。範例設定不得
包含真實 credential。

`load/main.go` 是配合此 example protocol 的最小 closed-loop client，不是第五個 product service。它提供 Gate
URL、connection 數、正式測量 duration、bounded payload size、setup timeout/concurrency、warm-up request count
與 loopback metrics address flags。它先以 bounded concurrency 建立全部 WebSocket connections，再依序完成
Login／EnterRoom 與固定次數 warm-up；全部準備完成後才開始正式 measured Echo。`duration` 是停止送出新
request 的 admission window，最後一筆 in-flight request 以單筆 request timeout 收尾，並輸出 setup、measurement
與完成數摘要。

Load 應使用獨立 Prometheus registry，在 `/metrics` 暴露三個 example-local metrics：

- `gaming_core_example_load_echo_round_trips_total{result}`；
- `gaming_core_example_load_echo_round_trip_duration_seconds{result}`；
- `gaming_core_example_load_echo_round_trips_in_flight`。

Client timer 從 Echo `WriteMessage` 前開始，到預期 response frame 完成 validation 後結束。`result` 只允許
`success`、`error`、`cancelled`；正式 duration 到期只停止新 request，不取消已送出的 request，避免同一筆
server 已處理而 client 被誤記為 cancelled。單筆 request timeout 記為 `error`；只有上層 context cancellation
才記為 `cancelled`。warm-up 不寫入 load-private metrics，server-side counter correctness 以測試前 baseline
扣除已知 warm-up 數後驗證。Load listener 只提供 `/metrics`，不提供 framework `/health`、`/ready` 或 DI
lifecycle。不同 process 的 Histogram 只在同一時間窗口比較趨勢，不相減 quantile。詳細執行與驗收方式記錄在
`ACTIONABLE_METRICS_EXAMPLE_VALIDATION.md`。

不加入 arbitrary command、failure injection、distributed workers、HTML report、自製 percentile engine
或 high-cardinality labels。

本次不加入 Docker Compose；部署編排尚未被需求定義。最小 load client 是必要項，因通用 HTTP/gRPC
壓測工具不能直接產生本 repository 的 WebSocket binary header 與 protobuf payload。

---

## 8. Infra pool collector lifecycle

`pkg/infra.Module` 的 Redis/Database managed constructors 改用小型 `dig.In` input，將
`prometheus.Registerer` 標記為 optional：

```go
type databaseInputs struct {
	dig.In
	Snapshot   config.SourceSnapshot
	Registerer prometheus.Registerer `optional:"true"`
}
```

Constructor 流程：

1. 以現有 `database.New`／`redis.New` 建立 client，不執行 I/O。
2. Registerer 存在時，呼叫該 package 的 `RegisterPoolMetrics`。
3. Registration error 以 `%w` 回傳，讓 DI build/start 失敗，不 panic。
4. 只有某個 hook/service 確實依賴 client 時，managed constructor 才會 resolve，維持 lazy lifecycle。
5. Registerer 不存在時維持目前行為，不要求所有 `infra.Module` 使用者先載入 observability。

Collector scrape 時取得 wrapper read lock 並讀取 stats pointer。Client 尚未 Start 或已 Stop 時不輸出
該 pool samples，也不讓整個 `/metrics` scrape 失敗；正常 App lifecycle 中，observability listener
最後停止，因此 shutdown 的短暫 sample 消失是預期行為。

不直接使用 `collectors.NewDBStatsCollector`，因為它要求 constructor 當下已有 `*sql.DB`，會破壞目前
「Start 才建立 pool」的 lifecycle。自有 collector 只轉譯相同的 `DBStats` 欄位，不複製 pool 邏輯。

---

## 9. 錯誤處理

| 邊界 | 行為 |
|---|---|
| Built-in/product/pool metric 重複或 descriptor 不一致 | Constructor/DI 回傳 wrapped registration error；禁止 panic |
| Malformed Login/EnterRoom/Echo protobuf | Handler error、command result=`error`、close reason=`handler_error` |
| Enter-room before login | close reason=`login_required`；不送 response，關閉 connection |
| Forward before room | close reason=`room_required`；不呼叫 Game，關閉 connection |
| Gate local handler panic | 記錄 result/close reason=`panic`，沿用既有 recover 後關閉 connection |
| Gate-to-Game error | Counter 使用標準 gRPC code；command error、close reason=`forward_error` |
| Game command 未註冊 | Game command=`unknown`、result=`error`，回 `Unimplemented` |
| Server-send target 不存在 | Gate receiver result=`ignored`；維持既有 delivery contract |
| Write queue full | enqueue error、queue-full counter、close reason=`write_queue_full` |
| Socket write error | 當前 write/delivery result=`error`；connection close，剩餘 queue=`dropped` |
| Pool client 尚未 started/已 stopped | Collector 暫不輸出 pool samples，不回 invalid metric |

`readLoop` 需要回傳 bounded termination category，不能再靜默丟棄所有 error。正常 close、invalid frame、
read failure 與 application 主動 close 應分開；原始 error 只進 log，不進 labels。

---

## 10. 具體檔案修改

### 10.1 修改既有 packages

- `pkg/observability/registry.go`
  - 註冊標準 Go/process collectors，讓 constructor 回傳 registration error。
- `pkg/observability/observability_contract_test.go`
  - 將「不含 go/process」舊斷言改為兩者存在且 App-local；保留 custom registry isolation tests。
- `pkg/dispatcher/dispatcher.go`
  - 新增 `IsRegistered`。
- `pkg/dispatcher/dispatcher_contract_test.go`
  - 驗證 channel normalization、registered/unregistered/nil receiver。
- `pkg/infra/module.go`
  - 使用 optional Registerer wrappers，只在 client 被 resolve 時註冊 pool collectors。
- `pkg/infra/module_contract_test.go`
  - 驗證有/無 Registerer、lazy resource 與 registration error。
- `pkg/infra/database/metrics.go`、`metrics_contract_test.go`
  - 新增 lifecycle-safe DBStats collector。
- `pkg/infra/redis/metrics.go`、`metrics_contract_test.go`
  - 新增 lifecycle-safe PoolStats collector。
- `products/gateproduct/metrics.go`
  - Gate collectors、bounded label helpers 與 recording methods。
- `products/gateproduct/websocket.go`
  - 注入 metrics、記錄 connection/read/dispatch/gRPC/write/queue，forward 前檢查 room state，並攜帶
    private outbound envelope。
- `products/gateproduct/session_registry.go`
  - 新增只讀 `SessionState`；增加 package-private timestamp-aware server-send delivery helpers。
- `products/gateproduct/server_send.go`
  - 在 Gate receive 保存時間、記錄 receiver result 並傳遞 delivery metadata。
- Gate 既有 contract tests
  - 補 state enforcement、close reason、queue/write/server-send terminal metrics；只調整被 constructor
    dependency/signature 直接影響的 fixtures。
- `products/gameproduct/metrics.go`
  - Game command 與 direct request-player server-send metrics/decorator。
- `products/gameproduct/grpc.go`
  - 以 dispatcher registration 決定 bounded command label 並記錄 handler metrics。
- `products/gameproduct/server_send.go`
  - 只包裝 `RequestPlayerSender`，以 request-scoped reply endpoint 建立 direct sender，不改其他 sender 行為。
- Game 既有 contract tests
  - 補 registered/unknown/error/server-send metrics contracts。
- `pkg/gatelink/metadata.go` 與 gatelink contract tests
  - 在 `RequestSource`／既有 metadata interceptor 傳遞可選 `ReplyEndpoint`，保留一般 request 的相容性。
- `pkg/serversend/sender.go` 與 sender contract tests
  - direct request-player sender 直接驗證並使用 request route，不依賴 per-message Redis `GateResolver`。
- `products/gateproduct/server_send_runtime.go`、`websocket.go` 與受影響 Gate tests
  - 保存 managed advertise endpoint，注入每筆 forwarded request；停止後清空 route。

### 10.2 Example wire contract

- 新增 `examples/metrics/internal/protocol/room.proto`、`room.pb.go`。
- 新增 `examples/metrics/internal/protocol/echo.proto`、`echo.pb.go`。
- 新增 `examples/metrics/internal/protocol/command.go`，集中 example command IDs。
- 新增 `examples/metrics/internal/protocol/protocol_contract_test.go`，鎖定 field numbers、full names、
  ID 固定值、唯一性與 request/response 配對。
- `pkg/gateproto` 只保留既有 Login schema、generated code 與 Login command IDs。

Generated `.pb.go` 與 `.proto` 必須同 commit，不手改 generated code。Example `.proto` 的 protobuf
namespace 使用 `metrics.example.v1`，Go package 使用
`examples/metrics/internal/protocol;protocol`。若 repository 在實作時仍沒有統一 codegen command，
只記錄實際使用且可重現的 `protoc`/plugins 版本；本次不為兩個 example proto 額外導入完整 Buf pipeline。

### 10.3 Runnable examples

- 新增第 7.4 節列出的四個 service `main`、最小 load client、兩個 workflow modules、設定與 README。
- `examples/metrics/load/metrics.go` 建立 load-private registry、三個 Echo metrics 與可 graceful shutdown
  的 loopback `/metrics` listener；`main.go` 以 bounded setup、warm-up、admission window 與 request-timeout
  drain 管理 Echo round trip。
- `examples/metrics/load/metrics_test.go` 驗證 success/error/cancelled terminal results、Histogram count、
  實際 Echo round trip、in-flight 歸零、HTTP exposition、非 metrics path 為 404 與 listener shutdown。
- `examples/metrics/load/main_test.go`
  - 驗證 setup concurrency、handshake／session／warm-up phase ordering、warm-up 不寫入 metrics，以及 admission
    window 到期後 drain in-flight request。
- 新增 `flow_contract_test.go` 覆蓋 Gate/Game full-flow 與兩個 state rejection；API/GMS 由各自 App
  contract 驗證可啟停。
- 不修改四個 product `NewApp` public signature，不把 example handler 放入預設 module。

### 10.4 明確不修改

- `pkg/framework` lifecycle、DI public interfaces 與 phases。
- Observability HTTP paths、listener config、health/readiness 語意。
- `gatelink.proto`、`serversend.proto` 與既有 RPC wire format。
- `WebSocketSession.SendBinary` public method signature。
- 既有 WebSocket packet header 與 `EncodeWebSocketPacket` 的 wire 語意。
- Server-send `DELIVERED`/`Receipt.AcceptedAt` 的 enqueue/acceptance 語意。
- Redis/Database 設定 schema 與 lazy connection 行為。
- RocketMQ implementation。

---

## 11. 測試策略與完成條件

### 11.1 Unit/contract tests

1. Metric constructor：所有 descriptors 可註冊；duplicate registration 回 error，不 panic。
2. Labels：只接受文件列出的 bounded values；未註冊 raw command 不出現在 gathered labels。
3. Dispatcher：`IsRegistered` 與 `Dispatch` 對 normalized channel 結果一致。
4. Session state：login/room snapshot 正確，remove/replacement 後不洩漏 stale state。
5. Gate state policy：未登入進房、未進房 forward 都不呼叫下游且關閉 connection。
6. Gate command/RPC metrics：local、forward、success、handler error、gRPC error 與 panic 路徑各只計一次。
7. Writer：queue depth/capacity、queue full、write success/error、close discard 都回到一致 Gauge。
8. Server-send：direct/player/room 從同一 receive time 到每個 write terminal result；ignored 不假裝
   write success。
9. Game：registered command 使用 ID、unregistered 使用 `unknown`；handler latency/in-flight 歸零。
10. DB/Redis collectors：已啟動時正確轉譯 stats，未啟動/已停止時 scrape 仍成功。
11. Infra lazy contract：沒有 component 使用 pool 時不建立 client、不出現 pool metrics；使用時才出現。
12. Observability：每個 App 都含 Go/process collectors，兩個 App registry 仍互相隔離。
13. Load observer：每筆 Echo 只有一個 `success`、`error` 或 `cancelled` terminal result，Histogram
    count 對應 Counter、in-flight 歸零，且 private `/metrics` listener 可停止。
14. Direct request route：metadata 保留 GateID／connection／reply endpoint；sender 不查 Redis，malformed
    endpoint 在 delivery 前失敗。
15. Load phases：所有 handshake 完成後才送 Login，所有 session ready 後才 warm-up，admission window 到期
    不取消既有 in-flight request，setup concurrency 不超過設定。

Counter/Histogram 以 Prometheus `testutil` 或 gather result 驗證，不依賴 sleep 判斷 duration exact value；
只驗證 sample count、label 與值大於等於零。Concurrency tests 需可在 `-race` 下穩定重複。

### 11.2 Full-flow integration contract

使用真實 framework Apps、ephemeral TCP ports、`miniredis` 與 Gorilla WebSocket client：

1. 啟動 Game 與 Gate，確認 readiness。
2. 建立兩條獨立 connections。
3. 第一條依序 login、enter-room、echo，收到相同 Echo payload。
4. 第二條未登入 enter-room，驗證 connection 被關閉且 Game 未收到 request。
5. 另建 connection 只 login 後送 echo，驗證 connection 被關閉且 Game 未收到 request。
6. 驗證 Gate/Game metrics counters、Histogram count、in-flight 與 queue gauges。
7. 停止 Apps，確認沒有 goroutine/listener leak。

測試不需要 MySQL；Database pool collector 由 package contract test 驗證。Redis 使用既有
`miniredis` dependency，不增加 container requirement。

Integration test 透過 test-only product module 捕捉 DI 中 `prometheus.Registerer` 的 concrete value，
確認它同時實作 `prometheus.Gatherer` 後讀取 samples；這是 repository 內部測試技巧，不把 Gatherer
新增為正式 DI contract。WebSocket/gRPC 實際 ephemeral addresses 同樣由 test hook 捕捉既有 server
instance，不為 examples 增加 production address-discovery API。

### 11.3 Build 與測試門檻

```text
go test ./...
go test -race ./examples/metrics/... ./pkg/observability ./pkg/dispatcher ./pkg/infra/... ./products/...
go vet ./...
```

所有 example `main` 必須被 `go test ./...` 編譯。實作完成但尚未執行實際 tests 時，需明確區分
「已編譯設計」與「已通過測試」，不可只以 code review 宣稱成功。

---

## 12. 關鍵決策與取捨

### 12.1 使用 dispatcher registration，不使用 proto enum allowlist

正式業務 command 會隨引用端 modules 增加，metrics 不可能維護完整 enum。Dispatcher registration
已是 runtime routing 的真實來源；使用它同時避免設定漂移與 untrusted raw ID cardinality。

Gate 對 forwarded command 只能標 `forward` 是刻意限制。若讓 Gate 複製 Game registration，部署版本
不同時反而會錯誤拒絕合法流量。詳細 command breakdown 在 Game 觀察即可。

### 12.2 保留 asynchronous WebSocket write

讓 server-send RPC 等待 socket write，會把慢 client 直接轉成 Game-to-Gate timeout 並改變現有
backpressure。Private envelope/callback 可得到真實 write outcome，同時保留 enqueue contract；這是
滿足 latency 需求所需的最小結構變更。

### 12.3 Pool metrics 隨 lazy resource 註冊

四個 product 都預設宣告 infra providers，但不是每個 process 都使用每個 resource。只有 resource
被 resolve 時才註冊 collector，可避免為了 `/metrics` 強迫 API/GMS 連 Redis/MySQL，也不輸出其實
不存在的 pool。

### 12.4 Example Echo API，而非 product default route

Echo 仍走完整 production transport path，可作 compatibility、smoke 與 load baseline；但 schema、command
IDs 與 handler 僅存在於 `examples/metrics/internal/protocol`／workflow，不讓尚未定義的業務 API 進入
framework product 的公開 package。其 payload bounded、無資料庫 side effect、無 delay/error knobs，
因此範例 endpoint 不提供額外資源放大能力。範例 authentication policy 與 framework transport 仍分離。

### 12.5 不全面 instrument 所有 infra/serversend operation

目前壓測流程只使用 direct request-player reply；直接攜帶 Gate request 的 reply endpoint 即可定位需求描述
中的 server-send stage，且不必為每筆 reply 查 Redis。Player/broadcast sender、Redis commands 與 SQL
statements 尚無 workload/SLO，現在加入只會增加未被驗證的 metric surface。

---

## 13. 已知限制與後續擴充

- Histogram 無法關聯單筆跨服務 request；需要逐筆因果關係時另加 tracing。
- Gate 的 `forward` label 不區分 Game command；Game metrics 才提供 registered ID breakdown。
- API/GMS 在正式 handler 尚未加入前只有 runtime/process，以及實際 resolve 後的 pool metrics。
- Example login 不是 authentication，production product 必須以自己的 module 替換 policy。
- Prometheus buckets 暫用 defaults；應以基準壓測與 SLO 調整，而不是先增加可配置 buckets。
- Redis Cluster pool stats 採 go-redis aggregate，不提供 per-node diagnosis；需要時另案評估 bounded
  topology labels。
- `collectors.NewProcessCollector` 依作業系統支援度提供 process samples；不支援 proc/Windows 的平台
  仍保留 collector，但該部分可能沒有 samples。
- 未提供 alert/dashboard 或通用 load framework；最小 client 只支援本次固定 workflow。
- Load client 的 metrics listener 只供 Prometheus pull，不提供 product health/readiness，也不持久化最終
  report；單次精確完成數仍以 load 結束 log 為準。Load 先 bounded 建立 connections、初始化 session 與
  warm-up，再以 duration 作 request admission window；最後一筆 request 由單筆 timeout bounded drain。
- 未來新增 sender metrics 時沿用同一 `operation` bounded set，但不在本次預先建立通用 middleware。

---

## 14. Self review

### 14.1 需求覆蓋

| 需求 | 設計對應 | 結果 |
|---|---|---|
| Latency | 五個本地 duration 邊界與 client-side E2E 分工 | 符合 |
| RPS | connection、command、gRPC、server-send、write Counters | 符合 |
| Error | bounded result/code/reason，禁止 raw errors | 符合 |
| Saturation | active/in-flight、write queue、pool、Go/process | 符合 |
| Gate 收到是 server-send receive | receive time 隨 outbound envelope 到 write terminal | 符合 |
| Read/write 分離 | read/dispatch 與 async writer metrics 分開 | 符合 |
| 登入/進房狀態限制 | SessionState + ingress enforcement + close contract | 符合 |
| Command 無法預先列舉 | dispatcher registration 為唯一合法來源 | 符合 |
| Example 測試 proto | EnterRoom/Echo schema 與 stable IDs | 符合 |
| 四個可啟動範本 | 四個 mains、workflow modules、configs、README | 符合 |
| 可直接進行簡單壓測 | bounded closed-loop load client | 符合 |
| Client 與 server 分開觀測 | load-private Echo metrics 與相同 Prometheus window 分析 | 符合 |
| Redis/DB pool | lazy lifecycle-safe collectors | 符合 |

### 14.2 必要性 review 後保留的調整

- Go/process collectors：沒有它們無法判斷 CPU、heap、GC、goroutine、FD 等 process saturation。
- `Dispatcher.IsRegistered`：避免每個 ingress 複製 registration slice 或維護另一份 command allowlist。
- `SessionState`：進房與 forward policy 需要從既有 canonical registry 查狀態，不能另建重複 state map。
- Private outbound envelope：現有 `SendBinary` 只知道 enqueue；沒有 envelope 就無法量到指定的 write
  成功時間，也無法正確維護 queue/dropped metrics。
- Optional Registerer infra wrappers：同時滿足 pool metrics、lazy resources 與未使用 observability 的
  compatibility；直接自動 resolve pools 或改 framework 都不正確。
- Example-local EnterRoom/Echo proto 與 runnable handlers：沒有可重現的 wire contract 就只能測 transport
  toy path，無法驗證使用者指定的狀態與完整 server-send 流程；它們不需要成為 product 公開 API。
- Load-private Echo metrics：framework stages 無法代表玩家端 round-trip；獨立 registry 可觀測 client
  latency，又不會擴大 product metric contract。

### 14.3 Review 後刪除或拒絕的非必要項目

- 不建立自製 metrics facade、全域 registry、dynamic label sanitizer 或 command catalog service。
- 不把 Prometheus dependency 放進 dispatcher。
- 不新增 per-command name metadata；已註冊 numeric ID 足以穩定定位。
- 不讓 Gate 同步取得或複製 Game command registry。
- 不讓 server-send RPC 等待 WebSocket write。
- 不增加 timestamp wire fields 或修改既有 gRPC proto。
- 不量測 Ping/Pong、bytes、每個 Redis command、SQL statement、RocketMQ 或未使用的 sender paths。
- 不加入假的 API/GMS traffic、debug endpoints、delay/error query 參數、pprof 或 tracing。
- 不新增 metrics config、bucket config、alerts、dashboards、Docker Compose 或通用 load framework。
- 不改 framework lifecycle，也不把 examples 併入 production product defaults。

### 14.4 最終範圍判定

Review 後的修改沒有刪減已確認需求。所有新增 production metrics 都能對應 Rate、Error、Duration 或
Saturation，且每個 label 都有封閉來源。額外程式結構只用於現存缺口：判斷 dispatcher registration、讀取
canonical session state、把 server-send receive time 帶到 asynchronous writer、攜帶 direct reply endpoint
以及讓 load setup 與 measured window 可分離。

若再移除其中任一項，會失去 command cardinality 安全、狀態 contract、指定 latency 邊界、pool
saturation 或可執行驗證之一；上述明確排除項若加入，則會超出目前 workload 與需求。因此本設計是
目前需求的最小閉合集合，沒有為未知業務預建抽象。

---

## 15. 建議實作順序

1. 加入 Go/process collectors、DB/Redis pool collectors 與 isolated contract tests。
2. 加入 `Dispatcher.IsRegistered`、`SessionState` 與 state policy tests。
3. 建立 Gate/Game metrics holders，先完成 command/gRPC metrics。
4. 將 write queue 改為 private envelope，完成 write/server-send terminal metrics 與 race tests。
5. 新增 example-local EnterRoom/Echo proto、generated files 與 schema contracts。
6. 新增 Gate/Game workflow modules 與 full-flow integration contract。
7. 新增四個 runnable service mains、最小 load client、configs 與 README，確認全部 examples 可編譯。
8. 完成 request-scoped direct reply route 與 load setup/warm-up/admission phases 的 contract tests。
9. 執行完整 test/race/vet，最後核對 diff 只包含第 10 節列出的必要範圍。

若實作中發現既有 API 無法維持本文件定義的 enqueue、lazy lifecycle 或 App-local registry 契約，應先
更新本設計說明原決策、實際問題、調整與取捨，再修改程式；不得以便利為由靜默擴大範圍。
