# Async Server-send Queue 與業務批次設計

> 本文件是 `BroadcastSender` 與 `PlayerSender` 非同步送出邊界的最新 contract。
> 它只取代 `BATCH_PLAYER_SEND_DESIGN.md` 中「不建立 Game-side queue／worker」及
> `GENERIC_BROADCAST_CHAIN_DESIGN.md` 中 producer 同步等待 transport 的部分；既有 routing、
> fallback、generic command、dispatcher、1 MiB payload 與 Gate WebSocket delivery contract
> 全部保留。

## 1. 目的與結論

目前 `Broadcast` 會同步等待 Redis `PUBLISH`，必要時再同步等待 gRPC fan-out；
`SendToPlayers` 會同步等待 Redis presence／endpoint pipeline、分組、chunking 與 gRPC。
效能驗證顯示同步 Player critical path 會跨過 16ms producer tick，Broadcast 16ms 也曾發生少量 missed
tick，因此本次只把 producer API 改成 bounded asynchronous acceptance，並把可安全合併的訊息放在
正確層次。

定案如下：

- `BroadcastSender.Broadcast` 與 `PlayerSender.SendToPlayers` 的 public method 保持不變；成功改為
  「輸入已複製且整批進入 bounded queue」，不再表示 Redis／gRPC 已完成；
- `RequestPlayerSender` 不變，仍透過原 Gate → Game unary response slot 同步接受一筆 reply；
- Broadcast 與 Player 各有一個獨立 queue 與一個 managed worker，避免兩種 dependency path
  互相 head-of-line blocking；
- Player worker 只等待第一筆；取得第一筆後，以 non-blocking receive 拿取當下可安全合併的工作，達到
  count／bytes 上限或當下無資料就立即送出；
- 不使用 flush timer、debounce、sleep、`runtime.Gosched` 或其他等待湊批機制；
- Broadcast 的多訊息能力由單一業務 command 的 protobuf `repeated messages` 表達；generic transport
  不解析、合併或改寫 opaque payload；
- Broadcast worker 一次處理一個已完成業務批次的 outer command，直接重用目前 Redis `PUBLISH` → gRPC
  fallback sender；第一版不加入 Redis pipeline；
- Player 合併可安全共用同一 W3C trace 的相鄰 queue jobs，再沿用既有 Redis routing、endpoint
  grouping 與 1 MiB gRPC chunks；
- queue 滿載時 caller 立即取得 typed error；不等待空位、不部分接受、不靜默丟棄；
- 第一版不建立 per-Gate persistent worker。既有每 endpoint 一個 reusable `grpc.ClientConn`、每批
  endpoint 間 bounded parallel、同 endpoint chunks sequential 的模型保持不變；
- 不新增 retry、durable storage、ack、dedupe、gRPC stream 或自動調參。

此設計的目標是移除 business goroutine 的 Redis／gRPC 等待，同時讓壓力明確呈現在 bounded queue、
queue wait 與 worker dependency metrics，而不是用無界 goroutine 或 queue 隱藏瓶頸。

## 2. 需求理解與合理假設

### 2.1 已確認行為

1. Player transport batching 是「當下有多少拿多少」；不為湊滿 batch 主動等待。Broadcast 業務批次由
   caller 在單次 API call 中提供，不由 generic transport 等待或合併。
2. Worker 閒置時可以 blocking wait 第一筆，這不是 batching delay。
3. Caller 只等待 validation、payload clone 與 queue admission，不等待 Redis 或 gRPC。
4. 背景 worker 必須等待 Redis／gRPC bounded result，才能執行既有 fallback、記錄 result 並安全管理
   shutdown；這不阻塞 caller。
5. Broadcast Redis primary 使用 Pub/Sub 單筆 `Publish`／`PSubscribe`，不改為 Streams 或 pipeline。
6. `BroadcastRoomCommand` 是一個業務 command ID；其 protobuf payload 可包含同一 room 的多筆 client
   message。其他情境仍各自使用自己的 command ID 與 protobuf，不共用 mixed-command envelope。
7. Player 的 fallback 仍只在 Redis routing operation unavailable 時發生；gRPC response、Gate handler
   或 WebSocket delivery error 不觸發 fallback。
8. Broadcast 的 fallback 仍在 Redis publish error 或 subscriber count 為零時，對該筆訊息使用既有
   DNS all-Gate gRPC fan-out。

### 2.2 Delivery 與 ordering 假設

- 功能維持 best-effort、non-durable；process crash 會遺失 memory queue 中尚未送出的工作。
- 正常路徑中，同一 queue 保持 admission 順序；同一 Gate endpoint 的 Player chunks 保持輸入順序。
- 不保證不同 Gate endpoints 的完成順序，也不保證 Player queue 與 Broadcast queue 的全域順序。
- Redis result 不明而 fallback 時仍可能 duplicate；Redis 與 gRPC 兩條 transport 間不保證順序。
- `Receipt.AcceptedAt` 表示 producer queue acceptance。實際 transport／Gate／WebSocket 結果只由 metrics
  與 structured log 觀測。

### 2.3 W3C trace 假設

Async boundary 必須保留目前 logging contract：沒有 trace 時自動建立，跨 Redis／gRPC 時傳遞。
Queue 只保存 `logging.Detach(ctx)` 產生的 trace state，不保存 caller cancellation、deadline 或其他
request graph。

一個 `BroadcastRoomCommand` 只有一組 outbound trace metadata。Generic transport 不合併不同 API calls，
因此每個 outer command 自然保留自己的 trace；其內的 client messages 視為同一業務操作。

Player 的一個 `SendPlayersCommand` 只對應一組 gRPC metadata，不能同時代表多個 trace。因此第一版只
合併 traceparent 相同的相鄰 Player jobs。不同 trace 不放進同一個 gRPC batch，避免把不相關 request
錯誤地歸入同一條 trace。本次不為跨 trace coalescing 修改 `PlayerDelivery` protobuf。

## 3. 範圍

### 3.1 必須完成

- 建立共用但 package-private 的 bounded async queue primitive；
- 建立 managed async Player sender 與 async Broadcast sender；
- public API admission validation、defensive clone、all-or-nothing enqueue；
- Player worker 即時 non-blocking drain、count／bytes limit 與 pending item；
- `BroadcastRoomCommand` 增加同一 room 的 `repeated messages`，Gate 依序送出；
- Broadcast worker 重用既有同步 Redis primary → gRPC fallback delegate；
- Player 同 trace jobs 合併後重用既有 batch routing sender；
- lifecycle start、停止 admission、graceful drain、timeout cancellation；
- 最低限度且可採取行動的 queue、worker、dependency、fallback 與 discard metrics；
- 更新 Game／Gate DI 與 example config；
- 補足 unit、contract、race 與既有 performance validation 所需斷言。

### 3.2 明確不做

- 修改 `RequestPlayerSender` 或原 unary reply；
- generic mixed-command batch envelope、專用 RPC、gRPC streaming 或 Redis Streams；
- Redis Broadcast pipeline；除非未來 metrics 證明多個 outer commands 同時累積且 Redis RTT 造成 backlog；
- framework 解析或合併業務 payload，或自動把多次 `Broadcast` API calls 合成一個 protobuf；
- per-Gate persistent worker、per-Gate application queue 或可調 worker pool；
- retry、delayed retry、durable queue、disk spool、ack、deduplication或 exactly-once；
- 等待湊批的 flush interval、debounce、`runtime.Gosched` 或 adaptive batching；
- 依 gRPC error 啟動 Player fallback；
- 修改 Redis subscription、Gate dispatcher、player delivery handler或 WebSocket writer；
- 修改現有 1 MiB generic command payload 上限；
- command ID、login name、Gate ID、room ID 或任意 payload-derived Prometheus label；
- dashboard、alert deployment、autoscaler 或 production capacity 數值承諾。

## 4. 架構與資料流

### 4.1 Player

```text
Game handler
  │ SendToPlayers(ctx, []PlayerMessage)
  │ validate + trace detach/new + clone + atomic admission
  ▼
bounded Player queue ──full──> immediate ErrQueueFull / ErrEnqueueTooLarge
  │ one managed worker
  │ first blocking receive
  │ immediate non-blocking drain; same trace only; count/bytes bounded
  ▼
existing BatchPlayerSender
  ├─ Redis presence pipeline
  ├─ Redis endpoint pipeline
  ├─ endpoint grouping
  ├─ <= 1 MiB SendPlayersCommand chunks
  └─ endpoints bounded-parallel; one endpoint sequential
  ▼
cached grpc.ClientConn per Gate endpoint
  ▼
Gate RemoteCommandChannel → existing player-delivery handler → WebSocket queue
```

### 4.2 Room Broadcast

```text
Game business module
  │ BroadcastRoom(ctx, roomID, []Message)
  │ marshal one BroadcastRoomCommand{room_id, repeated messages}
  ▼
BroadcastSender.Broadcast(
  Message{CommandID: BroadcastRoomCommandID, Payload: encoded command})
  │ validate + trace detach/new + clone + atomic admission
  ▼
bounded Broadcast queue ──full──> immediate ErrQueueFull / ErrEnqueueTooLarge
  │ one managed worker; one outer command at a time; no batching wait
  ▼
existing FallbackBroadcastSender
  ├─ Redis PUBLISH once
  └─ publish error/count=0 → existing gRPC fan-out once per Gate endpoint
  ▼
Gate RemoteCommandChannel → BroadcastRoomCommandID handler
  │ decode room_id + repeated client messages
  └─ one WebSocket frame per client message per local room session
```

Generic transport 始終只看到一筆 `command_id + opaque payload`。多筆 client message 是 room command
schema 自己的能力，不是 server-send transport batch。

## 5. 模組邊界

### 5.1 `pkg/serversend`

負責：

- async admission、queue ownership、worker 與 lifecycle；
- deterministic queue byte accounting；
- Player immediate drain 與 batch boundary；
- detached trace context；
- 委派既有 Broadcast Redis primary／gRPC fallback sender；
- 呼叫既有 Player routing／gRPC fan-out engine；
- typed admission errors與窄型 metrics observer contract。Observer只接受固定operation／stage／result與
  數值，不接受任意map或label，並允許nil no-op，讓core package不依賴Prometheus。

不負責：

- Product YAML 路徑與 Prometheus metric prefix；
- Game／Gate DI lifecycle phase；
- Redis client、gRPC listener、Gate sessions 或 WebSocket queue；
- 業務 payload 解析。

### 5.2 `products/gameproduct`

負責：

- 解析 `server_send.player` 與 `server_send.broadcast` async config；
- 建立既有 synchronous Player/Broadcast transport engine；
- 將 async sender 以 `framework.PhaseService` managed resource 啟動；
- 對外提供相同的 `serversend.PlayerSender`／`serversend.BroadcastSender`；
- 註冊 Game producer metrics 與 observer adapter。

`RequestPlayerSender` 保持現在的非 managed provider。

### 5.3 `products/gateproduct`

Gate 只對已啟用的 `BroadcastSender` 增加相同 async managed wrapper與 producer metrics。Redis
subscriber 仍是獨立 ingress managed resource；session ownership、endpoint refresh 與 Gate delivery
service 不修改。

### 5.4 `examples/metrics`

- config 顯示所有新參數、default 與必要性；
- `BroadcastRoomCommand` 與 helper 支援同一 room 的 repeated client messages；
- Gate room handler 只解析 room envelope，不解析每筆 client payload；
- validation 必須把 queue 完整排空與 background worker result 納入成功條件，避免只看到 producer
  tick 完整就誤判 transport capacity。

## 6. Public API 與核心資料模型

### 6.1 Public sender interface 保持不變

```go
type PlayerSender interface {
    SendToPlayers(context.Context, []PlayerMessage) (Receipt, error)
}

type BroadcastSender interface {
    Broadcast(context.Context, Message) (Receipt, error)
}
```

更新 `Receipt` 文件：

- async Player／Broadcast：message ownership 已轉移至 process-local queue；
- request-player：reply 已放入目前 unary response slot；
- 兩者都不表示 WebSocket write 或 client receive。

不增加 Future、callback、per-message result 或 Flush public API。

### 6.2 Room Broadcast 業務 schema

`examples/metrics` 的 room command 改為：

```proto
message BroadcastRoomCommand {
  string room_id = 1;
  repeated BroadcastClientMessage messages = 2;
}

message BroadcastClientMessage {
  uint32 client_command_id = 1;
  bytes client_payload = 2;
}
```

- 一個 command 只代表一個 room；`messages` 不可為空；
- `client_payload` 保持 opaque，framework 與 generic transport 不解析；
- 完整 encoded `BroadcastRoomCommand` 不得超過既有 `DefaultMaxPayloadBytes`（目前 1 MiB）；
- helper 在 enqueue 前 marshal 完整 command；超過上限整次拒絕，不自動拆分，避免一次業務批次部分接受；
- Gate handler 解析 `room_id` 與 message 邊界，依序把每筆 `client_command_id + client_payload` 交給既有
  room delivery；WebSocket writer contract 不變。

Player 已使用 `PlayerDeliveryCommandID + SendPlayersCommand{repeated PlayerDelivery}`，不需修改。未來
Gate → Gate kick 若採相同鏈路，應定義自己的 command ID 與 repeated protobuf；本文件不預先實作。

### 6.3 Admission errors

在既有 error 集合增加：

```go
var (
    ErrQueueFull        = errors.New("server send: async queue is full")
    ErrEnqueueTooLarge  = errors.New("server send: enqueue exceeds queue capacity")
    ErrSenderNotRunning = errors.New("server send: async sender is not running")
)
```

- 輸入本身超過 queue messages／bytes capacity：`ErrEnqueueTooLarge`，等待也不會成功；
- queue 目前剩餘容量不足：`ErrQueueFull`；
- Start 前、Stop 開始後或 Stop 完成後：`ErrSenderNotRunning`；
- caller context 在 admission 前已取消：直接回傳 context error；
- validation／trace 建立失敗：不 enqueue，直接回傳原錯誤。

Player 的一個 API call 必須全部接受或全部拒絕；不使用 non-zero Receipt 表達 partial enqueue。

### 6.4 Queue job

概念模型：

```go
type playerJob struct {
    acceptedAt time.Time
    trace      context.Context // 只含 logging RequestContext
    messages   []PlayerMessage // 已 defensive clone
    bytes      int
}

type broadcastJob struct {
    acceptedAt time.Time
    trace      context.Context
    message    Message
    bytes      int
}
```

`bytes` 是 deterministic queue accounting，不宣稱等於 Go heap size：

- Player 使用既有 `playerDeliveryWireSize` 加總；
- Broadcast 使用 command ID、payload 與既有 Redis envelope 的可計算 encoded size；
- Player count 計算 `PlayerMessage`；Broadcast count 計算 outer command，不解析內層業務 messages；
- count 同時限制零 payload 或極小 payload 的大量排隊。

Queue 內資料與 caller buffer 不共享 backing array。

## 7. Bounded queue 與 Player immediate drain

### 7.1 Admission

共用 internal queue 使用 buffered channel 傳遞 immutable jobs，並以短 critical section 維護 lifecycle、
queued messages 與 queued bytes。Mutex 只保護 admission accounting／close，不等待 queue 釋出容量。

Player admission：

1. validate 全部 messages 與每項 1 MiB invariant；
2. 計算整個 call 的 messages／bytes，若輸入自身超過 capacity 立即拒絕；
3. 在 admission lock 外 defensive clone，避免複製大量 payload 時阻塞其他 producers；
4. 進入短 critical section後重新檢查 caller context、sender running state與目前剩餘容量；
5. 建立一個 job，更新 counters後enqueue；若此時容量不足，釋放clone並立即拒絕；
6. 返回 `newReceipt()`。

Channel slots 的 capacity 可設為 `queue_capacity_messages`。每個 job 至少一個 message，因此 job 數不會
超過 message capacity；實際 backpressure 仍以 queued message／byte counters 判斷，而不是把一個
1000-player job 當成一個 message。

### 7.2 Player drain algorithm

Worker 每輪：

1. 若有 `pending`，先取 pending；否則 blocking receive 第一個 job；
2. 從第一個 job 取出不超過 count／bytes 上限的最大保序 prefix；若 job 尚有 messages，remainder
   保留為 pending並立即送出該 prefix；
3. 反覆以 `select { case job := <-queue: ...; default: ... }` non-blocking 取得當下資料；
4. 下一個完整 job 會超過上限時，只取可容納的保序 prefix並把 remainder保存為pending；Player trace
   不相同時則不拆該job，整個保存為pending；
5. 當下 channel 無資料時立即結束本輪；
6. 立即執行 Redis／gRPC，不呼叫 `len(channel)`、`Gosched`、timer 或 sleep；
7. 處理結束後開始下一輪。

`pending` 仍算 queued，直到下一輪真正移入 active batch。Queue metrics 不可因 item 已從 channel receive
但仍未處理而提早下降。

Normalization 已保證單一合法 message 不會大於 `batch_max_bytes`，所以 worker 每輪至少能取得一筆；
不會因第一筆永遠放不下而形成 livelock。

Batch count／bytes 上限保證 producer 持續 enqueue 時，Player worker 仍會定期送出，不會永遠 drain。

Broadcast worker 不執行此 drain／coalescing：每次從 queue 取得一個已完成的 outer command，立即委派既有
sender。Business batching 已由 `BroadcastRoomCommand.messages` 完成，generic worker 不得解析 payload。

### 7.3 為何不用 `runtime.Gosched`

grpc-go HTTP/2 writer 會在特定小 buffer 情況讓出一次 scheduler，嘗試讓其他 goroutine 補資料後再
flush。那是刻意增加一次 batching opportunity；不是 channel empty 或 drain completion contract。
本需求明確不等待湊批，因此應只依 non-blocking receive 的當下結果決定本輪邊界。

Shutdown drain 也不 polling／`Gosched`；使用 close、worker `done` 與 Stop context deadline。

## 8. Player worker

### 8.1 合併規則

- 同一 `SendToPlayers` job 內的 messages 保持順序；
- 只有 canonical traceparent 相同的相鄰 jobs 可以 flatten 成同一次 `BatchPlayerSender.SendToPlayers`；
- 不同 trace 的下一個 job 保存為 pending，上一批立即送出；
- 同一 trace 的合併仍受 batch count／bytes 上限；
- 不跨過不同 trace job 尋找後方可合併工作，避免重排 FIFO。

這保留既有 gRPC metadata 的單一 trace contract，也不需要修改 `SendPlayersCommand` schema。

### 8.2 既有 routing 保持不變

背景 worker 呼叫既有同步 engine；以下不重做：

- unique login names 的 presence pipeline；
- unique Gate IDs 的 endpoint pipeline；
- Redis infrastructure failure 才 all-Gate fallback；
- endpoint grouping；
- encoded `SendPlayersCommand` payload 最大 1 MiB；
- endpoint 間最多 16 個 bounded parallel workers；
- 同 endpoint chunks sequential；
- 每 endpoint cached `grpc.ClientConn`。

第一版只有一個 Player worker，因此不同 queue batches 不會同時對相同 Gate 建立 RPC。建立 per-Gate
persistent workers 只會增加動態 lifecycle、queue 與 ordering complexity；目前沒有數據證明需要。

## 9. Broadcast worker

### 9.1 一個 outer command 一次委派

Broadcast worker 每輪只取得一個 `Message`，立即呼叫既有 `FallbackBroadcastSender.Broadcast`：

1. Redis primary 對該 command 執行一次 `PUBLISH`；
2. `count > 0` 視為 Redis accepted；
3. publish error 或 `count == 0` 時，對同一 command 執行既有 all-Gate gRPC fan-out；
4. 完成後立即取得下一個 queue item。

沒有 batch wait、`batch_max_*` 或 transport-level payload merge。Room 的多訊息能力已由
`BroadcastRoomCommand.messages` 完成；其他情境仍用自己的 command ID 與 protobuf。

### 9.2 第一版不使用 Redis pipeline

目前效能資料的 outer Broadcast rate 約為 30／62 commands/s，Redis sender 平均約 0.6ms，尚未顯示
Redis RTT 是 bottleneck。`BroadcastRoomCommand.messages` 已把同一 room、同一業務 tick 的 client messages
合成一次 `PUBLISH`；只有一個 outer command 時，pipeline 仍是一個 RTT，沒有收益。

因此本次不修改 `RedisBroadcastSender`，也不增加 pipeline result／fallback orchestration。只有未來 metrics
同時顯示 outer queue 持續累積、Redis publish latency 上升，且 worker 每次可立即取得多個 outer commands，
才另案評估不等待的 opportunistic pipeline。

### 9.3 Result 語意

Async caller 已在 admission 返回。Worker 對每個 outer command 仍分類：

- Redis accepted；
- Redis failed但 gRPC fallback accepted；
- partial gRPC fallback；
- final error。

這些結果只更新 metrics／log，不重新呼叫 caller。Redis outcome ambiguous 造成的 duplicate risk維持既有
best-effort contract。

## 10. Config

### 10.1 共用型別

```go
type AsyncQueueConfig struct {
    QueueCapacityMessages int `config:"queue_capacity_messages" yaml:"queue_capacity_messages"`
    QueueCapacityBytes    int `config:"queue_capacity_bytes" yaml:"queue_capacity_bytes"`
}

type AsyncPlayerConfig struct {
    AsyncQueueConfig
    BatchMaxMessages      int `config:"batch_max_messages" yaml:"batch_max_messages"`
    BatchMaxBytes         int `config:"batch_max_bytes" yaml:"batch_max_bytes"`
}
```

0 使用 default；負值拒絕。第一版 defaults：

| 參數 | Default | 理由 |
|---|---:|---|
| `queue_capacity_messages` | 10,000 | 對應已知單節點約 10k players，允許一次 node-wide enqueue |
| `queue_capacity_bytes` | 64 MiB | 同時限制大 payload 的實際 backlog，不讓 message count 成為唯一記憶體界線 |
| `batch_max_messages` | 10,000 | 僅 Player；一輪最多處理一個預估 node-wide player set |
| `batch_max_bytes` | 8 MiB | 僅 Player；限制單輪處理量，transport 仍切成每個最大 1 MiB command |

Player normalization 必須保證四個值皆為正值、batch 上限不大於 queue capacity；Broadcast 只驗證兩個
queue capacity 值。兩者另須保證：

- Player 的 `batch_max_bytes >= DefaultMaxPayloadBytes`，確保任一合法單項可被處理；
- capacity 與 Player batch count 必須有合理的 `int` overflow 檢查。

這些是安全／容量邊界，不包含 worker count、flush interval 或 retry 設定。

### 10.2 Game example

```yaml
server_send:
  player:
    # 以下皆非必要；0 或省略使用 default。
    queue_capacity_messages: 10000
    queue_capacity_bytes: 67108864
    batch_max_messages: 10000
    batch_max_bytes: 8388608
  broadcast:
    primary: redis
    queue_capacity_messages: 10000
    queue_capacity_bytes: 67108864
```

Player section 省略時，只要既有 `grpc.clients.gate` 啟用，使用 defaults 提供 async `PlayerSender`。
Broadcast 是否提供仍沿用目前 `server_send.broadcast` enablement；不能因 async config 隱式啟動原本未啟用
的 Broadcast transport。

### 10.3 Gate example

Gate 只接受 `server_send.broadcast` 的 queue capacity fields，不接受 `server_send.player` 或 Broadcast
`batch_max_*`。Game／Gate 都採
strict binding，unknown key 在 startup 失敗。Shutdown timeout 沿用 framework 傳入 `Stop(ctx)` 的 deadline，
不增加重複設定。

## 11. Lifecycle

Async sender 是 `framework.ManagedResource`，註冊於 `PhaseService`：

- Infrastructure Redis／gRPC transport 先 Start；
- async worker 再 Start；
- Ingress 最後 Start，因此 ready 前 worker 已可接受工作；
- shutdown 先停止 ingress，再停止 async worker，最後關閉 Redis／gRPC transport。

### 11.1 Start

- one-shot；重複 Start 回 error；
- 建立 worker-owned cancellable context；
- 啟動唯一 worker並將 state 設為 running；
- 不預先 dial、不執行 Redis I/O。

### 11.2 Stop

1. 在 admission lock 內把 state 改為 stopping並 close ingress channel；
2. 新呼叫立即取得 `ErrSenderNotRunning`；
3. worker 繼續處理 pending 與 channel 內已接受工作；
4. 等待 worker `done`；
5. `Stop(ctx)` deadline 到期時 cancel worker的 in-flight Redis／gRPC；
6. 對未處理 messages 增加 discarded metric並返回 context error；
7. 已停止後再次 Stop 為 no-op。

Transport error不終止 worker；記錄 bounded result 後繼續下一批。Queue 不持久化，process crash無法執行
discard accounting，這是已知限制而非 retry trigger。

## 12. Metrics 與操作方式

Metrics 只回答三件事：是否已影響業務、是否即將飽和、成本位於 queue／Redis／gRPC 哪一段。
所有 labels 都是固定集合。

### 12.1 必要 metrics

以下 family 在 Game 使用 `gaming_core_game_server_send_` prefix。Gate outbound Broadcast 使用
`gaming_core_gate_server_send_outbound_` prefix，避免與既有 Gate inbound
`gaming_core_gate_server_send_requests_total` 混淆。

| suffix | Type／labels | 用途 |
|---|---|---|
| `queue_messages` | Gauge `{operation}` | Player messages 或 Broadcast outer commands 的待處理量 |
| `queue_bytes` | Gauge `{operation}` | deterministic queued bytes |
| `queue_capacity_messages` | Gauge `{operation}` | 供 utilization query 使用的設定上限 |
| `queue_capacity_bytes` | Gauge `{operation}` | 供 bytes utilization query 使用的設定上限 |
| `queue_rejected_total` | Counter `{operation,reason}` | `full`、`too_large`、`not_running`；任何增加都可採取行動 |
| `queue_wait_duration_seconds` | Histogram `{operation}` | admission 到 worker 開始處理的時間；判斷 backlog 是否影響 latency |
| `worker_duration_seconds` | Histogram `{operation,result}` | 一輪背景處理總時間；`success`、`partial`、`error` |
| `player_batch_messages` | Histogram | Player 實際 coalescing 程度與 count limit 壓力 |
| `player_batch_bytes` | Histogram | Player 實際 bytes limit 壓力 |
| `dependency_duration_seconds` | Histogram `{operation,dependency,result}` | bounded dependency：`redis_presence`、`redis_endpoint`、`redis_publish`、`grpc` |
| `fallback_total` | Counter `{operation,reason}` | `redis_error` 或 `no_subscriber`；偵測 primary degradation |
| `discarded_total` | Counter `{operation,reason}` | 第一版只有 `shutdown_timeout` |

`operation` 只允許 `player`／`broadcast`。不加入 endpoint、Gate ID、player、room、command ID、error text
或 payload-derived label。

Broadcast queue 與 worker metrics 計算 outer commands，不解析 `BroadcastRoomCommand.messages`。內層實際
client message throughput 由 example workload 與既有 Gate delivery metrics 對照，不在 framework 新增
payload-derived metrics。

既有 `gaming_core_game_server_send_requests_total` 與 `...duration_seconds` 更新 help／文件，對 Player／
Broadcast 表示 admission result／duration；`request_player` 仍表示 unary reply slot acceptance。背景真正
完成狀況以上述 worker／dependency metrics 為準。Async Player／Broadcast 不再產生 `partial` admission。

### 12.2 判讀與處理

| 觀測 | 判斷 | 處理 |
|---|---|---|
| `queue_rejected_total` 增加 | 已丟回業務錯誤 | 立即檢查 dependency latency、發送率與容量；不可只放大 queue |
| `discarded_total` 增加 | shutdown 未在 deadline 內排空 | 立即檢查 shutdown timeout與卡住的 dependency |
| queue messages／bytes 長時間上升 | producer rate 大於 worker service rate | 比對 dependency duration；降低 rate、擴展 product或處理慢 dependency |
| utilization 持續接近上限且 queue wait 上升 | 即將 queue full | 準備擴容或在數據支持下調整 queue／batch |
| Player batch messages長期碰到上限 | Player worker每輪一直拿滿 | 配合queue wait判斷是否調batch或擴展，不單獨視為故障 |
| `redis_publish` 上升、fallback增加 | Redis Pub/Sub primary degraded | 檢查 Redis latency、pool與subscriber lifecycle |
| Player Redis stage 上升但 gRPC穩定 | routing 成本 | 檢查 Redis、key數與pipeline batch |
| gRPC stage上升且 queue wait累積 | Gate transport／handler path 壓力 | 檢查 Gate ingress、HTTP/2與Gate delivery metrics |
| worker error／partial 增加但queue低 | downstream failure而非queue capacity | 依 dependency/result 處理，不擴 queue |

建議初始操作門檻：任一 rejected／discarded 立即處理；message 或 bytes utilization 超過 80% 且持續一個
業務觀測窗口時準備處理。Queue wait 的可接受值必須由產品 delivery SLO 決定，不在 framework 寫死。

不新增 queue length per Gate、worker utilization estimation 或自製 saturation score。單 worker是否不足可由
queue wait、worker duration與dependency duration共同判斷。

## 13. Structured log

只記錄 metrics 無法承載的 bounded failure context：

- background final error／partial；
- Redis publish error與gRPC fallback結果；
- shutdown timeout與discard count。

Log 使用 queue job 的 detached trace context；Broadcast 錯誤使用該 outer command 的 trace記錄。
不記錄 payload、token、credential，也不把 login name list寫入 log。正常 batch success不逐批寫 log，避免
高流量 log amplification。

## 14. 錯誤處理

| 情況 | Caller | Worker |
|---|---|---|
| invalid／oversized input | 立即 error，不 enqueue | 不執行 |
| canceled caller context | 立即 context error | 不執行 |
| queue input自身超過capacity | `ErrEnqueueTooLarge` | 不執行 |
| queue剩餘容量不足 | `ErrQueueFull` | 既有工作繼續 |
| sender未running／stopping | `ErrSenderNotRunning` | 依 lifecycle drain |
| Redis Player routing unavailable | caller早已accepted | worker執行既有DNS fallback並記metrics/log |
| Redis Broadcast publish失敗／count=0 | caller早已accepted | 對該outer command執行一次gRPC fallback |
| gRPC／Gate error | caller早已accepted | 不retry；記partial/error後繼續 |
| Stop timeout | 不再接受caller | cancel in-flight、計算可知的discard並返回context error |

Async admission 不捕捉最終 delivery error，因此 product handler若業務上必須同步得知 Gate acceptance，應使用
request-player unary reply或另行定義同步 API；不能誤用 async Player／Broadcast。

## 15. 具體檔案修改

### 15.1 `pkg/serversend`

- `message.go`
  - 更新 `Receipt`／sender interface註解；
  - 增加三個 admission typed errors；
  - 不修改 Message／PlayerMessage public shape。
- 新增 `async_queue.go`
  - package-private bounded queue、state、messages／bytes accounting、atomic admission、close與immediate drain；
  - 不建立 generic scheduler、worker pool或公開 queue API。
- 新增 `async_player.go`
  - `AsyncPlayerSender` managed lifecycle；
  - same-trace coalescing並委派既有 `BatchPlayerSender`。
- 新增 `async_broadcast.go`
  - `AsyncBroadcastSender` managed lifecycle；
  - 一個 queue item 委派一次既有 `BroadcastSender.Broadcast`，不解析 payload、不做 pipeline。
- `redis_broadcast.go`
  - Redis publish／fallback 行為不改；只在既有 dependency 邊界補必要 observer 通知。
- `sender.go`
  - 保留 `BatchPlayerSender`、`FanoutSender` routing與concurrency；
  - 在既有presence、endpoint與gRPC呼叫邊界通知窄型observer；
  - 只增加async processor需要的最小internal adapter，不重構既有演算法。
- `batch_routing.go`
  - 保留既有Redis pipeline演算法，只補presence／endpoint stage duration與bounded result通知；
  - 不新增第二套resolver或回傳profiling專用資料模型。

### 15.2 `products/gameproduct`

- `server_send.go`
  - 擴充 strict config structs與normalization；
  - synchronous engine保留 concrete provider；
  - managed async sender包裝後才提供 public interface。
- `app.go`
  - Player與已啟用Broadcast各註冊一個 `PhaseService` managed resource；
  - 不改 Redis／gRPC infrastructure與ingress順序。
- `metrics.go`
  - 既有 API metrics改為admission語意；
  - 增加本文件列出的 bounded async metrics與observer adapter。
- contract tests
  - 鎖定config defaults／validation、DI只暴露async interface與lifecycle順序。

### 15.3 `products/gateproduct`

- `server_send_runtime.go`／`app.go`
  - Broadcast producer增加async config、managed wrapper與public interface alias；
  - Redis subscriber lifecycle不變。
- `metrics.go`
  - 增加 outbound Broadcast queue／worker metrics；既有 inbound server-send metrics不改名、不改語意。
- contract tests
  - 驗證primary redis／grpc都經async sender，subscriber仍獨立啟停。

### 15.4 `examples/metrics`

- `configs/game.yaml`／`gate.yaml` 列出所有async參數、default與是否必要；
- `internal/protocol/broadcast.proto` 與 generated file：`BroadcastRoomCommand` 改為
  `room_id + repeated BroadcastClientMessage messages`；
- `internal/workflow/game.go`：room helper 接受同一 room 的 `[]serversend.Message`，marshal 一次後呼叫
  `Broadcast` 一次；
- `internal/workflow/gate.go`：依序處理 messages，inner payload 保持 opaque；
- metrics contract補queue／worker family；
- validation script在成功判定中要求：
  - enqueue rejected delta = 0；
  - worker partial/error delta = 0；
  - discarded delta = 0；
  - measured工作最終queue messages／bytes = 0；
  - Gate delivery與client receive仍對齊實際accepted messages。

本設計文件不要求立即重跑壓測；實作階段先完成source、tests與compile，待使用者review後再執行實際測試。

## 16. 測試策略

### 16.1 Queue unit tests

- 空queue時worker只等待第一筆；第一筆後不等待即可形成長度一batch；
- 預先排入N筆時單輪拿到min(N, count limit, bytes limit)；
- 下一筆放不下時成為pending且metrics仍算queued；
- producer持續enqueue時batch仍受上限並實際送出；
- 不使用clock advance即可完成drain，證明沒有flush timer／sleep；
- Player API call all-or-nothing，queue不足不出現partial admission；
- count與bytes任一超限都拒絕；
- caller修改原slice／payload不影響queued job；
- concurrent producers、Stop與worker drain以`go test -race`驗證。

### 16.2 Player tests

- blocked fake delegate不阻塞caller admission；
- 同trace相鄰jobs可合併且保持順序；
- 不同trace不進同一delegate call且各自傳到gRPC metadata；
- 合併後仍沿用presence／endpoint pipelines與1 MiB chunking；
- Redis failure仍只做既有fallback，gRPC failure不fallback；
- final error只進metrics/log，不反向改變已返回Receipt。

### 16.3 Broadcast tests

- blocked fake delegate 不阻塞 caller admission；
- 一個 queue item 只委派一次，沒有 clock advance、Redis `Pipelined` 或 generic batch envelope；
- 不同 API calls 不合併，各自保留 traceparent 與 FIFO；
- Redis success 不 fallback；error／count=0 對同一 outer command fallback 一次；
- `BroadcastRoomCommand` 長度一與多 message 都使用同一 command ID，Gate 依序送出且不解析 inner payload；
- 完整 command 超過 1 MiB 整次拒絕；payload mutation、queue full、Stop race 與 ordering contract。

### 16.4 Lifecycle／metrics tests

- Start前與Stop開始後admission拒絕；重複Start error、重複Stop no-op；
- normal Stop完整drain後queue gauges為0；
- Stop timeout取消blocked dependency並增加正確discard count；
- queue counters與capacity一致，不因pending提早下降；
- queue wait、worker duration、Player batch size／bytes、dependency result、fallback與reject只更新一次；
- label value只來自本文件固定集合。

### 16.5 驗證順序

實作完成後依既定流程分兩階段：

1. 使用者review前：新增測試檔並確認可編譯；不執行live Redis／gRPC或壓測。
2. 使用者確認後：`go test -race ./...`、`go vet ./...`、live Redis contract與相同1000 connections
   33ms／16ms performance campaign。

效能報告必須同時呈現producer ticks、queue wait/depth、worker、Player batch、dependency duration、Gate delivery、
client receive與host/runtime負載。若producer ticks完整但queue未排空，仍判定capacity failure。

## 17. 關鍵設計決策與取捨

### 17.1 保持public API，改為acceptance語意

現有interface已能表達fire-and-forget admission；新增Future或callback會迫使每個caller管理completion，違反
本次非同步目的。代價是transport error不再同步返回，因此metrics/log與文件必須清楚。

### 17.2 Bounded queue，不用goroutine-per-call

每次呼叫啟動goroutine會失去明確memory與concurrency上限，也無法做可靠shutdown。固定queue與單worker
提供可觀測backpressure，符合目前壓測規模。

### 17.3 Immediate batching，不等待湊批

Non-blocking drain只利用已存在的並發工作，不增加低流量latency。`Gosched`或micro-batch timer雖可能提高
batch命中率，但會引入不可預測等待且沒有目前數據支持。

### 17.4 Broadcast 在業務 schema 批次，不在 generic transport 批次

`BroadcastRoomCommand.messages` 能用一次 command 表達同一 room 的多筆 client messages，並完全重用
subscriber、dispatcher 與 generic gRPC API。Framework transport 保持 opaque；其他情境各自擁有 command
ID 與 protobuf，避免 mixed-command envelope 與 payload 解析。

### 17.5 第一版不做 Redis Broadcast pipeline

業務批次已把同一 tick 合成一次 Publish，現有數據也沒有顯示 Redis RTT bottleneck。長度一 pipeline 不會
減少 RTT，提前加入只會增加 batch result／fallback 分支；待 queue 與 dependency metrics 提供證據再評估。

### 17.6 Player不跨trace合併

跨request合併可以增加batch size，但現有gRPC metadata只能帶一個traceparent。限制same-trace合併可保留
既有觀測契約，也避免為效能優化擴張protobuf。

### 17.7 第一版不做per-Gate worker

單Player worker已讓caller非同步，且既有batch engine會跨Gate bounded parallel、同Gate sequential。
Per-Gate worker只有在queue持續累積、dependency metrics顯示單worker orchestration而非Redis／gRPC是限制時
才有依據；提前加入會增加worker回收、per-key queue、ordering與shutdown複雜度。

### 17.8 Queue不能掩蓋capacity failure

Async只把等待移出caller，沒有消除下游成本。因此validation把queue最終排空、queue wait與worker result列為
必要成功條件；不能只以producer不再miss tick宣稱效能改善。

## 18. 已知限制與擴充方向

- Process crash會遺失尚未送出的memory queue；若未來要求durability，需另案選擇message broker與delivery
  semantics，不能在本queue加無界retry。
- 單worker遇到接近transport timeout的慢批次會造成queue head-of-line blocking；先由metrics證明後，再評估
  bounded sharding或per-Gate workers。
- 不同trace的Player jobs不會合併成同一gRPC batch；若實測證明這是主要限制，需另案設計per-item trace
  propagation或明確接受trace linking語意。
- Redis Pub/Sub 不提供 replay；publish outcome ambiguous 時，fallback 仍可能造成 duplicate。
- `BroadcastRoomCommand` 超過 1 MiB 時整次拒絕，不自動拆分；caller 必須建立符合 contract 的業務批次。
- Queue bytes是deterministic payload accounting，不是Go allocator精確heap；heap／GC仍由Go runtime metrics與
  pprof判斷。
- Default capacity是安全起點，不是production capacity承諾；應依queue utilization、wait、payload
  distribution與memory headroom調整。
- 若未來需要多worker，必須先定義同player／同Gate ordering；本次不預留未使用的shard abstraction。

## 19. Self Review

### 19.1 需求覆蓋

| 需求 | 設計結果 |
|---|---|
| Broadcast與SendToPlayers不阻塞caller等Redis／gRPC | bounded async acceptance |
| 當下拿滿或不足就送，不等待湊批 | Player immediate drain；Broadcast one-command immediate worker |
| 不使用Gosched判斷drain | 明確禁止，channel/default與done負責 |
| Room Broadcast支援一個command內多messages | `BroadcastRoomCommand{room_id,repeated messages}` |
| 其他情境仍是command ID +自己的protobuf | 不加generic mixed-command envelope |
| Redis Broadcast pipeline是否必要 | 目前數據不支持，第一版不加入 |
| 多筆Player合併並依endpoint批次 | same-trace coalesce +既有routing/group/chunk |
| 注意單筆／batch大小 | fixed 1 MiB transport limit + count／bytes queue與batch bounds |
| 注意queue大小 | messages與bytes雙重capacity、立即typed reject |
| gRPC單Gate worker評估 | 第一版不加；單Player worker +既有per-endpoint sequential已足夠 |
| metrics可直接對應處理與瓶頸 | reject/discard、queue wait、batch、Redis/gRPC dependency |
| shutdown可控 | ingress先停、queue drain、deadline cancel與discard |
| 保留trace | detached context；Player不跨trace合併；Broadcast每個outer command獨立 |

### 19.2 必要性檢查

- Async queue、容量、拒絕策略與shutdown是改成fire-and-forget後不可缺少的正確性邊界。
- Count與bytes雙重限制是必要的：只限制channel slots無法限制一個`SendToPlayers` job內的玩家與payload
  memory。
- Queue wait、dependency duration、fallback與reject／discard是必要的：沒有它們只會把同步瓶頸藏到
  background，無法上線判斷問題點。
- Same-trace Player batching是維持既有W3C contract所需；它避免新增per-item trace protobuf。
- Room repeated payload 是本情境在單一 command 內表達多訊息的必要 schema；generic transport 不需改造。
- Managed lifecycle是確保ready前worker可用、shutdown先drain再關transport所需。

### 19.3 無超出需求確認

本設計沒有加入 Redis Broadcast pipeline、generic mixed-command batch、per-Gate worker、worker pool config、
flush interval、`Gosched`、adaptive batching、retry、durable queue、ack、dedupe、streaming、新RPC或新的
dispatcher抽象。沒有修改RequestPlayer、WebSocket writer與session ownership。新增config只控制必要的
queue／Player batch安全邊界；新增metrics都有明確的立即處理、容量準備或瓶頸定位用途。

Review後結論：本設計符合目前需求，列出的調整都是將既有同步Broadcast／Player sender改為可維運的
bounded async sender所需的最小完整集合，沒有為未確認的未來需求預建額外架構。
