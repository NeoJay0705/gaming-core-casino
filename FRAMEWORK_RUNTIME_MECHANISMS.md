# Framework Runtime 機制

## 1. 文件目的與邊界

本文件說明四個 framework product 背後已實作的組裝、啟停、請求路由、server-send、
logging 與 observability 機制，供維護者判斷一個 dependency 為何會啟動、請求經過哪些
邊界，以及錯誤會在哪一層被處理。

Product 開發 API 與部署設定請先閱讀
[`PRODUCT_DEVELOPMENT_GUIDE.md`](PRODUCT_DEVELOPMENT_GUIDE.md)。本文件不保留歷史
設計方案，也不把尚未實作的能力描述成現有行為。

## 2. Application 組裝流程

每個 `xxxproduct.NewApp` 都執行相同的高階流程：

```text
Merged config files（依順序）
          + named source files
          + environment overrides（最後套用）
                         │
                         ▼
              immutable SourceSnapshot
                         │
                         ▼
       product base Module + business Modules
                         │
                         ▼
       Provide / ProvideManaged / AddHook 註冊
                         │
                         ▼
              Configure callbacks 執行
                         │
                         ▼
        解析 Hook graph 與已使用 managed resources
                         │
                         ▼
             驗證、排序後建立 one-shot App
```

Framework 使用 App-local `dig.Container`。沒有 package-global DI container，因此同一個 test
process 可以建立多個彼此隔離的 App。

### 2.1 Registry 四種操作

| API | 發生時間 | 用途 | 是否有 lifecycle |
| --- | --- | --- | --- |
| `Provide` | 註冊 constructor；被依賴時才執行 | 一般 config、service、handler、metrics | 否 |
| `Configure` | 所有 module 註冊完成後、Hook 解析前執行一次 | Dispatcher handler、gRPC service 等 composition | 否 |
| `ProvideManaged` | 註冊 lazy constructor；被 Hook graph 解析後轉成 Hook | Redis、DB、gRPC client/server、async worker | 是 |
| `AddHook` | 註冊產生 `framework.Hook` 的 constructor | 已有物件的顯式啟停或 product-owned lifecycle | 是 |

`ProvideManaged` 的「lazy」有兩層意義：constructor 沒有被解析就不會執行，也不會加入
App hooks；一旦被 Configure 或 Hook dependency graph 解析，就會加入對應 lifecycle phase。

所有 Configure callback 都在 network listener Start 之前完成。這保證 Dispatcher route 與
generated gRPC service 不會在 server 已開始接流量後才註冊。

## 3. Lifecycle

Framework phase 固定如下：

| Phase | 值 | 典型元件 |
| --- | ---: | --- |
| `PhaseInfrastructure` | 100 | Observability HTTP、pprof、Redis、DB、RocketMQ、gRPC clients |
| `PhaseService` | 200 | session renewal、async player sender、async broadcast sender |
| `PhaseIngress` | 300 | WebSocket、product gRPC server、endpoint registration、Redis subscriber |
| `PhaseReadiness` | 400 | 唯一的 readiness hook |

同一 phase 依註冊順序啟動；停止時嚴格反向。其結果是 dependency 與 worker 先啟動，ingress
之後才接收流量；停止時先關 readiness 與 ingress，再停止 worker，最後釋放 infrastructure。

App 必須且只能有一個名稱為 `readiness` 的 `PhaseReadiness` hook。Hook name 必須唯一；
空名稱、未知 phase、重複名稱或沒有 callback 都會在 `NewApp` 階段失敗。

### 3.1 啟動與失敗

- `Start` 依排序後的 Hook 順序同步呼叫 `OnStart`。
- 任一 Start 失敗時，不會繼續啟動後續元件；已成功啟動的元件會反向 rollback。
- rollback 使用移除 cancellation、但保留 context values 的新 context，並受 30 秒預設
  shutdown timeout 限制。
- Start 成功或失敗後，App 都不可再次 Start。

### 3.2 停止

- `Run` 在 Start 前先註冊 `SIGINT`／`SIGTERM`，因此慢速啟動期間的終止訊號不會遺失。
- context 取消或收到 signal 後，以 shutdown timeout 呼叫 Stop。
- Stop 即使遇到錯誤仍繼續停止其他 Hook，最後用 `errors.Join` 回傳所有錯誤。
- Stop 具冪等性；再次呼叫會回傳先前彙整的停止結果。

## 4. Product Runtime Graph

### 4.1 共通 Graph

四個 product 都註冊：

- immutable `config.SourceSnapshot` 與窄型 `config.Snapshot`。
- service identity 固定的 `logging.Factory`。
- App-local Prometheus registry 與 Observability HTTP server。
- lazy Redis、Database、RocketMQ managed providers。
- 唯一 readiness hook。

Logging service identity 由 product code 固定為 `game`、`gate`、`api` 或 `gms`，不接受部署
設定覆寫，避免不同服務的 log 被錯誤歸類。

### 4.2 Game Graph

Game 另外註冊：

- 一個 Dispatcher 與 `gameproduct.GateRequestChannel`。
- product metrics。
- 單一 product-level gRPC server 與 GateRequest generic service。
- 永遠可用、只寫 unary reply slot 的 `RequestPlayerSender`。
- 設定 `grpc.clients.gate` 時，加入 Gate gRPC transport、DNS fan-out directory、Redis
  presence/endpoint resolver 與 async `PlayerSender`。
- 設定 `server_send.broadcast` 時，加入 async `BroadcastSender`；primary 為 Redis 時建立
  Redis publisher，並保留 gRPC fan-out fallback。

### 4.3 Gate Graph

Gate 另外註冊：

- 一個 Dispatcher，供 WebSocket 與 remote command ingress 共用但以 channel 隔離。
- product metrics、SessionRegistry 與 WebSocket server。
- distributed session ownership registry 與 renewal scheduler。
- 單一 product-level gRPC server 與 generic GateDelivery service。
- Gate-to-Game hash-routing gRPC client。
- Gate runtime identity、Redis endpoint lease registration 與 refresh。
- 設定 broadcast 時的 async producer；primary 為 Redis 時另啟動 Pub/Sub subscriber。

Gate 的 ownership 與 endpoint registration 是核心 graph，不是可選 server-send feature；因此
Gate 啟動一定會解析 Redis client。

### 4.4 API 與 GMS Graph

API 與 GMS 沒有內建 business ingress 或 Dispatcher。它們提供共通 graph，等待 product module
明確加入 HTTP/gRPC server、handler 與其 lifecycle。這是目前實作邊界，不代表 framework
已提供通用 API router。

## 5. Config 機制

### 5.1 Merge 與 Snapshot

- `MergedPaths` 依序讀取並 canonicalize；後面的文件覆蓋前面的相同 leaf。
- EnvPrefix 對應的環境變數最後 merge，雙底線切割巢狀 key，key 轉成小寫。
- 同一路徑在一次 bootstrap 中只讀取與解析一次。
- Snapshot 建立後沒有 mutation API，可安全讓多個 goroutine 同時 Bind。
- Product subsystem 使用 typed struct 加 `config.Strict()` 綁定；已解析 section 內的未知 key
  會直接回錯，避免 typo 被靜默忽略。

Named source 不參與 merged tree。它以 logical name 被 `BindSource` 讀取；Game/API 選用
input integrity 時，MD5 取自實際載入 snapshot 的同一份 bytes，並驗證 manifest 與實際
source set 一致。

### 5.2 建構期與啟動期驗證

Config bind 與格式驗證發生在 App composition。建立 infra wrapper 不進行 network I/O；
實際連線驗證在 Infrastructure phase：

- Redis 建立 universal client 後執行 `PING`。
- Database 開啟 GORM/database/sql pool 並執行 ping。
- RocketMQ 啟動 producer。
- gRPC server 在 Start 時 bind listener。

因此 config schema 錯誤會讓 `NewApp` 失敗；DNS、credential、port bind 等 runtime 問題會讓
`Run`/`Start` 失敗並觸發 rollback。

## 6. Dispatcher 與 Command Contract

Dispatcher key 是 `(channel, command ID)`，不是單獨的 command ID：

```text
gate-websocket        + command ID -> Gate WebSocket handler
gate-request          + command ID -> Game handler
gate-remote-command   + command ID -> Gate server-send handler
```

- command ID 必須非 0。
- channel 去除前後空白後必須非空。
- handler 不可為 nil。
- 重複 key 在 composition 時失敗。
- `Dispatch` 找不到 route 時回傳 `handled=false, err=nil`，由各 ingress 決定 protocol policy。
- `RegisteredCommandIDs` 與 `IsRegistered` 也是 metrics bounded command label 的可信來源。

Transport 只傳遞 `command ID + opaque payload`。Framework 不依 command ID 猜 protobuf type；
只有已註冊的業務 handler 解碼自己的 schema。

## 7. Client → Gate → Game Unary 流程

```text
WebSocket binary frame
  -> Gate 建立 root trace
  -> 驗證 16-byte packet header、command ID 與 payload length
  -> gate-websocket Dispatcher
  -> Gate local handler，或依 login name affinity 呼叫 Game Forward
  -> gRPC client 建立 child span並附帶 traceparent
  -> Game gRPC interceptor continue trace
  -> gate-request Dispatcher
  -> Game handler
  -> 可選：RequestPlayerSender 寫入 unary reply slot（最多一次）
  -> handler 返回後組成 ForwardResponse
  -> Gate 驗證 expected login name
  -> enqueue 原 connection 的 bounded WebSocket write queue
  -> 單一 writer 寫出 16-byte header + payload
```

Gate-to-Game client 使用 login name 作為 affinity key。對 `dns:///` target 定時 resolve endpoint，
每個 endpoint 建立設定數量的 HTTP/2 connections，再以穩定 hash 儘量把同一玩家送到同一個
Game endpoint/connection。Scale up/down 後 mapping 可能改變，因此 local cache 仍必須有自己的
過期或版本 contract。

`RequestPlayerSender` 不執行 Redis、DNS 或第二次 Game-to-Gate RPC。它只把一筆 response
寫入目前 request context 的 slot；第二次寫入回 `ErrRequestReplyAlreadySet`，不在有效 request
context 內使用則回 `ErrRequestRouteUnavailable`。Handler 沒有設定 reply 時，原 unary response
合法完成但沒有 client message。

## 8. Server Send

Framework 將 server-originated delivery 分成三個 contract，避免 fallback 改變訊息語意：

| Contract | 目的地 | 執行模型 |
| --- | --- | --- |
| `RequestPlayerSender` | 發出目前 request 的原 connection | 同步寫 unary slot，無 network I/O |
| `PlayerSender` | 一批 login name 的 current owner Gate | 呼叫端先進 bounded async queue，worker routing/batching |
| `BroadcastSender` | 所有 Gate | 呼叫端先進 bounded async queue，worker 走 Redis 或 gRPC fan-out |

### 8.1 Async Queue

- enqueue 前會複製 command 與 payload，caller 返回後可安全重用原 slice。
- queue 同時限制 logical message count 與 deterministic encoded bytes。
- worker 被喚醒後立即處理當下已存在的工作，不增加 batching wait 或 timer。
- Player worker 會 opportunistic drain 多筆並受每批 message count 與 encoded bytes 限制；
  Broadcast worker 每次仍轉交一個完整 business command，不合併或解析不同 payload。
- Stop 先停止接受新工作，再 drain 已接受工作；不合作的 delegate 仍受 stop context 約束。
- queue saturation、in-flight、batch size、dependency latency 與結果都有 bounded metrics。

### 8.2 SendToPlayers

```text
[]PlayerMessage enqueue
  -> worker opportunistic batch
  -> Redis pipeline resolve login -> ownership
  -> Redis pipeline resolve Gate ID -> endpoint
  -> 按 endpoint 分組
  -> 按 1 MiB encoded envelope 上限切批
  -> 同 endpoint 的 batches 依序送；不同 endpoint 可 bounded 並行
  -> Gate generic Forward
  -> gate-remote-command Dispatcher 的 framework player-delivery handler
  -> 只解 routing envelope/login name，不解 client payload
  -> SessionRegistry lookup
  -> WebSocket packet enqueue
```

只有 Redis routing operation 在任何 exact gRPC 開始前失敗時，才改走 DNS all-Gate fan-out；
presence not found、Gate 沒有該玩家、gRPC delivery error 或部分失敗都不觸發 fallback。Fallback
Gate 仍依 login name 過濾，沒有該玩家的 Gate 忽略該 item。

### 8.3 Broadcast

一筆 `BroadcastSender.Broadcast` 仍是一個業務 command：

```text
command ID + protobuf payload
```

同一業務情境要一次攜帶多個 client message，應在該 command 自己的 protobuf payload 使用
`repeated` 欄位，而不是把不同 command ID 混進 transport batch。

Redis primary 流程：

```text
Async queue -> Redis PUBLISH 一個 envelope
            -> 每個 Gate PSubscribe
            -> gate-remote-command Dispatcher
            -> 對應業務 handler
```

Envelope 包含 W3C `traceparent` 與 generic command。Redis Pub/Sub 是 best-effort，subscriber
斷線期間不 replay。Subscriber 斷線後用 bounded exponential backoff 與 jitter 重訂閱。

Redis publish error 或沒有 subscriber 時才由 sender fallback 到 DNS 列出的各 Gate generic
gRPC endpoint。gRPC fan-out 本身失敗不會再次切換 transport。Redis 與 gRPC ingress 最終都進入
相同 `RemoteCommandChannel` Dispatcher，因此 handler 行為一致。

## 9. Gate Session Ownership

Gate `SessionRegistry` 同時管理 process-local session state；Redis presence lease 則是跨 Gate 的
authoritative ownership。登入時的主要流程為：

```text
WebSocket session
  -> Redis atomic Claim(login, gate, connection, epoch, TTL)
  -> 排入 renewal scheduler bucket
  -> 成功後 commit 到 local SessionRegistry
```

新 ownership epoch 可讓舊 Gate 的 renew/release 被辨識為 stale，不會刪除新連線的 lease。
同一 login 的新 session 取代舊 session 時，舊連線會從 local registry detach 並關閉。

Renewal scheduler 使用 `interval / buckets` 得到 tick，把約 10k player 的 renew 分散到多個
bucket。每個 tick 只處理到期 bucket，使用 Redis batch/pipeline renew；失敗採 bounded retry，
超過安全期限的 lease 會反映在 overdue metrics，讓維運可在 ownership 真正過期前處理。

Gate gRPC endpoint 另使用獨立 lease 與 refresh，讓 Game 的 exact player routing 能把 Gate ID
解析成可達 endpoint。這和 player session ownership 是同一 server-send keyspace 下不同資料，
不可混為同一個 timer 或業務 lease。

Endpoint registration 使用實際 bind 後的 listener port；host 依序取 `POD_IP`、明確的
non-wildcard listener host、可解析到非 loopback 位址的 hostname。Wildcard listener 又沒有
可路由 host 時會 fail closed，不會把 loopback 或 unspecified address 發佈給其他服務。

## 10. Logging 與 W3C Trace

Structured logger 使用 `slog.JSONHandler` 輸出 stdout，固定欄位包含：

- `timestamp`、`level`、`message`
- product 固定的 `service`
- constructor 建立 logger 時固定的 `component`
- 每次呼叫指定的 `operation`
- context 有 trace state 時的 `trace_id`、`span_id`
- Error 呼叫且 err 非 nil 時的 `error`

Trace 行為：

- WebSocket 每個 packet 建立新的 root trace。
- inbound unary gRPC 驗證單一 W3C version-00 `traceparent`；缺少、重複或格式錯誤時建立新 root，
  不因 tracing metadata 品質拒絕原本合法的 RPC。
- outbound unary gRPC 建立 child span並注入 canonical `traceparent`。
- Redis broadcast envelope 顯式攜帶 `traceparent`，subscriber 收到後 continue trace。
- async queue 與 WebSocket writer 使用 `logging.Detach` 保留 trace state，但移除 caller 的
  cancellation、deadline 與其他 request graph，避免 request 返回後取消已接受工作。

目前沒有 OpenTelemetry SDK 或 OTLP exporter。保留的 W3C context 與固定 log schema 讓未來可以
替換 tracing backend，但現在不宣稱提供 distributed span export。

## 11. Observability

每個 App 使用自己的 Prometheus registry，不使用 global DefaultRegisterer。Observability HTTP
listener 固定提供：

| Endpoint | 意義 |
| --- | --- |
| `/health` | process 內的 Observability HTTP server 可服務 |
| `/ready` | readiness hook 完成後為 200；啟動中與停止流程為 503 |
| `/metrics` | App-local Prometheus metrics |

Registry 內建 Go runtime、process 與 `go_sched_latencies_seconds`。Gate/Game 另提供 request、
latency、error、in-flight、queue、delivery 與 dependency 等 bounded product metrics。Redis/DB
pool collector 只有對應 lazy client 實際被解析時才註冊，這能避免沒有使用某項 infra 的服務
輸出誤導性零值。

`observability.pprof_listen_addr` 預設空字串，表示停用。啟用時會另開 listener，且只接受
loopback address；pprof 不掛在公開 metrics listener。Production 是否允許存取仍需由部署層
控制 namespace、port-forward 或 host 權限。

## 12. 錯誤邊界與維運判讀

| 階段 | 典型錯誤 | Framework 行為 |
| --- | --- | --- |
| Composition | config typo、duplicate handler/service、缺 dependency | `NewApp` 失敗，不開 listener |
| Infrastructure Start | Redis/DB ping、RocketMQ、client initialization | Start 失敗，反向 rollback |
| Ingress Start | port bind、subscription、endpoint registration | Start 失敗，readiness 不會成功 |
| Request | malformed packet/protobuf、unregistered command、timeout | 依 transport mapping 回錯；必要時關 WebSocket |
| Async enqueue | queue full、single item too large、sender stopping | 立即回明確 error，工作不被部分接受 |
| Async delivery | Redis/gRPC/write failure | metrics/log 記錄；只依明確 contract fallback |
| Shutdown | drain 或 Stop error | 繼續停止其餘元件，最後彙整錯誤 |

維運時不應只看 RPS。先用 application metrics 同時確認 latency、error 與 saturation；若定位到
runtime/transport CPU 或 scheduler 問題，再短時間開啟 loopback pprof，並搭配 host CPU、socket、
context switch 等 host-level 資料。pprof 是按需診斷工具，不取代持續 metrics。

## 13. 不在目前 Contract 內的能力

- API/GMS 通用 business HTTP/gRPC server 或 Dispatcher 預設 wiring。
- OpenTelemetry SDK、collector 或 OTLP span export。
- Redis Pub/Sub message replay 或 exactly-once broadcast。
- `RequestPlayerSender` 的多次 reply 或 streaming response。
- 以 server-send gRPC error 觸發另一種 delivery 語意。
- 自動使 Game local cache 一致的 epoch/version protocol。

新增這些能力前應另行設計；維護者不可從現有 package 名稱或 internal constructor 推定它們
已經受到 framework contract 保證。
