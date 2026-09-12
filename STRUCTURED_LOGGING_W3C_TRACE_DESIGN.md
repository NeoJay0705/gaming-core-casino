# Structured Logging 與 W3C Trace Context 設計

## 1. 結論

本次建立一個最小、App-local 的 logging 與 trace correlation 基礎：

- 使用 Go 標準庫 `log/slog` 產生一行一筆的 JSON structured log；
- production 固定輸出到 stdout，由部署環境既有的外部 log collector 送往 Elasticsearch/Kibana、Loki
  或其他 backend；Application 不直接連接這些 backend，也不寫本機檔案；
- 實作 W3C `traceparent` version `00` 的產生、驗證、延續與傳遞；
- 每個 request／message 在 ingress 沒有合法 `traceparent` 時自動建立新的 root trace；
- trace state 只由 `pkg/logging.RequestContext` 保存，透過一個 private context key 傳遞；
- gRPC client/server 自動 inject／extract `traceparent`；
- Redis 一般 command 只沿用 Go `context.Context` 做本機關聯，Redis Pub/Sub 因為會跨 App，使用一層
  transport envelope 傳遞 `traceparent`；
- 移除 `gatelink` 與 `serversend` 各自維護的自訂 Trace ID，避免同一條請求出現多個 trace identity；
- 四個 framework products 自動提供 logger factory，外部 product module 可透過 DI 取得並建立自己的
  component logger。

本次不導入 OpenTelemetry SDK、OTLP exporter、span exporter 或 tracing backend。公開 API 不暴露內部
ID 儲存方式，未來可在不修改 product handler signature 的前提下，將內部 trace 實作替換為
OpenTelemetry。

## 2. 需求理解與合理假設

### 2.1 已確認需求

1. Log 必須適合 production 維運查詢，而不是人類專用的自由格式文字。
2. 沒有既有 trace 的 request／message 必須自動產生 trace；不能要求每個 business handler 自行建立。
3. Trace 經由 `context.Context` 傳遞，logging package 只存放一個 request-scoped struct。
4. 跨服務呼叫必須延續 W3C trace；本次至少涵蓋 gRPC 與 Redis Pub/Sub。
5. Application 不管理 log file、rotation 或 retention。
6. 初期以 log 中的 `trace_id`／`span_id` 做跨服務 correlation；未來才可能增加完整
   OpenTelemetry tracing。
7. 本次不實作敏感欄位 allowlist、redaction engine 或可設定的遮罩政策。

### 2.2 合理假設

- container／process stdout 已由 Fluent Bit、Vector、Promtail、Filebeat 或等價 collector 收集；collector
  與 backend 的部署設定不屬於此 repository。
- `Kibana` 是 Elasticsearch log 的查詢介面，不是 Application 直接呼叫的 exporter endpoint。
- 一個 framework product process 只有一個固定 service identity：`gate`、`game`、`api` 或 `gms`。
- WebSocket 現有 16-byte packet header 沒有 trace 欄位，因此每個 inbound WebSocket packet 建立一條
  root trace；不修改 client protocol。
- 一般 Redis command protocol 沒有可由另一個 Application extract 的 W3C metadata channel。把
  `traceparent` 塞進 Redis key、hash field 或 value 會污染資料模型，因此不做。
- Redis Pub/Sub payload 是 App-to-App message，允許加入 transport envelope；dispatcher 最終收到的
  contract 仍是原本的 `command_id + opaque protobuf payload`。
- 已確認目前沒有外部 repo 相容性需求，因此可直接移除尚未發布的自訂 trace metadata。

## 3. 目標與非目標

### 3.1 必須完成

- 新增共用 logging package、設定、DI module 與 W3C trace context。
- 四個 products 使用固定 service identity 建立各自的 root logger。
- component 由擁有該程式碼的 module／constructor 固定綁定；operation 由操作入口指定。
- 將 production code 內既有 `log.Printf` 改成 structured log，保留原本的錯誤處理語意。
- gRPC ingress／egress 自動延續或建立 trace。
- Gate WebSocket 每個 packet 自動建立 trace，Gate → Game → Gate reply 沿用同一個 trace ID。
- Redis Pub/Sub publisher／subscriber 延續 trace；缺少或不合法時由 subscriber 建立新 trace。
- async queue 若需要在稍後寫 log，只攜帶 detached logging context，不保留整個 request object graph。
- 提供 product module 可直接使用的 logger API 與 contract tests。

### 3.2 明確不做

- OpenTelemetry SDK、OTLP、Jaeger、Tempo、Zipkin 或其他 trace exporter。
- distributed span 儲存、trace sampling policy、tail sampling 或 continuous profiling。
- Application 直接送 log 到 Elasticsearch、Kibana、Loki 或 vendor API。
- log file、rotation、compression、retention 或 sidecar／DaemonSet 設定。
- `tracestate`、W3C `baggage` 或跨服務任意 key/value metadata。
- HTTP business middleware；目前四個 products 沒有本需求範圍內的 HTTP business ingress。
- Database、RocketMQ 等尚未要求的 transport propagation。
- access log、每個成功 gRPC／Redis request 的 log，或新增 metrics families。
- 敏感欄位 schema、redaction middleware、payload scanner 或 log field allowlist。
- 動態 reload log level、每個 component 獨立 level 或 exporter-specific JSON schema。

## 4. 系統架構與模組邊界

```text
product identity + logging.level
              │
              ▼
       pkg/logging.Factory
       slog JSON → stdout
              │ Component("websocket" / "grpc" / ...)
              ▼
       pkg/logging.Logger
              │ reads one RequestContext from ctx
              ▼
JSON: timestamp, level, message, service, component, operation,
      optional trace_id/span_id, error and bounded event fields

Ingress                                       Egress
────────────────────────────────────────────────────────────────────
WebSocket packet ── NewRoot ────────────────► gRPC client Child+Inject
gRPC metadata ──── ContinueOrNew ───────────► Redis Pub/Sub Child+Envelope
Redis envelope ─── ContinueOrNew ───────────► async Detach when required
```

### 4.1 `pkg/logging`

負責：

- bind／validate `logging.level`；
- 建立 service-scoped JSON logger factory；
- 建立 component-scoped logger；
- 定義唯一的 observability `RequestContext` 與 private context key；
- 使用 `crypto/rand` 產生 W3C-compatible Trace ID／Span ID；
- parse、format、continue root／child `traceparent`；
- 從 context 自動補上 `trace_id` 與 `span_id`；
- 提供 gRPC unary client/server interceptors；
- 提供只保留 logging state 的 async detach helper。

不負責：

- business identity、session、room、command payload 或 authorization；
- Redis wire schema；
- log shipping、storage 或 UI；
- Prometheus、pprof 或 readiness。

`pkg/logging` 不使用 package-global mutable logger，也不呼叫 `slog.SetDefault`。每個 App 擁有自己的
factory，避免同一 test process 建立多個 App 時互相覆蓋 logger。

### 4.2 Products

每個 framework product 在既有 base module 註冊一次 logging module：

| Product | `service` 固定值 |
|---|---|
| `gateproduct` | `gate` |
| `gameproduct` | `game` |
| `apiproduct` | `api` |
| `gmsproduct` | `gms` |

外部 product module 從 DI 取得 `*logging.Factory`，再以自己擁有的穩定 component 名稱建立 logger。
Product 不自行建立 stdout handler，也不自行解析 `traceparent`。

### 4.3 Transport owners

- `pkg/grpcserver`：在共用 product-level gRPC server 最外層安裝 logging server interceptor；所有註冊
  service 因此共用相同行為。
- `pkg/gatelink`、`pkg/serversend.GRPCTransport`：在各自建立的 client channel 安裝同一個 logging
  client interceptor；transport-specific metadata 仍由 transport 自己處理。
- `pkg/infra/redis`：確保所有 wrapper API 保留 caller `ctx`。一般 Redis command 不修改 key/value。
- `pkg/serversend`：只為 Redis Pub/Sub message encode／decode trace envelope。
- `products/gateproduct`：在 WebSocket packet ingress 建立 root trace，並讓 async outbound queue 保留
  detached logging context。

## 5. 核心資料模型與介面

### 5.1 Logging config

新增唯一設定：

```yaml
logging:
  # 非必要：debug、info、warn、error；預設 info。
  level: info
```

不新增 `format`、`output`、file path、backend endpoint 或 per-component level。Production format 固定
JSON、output 固定 stdout，避免無效組合與不同 service 產生不一致 schema。

設定規則：

- `logging` 整段省略時使用 `info`；
- level trim、轉小寫後只接受 `debug`、`info`、`warn`、`error`；
- 未知欄位沿用 `config.Strict()` 拒絕；
- service 不從 YAML 取得，由 product base module 明確傳入，避免 deployment 誤把 Gate log 標成 Game。

### 5.2 Logger factory

預期介面維持窄且可測試：

```go
type Factory struct {
    // unexported immutable slog base logger
}

func Module(r framework.Registry, service string) error
func (f *Factory) Component(name string) (*Logger, error)

type Logger struct {
    // unexported component-bound slog logger
}

func (l *Logger) Debug(ctx context.Context, operation, message string, attrs ...slog.Attr)
func (l *Logger) Info(ctx context.Context, operation, message string, attrs ...slog.Attr)
func (l *Logger) Warn(ctx context.Context, operation, message string, attrs ...slog.Attr)
func (l *Logger) Error(ctx context.Context, operation, message string, err error, attrs ...slog.Attr)
```

必要 contract：

- service 與 component 在建構時 trim 並拒絕空值；
- operation 由 call site 或 interceptor 使用固定、低 cardinality 值；
- `Error` 統一加入字串欄位 `error`；nil error 不輸出 `error`；
- nil context 視為 `context.Background()`，可記 lifecycle log，但沒有 trace fields；
- Logger 不因 stdout write failure 影響 business request，也不遞迴記錄 logging failure。

不直接把 `*slog.Logger` 放入 DI，因為這會讓 product code 輕易漏掉 component、operation 或 trace
correlation。Wrapper 只規範必要欄位，attrs 仍沿用標準 `slog.Attr`，不另造 field type system。

### 5.3 單一 request context

`pkg/logging` 定義唯一的 trace/log correlation state：

```go
type RequestContext struct {
    // unexported traceID [16]byte
    // unexported spanID  [8]byte
    // unexported flags   byte
}

func NewRoot(ctx context.Context) (context.Context, error)
func ContinueOrNew(ctx context.Context, traceparent string) (context.Context, error)
func Child(ctx context.Context) (context.Context, error)
func Detach(ctx context.Context) context.Context
func TraceParentFromContext(ctx context.Context) (string, bool)
func IDsFromContext(ctx context.Context) (traceID, spanID string, ok bool)
```

規則：

- package 只使用一個 private key 將一個 immutable `RequestContext` value 放入 ctx；
- 不把 logger、service、component、operation 或任意 attributes 放進 request context；
- `NewRoot` 明確覆蓋 parent，適合每個獨立 WebSocket packet；
- `ContinueOrNew` 對合法 remote parent 保留 Trace ID 與 flags，但建立新的 local Span ID；沒有或不合法
  時建立 root；不因 malformed trace metadata 拒絕 business request；
- `Child` 保留 Trace ID 與 flags，並建立新 Span ID；ctx 尚無 trace 時直接建立 root context；
- `Detach` 回傳以 `context.Background()` 為 base、只複製 `RequestContext` 的 ctx，避免 async queue
  長時間持有 session、payload 或已取消 deadline；
- `RequestContext` 不提供可變 map，避免 data race、無界資料與 transport-specific coupling。

這裡的「single struct」只約束 logging／trace state。既有 WebSocket session、Gate request source 或
deadline 仍可使用各自的 context value；但它們不得再複製保存 Trace ID。

### 5.4 W3C `traceparent`

第一版只產生 canonical version `00`：

```text
00-{32 lowercase hex trace-id}-{16 lowercase hex parent-id}-{2 lowercase hex flags}
```

必要驗證：

- 必須正好四段且長度正確；
- version 必須為 `00`，`ff` 與 future versions 本次視為 unsupported；
- Trace ID 與 Parent ID 必須是 hex 且不可全為零；
- version `00` flags 只接受兩個 hex 字元；root 預設 `00`，incoming flags 原樣保留；
- ID 使用 `crypto/rand`，entropy failure 回傳 error，不以時間戳或 `math/rand` 降級；
- outbound `parent-id` 使用目前 local Span ID；下游收到後建立自己的 local Span ID。

本階段 Span ID 用於 log correlation 與正確 W3C parent chain，並不代表已有可查詢的 exported span。

### 5.5 JSON log schema

每筆 log 是單行 JSON：

```json
{"timestamp":"2026-09-12T10:15:30.123456789Z","level":"error","message":"forward to Game failed","service":"gate","component":"websocket","operation":"forward_game","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","span_id":"00f067aa0ba902b7","error":"context deadline exceeded","command_id":1003}
```

固定欄位來源：

| 欄位 | 必要性 | 來源 |
|---|---|---|
| `timestamp` | 必有 | logger；UTC RFC3339Nano |
| `level` | 必有 | logging method；lowercase |
| `message` | 必有 | call site 的穩定文字 |
| `service` | 必有 | product base module |
| `component` | 必有 | component owner constructor |
| `operation` | 必有 | call site；gRPC interceptor 使用 full method |
| `trace_id` | request log 才有 | `RequestContext` |
| `span_id` | request log 才有 | `RequestContext` |
| `error` | error log 且 err 非 nil | `Error` method |

其他 event fields 使用 flat、snake_case key。Loki collector 不應把 `trace_id`、`span_id`、session 或
player identity 設為 label；它們是高 cardinality 查詢欄位。Application 不輸出 ANSI color、文字 prefix
或 multi-line message。只有 recovered panic 可額外帶 `stack`，因缺少 stack 會使 production panic 無法
定位；stack 仍作為同一筆 JSON 的字串欄位。

本次不建立全域 field registry 或自動 redaction。Framework 新增的 log 僅帶診斷所需的 bounded fields；
外部 product 對自己加入的 attrs 負責。

## 6. Trace 資料流

### 6.1 WebSocket → Gate → Game → Gate write

1. Gate `dispatchPacket` 對每個 packet 呼叫 `logging.NewRoot`；同一條 WebSocket connection 的不同 packet
   不共用 trace。
2. `WebSocketRequestContext` 繼續保存 session、packet 與 Gate source，但不保存 Trace ID。
3. Gate local dispatcher 與 product handler 沿用該 ctx。
4. Gate → Game gRPC client interceptor 建立 child context，將其 `traceparent` 寫入 gRPC metadata。
5. Game gRPC server interceptor extract remote parent，建立 Game local Span ID，再把 ctx 傳給 dispatcher。
6. request-player unary reply 沿同一個 unary call 回到 Gate，不建立反向 RPC；Gate caller 仍保有原本
   request trace。
7. reply enqueue 時以 `logging.Detach(ctx)` 保存 correlation state；WebSocket writer 的 terminal log 因此
   仍有相同 Trace ID，即使原 handler context 已取消。
8. 任何由 handler 或 room broadcast enqueue WebSocket data 的 API 都必須明確接收 caller `ctx`；queue
   boundary 才呼叫 `logging.Detach`。不得從 connection lifetime context 或 session 共享欄位推測目前
   request 的 trace，避免不同 packet／broadcast 互相污染。

### 6.2 gRPC ingress／egress

Server interceptor：

1. 從 incoming metadata 讀取唯一的 `traceparent`；缺少、重複或不合法皆視為沒有 remote parent。
2. 呼叫 `ContinueOrNew` 後再進入 recovery 與 service handler。
3. gRPC full method 作為 interceptor 自己產生 log 時的 operation。
4. 既有 recovery log 改成 structured error，帶 method、panic value 與 stack；RPC 仍回 `Internal`。

Client interceptor：

1. 呼叫 `Child`；若 caller ctx 沒有 trace，會先建立 root。
2. copy 既有 outgoing metadata，再 `Set("traceparent", value)`，不覆蓋 transport-specific metadata。
3. 以 child ctx 呼叫 invoker，讓下層 log 與 remote service 使用相同 outbound Span ID。
4. 本次不為每次成功或失敗 RPC 自動寫 access log；既有 terminal owner 繼續決定是否記錄錯誤，避免
   高 RPS 下 log flood 與同一錯誤重複記錄。

Interceptor ordering 必須是 trace server interceptor 在 recovery interceptor 外層，使 panic log 能讀到
trace；client 端 trace interceptor 在 transport-specific metadata interceptor 外層，兩者都以 copy/set
方式合併 metadata。

### 6.3 Redis command

一般 `GET`、`HGETALL`、`SET`、lease renewal 與 pipeline：

- 呼叫端必須把原 ctx 傳給 go-redis；不可改用 `context.Background()`；
- Redis server 不會 extract W3C context，因此不改 Redis key/value，也不宣稱已完成 remote trace；
- async lifecycle 本來就沒有 request parent 時可記 service/component/operation，但不製造假的跨 request
  關聯；
- 本次不自動記錄每個 Redis command，也不新增 Redis command latency log。既有 Prometheus pool metrics
  保持不變。

### 6.4 Redis Pub/Sub

現有 Redis broadcast 直接 marshal `gatelink.GateRequest`，沒有 metadata 空間。新增 transport-only
protobuf envelope：

```proto
message RedisBroadcastEnvelope {
  string traceparent = 1;
  gatelink.v1.GateRequest command = 2;
}
```

Publisher：

1. 由 caller ctx 建立 child；
2. envelope 放入 child `traceparent` 與既有 command；
3. marshal 後 Publish；business payload 保持 opaque。

Subscriber：

1. unmarshal envelope；缺少／不合法 `traceparent` 時建立 root；
2. 合法時延續 Trace ID 並建立 subscriber local Span ID；
3. 只把 envelope 中的 `command_id + payload` 交給既有 dispatcher；
4. reconnect／decode／dispatch terminal errors 使用 subscriber component logger。

這是新 tracing 需求對 `GENERIC_BROADCAST_CHAIN_DESIGN.md` 中「Redis payload 只有 GateRequest」限制的
必要取代。它沒有改變 `BroadcastSender`、dispatcher 或 business protobuf API，也不讓 transport 解碼
inner payload。

Redis primary publish 失敗後使用 gRPC fallback 時，fallback client interceptor 從原 caller ctx 建立另一個
child；不得把失敗 Redis attempt 當成 gRPC parent。

## 7. 既有 context 與 metadata 收斂

目前存在兩套非 W3C trace：

- `pkg/gatelink.GateRequestContext.TraceID` + `x-gate-request-trace-id`；
- `pkg/serversend.RequestContext.TraceID` + `x-server-send-trace-id`。

必要修改：

1. 從 `gatelink.GateRequestContext` 移除 `TraceID`，保留 Gate ID／Connection ID source metadata。
2. `gatelink` client 不再處理 trace key，只處理自己的 source metadata；共用 logging interceptor 負責
   `traceparent`。
3. 移除 `pkg/serversend/context.go` 與 `x-server-send-trace-id` 的 inject／extract；GateDelivery service 的
   trace 已由共用 gRPC server interceptor 建立。
4. 更新 WebSocket carrier、direct service tests 與 load examples，讓 trace 與 source identity 分屬各自
   owner，不再互相 fallback 讀取。

不保留舊 header alias 或雙寫，因目前沒有外部 repo；雙寫只會讓 operator 無法判定哪個 ID 才是完整
鏈路的 correlation key。

## 8. Logging 使用與錯誤策略

### 8.1 記錄責任

- 同步 helper／repository／transport 優先回傳 wrapped error，不在每一層重複記錄。
- 無法再把 error 回傳給 owner 的 goroutine、subscription loop、writer loop 與 recovery boundary 才記
  terminal error。
- 本次只把既有 production `log.Printf` 事件改為 structured log；不新增成功 request access log。
- lifecycle start/stop 若正常完成不逐項記錄；unexpected Serve exit、renewal failure、subscriber retry 等
  原本已有的維運事件保留。
- 高頻、預期中的 client disconnect 不升級為 error；沿用既有判斷與 close reason。

### 8.2 Trace 錯誤

| 情境 | 行為 |
|---|---|
| inbound 沒有 `traceparent` | 建立 root，繼續 request |
| malformed／zero／duplicate `traceparent` | 視為沒有 parent，建立 root，不回 client error |
| `crypto/rand` 失敗 | 無法建立可靠 ID；中止該 ingress／egress 並回 wrapped internal error |
| Redis envelope 無 trace | 建立 root，繼續 dispatch |
| Redis envelope 無 command／protobuf 壞掉 | 沿用既有 malformed message 處理並記 terminal error |
| stdout write failure | 不影響 business request；不遞迴 logging |
| nil context | lifecycle log 可輸出；需要 propagation 的 ingress／egress 自動建立 root |

W3C metadata 不合法不屬於 business validation error。拒絕整個 RPC 會讓 observability metadata 變成服務
可用性的必要條件，沒有必要。

## 9. 具體檔案修改

### 9.1 新增

- `pkg/logging/config.go`：optional `logging.level` bind 與 level validation。
- `pkg/logging/logger.go`：JSON stdout handler、Factory、component Logger 與固定 key normalization。
- `pkg/logging/context.go`：單一 `RequestContext`、ID generation、W3C parse／format、child／detach。
- `pkg/logging/grpc.go`：unary server/client interceptors。
- 對應 `*_contract_test.go`：schema、level、context、W3C、gRPC propagation 與 concurrency contracts。

### 9.2 修改

- 四個 `products/*product/app.go`：以固定 service value 註冊 `logging.Module`。
- `pkg/grpcserver/server.go`：注入 component logger、安裝 trace interceptor、結構化 Serve／panic log。
- `pkg/gatelink/client.go`、`metadata.go`：加入共用 client interceptor；移除自訂 Trace ID，只保留 source
  metadata。
- `pkg/serversend/grpc_transport.go`、`receiver.go`：使用共用 client/server trace；移除自訂 trace header。
- `pkg/serversend/serversend.proto` 與 generated files：增加 `RedisBroadcastEnvelope`，不修改 gRPC method。
- `pkg/serversend/redis_broadcast.go`：publisher inject、subscriber extract，並改用 structured logger。
- `pkg/serversend/gate_registry.go`：renewal terminal log 改 structured log。
- `products/gateproduct/websocket.go`：per-packet root trace、structured logs 與 async detached context。
- `products/gateproduct/session_registry.go`：既有 cleanup／ownership logs 改 structured log。
- `pkg/observability/server.go`、`pprof.go` 與 `internal/profilehttp/server.go`：注入 logger 並移除 production
  `log.Printf`。
- example YAML：補 `logging.level` 的必要性、預設值與合法值註解。
- 受 Trace ID API 影響的 contract tests／load examples：改用標準 logging context。

### 9.3 移除

- `pkg/serversend/context.go`。
- `x-gate-request-trace-id`、`x-server-send-trace-id` constants 與相關測試。
- 上述 production packages 對標準 `log` package 的直接依賴。

Examples 自己的 command-line completion log 可留在 example 範圍；framework／product runtime packages
不得再產生非 JSON production log。

## 10. 測試策略

### 10.1 Logging contracts

- 每行可獨立 `json.Unmarshal`；固定 key 名稱與型別正確。
- service、component、operation 來源正確，level filter 符合 config。
- request ctx 有 trace 時自動加入 IDs；lifecycle ctx 沒有 trace 時不輸出空字串欄位。
- error、attrs、panic stack 保持單筆 JSON，不出現文字 prefix 或額外換行 record。
- 不依賴 global default logger；兩個 Factory 並行使用時 service 不互相污染。

### 10.2 W3C contracts

- root Trace ID／Span ID 長度、hex、non-zero 與 canonical formatting。
- valid version-00 remote parent 保留 Trace ID／flags，並建立不同 local Span ID。
- absent、malformed、zero ID、duplicate value 建立新 trace，而非拒絕 request。
- child 保留 Trace ID、更新 Span ID；outbound `traceparent` 使用 child Span ID。
- detach 只保留 logging context，不保留 cancellation/deadline 或其他 context value。
- injectable unexported entropy reader 模擬 `crypto/rand` failure，驗證 error path。

### 10.3 Transport integration

- bufconn gRPC：有／無／malformed metadata、Gate → Game、Game／Gate delivery 都能取得預期 trace。
- gRPC source Gate ID／Connection ID metadata 與 `traceparent` 可同時存在，彼此不覆蓋。
- WebSocket 兩個 packet 產生不同 Trace ID；單一 packet 的 Game handler 與 Gate write log Trace ID 相同。
- WebSocket handler／room broadcast enqueue 的 detached context 保留原 Trace ID，且 request cancellation
  不會取消 writer correlation context。
- Redis Pub/Sub envelope round trip 保留 Trace ID 並更新 subscriber Span ID，command payload byte-for-byte
  不變。
- Redis Pub/Sub 無 trace 時仍 dispatch；invalid command 仍沿用原錯誤策略。
- Redis publish → gRPC fallback 保留同一 Trace ID，但兩個 attempt 使用不同 child Span ID。

### 10.4 Repository verification

實作階段至少執行：

```text
go test -count=1 ./...
go test -race -count=1 ./pkg/logging ./pkg/gatelink ./pkg/grpcserver ./pkg/serversend ./products/gateproduct ./products/gameproduct
go vet ./...
git diff --check
```

## 11. 關鍵決策與取捨

### 11.1 先做 W3C correlation，不做完整 tracing

目前維運入口是 centralized log。直接加入 SDK、collector protocol、sampling 與 exporter lifecycle 會增加
尚未使用的部署與故障面。先維持 opaque internal context API，未來可讓 `NewRoot`／`Child` 改由
OpenTelemetry tracer 建立 span，Logger 再從 OTel SpanContext 讀 IDs；product handler 仍只接受 ctx。

代價是現在沒有完整 span timing、parent graph UI 或 sampling；`span_id` 只能在 log 中查詢。

### 11.2 使用標準庫 `slog`

Go 1.24 已內建 structured logging。`slog` 的 handler、level 與 attrs 足以完成本次需求，不新增 Zap、
Zerolog 或 Logrus dependency。小型 wrapper 用來固定 schema 與 context correlation，不建立完整 logging
framework。

### 11.3 Redis 只在 message transport 放 trace

Redis data command 沒有標準 propagation channel；污染業務資料只會製造 coupling。Pub/Sub 是跨 App
message，因此 envelope 是唯一必要 wire change。這也保留 dispatcher 的 generic command contract。

### 11.4 不自動記錄所有成功操作

Gate/Game 壓測已達高 RPS；逐 request log 會成為新的 I/O 瓶頸與 storage 成本。Prometheus 繼續負責
rate、latency、error、saturation；log 負責 terminal error、狀態轉換與 incident detail。兩者不重複。

## 12. 已知限制與未來擴充

- 只接受／產生 W3C `traceparent` version `00`，不處理 future version 或 `tracestate`。
- 沒有 tracing backend，因此不能只靠 trace UI 查看完整 critical path。
- background work 若 owner 沒有呼叫 `Detach`／`NewRoot`，lifecycle log 不會有 trace，這是正確語意。
- Redis server latency 仍應由 metrics 或外部 Redis monitoring 判斷，不能由 propagation 本身得知。
- HTTP、database、RocketMQ propagation 應在對應 business ingress／message contract 確定後另行加入；本次
  不預先建立未使用的 adapter。
- 未來導入 OpenTelemetry 時，新增 SDK／exporter configuration 與 shutdown lifecycle，並在
  `pkg/logging` 內部橋接 OTel SpanContext；不改 business logger 與 handler signature。

## 13. Self-review

### 13.1 需求符合性

- 有 structured log：固定 JSON schema、stdout、外部 collector friendly。
- 有 W3C trace：合法 parent 延續，缺少或不合法時自動建立。
- 有 ctx 與 single struct：只有 `pkg/logging.RequestContext` 保存 trace，移除兩套舊 Trace ID。
- 有 external propagation：gRPC metadata 自動傳遞；Redis Pub/Sub envelope 傳遞；一般 Redis command 的
  protocol 限制已明確處理。
- 不寫檔案：沒有 file path、rotation 或 application-side backend exporter。
- 可供 framework 與外部 product 使用：四個 products 自動組裝，product module 只需 DI Factory。
- 保留 OTel 彈性：對外只暴露 ctx／logger API，沒有把 custom ID fields 散落在 business code。

### 13.2 必要性檢查

保留的新增抽象只有三個：Factory／Logger、RequestContext、gRPC propagation adapter。它們分別解決
service/component 固定欄位、單一 trace identity、跨服務傳遞，不能再合併而不讓 transport 或 business
code 重複實作。

Redis Pub/Sub envelope 是唯一新增 protobuf，因 Redis message 沒有 metadata channel；不是為未來功能
預建。`Detach` 只解決現有 WebSocket writer／async subscriber 的 context lifetime，不是 generic job
framework。

### 13.3 過度設計檢查

已排除 exporter、OTel SDK、sampling、access log、HTTP/DB/RocketMQ adapter、file sink、redaction engine、
動態設定、vendor schema 與全域 field registry。沒有新增 listener、background exporter goroutine、metrics
或 business API。

結論：本設計剛好覆蓋目前 logging 與 W3C correlation 需求。實作時若新增上述排除項目，除非另有新
需求，應視為超出範圍。
