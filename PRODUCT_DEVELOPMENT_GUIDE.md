# Product 開發與部署指南

## 1. 文件目的

本文件說明外部 repository 如何以 `gameproduct`、`gateproduct`、`apiproduct` 或
`gmsproduct` 作為 application boundary，加入自己的業務 module、取得 framework
已提供的 dependency，並準備最小部署設定。

本文件只列出目前程式碼已提供的 contract。`products/*` 內未匯出的 constructor
與 config struct 屬於實作細節，不應由 product code 直接依賴。

## 2. 選擇 Product

| Product | 目前內建 ingress | 適合用途 | 內建 Dispatcher |
| --- | --- | --- | --- |
| `gameproduct` | Gate-to-Game generic gRPC `Forward` | 遊戲 command handler、回覆玩家、指定玩家推送、廣播 producer | 是 |
| `gateproduct` | WebSocket、Gate generic gRPC、Redis Pub/Sub subscriber | 登入與連線狀態、Gate command、Game-to-Gate delivery | 是 |
| `apiproduct` | 無業務 ingress | 自行加入 API transport 與 handler 的服務 | 否 |
| `gmsproduct` | 無業務 ingress | 自行加入 GMS transport 與 handler 的服務 | 否 |

`apiproduct` 與 `gmsproduct` 目前只建立共用 config、logging、observability 與
lazy infrastructure graph。若業務需要 Dispatcher，必須由 product module 明確呼叫
`dispatcher.Module`，並自行建立使用它的 ingress；文件不假設兩者已經具備 HTTP 或
gRPC business server。

## 3. 最小啟動程式

以下以 Game 為例。其他 product 使用相同流程，只需替換 import、`AppOptions` 與
`NewApp`：

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/products/gameproduct"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app, err := gameproduct.NewApp(ctx, gameproduct.AppOptions{
		Config: config.ConfigInputs{
			MergedPaths: []string{"config/infra.yaml", "config/game.yaml"},
		},
		EnvPrefix: "MY_GAME__",
	}, BusinessModule())
	if err != nil {
		log.Fatal(err)
	}
	if err := app.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
```

`Run` 會啟動 application、等待 context 取消或 `SIGINT`／`SIGTERM`，再以 framework
預設的 30 秒 timeout 反向停止已啟動元件。Product app 是 one-shot，停止或啟動失敗後
不可再次啟動。

### 3.1 ConfigInputs

- `MergedPaths` 至少要有一個檔案，依陣列順序 merge，後面的值覆蓋前面的值。
- product input 支援 JSON 與 YAML；`apiproduct` 額外限制所有輸入必須是 YAML/YML。
- `SourcePaths` 是不參與 merge 的 named documents，供業務以
  `config.SourceSnapshot.BindSource` 讀取。
- `EnvPrefix` 必須是非空字串。環境變數在檔案之後套用，以雙底線表示巢狀路徑，例如
  `MY_GAME__GRPC__SERVER__LISTEN_ADDR=:9090`。
- `gameproduct`、`apiproduct` 可設定 `RequireInputIntegrity`，要求 named source 與
  `file_integrity` manifest 完整吻合。Gate 與 GMS 目前沒有公開這個 option。

## 4. 撰寫業務 Module

Product 的最後一個參數接受任意數量的 `framework.Module`。建議每個業務模組只負責：

1. `Provide` 自己的 config、repository、service、metrics 或 handler。
2. `Configure` 完成 Dispatcher 或 gRPC service 等組裝期註冊。
3. 只有真正需要 background lifecycle 時才使用 `ProvideManaged` 或 `AddHook`。

```go
func BusinessModule() framework.Module {
	return func(r framework.Registry) error {
		if err := r.Provide(newGameHandlers); err != nil {
			return err
		}
		return r.Configure(registerGameHandlers)
	}
}

type gameHandlers struct {
	logger *logging.Logger
	reply  serversend.RequestPlayerSender
}

func newGameHandlers(factory *logging.Factory, reply serversend.RequestPlayerSender) (*gameHandlers, error) {
	logger, err := factory.Component("round")
	if err != nil {
		return nil, err
	}
	return &gameHandlers{logger: logger, reply: reply}, nil
}

func registerGameHandlers(d *dispatcher.Dispatcher, h *gameHandlers) error {
	return d.Register(gameproduct.GateRequestChannel, 1001, h.handleBet)
}
```

`Configure` 在 App 建構時只執行一次，適合註冊 handler；它不是 lifecycle hook。
同一個 `(channel, command ID)` 重複註冊會讓 App 建構失敗，不會覆蓋舊 handler。

## 5. 可注入能力

### 5.1 四個 Product 共通

| 注入型別 | 用途 | 何時實際使用 |
| --- | --- | --- |
| `config.SourceSnapshot` | bind merged config 或 named source | 被 constructor／Configure／Hook 注入時 |
| `config.Snapshot` | 只需要 merged config 的窄介面 | 被注入時 |
| `*logging.Factory` | 建立固定 component 的 structured logger | 被注入時；不需要 lifecycle |
| `prometheus.Registerer` | 註冊 product 自訂 metrics | 被 metrics constructor 注入時 |
| `*redis.Client` | 共用 go-redis client 與 pool | dependency 被已解析 graph 使用後，Infrastructure phase 啟動並 `PING` |
| `redis.KeyPrefix` | 共用 Redis root namespace | 解析它會同時解析 `*redis.Client` |
| `*database.Client` | 共用 GORM/database/sql pool | dependency 被已解析 graph 使用後，Infrastructure phase 開啟並驗證 DB |
| `*rocketmq.Client` | RocketMQ producer | dependency 被已解析 graph 使用後，Infrastructure phase 啟動 producer |

Infra provider 是 lazy 的。僅僅因為 product base module 註冊了 Redis、Database 與
RocketMQ，不代表啟動時一定連線；只有 `Configure` 或某個 Hook 的 dependency graph
實際解析該 client，它才會加入 lifecycle。Business constructor 應保存 client wrapper，
不要在組裝期呼叫尚未 Start 的 `Client()` 或 `DB()`。

Observability 不是 lazy optional feature。四個 product 都有唯一 readiness hook，它會解析並
啟動 Observability HTTP server，因此 `observability.listen_addr` 永遠是部署必填。

### 5.2 Game Product

| 注入型別 | Contract | 啟用條件 |
| --- | --- | --- |
| `*dispatcher.Dispatcher` | 在 `gameproduct.GateRequestChannel` 註冊 Gate-to-Game command | 永遠提供 |
| `serversend.RequestPlayerSender` | 最多設定一次原始 unary request 的 reply；不查 Redis、不另外呼叫 Gate | 永遠提供 |
| `serversend.PlayerSender` | 接受 `[]serversend.PlayerMessage`，依 Redis ownership/endpoint 分組後送到 Gate | 設定 `grpc.clients.gate` |
| `serversend.BroadcastSender` | 將一個 generic `command ID + protobuf payload` 非同步廣播至所有 Gate | 設定 `server_send.broadcast` 與 `grpc.clients.gate` |
| `*grpcserver.Server` | 在同一 Game listener 註冊額外 generated gRPC service | 永遠提供；必須在 `Configure` 註冊 |

Game handler 必須註冊在：

```go
gameproduct.GateRequestChannel
```

Payload 對 Dispatcher 是 opaque `[]byte`，業務 handler 再依 command ID 解碼對應 protobuf。
未註冊 command 會由 gRPC ingress 回傳 `Unimplemented`。

`RequestPlayerSender` 的 `Receipt` 只代表 reply 已放入目前 unary reply slot。每個 request
最多接受一筆；handler 返回後 framework 才把它放入原始 `ForwardResponse`。沒有呼叫 sender
時，unary response 不含 reply。

`PlayerSender` 與 `BroadcastSender` 的 production implementation 是 bounded async queue。
成功回傳的 `Receipt` 只代表 defensive copy 已入本機 queue，不代表 Gate 或 WebSocket 已收到；
queue 已滿、單筆過大或 sender 正在停止時會立即回錯。

### 5.3 Gate Product

| 注入型別 | Contract | 啟用條件 |
| --- | --- | --- |
| `*dispatcher.Dispatcher` | 註冊 WebSocket 與 remote command handler | 永遠提供 |
| `*gateproduct.SessionRegistry` | 登入、進房、離房、本機推送、房間廣播與 kick | 永遠提供 |
| `*gatelink.Client` | 依 login name affinity 將請求送到 Game | 永遠提供並由 framework 管理 |
| `serversend.BroadcastSender` | Gate 作為 producer 時使用相同 generic broadcast chain | 設定 `server_send.broadcast` 與 `grpc.clients.gate` |
| `*grpcserver.Server` | 共用 Gate listener 註冊額外 generated gRPC service | 永遠提供；必須在 `Configure` 註冊 |

Gate 有兩個 product handler channel：

| Channel | 請求來源 | Handler 責任 |
| --- | --- | --- |
| `gateproduct.WebSocketChannel` | 玩家 WebSocket packet | 登入、進房或 Gate-local command |
| `serversend.RemoteCommandChannel` | Gate generic gRPC 或 Redis Pub/Sub | 廣播、kick 等 server-originated command |

兩個 channel 的 command ID 空間互相隔離。`serversend.PlayerDeliveryCommandID` 已由 framework
保留在 `RemoteCommandChannel`，product 不得覆寫；其他 server-send 業務 command 必須用各自的
protobuf schema 與 command ID 註冊。

WebSocket handler 可透過 `gateproduct.WebSocketRequestContextFrom(ctx)` 取得目前 session、
packet 與已驗證的 Gate request metadata。Framework 會依 handler error 執行既定連線政策，
例如需要登入或需要房間的非法操作會關閉連線。

### 5.4 API 與 GMS Product

目前沒有內建 business Dispatcher、HTTP router 或 gRPC server。Product module 可以使用共同的
config、logger、metrics 與 lazy infra，自行提供 transport lifecycle。例如自訂 HTTP server
可實作 `framework.ManagedResource`，以 `ProvideManaged(..., framework.PhaseIngress, ...)`
註冊，並由 `Configure` 或另一個 Hook dependency 明確解析它，才會加入 lifecycle；不要假設
Observability listener 可承載 business route。

## 6. 常用能力範例

### 6.1 Logger

```go
func newService(factory *logging.Factory) (*Service, error) {
	logger, err := factory.Component("wallet")
	if err != nil {
		return nil, err
	}
	return &Service{logger: logger}, nil
}

func (s *Service) Handle(ctx context.Context) {
	s.logger.Info(ctx, "settle", "settlement completed", slog.String("result", "success"))
}
```

- `service` 由 product 固定為 `game`、`gate`、`api` 或 `gms`。
- `component` 由 `Factory.Component` 設定，必須是穩定模組名稱。
- `operation` 由每次 `Debug`／`Info`／`Warn`／`Error` 呼叫傳入。
- context 帶有 trace state 時，自動輸出 `trace_id` 與 `span_id`。
- Logger 只輸出 JSON 到 stdout；部署端由 collector 傳往 Loki、Kibana 等 backend。

### 6.2 自訂 Metrics

```go
type Metrics struct {
	requests prometheus.Counter
}

func newMetrics(registerer prometheus.Registerer) (*Metrics, error) {
	requests := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wallet_requests_total",
		Help: "Total wallet requests.",
	})
	if err := registerer.Register(requests); err != nil {
		return nil, fmt.Errorf("register wallet_requests_total: %w", err)
	}
	return &Metrics{requests: requests}, nil
}
```

使用注入的 App-local `prometheus.Registerer` 與 `Register`；不要使用 global
`prometheus.DefaultRegisterer` 或會 panic 的 `MustRegister`。Label 必須是 bounded set，
不可使用 player、room、trace ID 等高基數值。

### 6.3 使用 Infra Client

```go
type Repository struct {
	redis *redisinfra.Client
	db    *database.Client
}

func newRepository(redisClient *redisinfra.Client, dbClient *database.Client) *Repository {
	return &Repository{redis: redisClient, db: dbClient}
}
```

只要 `Repository` 最終被 `Configure` 或 Hook 解析，兩個 client 就會在 ingress 之前啟動，
且 Redis／DB pool metrics 會自動註冊。實際 handler 執行時再呼叫 wrapper 的 `Client()` 或
`DB()` 取得已啟動的 SDK client。

### 6.4 三種 Server Send

回覆目前 Gate-to-Game request；同一個 `ctx` 最多呼叫成功一次：

```go
_, err := requestPlayer.SendToRequestPlayer(ctx, serversend.RequestPlayerMessage{
	ExpectedLoginName: "player-1", // 登入前回覆可省略
	Message: serversend.Message{
		CommandID: 2001,
		Payload:   responsePayload,
	},
})
```

推送一批指定玩家；每筆可有自己的 command ID 與 protobuf payload：

```go
_, err := players.SendToPlayers(ctx, []serversend.PlayerMessage{
	{LoginName: "player-1", Message: serversend.Message{CommandID: 3001, Payload: payload1}},
	{LoginName: "player-2", Message: serversend.Message{CommandID: 3001, Payload: payload2}},
})
```

廣播一個業務 command；房間與 repeated client messages 等語意由該 command 的 protobuf
schema 定義：

```go
commandPayload, err := proto.Marshal(command)
if err != nil {
	return err
}
_, err = broadcast.Broadcast(ctx, serversend.Message{
	CommandID: 4001,
	Payload:   commandPayload,
})
```

所有 `Message.CommandID` 必須非 0，單一 logical payload 上限為 1 MiB。Player/Broadcast
成功只表示 async queue 接受；delivery 結果要透過 metrics 與 structured log 觀察。

## 7. 部署 Config

部署時通常把共用 infra 與 product 設定拆成兩個檔案，再透過 `MergedPaths` 合併。Repository
內現有的 deployment examples 如下；本節表格另外補齊實際 config binding 支援的欄位與預設：

- [`configs/examples/infra.yaml`](configs/examples/infra.yaml)
- [`configs/examples/gameproduct.yaml`](configs/examples/gameproduct.yaml)
- [`configs/examples/gateproduct.yaml`](configs/examples/gateproduct.yaml)

### 7.1 所有 Product 必填

```yaml
observability:
  listen_addr: ":8081"
```

固定提供：

- `GET /health`：process HTTP server 可回應時為成功。
- `GET /ready`：全部 lifecycle phase 啟動完成後才成功。
- `GET /metrics`：Prometheus exposition。

可選設定：

```yaml
logging:
  level: info # 預設 info；允許 debug、info、warn、error

observability:
  listen_addr: ":8081"
  pprof_listen_addr: "127.0.0.1:6060" # 預設空字串，即停用；只允許 loopback
```

同一台機器部署多個 process 時，每個 process 的 observability、business、gRPC 與 pprof
listener 都必須使用不同 port。

### 7.2 Game 最小設定

只提供 Gate-to-Game request/reply，不使用 routed player send 或 broadcast 時：

```yaml
observability:
  listen_addr: ":8081"

grpc:
  server:
    listen_addr: ":9090"
```

`grpc.server` 可選 tuning：

| Key | 預設 | 說明 |
| --- | --- | --- |
| `max_concurrent_streams` | `0` | 保留 grpc-go default；正值限制每條 connection |
| `stream_workers` | `0` | 保留 grpc-go default；experimental goroutine reuse，不是 concurrency limit |
| `write_buffer_size_bytes` | `0` | 保留 grpc-go default |

啟用 `PlayerSender` 時必須設定 `grpc.clients.gate`：

```yaml
grpc:
  clients:
    gate:
      timeout: "3s" # 可省略，預設 3s
      fanout:
        target: "dns:///gate-grpc-headless:9091"
        max_endpoints: 256 # 可省略，預設 256
```

同時必須提供 Redis `addr`/`addrs` 與 `key_prefix`。`server_send.player` 整段皆可省略；
省略時使用預設 queue/batch 值。需要調整時才加入：

```yaml
server_send:
  player:
    queue_capacity_messages: 10000
    queue_capacity_bytes: 67108864
    batch_max_messages: 10000
    batch_max_bytes: 8388608
```

可調欄位與預設如下：

| Key | 預設 |
| --- | --- |
| `queue_capacity_messages` | `10000` |
| `queue_capacity_bytes` | `67108864`（64 MiB） |
| `batch_max_messages` | `10000` |
| `batch_max_bytes` | `8388608`（8 MiB） |

啟用 broadcast 時需加 `server_send.broadcast`；它也要求 `grpc.clients.gate.fanout.target`
作為 Redis 失敗時的 gRPC fan-out directory：

```yaml
server_send:
  broadcast:
    primary: redis # 預設 redis；也可明確使用 grpc
    queue_capacity_messages: 10000
    queue_capacity_bytes: 67108864
```

### 7.3 Gate 最小設定

Gate 的 distributed session ownership、Gate endpoint registration、WebSocket、Gate gRPC server
與 Gate-to-Game client 都是核心 graph，因此以下設定皆必須提供：

```yaml
observability:
  listen_addr: ":8081"

redis:
  addr: "redis:6379"
  key_prefix: "core-casino"

websocket:
  client_addr: ":8082"

grpc:
  server:
    listen_addr: ":9091"
  clients:
    game:
      target: "dns:///gameproduct-headless:9090"
  endpoint_registration:
    ttl: "30s"

session_ownership:
  lease_ttl: "5m"
```

Gate 啟動時會把實際 gRPC listener port 與可路由 host 寫入 Redis。若
`grpc.server.listen_addr` 使用 `:9091`、`0.0.0.0:9091` 或 `[::]:9091`，部署環境必須讓
hostname 可解析成非 loopback 位址，或另外設定不經 EnvPrefix 的 `POD_IP` 環境變數。例如
Kubernetes 可由 Downward API 注入 Pod IP；無法取得可路由位址時 Gate 會 fail closed，
readiness 不會成功。

Gate 可選設定與預設：

| Key | 預設 | 說明 |
| --- | --- | --- |
| `websocket.write_chan_size` | `256` | 每條玩家連線的 write queue |
| `websocket.write_timeout_ms` | `5000` | 單一 frame write deadline |
| `grpc.server.max_concurrent_streams` | `0` | 保留 grpc-go default；正值限制每條 connection |
| `grpc.server.stream_workers` | `0` | 保留 grpc-go default；experimental goroutine reuse |
| `grpc.server.write_buffer_size_bytes` | `0` | 保留 grpc-go default |
| `grpc.clients.game.timeout` | `10s` | Gate-to-Game unary timeout |
| `grpc.clients.game.dns_refresh_interval` | `10s` | 僅適用 `dns:///` target |
| `grpc.clients.game.connections_per_host` | `1` | 每個 resolved Game endpoint 的 HTTP/2 connections |
| `grpc.clients.game.write_buffer_size_bytes` | `0` | 保留 grpc-go default |
| `grpc.endpoint_registration.refresh` | `ttl / 3` | 必須小於 TTL |
| `session_ownership.renewal.interval` | `lease_ttl / 3` | 不得大於 `lease_ttl / 2` |
| `session_ownership.renewal.buckets` | `100` | derived tick 不得大於 5 秒 |

Gate 作為 broadcast producer 或 Redis subscriber 時，加上：

```yaml
grpc:
  clients:
    gate:
      timeout: "3s"
      fanout:
        target: "dns:///gate-grpc-headless:9091"
        max_endpoints: 256

server_send:
  broadcast:
    primary: redis
```

若完全不使用 broadcast，可同時省略 `grpc.clients.gate` 與 `server_send.broadcast`。

### 7.4 API 與 GMS 最小設定

目前兩者只要求 Observability：

```yaml
observability:
  listen_addr: ":8081"
```

業務 module 注入哪一個 infra client，部署才需要加入對應 config。
API/GMS 自建 ingress 時也要在 ingress boundary 呼叫 `logging.NewRoot`／
`logging.ContinueOrNew`，或使用 framework 的 gRPC logging interceptor；否則一般 logger 仍可
輸出，但 context 沒有 trace state 時不會憑空產生 `trace_id`。

### 7.5 共用 Infra 設定

| 能力 | 必填 key | 重要選填 key |
| --- | --- | --- |
| Redis single | `redis.addr`、`redis.key_prefix` | ACL、DB、pool size、idle、dial/read/write timeout、connection lifetime |
| Redis Cluster | `redis.addrs`、`redis.key_prefix` | ACL、pool 與 timeout；Cluster 的 DB 必須為 0 |
| Database | `database.dsn` | max open/idle、connection max lifetime/idle time |
| RocketMQ | `rocketmq.endpoint` | namespace、ACL、producer retry times |

`redis.addr` 與 `redis.addrs` 只能擇一。帳密、DSN 與 access key 不應提交到 repository，
應由 deployment secret 轉成相同 EnvPrefix 下的環境變數覆蓋。

## 8. 驗收清單

新增一個 product module 時至少確認：

- constructor 只宣告真正使用的 dependency。
- Dispatcher 使用正確 channel，command ID 非 0 且沒有重複。
- protobuf decoding 留在業務 handler，transport payload 維持 opaque。
- background resource 有明確 Start/Stop，且放在正確 lifecycle phase。
- logger component 與 operation 是 bounded stable values。
- metrics label 不使用 player、room、trace 或任意 command ID。
- async server-send 正確處理 queue rejection，沒有把 Receipt 當成終端送達。
- config 包含被解析 dependency 所需的 section，並通過 strict binding。
- `/ready` 只有全部必要 dependency 啟動成功後才回 200。
