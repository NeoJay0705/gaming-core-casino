# Observability Runtime Diagnostics 設計

## 1. 結論

本次只做兩項必要補強：

1. 在每個 App-local Prometheus registry 精確啟用 Go runtime
   `/sched/latencies:seconds`，輸出 `go_sched_latencies_seconds` Histogram。
2. 讓 `pkg/observability` 管理一個預設關閉、只允許 loopback address 的獨立 pprof HTTP
   listener，使 `gameproduct`、`gateproduct`、`apiproduct` 與 `gmsproduct` 都能用相同設定在線上按需診斷。

pprof 不加入既有 `/health`、`/ready`、`/metrics` listener，也不加入 authentication、TLS、
Channelz、distributed tracing、continuous profiling、business metrics 或 host metrics。這些都不是完成
本次兩項需求所必要。

現有 `examples/metrics/internal/profilehttp` 已驗證 loopback validation、pprof routes 與 shutdown
行為。實作時將其移到 repository-root `internal/profilehttp` 作為唯一共用 helper；example 與 production
adapter 共同使用，避免複製兩套安全與 lifecycle 邏輯。

## 2. 需求理解與合理假設

### 2.1 目標

- 線上 Prometheus 能持續看見 Go goroutine runnable 後等待排程的 latency 分布。
- metrics 顯示 CPU／latency 異常後，operator 能對單一 App instance 短時間取得 CPU、heap、goroutine
  或 Go execution trace 證據。
- pprof 未設定時不 bind port、不增加可連線的 attack surface，且不影響既有 App 啟停。
- pprof 有設定時必須確實 bind 在 loopback；設定錯誤或初始 bind 失敗不能靜默降級。
- 四個 framework products 透過既有 `observability.Module` 自動取得能力，不在各 product 複製設定或 server。

### 2.2 假設

- production 執行環境允許 operator 以 SSH tunnel、`kubectl port-forward` 或等價方式連到 process
  所在 network namespace 的 loopback port。
- pprof 是 incident diagnostics，不是 readiness dependency，也不是 Prometheus scrape target。
- `pprof_listen_addr` 由可信任的部署設定提供；本次仍在程式內強制 loopback，避免設定疏失直接公開。
- Go runtime metric name 以目前 `client_golang v1.23.2` 的轉換契約為準；不鎖定 runtime 產生的
  Histogram bucket boundaries，避免 Go toolchain 更新造成不必要破壞。

## 3. 現況與必要缺口

### 3.1 Scheduler metrics

`pkg/observability/registry.go` 目前呼叫無 options 的 `collectors.NewGoCollector()`。它會提供
`go_goroutines`、GC、MemStats 與 `go_sched_gomaxprocs_threads`，但 default runtime metric matcher
不包含 `/sched/latencies:seconds`。

因此目前 metrics 可以知道 process CPU、GOMAXPROCS 與 goroutine 數量，不能知道 goroutine runnable
後等候 scheduler 的分布。前次瓶頸診斷只能用短時間 Go trace 取得這項證據。精確啟用單一 scheduler
Histogram 是將此缺口轉成低 cardinality、可長期 scrape 資訊的最小修改。

### 3.2 Production pprof

目前只有 `examples/metrics/internal/profilehttp`，且只由 example `game`、`gate`、`grpcload` 的
`-pprof-addr` flag 直接啟動。由於 Go `internal` import rule，正式 `pkg/observability` 不能引用位於
`examples/metrics/internal` 的 package；四個 products 也沒有共同的 production pprof lifecycle。

直接把 pprof handlers 掛到現有 observability listener 會讓原本可能供 probe／Prometheus 存取的
listener 同時暴露 process internals，違反最小權限。正確邊界是另一個只接受 loopback 的 optional
listener。

## 4. 系統架構與模組邊界

```text
config.SourceSnapshot
        |
        v
pkg/observability.Config
  - listen_addr                 required
  - pprof_listen_addr           optional; empty = disabled
        |
        +-----------------------------+
        |                             |
        v                             v
observability httpServer       observability pprofServer adapter
health/ready/metrics           framework ManagedResource
        |                             |
        |                             v
        |                     internal/profilehttp
        |                     loopback validation + pprof mux
        v                             v
configured address             loopback-only address
```

### 4.1 `pkg/observability`

負責：

- Config schema 與 optional pprof address；
- App-local registry 及精確選取 scheduler runtime metric；
- 把 pprof 包裝成 `framework.ManagedResource`；
- 將 observability HTTP 與 pprof HTTP 兩個 resources 註冊於 `PhaseInfrastructure`；
- 讓既有 readiness hook resolve 兩個 resources，但 readiness status 只表示 App lifecycle readiness，
  不持續探測 pprof。

不負責：

- pprof handler 的內容；
- remote access、authentication、TLS 或 port-forward；
- node/container saturation metrics。

### 4.2 `internal/profilehttp`

由現有 `examples/metrics/internal/profilehttp` 移動而來，作為 repository 內唯一 pprof HTTP helper。
它負責：

- empty address 的 disabled 語意；
- loopback address 驗證及實際 bind 後的二次確認；
- explicit `http.ServeMux` 與標準 `net/http/pprof` handlers；
- listener、serve goroutine 與 graceful/forced shutdown；
- unexpected `Serve` failure 的單筆錯誤紀錄。

它不 import framework，也不知道 product、readiness 或 Prometheus。`pkg/observability` 以小型 adapter
把它接進 framework；standalone `grpcload` 與既有 examples 仍可直接使用 helper。

### 4.3 Products

四個 product 不新增 pprof-specific module、flag 或 lifecycle code。它們已呼叫
`observability.Module`，所以只要設定 `observability.pprof_listen_addr` 即可啟用。

## 5. 核心資料模型與介面

### 5.1 Config

在既有 struct 增加一個欄位：

```go
type Config struct {
    ListenAddr      string `config:"listen_addr" yaml:"listen_addr"`
    PprofListenAddr string `config:"pprof_listen_addr" yaml:"pprof_listen_addr"`
}
```

YAML：

```yaml
observability:
  listen_addr: "0.0.0.0:8081"
  pprof_listen_addr: "" # 預設關閉
```

啟用範例：

```yaml
observability:
  listen_addr: "0.0.0.0:8081"
  pprof_listen_addr: "127.0.0.1:6060"
```

`pprof_listen_addr` 規則：

- trim 後為空：合法且 disabled；
- port 必須存在，允許 `0` 供 tests 使用；
- host 只接受 IPv4/IPv6 loopback literal，或大小寫不敏感的 `localhost`；
- 拒絕空 host、wildcard、非 loopback IP 與其他 hostname；
- `localhost` bind 後仍檢查 `listener.Addr()` 是 loopback，避免只信任名稱。

不增加 `enabled` boolean；address 是否為空已完整表達狀態，兩個控制來源會產生無效組合。

### 5.2 Scheduler collector

`newRegistryOwner` 將 Go collector 改為只額外匹配一個 runtime metric：

```go
collectors.NewGoCollector(
    collectors.WithGoCollectorRuntimeMetrics(
        collectors.GoRuntimeMetricsRule{
            Matcher: regexp.MustCompile(`^/sched/latencies:seconds$`),
        },
    ),
)
```

輸出名稱為 `go_sched_latencies_seconds`，型別為 Histogram。原有 Go/process metrics 不變；不使用
`MetricsScheduler` 或 `MetricsAll`，避免無需求地擴大 time-series schema。

### 5.3 Production adapter

`pkg/observability` 增加 unexported `pprofServer`：

```go
type pprofServer struct {
    cfg Config
    // lifecycle state 與實際 internal/profilehttp.Server
}

func newPprofServer(Config) (*pprofServer, error)
func (*pprofServer) Start(context.Context) error
func (*pprofServer) Stop(context.Context) error
```

adapter 必須是非 nil managed resource，即使 pprof disabled 亦同；disabled 時 `Start`／`Stop` 不 bind
listener。這符合 framework 禁止 managed constructor 回傳 typed nil 的 contract。

不把 `pprofServer` 或其 bound address 放入 product-facing DI API；測試可在同 package 透過 unexported
accessor 取得 ephemeral address。

### 5.4 HTTP routes

獨立 pprof listener 只掛標準 routes：

- `/debug/pprof/`
- `/debug/pprof/cmdline`
- `/debug/pprof/profile`
- `/debug/pprof/symbol`
- `/debug/pprof/trace`

named profiles（例如 `/debug/pprof/goroutine`、`/debug/pprof/heap`）由 pprof index handler 處理。
該 listener 不提供 `/health`、`/ready` 或 `/metrics`。

使用 explicit `http.ServeMux`，不 import pprof for side effects，也不使用 global
`http.DefaultServeMux`。設定 `ReadHeaderTimeout`；不設定會中斷合法長時間 CPU profile 的
`WriteTimeout`。

## 6. Lifecycle 與資料流

### 6.1 啟動

1. `newConfig` strict bind observability config 並完成 address validation。
2. framework resolve observability HTTP 與 pprof managed resources。
3. `PhaseInfrastructure` 先啟動既有 observability listener，再啟動 pprof listener。
4. pprof disabled 時第二步仍建立 non-nil adapter，第四步為 no-op。
5. pprof enabled 時 `Start` 先檢查 context，再同步 `net.Listen`；bind 後再次檢查 context，取消時立即
   關閉 listener，否則才保留 Serve goroutine。
6. 所有 infrastructure/service/ingress 完成後，既有 readiness hook 才設為 ready。

若 operator 明確啟用 pprof 而 address 無效或 port 被占用，App startup 返回 wrapped error；framework
依既有規則 rollback 已啟動的 observability listener。不能靜默關閉 pprof 後繼續，否則部署設定錯誤
只會在 incident 時才被發現。

### 6.2 執行中

Prometheus 定期 scrape `go_sched_latencies_seconds`。pprof listener 不主動做 profiling；只有 operator
發送對應 HTTP request 時才收集 profile。

pprof listener 在成功啟動後若意外停止，只記錄 bounded error，不改變 readiness，也不終止 business
service。pprof 是 diagnostics capability，不是 serving dependency；把 incident tooling failure 變成
traffic outage 沒有合理收益。

### 6.3 停止

- readiness 先切為 not ready；
- framework 依反向 lifecycle 順序停止 pprof 與 observability resources；
- pprof `Stop` 使用 caller context 做 graceful shutdown；deadline/cancellation 時呼叫 `Close` 釋放 listener；
- `Stop` 必須 idempotent，nil context 依現有 package 慣例改用 `context.Background()`；
- stopped resource 不允許再次 `Start`，維持 framework one-shot lifecycle。

## 7. 錯誤處理與安全策略

| 情境 | 行為 |
|---|---|
| pprof address 空白 | disabled；不 bind、不 log error |
| address 格式錯誤／無 port | App construction error |
| wildcard／非 loopback | App construction error |
| bind 後不是 loopback | 關閉 listener 並回傳 startup error |
| port occupied | startup error，framework rollback |
| start context 已取消 | 不 bind，返回 wrapped cancellation |
| runtime Serve unexpected failure | 記錄一次 error；business readiness 不變 |
| graceful shutdown deadline | 強制 close，合併並回傳 shutdown errors |

Loopback restriction 是本次唯一 access-control boundary。pprof 可能揭露 stack、command line、memory
與程式結構，因此不得允許 `0.0.0.0`、`::`、pod IP 或 public hostname。本次不實作 application-level
auth/TLS；若未來必須跨網路直接存取，應另案建立 authenticated admin plane，而不是放寬此 contract。

## 8. 具體檔案修改

### 8.1 必要修改

- `pkg/observability/registry.go`
  - import `regexp`；
  - 只啟用 `/sched/latencies:seconds` runtime metric。
- `pkg/observability/config.go`
  - 增加 `PprofListenAddr`；
  - trim 並驗證 optional loopback address。
- `pkg/observability/pprof.go`（新增）
  - 實作 framework managed adapter 與 disabled/start/stop state。
- `pkg/observability/module.go`
  - 註冊 `observability-pprof` managed resource；
  - 讓 readiness hook constructor resolve pprof resource，確保它加入 lifecycle；
  - readiness 的狀態語意不變。
- `pkg/observability/doc.go`
  - 文件化 scheduler metric、optional pprof config、安全邊界與存取方式。
- `pkg/observability/observability_contract_test.go`
  - 補 scheduler metric、config、pprof lifecycle、route isolation 與 module registration contracts。
- `examples/metrics/internal/profilehttp/server.go`
- `examples/metrics/internal/profilehttp/server_test.go`
  - 移動到 `internal/profilehttp/`，不保留舊 package，避免雙份實作。
- `examples/metrics/game/main.go`
- `examples/metrics/gate/main.go`
- `examples/metrics/grpcload/main.go`
  - 只更新共用 helper 的 import path；保留現有 `-pprof-addr` example/benchmark CLI contract。
- `examples/metrics/README.md`
  - 區分 example flag 與 framework product config；
  - 說明 production 使用 loopback port-forward、一次只抓一種 profile，trace 使用短窗口。
- `examples/metrics/configs/{game,gate,api,gms}.yaml`
  - 加入空白 `pprof_listen_addr`，明示預設關閉；不配置新的 port。

### 8.2 不修改

- `products/*product`：四個 products 已使用 `observability.Module`，不需要個別 wiring。
- 現有 Gate/Game business metrics、Histogram buckets 與 labels。
- `/health`、`/ready`、`/metrics` handler 與 listener contract。
- gRPC client/server options、`MaxConcurrentStreams`、connection pool 與 transport 行為。
- Redis、Database、RocketMQ metrics 或 lifecycle。
- deployment manifests：repository 尚未提供通用部署目標；只文件化必要安全條件。

## 9. 測試策略與完成條件

### 9.1 Unit／contract tests

1. Registry gather 結果包含 `go_sched_latencies_seconds`，且 metric family 是 Histogram；不鎖定 buckets。
2. 未設定或空白 `pprof_listen_addr` 時 config 合法，managed resource Start 後 address 仍為空。
3. 接受 `127.0.0.1:0`、`[::1]:0` 與 `localhost:0`，且 trim 後保存 canonical input。
4. 拒絕 `:6060`、`0.0.0.0:6060`、`[::]:6060`、非 loopback IP、其他 hostname 與缺少 port。
5. enabled server 可存取 pprof index、goroutine 與 symbol；`/metrics`、`/health`、`/ready` 回 404。
6. 實際 bound listener address 必須為 loopback。
7. occupied port、cancelled start 與 startup rollback 都釋放 listener並返回可辨識 error。
8. Stop 正常、重複 Stop、cancelled Stop 與 Start-after-Stop 遵守 one-shot contract。
9. `Module` 恰好增加一個 `observability-pprof` infrastructure managed resource；readiness hook 仍只有一個。
10. pprof disabled 與 enabled 都不改變 readiness 在 startup／running／shutdown 的既有狀態轉換。

不在 unit test 呼叫耗時的 CPU profile 或 trace endpoint；route registration 與短 request 已足以驗證
server contract，實際 profile 由 manual validation 驗證。

### 9.2 Build/test 門檻

```bash
go test ./pkg/observability ./internal/profilehttp ./examples/metrics/...
go test ./products/...
go test ./...
go test -race ./pkg/observability ./internal/profilehttp
go vet ./...
```

本輪實作階段若依要求只先確認可編譯，至少先執行受影響 packages 的 compile-only test；實際測試需在
使用者確認後再執行，且不得改變既有 staged changes。

### 9.3 Manual validation

以任一 framework product 設定 `pprof_listen_addr: "127.0.0.1:6060"` 後啟動：

```bash
curl http://127.0.0.1:6060/debug/pprof/
go tool pprof 'http://127.0.0.1:6060/debug/pprof/profile?seconds=20'
curl -o /tmp/service.trace \
  'http://127.0.0.1:6060/debug/pprof/trace?seconds=5'
go tool trace /tmp/service.trace
```

再確認 observability listener 仍只有 health、ready、metrics，而 pprof listener 的 `/metrics` 為 404。
production 操作一次只對單一 replica 收集一種 profile；CPU profile 建議 15–30 秒，trace 建議 3–5 秒，
結果不作為未開 profiler 的容量數據。

## 10. 關鍵設計決策與取捨

### 10.1 精確選一個 runtime metric

`MetricsScheduler` 會啟用整組 `/sched/*`，`MetricsAll` 更會擴大為全部 runtime metrics。本需求只缺
runnable scheduling latency，因此 exact matcher 最小、schema 穩定且不引入無 action 的 series。

### 10.2 pprof 使用獨立 listener

共用 listener 可以少一個 port，但會把 debug surface 綁到 probe/Prometheus 的 network policy，無法保證
loopback-only。獨立 optional listener 是必要的 security boundary，不是額外網路功能。

### 10.3 空 address 代表 disabled

不增加 `pprof_enabled`，避免 `enabled=true/address=""` 或 `enabled=false/address=...`。單一 optional
address 已能完整表達需求。

### 10.4 共用 helper，不公開 framework 細節

將 helper 放在 repository-root `internal/profilehttp`，可同時供 `pkg/observability` 與 examples 使用，
又不把底層 server API 承諾給外部 module consumers。外部 repo 的正式 contract 仍只是
`observability` config 與 Module。

### 10.5 啟用後 bind 失敗視為 startup failure

pprof 本身不是 serving dependency，但 operator 明確配置後若靜默失效，incident 時才發現不可用。初始
配置／bind error 因此 fail fast；成功啟動後的意外停止只記錄，不讓診斷工具故障中斷 business traffic。

## 11. 已知限制與可擴充方向

- `go_sched_latencies_seconds` 能顯示 process 內 scheduler delay，不能說明 host CPU steal、container
  throttling 或其他 process contention；必須搭配 node/container metrics。
- pprof 是 sampled diagnostics，不能保證捕捉短暫事件；需要長期 fleet-wide profiles 時再評估 continuous
  profiler。
- loopback-only 假設 operator 能進入相同 network namespace；若部署平台不支援 tunnel／port-forward，
  需另案設計 authenticated admin endpoint。
- pprof 可找 function、allocation、mutex 與 goroutine 根因，不能取代跨 process request tracing。
- 本次不加入 Channelz；只有確認線上問題需要 resolver/subchannel/socket state 時才另案評估。

## 12. Self review

### 12.1 需求覆蓋

| 需求 | 設計對應 | 結果 |
|---|---|---|
| 立即補 scheduler latency | exact runtime matcher 與 contract test | 符合 |
| production pprof 預設關閉 | 空 `pprof_listen_addr`、disabled managed resource | 符合 |
| loopback-only | config validation、bind 後二次確認、拒絕 wildcard | 符合 |
| 四個 products 共用 | `observability.Module` 統一 wiring | 符合 |
| lifecycle 正確 | infrastructure managed resource、rollback、idempotent Stop | 符合 |
| 不暴露在 metrics listener | 獨立 mux、listener 與 route isolation tests | 符合 |

### 12.2 必要性 review 後保留

- 單一 scheduler Histogram：現有 metrics 的明確盲點，前次必須靠 trace 才能確認。
- optional config field：沒有它就無法預設關閉並由部署明確啟用。
- 獨立 listener：loopback security boundary 所必需。
- managed adapter：四個 products 一致 lifecycle、startup rollback 與 shutdown 所必需。
- 共用 helper 移動：避免 production 與 example 各維護一套地址驗證和 HTTP server。
- route、address、rollback tests：pprof 的主要風險正是誤暴露與 listener lifecycle，不能只測 happy path。
- README/config samples：operator 若不知道 port-forward、profile window 與預設狀態，功能無法安全使用。

### 12.3 Review 後拒絕的非必要項目

- Authentication、TLS、RBAC middleware：loopback listener 不直接跨網路，加入會建立新的 credential
  lifecycle；若未來允許 remote bind 再設計。
- 把 pprof 掛到 observability listener：破壞 security boundary。
- `enabled` boolean、profile duration config、rate limit config：現有 address 與 pprof request query 已足夠。
- Channelz、OpenTelemetry tracing、continuous profiling agent：是不同診斷問題與部署系統。
- 全部 Go runtime metrics：增加無直接處置用途的 series。
- 自製 gRPC writer、syscall、stream-wait metrics：無穩定 public measurement boundary，pprof／trace 才是
  正確工具。
- 修改四個 product wiring、business metrics、transport 或 readiness 語意：既有 Module 邊界已足夠。
- CPU/trace endpoint 的耗時 unit tests：增加測試時間與 flakiness，沒有額外 contract coverage。

### 12.4 最終範圍判定

Review 後的設計沒有超出兩項需求。Scheduler 修改只增加一個標準 metric；pprof 修改只增加安全啟用、
共用 server 與 framework lifecycle 所必要的 config、adapter、tests 和文件。沒有藉此加入新的觀測系統、
業務指標或 transport 行為，也沒有非必要改寫現有 observability HTTP server。

## 13. 建議實作順序

1. 移動並補強 `internal/profilehttp`，先固定 loopback 與 shutdown contract。
2. 擴充 observability config 與 pprof managed adapter。
3. 更新 Module lifecycle wiring 與 contract tests。
4. 精確啟用 scheduler Histogram 並補 registry test。
5. 更新 examples import、sample configs、package docs 與 README。
6. 依實作階段要求先確認編譯，再由使用者確認後執行完整 tests 與 manual validation。
