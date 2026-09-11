# GateLink gRPC 根因診斷設計

## 1. 結論

目前的 local/game Echo 對照已把主要額外等待縮小到 Gate 呼叫 `gatelink.Client.Forward` 至收到
`ForwardResponse` 的 unary interval，但這個 interval 同時包含 client-side transport waiting、HTTP/2
request/response、Game gRPC ingress、protobuf encode/decode 與 Game handler，尚不能把根因直接命名為
`MaxConcurrentStreams`、grpc-go bug 或硬體極限。

本次只增加兩種互補的 example-only 診斷能力：

1. Gate、Game 與新增的 direct gRPC load client 可選擇啟動 loopback-only pprof listener，用 CPU、goroutine
   與 trace profile 找出 CPU、scheduler、network 或 transport contention 實際落在哪個 process/function。
2. 新增 direct gRPC Echo load client，在總 concurrency、payload、duration 與 `GOMAXPROCS` 固定時，只改變
   `ClientConn` 數量 `1／2／4`，驗證單一 HTTP/2 transport 是否構成可重現的容量限制。

pprof 顯示「時間花在哪裡」，controlled A/B 顯示「改變單一變因是否改變結果」；必須合併兩者才能形成
根因結論。本次不修改 `pkg/gatelink`、framework product、production gRPC topology、現有 metrics contract、
protobuf 或 `MaxConcurrentStreams`。

本文件是 `ACTIONABLE_METRICS_BOTTLENECK_DIAGNOSIS_DESIGN.md` 第 10.4、11 節所列後續工作的窄幅延伸；
既有 local/game 對照、Histogram buckets 與 unary reply 設計保持不變。

---

## 2. 需求理解與合理假設

### 2.1 要回答的問題

完成後必須能以可重複的測試回答：

- 目前約 `5ms` Gate unary latency 是 Gate client-side CPU／scheduler、單一 grpc-go HTTP/2 transport、
  Game server transport，或 Game application handler 所造成；
- 一個 backend address 下，增加獨立 `ClientConn` 是否提高 aggregate RPS 並降低 latency；
- 在固定 `ClientConn` 下，提高 closed-loop concurrency 是否已到達 capacity knee，以及 bottleneck 是
  client／Game CPU 還是 gRPC transport；
- profile 中的 grpc-go hot path／wait path 是否與 A/B 結果一致；
- profile run 是否因 profiler overhead 而不得當成正式 capacity 數字；
- 何種證據才足以進入 production connection topology 的後續設計。

### 2.2 已確認事實

- 400 個 closed-loop WebSocket connections、30 秒、`GOMAXPROCS=4` 的既有結果約為：local Echo
  `188,890 RPS`，game Echo `65,843 RPS`。
- Game Echo 下，Gate unary average 約 `5.137ms`，Game handler average 約 `1.881µs`；delivery 與
  WebSocket write 也是微秒級。
- `65,843 × 0.005137 ≈ 338`，表示測試期間平均有約 338 筆 request 位於 broad unary interval；這只描述
  concurrency／latency 關係，不代表 338 個 active HTTP/2 streams。
- `pkg/gatelink.Client` 對單一 target 建立一個長生命週期 `grpc.ClientConn`。目前 target 只有一個 resolved
  Game address，因此 steady state 通常只有一個 active HTTP/2 transport；`round_robin` 不等於同 address
  connection pool。
- 專案使用 grpc-go `v1.72.0`。Game server 沒有設定有限的 `grpc.MaxConcurrentStreams`，grpc-go server
  default 是 `math.MaxUint32`；不能把目前平台直接解釋為預設 100 streams。
- 現有 `/health`、`/ready`、`/metrics` mux 沒有掛載 pprof。這是正確的 production default，不應為一次
  診斷直接擴張其公開 contract。
- Echo request-player reply 走原始 unary response，不使用 Redis、GateResolver 或 reverse server-send RPC。

### 2.3 合理假設

- 測試仍在同一台開發機、loopback network、32-byte Echo payload 與 `GOMAXPROCS=4` 執行；結果只代表這個
  controlled environment，不外推為 production network capacity。
- direct gRPC 測試沿用既有 example Echo command、protobuf、Game dispatcher 與 unary reply slot，避免比較
  不同 handler workload。
- benchmark run 與 profile run 分開。正式 RPS／latency 取自未抓 profile 的 run；profile 只用來解釋
  hotspot／waiting，避免 profiler overhead 污染容量比較。
- 同一矩陣至少重複三次，以 median 與 run-to-run variation 判斷；不以單次偶然差異決定 production 修改。
- `in_flight` 只表示 active unary requests；saturation 必須由 concurrency staircase 的 RPS／latency
  knee，並配合 CPU／runtime metrics 與 profile 判斷，不以單一 Gauge 推導。
- host-level CPU、thermal throttling 與 kernel/network 狀態仍由 OS 工具觀察，不新增為 application metrics。

---

## 3. 範圍

### 3.1 本次必要修改

- 新增 example-internal、預設關閉、只允許 loopback address 的 pprof HTTP server。
- Gate 與 Game example main 增加 `-pprof-addr`，只負責啟停上述 server。
- 新增 `examples/metrics/grpcload`，直接呼叫既有 Game `gatelink.Forward`，支援固定總 concurrency 下的
  `1／2／4` 個獨立 `ClientConn`。
- grpcload 提供 bounded RPS、latency、error、in-flight 與 Go/process metrics，並可選擇啟動同一個
  example-internal pprof server。
- 新增對應 unit／contract tests，並更新 `examples/metrics/README.md` 的執行與判讀方式。

### 3.2 明確不修改

- `pkg/gatelink` client/server、public config、protobuf、metadata 或 unary reply contract。
- `products/gateproduct`、`products/gameproduct`、`pkg/observability` 或 framework lifecycle。
- production `ClientConn` pool、resolver、load balancer、retry、keepalive、HTTP/2 flow-control window 或
  `MaxConcurrentStreams`。
- 現有 Gate／Game／WebSocket load metric 名稱、labels、buckets 或計時邊界。
- pprof 掛載至 `/metrics` listener、非 loopback bind、authentication 或 remote profiling policy。
- grpc-go Channelz、`stats.Handler`、OpenTelemetry、distributed tracing 或跨 process timestamp。
- block／mutex sampling 的新 CLI knobs。若 CPU、goroutine 與短 trace 仍無法定位，再另案評估；本次不先增加
  高 overhead 的 runtime global settings。
- 通用 benchmark framework、任意 RPC method／command、failure injection、target RPS、ramp pattern 或
  production performance fix。

---

## 4. 架構與資料流

### 4.1 既有完整路徑：用於 profile 實際整合點

```text
WebSocket load
  → Gate WebSocket reader / dispatcher
  → one gatelink.Client / one ClientConn
  → grpc-go client HTTP/2 transport
  → Game grpc-go server transport
  → gatelink.Server.Forward
  → Game dispatcher / Echo handler / reply slot
  → unary response
  → Gate queue / writer
  → WebSocket load
```

Gate 與 Game 在各自 process 使用獨立 pprof listener。這一輪回答 hot path 是否只在 Gate 的 client transport、
Game 的 server transport、Go runtime，或其他整合層出現。

### 4.2 新增 direct gRPC 路徑：用於單一變因 A/B

```text
grpcload workers（固定總 concurrency）
  → deterministic client selection: workerIndex % clientConnectionCount
  → N 個獨立 gatelink.Client / grpc.ClientConn
  → 同一個 Game endpoint
  → 既有 gatelink.Server.Forward
  → 既有 Game dispatcher / Echo handler / reply slot
  → grpcload 驗證 command 與 Echo payload
```

direct 路徑刻意移除 WebSocket、Gate dispatcher 與 Gate writer，但保留目前被懷疑的 grpc-go client/server
transport、Game ingress、Game handler 與 unary reply。總 workers 固定為 400，只有 `ClientConn` 數量改變，
因此 aggregate RPS／latency 的變化可以檢驗 per-connection transport 假設。

### 4.3 為何兩條診斷路徑都必要

| 證據 | 可以回答 | 單獨不能回答 |
|---|---|---|
| 完整路徑 pprof | 真實 Gate/Game process 的 CPU hot path、goroutine 與 scheduler 狀況 | 增加 connection 是否會改善容量 |
| direct gRPC `1/2/4 ClientConn` | 單一 HTTP/2 transport 是否為可重現限制 | hotspot 位於 client writer、server reader、runtime 或 syscall |
| 兩者一致 | 可形成具體且可驗證的 root-cause statement | production 修正的成本與 topology 仍需另案設計 |

---

## 5. Example-only pprof server

### 5.1 模組邊界

新增 `examples/metrics/internal/profilehttp`，只提供最小 lifecycle：

```go
type Server struct {
	// package-private HTTP lifecycle fields
}

func Start(listenAddr string) (*Server, error)
func (s *Server) Addr() string
func (s *Server) Shutdown(context.Context) error
```

- 空白 address 表示 disabled，`Start` 回傳 `nil, nil`。
- address 必須是 explicit loopback IP 或 `localhost` 且包含 port；拒絕空 host、wildcard、public IP 與任意
  hostname。port `0` 只供 tests 使用。
- 先 `net.Listen` 再啟動 goroutine，bind failure 同步回傳，避免主程式已運行但 profile endpoint 不存在。
- 使用 private `http.NewServeMux()` 手動掛載標準 `net/http/pprof` handlers；禁止使用
  `http.DefaultServeMux`，避免其他 side-effect imports 意外擴張 endpoints。
- 只提供 `/debug/pprof/`、`cmdline`、`profile`、`symbol`、`trace` 與標準 named profiles；不提供
  `/health`、`/ready`、`/metrics`。
- `Shutdown` 使用 bounded context，等待 Serve 結束並合併 shutdown／serve errors；nil receiver 為 no-op。

### 5.2 Example main 整合

`examples/metrics/gate` 與 `examples/metrics/game` 各新增：

```text
-pprof-addr string  optional loopback pprof listen address; empty disables profiling
```

main code 只做必要的小型 `run(ctx, options) error` 整理，使 pprof server 在 app 建立／啟動失敗、signal shutdown
與正常結束時都會清理。pprof bind failure 必須使該 example 啟動失敗；不得靜默忽略或降低 readiness 語意。

這個 flag 不進入 YAML、environment prefix、framework DI 或 product config，因為它只屬於本地診斷工具。

### 5.3 Profile 使用原則

- CPU profile：在 steady-state measurement 中抓 15～20 秒，檢查 CPU-consuming stacks。
- goroutine profile：在 latency／in-flight 穩定後抓一次，檢查大量 goroutine 是否停在相同 transport wait。
- trace：只抓約 5 秒，檢查 runnable delay、network block、GC 與 scheduler；避免長 trace 改變負載與產生過大檔案。
- profile run 不納入 RPS median，不與未 profile 的 baseline 混合。
- 本次不自動開啟 `runtime.SetBlockProfileRate` 或 `SetMutexProfileFraction`；trace 已能先回答 blocking／scheduler
  問題，且全量 block/mutex sampling 可能顯著影響高 RPS workload。

---

## 6. Direct gRPC load client

### 6.1 CLI contract

新增 `examples/metrics/grpcload`，只接受以下 bounded flags：

| Flag | Default | 語意 |
|---|---:|---|
| `-game-target` | `127.0.0.1:19090` | 既有 Game GateLink endpoint |
| `-client-connections` | `1` | 獨立 `gatelink.Client`／`ClientConn` 數量 |
| `-concurrency` | `400` | 全部 connections 共用的 closed-loop workers |
| `-duration` | `30s` | 正式 request admission window |
| `-payload-bytes` | `32` | Echo payload；沿用既有 bounded application payload 上限 |
| `-warmup-requests` | `1` | 每個 ClientConn 在測量前完成的 request 數 |
| `-request-timeout` | `10s` | 每筆 unary request timeout |
| `-metrics-addr` | `127.0.0.1:22082` | grpcload private `/metrics` listener |
| `-pprof-addr` | empty | optional loopback-only profile listener |

一般參數在建立 metrics listener、pprof listener 或 gRPC client 前完成驗證：

- target 與 metrics address 不可為空；
- `client-connections`、`concurrency`、`duration`、`request-timeout` 必須大於零；
- `client-connections` 不可大於 `concurrency`；
- `warmup-requests` 不可小於零；
- payload 必須符合既有 example Echo limit；
- pprof address 由 `profilehttp.Start` 在 bind 前執行 loopback validation；grpcload 先啟動它，後續資源若失敗
  必須立即關閉已建立的 profile server。

不提供任意 command ID 或 method flag；grpcload 固定呼叫 example `EchoRequestCommandID`，避免錯誤輸入擴張
成通用 RPC 工具。

### 6.2 Client 與 request model

啟動時建立恰好 `client-connections` 個 `gatelink.Client`，每個 client 在整輪測試中重用並於結束時停止。
不修改 `gatelink.Client` 以支援 pool；多 connection 只存在於 grpcload process。

每個 worker：

1. 使用固定 index 選擇 `clients[workerIndex % len(clients)]`；hot path 不使用 random 或共享 round-robin lock。
2. 建立穩定且 bounded 的 `ConnectionID`，透過 `gatelink.WithGateRequestContext` 滿足既有 metadata contract；
   不產生高 cardinality Prometheus label。
3. 同時最多只有一筆 unary request in flight。
4. 呼叫 `Forward`，驗證 reply 非 nil、response command ID 正確，並 unmarshal／比較 Echo payload。
5. request admission window 結束後不再發新 request；已送出的最後一筆允許在 `request-timeout` 內取得唯一
   terminal result。

Echo request protobuf 可在啟動時 marshal 一次並由 `gatelink.Client` 按既有 contract copy，避免把重複建立測試
資料算成 transport 成本；response 仍逐筆驗證 correctness。這項選擇在所有 `ClientConn` 組合一致，不影響 A/B。

### 6.3 Warm-up 與測量

- 每個 ClientConn 在正式測量前各完成 `warmup-requests` 次 Echo，確保 lazy dial、HTTP/2 SETTINGS 與 Game
  handler 已熱機。
- warm-up 必須逐 connection 成功；任一失敗即中止，不產生 partial measurement。
- warm-up 不寫入 grpcload request metrics。
- 所有 workers 先到達 ready barrier，再以同一個 start barrier 開始；`measurementStart` 與 `admissionEnd`
  在 barrier 釋放前設定，所有 workers 共用相同 `admissionEnd`，不把 worker 建立時間算入測量窗口。
- worker 遇到第一個非 cancellation error 時停止該 worker並記錄 terminal error；其他 workers 完成自己的
  admission window。結束 log 必須明確輸出 failed workers，任何 failure 都使 run 不具 capacity 比較資格。

### 6.4 grpcload metrics

grpcload 使用獨立 `prometheus.NewRegistry()`，註冊 Go/process collectors 與三個 bounded metrics：

| Metric | Type／labels | 用途 |
|---|---|---|
| `gaming_core_example_grpc_load_round_trips_total` | Counter `{result}` | success／error／cancelled rate |
| `gaming_core_example_grpc_load_round_trip_duration_seconds` | Histogram `{result}` | direct unary E2E latency |
| `gaming_core_example_grpc_load_round_trips_in_flight` | Gauge | client-side active unary calls；不是 saturation ratio |

Histogram 沿用既有 request/E2E buckets。`result` 只允許 `success`、`error`、`cancelled`；不加入
`client_connections`、worker、connection、command 或 target labels。每個 process 只跑一個 connection-count
組合，設定由 completion log 與外部 run metadata 辨識。

metrics listener 只提供 `GET /metrics`，不提供 health、readiness 或 pprof。pprof 使用獨立的 optional listener，
避免將 diagnostics 與 Prometheus scrape lifecycle 混合。

### 6.5 Completion log

測試結束至少輸出：

```text
client_connections
concurrency
warmup_requests
successful_requests
failed_workers
measurement_start
admission_end
measurement_end
measured_duration
completed_rps
```

`completed_rps = successful_requests / measured_duration`，使用實際 measurement end，不假設剛好等於 flag
duration。Prometheus Histogram 提供分布；completion log 不另做 percentile engine。

---

## 7. 錯誤處理與 lifecycle

| 邊界 | 必要行為 |
|---|---|
| pprof address 非 loopback 或格式錯誤 | 啟動前回傳明確錯誤，不建立其他 listener/client |
| pprof bind failure | example 啟動失敗，不假裝 profile 可用 |
| grpcload config 無效 | 建立任何外部資源前失敗 |
| 任一 ClientConn warm-up 失敗 | 關閉全部 clients/listeners，不開始測量 |
| request timeout／gRPC error／reply malformed | 記錄唯一 terminal result；該 worker 停止，run 標記 invalid |
| 上層 signal cancellation | 停止 admission、等待或取消 active request、關閉 clients、metrics 與 pprof server |
| pprof／metrics shutdown timeout | force close listener並回傳 joined error |

資源建立後以反向順序釋放：workers → gRPC clients → metrics observer → pprof server。所有 stop/shutdown 都應
bounded 且 idempotent；不得因 `log.Fatal` 提前 `os.Exit` 而跳過已建立資源的 cleanup。

---

## 8. 具體檔案修改

### 8.1 新增

- `examples/metrics/internal/profilehttp/server.go`
  - loopback validation、private pprof mux、bind-first startup 與 bounded shutdown。
- `examples/metrics/internal/profilehttp/server_test.go`
  - 驗證 disabled、loopback／public address、endpoint、occupied port 與 shutdown。
- `examples/metrics/grpcload/main.go`
  - config validation、N 個 clients、per-connection warm-up、fixed-concurrency measured loops、reply validation
    與 completion log。
- `examples/metrics/grpcload/main_test.go`
  - 使用 in-process `gatelink.Server` 驗證 metadata、Echo reply、client selection、warm-up exclusion、admission
    與 error cleanup；不加入 throughput assertion。
- `examples/metrics/grpcload/metrics.go`
  - private registry、三個 direct gRPC load metrics、Go/process collectors與 `/metrics` lifecycle。
- `examples/metrics/grpcload/metrics_test.go`
  - 驗證 bounded labels、Histogram buckets、in-flight 歸零、runtime/process collectors 與 shutdown。

### 8.2 修改

- `examples/metrics/gate/main.go`
  - 新增 `-pprof-addr`；使用小型 `run` helper確保 pprof lifecycle。
- `examples/metrics/game/main.go`
  - 同 Gate，只增加 example-only pprof lifecycle。
- `examples/metrics/README.md`
  - 加入 profile commands、direct `1／2／4 ClientConn` matrix、必要 PromQL 與判讀限制。

### 8.3 不修改

- `go.mod`／`go.sum`：只使用 Go standard library、現有 grpc-go 與 client_golang。
- `pkg/gatelink/**`、`pkg/observability/**`、`products/**`、既有 proto/generated code 與 YAML configs。
- 既有 WebSocket load、local/game workflow 與已完成的 design documents。

預估實作約新增／修改 9 個程式與文件檔案，主要程式約 450～650 行，包含 lifecycle 與 contract tests；不含
本設計文件。若實作需要修改 production package 或新增 dependency，表示設計假設不成立，必須先停止並說明。

---

## 9. 測試策略

### 9.1 Unit／contract tests

1. profile server 空 address 是 no-op；只接受 `127.0.0.1`、`::1` 或 `localhost`。
2. private mux 可取得 index、goroutine 與 symbol endpoint；unknown route 為 404。
3. bind failure 同步返回；Shutdown 關閉 listener且不洩漏 Serve goroutine。
4. grpcload 在建立 listener/client 前拒絕所有非法設定。
5. worker mapping deterministic 且能覆蓋全部 clients；多 ClientConn warm-up 每條 connection 都完成指定次數。
6. warm-up 不增加 measured counters。
7. measured request 攜帶必要 `ConnectionID`，並驗證 response command、protobuf 與 payload。
8. success／error／cancelled 各有唯一 terminal metric，任何路徑結束後 in-flight 為零。
9. request failure 與 context cancellation 會留下唯一 terminal metric，並在 worker 結束後使 in-flight 歸零。
10. partial startup 的反向 cleanup 由明確的 defer／close helper 保證；Gate/Game 未提供 `-pprof-addr` 時，
    保持原啟動、readiness 與 shutdown 行為。

不在 automated tests 斷言 RPS、p95 或特定 grpc-go function，避免硬體與 scheduler 造成 flaky tests。

### 9.2 Build/test 門檻

```text
go test ./...
go test -race ./examples/metrics/...
go vet ./...
```

### 9.3 實際診斷順序

#### A. 未 profile 的 direct A/B

先固定 `ClientConn=1`，在 Game 與 grpcload `GOMAXPROCS=4`、payload 32 bytes、duration 30 秒下，
依序測試 concurrency `100`、`200`、`400`、`800`；每組至少三個 samples。找出 RPS 增加已落在
run-to-run variation 內、但 p95/p99 latency 仍上升的 capacity knee。

再固定該 knee 的總 concurrency，依交錯順序測試 ClientConn：

```text
1 → 2 → 4 → 4 → 2 → 1 ClientConn
```

補足每組至少三個有效 samples。每輪保存 grpcload/Game 測試前、測試中與收尾 metrics；正式比較使用：

- completed RPS；
- unary E2E average、p50、p95、p99；
- errors、in-flight；
- grpcload/Game CPU delta、RSS、goroutines、GC 與 `GOMAXPROCS`；
- Game handler RPS／latency／in-flight。

`in_flight` 在 closed-loop 穩態接近 concurrency 是預期的 offered load，不是 saturation ratio。CPU
比例可用 `rate(process_cpu_seconds_total[30s]) / go_sched_gomaxprocs_threads` 估算；接近 `1` 才表示
該 process 接近可用 Go CPU capacity。

#### B. 完整 Game Echo profile

使用既有 400-connection game route，在測量窗口內分別抓 Gate 與 Game CPU profile、goroutine snapshot 與
短 trace。profile run 不納入 A/B throughput 統計。

範例命令：

```sh
go run ./examples/metrics/game -pprof-addr 127.0.0.1:19082
go run ./examples/metrics/gate -pprof-addr 127.0.0.1:18082

go tool pprof -http=:0 \
  'http://127.0.0.1:18082/debug/pprof/profile?seconds=20'

curl -o /tmp/gate-gatelink.trace \
  'http://127.0.0.1:18082/debug/pprof/trace?seconds=5'
go tool trace /tmp/gate-gatelink.trace
```

Game 使用 `19082` 執行相同操作。測試 binary 使用一般 `go build`／`go run`，不得以 `-s -w` 移除 symbols。

#### C. direct client profile

只對 `ClientConn=1` 與第一個顯示明確差異的 connection count 各做一次 profile run，檢查 grpcload client
hotspot 是否隨 connection count 分散。不要對每個重複 run 都 profile，避免無必要的測試成本。

---

## 10. 根因判讀與完成條件

| 結果 | 可支持的結論 | 後續 |
|---|---|---|
| direct 1-conn 重現低 RPS／高 latency，2/4-conn 超過 run variation 持續改善；client profile 集中在單一 transport reader/writer／control path | 單一 grpc-go HTTP/2 transport 是主要 serialization/contention point | 另案設計 bounded production multi-connection 或更適合的 RPC shape |
| direct 1-conn 明顯高於完整 game route，connection count 影響小 | 問題位於 Gate process integration／CPU／scheduler，而非純 gRPC server path | 以完整 Gate profile 定位呼叫端 hot/wait path |
| 增加 ClientConn 不改善，Game CPU 接近可用 `GOMAXPROCS`，Game profile 集中 transport/protobuf/runtime | Game server-side CPU capacity 是主要限制 | 依 profile hot function另案最佳化 |
| 增加 ClientConn 不改善，grpcload client CPU 接近可用 `GOMAXPROCS` | load client 自身先飽和，server capacity 結論無效 | 分離 load host或增加 client CPU，再重測 |
| CPU 不高但 trace 顯示大量 runnable delay、network block 或共同 lock wait | scheduler、syscall 或 contention 是主要候選 | 依 trace stack做單一變因驗證，不先增加 production connections |
| Game handler latency隨負載升高且占 unary interval 顯著比例 | application handler 才是主要限制 | 回到 handler dependency/profile；不歸因於 grpc-go transport |
| 固定 ClientConn 提高 concurrency 後 RPS 趨於平台、p95/p99 持續上升 | 該組合到達 capacity knee；仍需 CPU／profile 區分根因 | 於 knee 固定總 concurrency 再做 1/2/4 ClientConn A/B |

正式 root-cause statement 至少需要：

1. 同設定三輪以上可重現的結果；
2. controlled A/B 只改變一個核心變因；
3. process CPU／runtime metrics未顯示另一個 process 先飽和；
4. profile／trace stack 與 A/B 方向一致；
5. error、timeout、queue failure 為零或已明確排除。

未同時滿足以上條件時，只能描述為 candidate bottleneck，不能提交 production connection pool 或宣稱
grpc-go defect。

---

## 11. 關鍵設計決策與取捨

### 11.1 使用 pprof，而不是永久細碎 transport metrics

grpc-go public metrics 無法完整且穩定地暴露 stream quota、HTTP/2 writer scheduling 或 internal lock wait。
為每個內部事件增加 Prometheus metric仍可能只得到另一個 broad interval，且會永久增加 hot-path overhead。
pprof／trace 是 function-level 問題更合適的短期工具，因此只放在 examples 且 default disabled。

### 11.2 pprof 使用獨立 loopback listener

沿用 production observability listener 需要修改公開 HTTP contract，並可能暴露 stack、paths 與 memory data。
獨立、optional、loopback-only listener增加一個本地診斷 port，但避免 product security與deployment責任，範圍
反而更小。

### 11.3 新增專用 grpcload，不擴張既有 WebSocket load

既有 load 的核心責任是 WebSocket Login → EnterRoom → Echo。加入另一種 transport 會使大量 flags、prepared
connection model與錯誤語意變成條件分支。獨立 grpcload 僅實作 direct unary closed-loop，避免建立通用 scenario
abstraction，也不改動已驗證的 WebSocket load。

### 11.4 多 ClientConn 只存在於診斷端

在 production `gatelink.Client` 加 pool 會改變 connection lifecycle、resolver、load balancing、resource use與
failure behavior；目前還沒有根因證據支持。grpcload 以多個現有 client instance完成同一變因實驗，不形成
framework API 承諾。

### 11.5 不先增加 block／mutex knobs、Channelz 或 stats.Handler

CPU、goroutine 與短 trace 已能先識別 CPU、runnable、network blocking與共同 wait stack。額外 runtime sampling
與 per-RPC stats handler可能在 60k+ RPS 改變被測系統；只有前述證據仍不足時才有理由加入。

---

## 12. 已知限制與可擴充方向

- loopback、plaintext gRPC 結果不包含 production TLS、network RTT、proxy、service mesh 或 container quota。
- pprof 是 statistical profile；短 profile可能漏掉低頻事件，profile run 也具有 observer effect。
- direct grpcload 移除 Gate/WebSocket，因此只能搭配完整路徑 profile判讀，不能單獨取代 game/local benchmark。
- 多 ClientConn 改善 throughput只證明 per-connection capacity，仍不代表 production 應無上限增加 connections。
- Prometheus Histogram quantile 是 bucket interpolation；所有比較必須使用同 bucket與相同測量窗口。
- 若本設計確認單一 transport root cause，production 修正需另行決定 bounded connection count、selection、
  readiness、shutdown、resolver changes與resource budget，不在本次預先設計。
- 若 profile 指向 grpc-go 已知問題，後續應以目前 pinned version建立最小 reproduction，再評估升級；本次不先
  變更 dependency version。

---

## 13. Self review

### 13.1 需求覆蓋

| 需求 | 設計對應 | 結果 |
|---|---|---|
| 找出 broad gRPC interval 內的 function-level 根因 | Gate／Game／grpcload optional pprof與trace | 符合 |
| 驗證是否為單一 gRPC connection | 固定總 concurrency 的 `1／2／4 ClientConn` direct A/B | 符合 |
| 保留原 Echo workload | 沿用既有 command、protobuf、Game handler與unary reply | 符合 |
| 能觀察 RPS／latency／error／saturation | grpcload三個 bounded metrics、Go/process collectors與 concurrency staircase | 符合 |
| 不把診斷功能變成 production contract | 全部新增內容位於 `examples/metrics`，pprof default disabled | 符合 |
| 不先修改可疑參數掩蓋根因 | 不改 `MaxConcurrentStreams`、pool、window、retry或resolver | 符合 |
| 可直接實作與驗證 | 指定介面、flags、資料流、檔案、tests、矩陣與判讀門檻 | 符合 |

### 13.2 Review 後保留的必要內容

- Gate/Game pprof：沒有 process/function evidence，只能知道 unary interval慢，不能區分 client與server transport。
- direct grpcload：沒有移除 WebSocket/Gate 的直連基準，無法知道 broad interval是否可在純 gRPC 重現。
- `ClientConn` count flag：這是驗證單一 HTTP/2 transport假設的唯一核心變因；固定總 concurrency避免把更多
  workers誤認為更多 connections帶來的改善。
- grpcload metrics：沒有 RPS、latency、error、active in-flight 與 process collectors，就無法判定 A/B是否有效；
  saturation 仍需透過 concurrency staircase 判讀。
- reply correctness：只看成功 status可能把空 reply／錯誤 payload當成高 throughput，會使性能結論失效。
- warm-up：grpc-go lazy dial與HTTP/2 SETTINGS若混入正式窗口，1-conn與多-conn setup成本不相等。
- lifecycle與tests：profiling/listener/client屬於實際資源；沒有 bind-first、bounded shutdown與failure tests容易留下
  port、goroutine或partial run，會污染下一輪結果。

### 13.3 Review 後拒絕的非必要項目

- 直接設定 `MaxConcurrentStreams`：目前 server default不是有限的100 streams，且改設定不能證明真正等待點。
- production connection pool：屬於根因確認後的修正，不是診斷工具。
- pprof 放入 `pkg/observability`：會擴張所有 products 的公開與安全 contract。
- pprof authentication／remote access：loopback-only examples不需要；若未來要部署再另案處理。
- grpc-go Channelz／stats.Handler／OpenTelemetry：第一階段 pprof、trace與controlled A/B已覆蓋問題，先加入會
  增加每RPC overhead和維護面。
- block/mutex sampling flags：短 trace與goroutine profile仍未證明不足，暫不增加 runtime global knobs。
- 任意 method、command、scenario、target RPS、ramp或failure injection：與單一 Echo transport假設無關。
- 新 proto、Redis/DB traffic、Gate reverse server-send或production IDs：不在被診斷的 unary Echo路徑。
- macOS `sample` integration：它可作零改碼備援，但 pprof對Go goroutine/runtime更準確，不需寫入專案。

### 13.4 最終範圍判定

Review 後，設計只增加「function-level profile」與「single-variable ClientConn A/B」兩類必要證據，並為這兩類
工具提供最低限度的 metrics、correctness、lifecycle、tests與文件。沒有修改 production transport或先套用
推測性修正，也沒有建立通用 benchmark／observability framework。

再刪除 pprof會失去具體 hotspot，再刪除 ClientConn A/B會失去因果驗證，再刪除 correctness／saturation會讓
性能結果不可採信；其餘候選功能皆已排除。因此本設計符合目前根因診斷需求，沒有過度刪減、冗餘或超出
需求。

---

## 14. 建議實作順序

1. 實作 `profilehttp` 與 contract tests。
2. 在 Gate／Game example main加入optional pprof lifecycle，確認未啟用時行為不變。
3. 實作 grpcload config、single-ClientConn round trip與in-process tests。
4. 增加多 ClientConn selection、warm-up barrier、measurement lifecycle與metrics tests。
5. 更新 README，執行 compile/test/race/vet。
6. 使用者確認實作後，先跑未 profile 的 `1／2／4` A/B，再跑最少必要的 Gate/Game/grpcload profiles。

若實作過程需要修改 `pkg/gatelink`、framework products、production observability、protobuf或新增 dependency，
表示目前最小診斷方案有未預期缺口；應先更新設計說明原因與取捨，不得直接擴張範圍。
