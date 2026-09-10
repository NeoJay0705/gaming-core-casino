# Actionable Metrics 瓶頸診斷補強設計

## 1. 結論

本次只補強三個已由實際基準壓測證明存在的診斷缺口：

1. Load client 的 private Prometheus registry 缺少 Go runtime 與 process collectors，因此目前無法排除
   load generator 自身先達到 CPU、GC、memory 或 goroutine saturation。
2. 既有 latency Histograms 全部使用 `prometheus.DefBuckets`，第一個有限 bucket 是 `5ms`；實測 Game
   handler、Gate delivery 與 WebSocket write 多在數微秒，Gate-to-Game unary request 多在數毫秒，現有
   buckets 無法提供可用的 p50／p95 分布。
3. 現有 Echo 只有完整的 Gate → Game → Gate 路徑，缺少保留相同 WebSocket、Login、EnterRoom、payload
   與 load client 行為，但只在 Gate 本機回覆的對照組，因此無法判定容量平台位於 load/WebSocket/Gate，
   或位於 gRPC/Game 路徑。

本次不修改 `gatelink` transport、不設定 `MaxConcurrentStreams`、不建立 gRPC connection pool，也不新增
distributed tracing、pprof endpoint、跨 process timestamp、Redis traffic 或新的 production metrics。
新增的 local Echo 只存在於 `examples/metrics`，是可重複使用的瓶頸隔離測試 contract，不會成為 framework
product 的預設業務 API。

本文件是 `ACTIONABLE_METRICS_DESIGN.md` 與 `ACTIONABLE_METRICS_EXAMPLE_VALIDATION.md` 的窄幅補充。
原設計中「第一次基準壓測後再決定 buckets」的條件已經成立，因此本文件只覆蓋原本沿用
`prometheus.DefBuckets` 的決策；其他 metric 名稱、labels、計時邊界、lifecycle 與錯誤語意保持不變。

---

## 2. 需求理解與合理假設

### 2.1 要回答的問題

修改完成後，使用同一組 connection 數、payload、duration、warm-up 與 `GOMAXPROCS` 分別執行
`game` 與 `local` Echo，必須能回答：

- RPS 平台是否由 load client 自身的 CPU、GC 或 scheduler 造成；
- 移除 gRPC/Game 路徑後，Gate WebSocket 路徑的 RPS 是否明顯提高；
- latency 增加主要出現在 client E2E、Gate command、Gate gRPC、Game handler、delivery 或 write 哪一段；
- 測試過程是否有 error、queue full、connection close 或其他 terminal failure；
- 現有 process metrics 是否足以排除單一 service 的整體 CPU、memory 或 GC saturation。

### 2.2 已確認事實

- Gate 與 Game 已透過 `pkg/observability` 註冊 `collectors.NewGoCollector()` 與
  `collectors.NewProcessCollector()`；不得重複實作。
- Load observer 使用獨立 `prometheus.NewRegistry()`，目前只註冊三個 Echo metrics，沒有 runtime/process
  collectors。
- 現有壓測是 closed-loop：每條 connection 同時最多只有一筆 Echo round trip；connection 增加而 RPS
  不增加時，latency 會因等待而上升。
- Gate 的 `gaming_core_gate_game_grpc_duration_seconds` 是完整 unary call duration，包含 client-side waiting、
  request/response transport、Game handler 與 protobuf encode/decode；它不是單向 Gate → Game 網路 latency。
- Echo steady-state 不使用 Redis 或 Database。Login、EnterRoom 或 presence 產生的 Redis samples 不可解讀為
  Echo handler dependency。
- grpc-go server 目前沒有設定有限的 `MaxConcurrentStreams`；client 在初始 HTTP/2 SETTINGS 未提供該值時，
  會採用 `math.MaxUint32`。既有約 60k～66k RPS 平台不能歸因於預設 100 streams。

### 2.3 合理假設

- `game` 與 `local` 模式分開執行，不在同一個 load process 內混合流量。
- 每輪測試使用相同 binary、相同機器與相同設定；至少重複三輪並比較 median，避免把單次約 5%～10%
  的排程變異判定為架構差異。
- 若 load client 與 services 同機執行，process metrics 只能顯示各 process，自身仍不能取代 host-level CPU、
  network 與 kernel metrics。Host saturation 應由 node exporter 或測試環境工具提供，不加入 application
  metric API。
- 本次目的為隔離 subsystem，不宣稱只靠 Prometheus metrics 能定位到某一個 Go function；若已縮小至
  gRPC/Game 路徑，function-level 根因應使用測試期間的 CPU profile，而不是永久新增細碎 metrics。

---

## 3. 範圍

### 3.1 本次必要修改

- Load observer 註冊標準 Go/process collectors。
- 依實測 latency 範圍調整既有 Histogram buckets。
- Example protocol 新增 local Echo request/response command IDs，沿用既有 Echo protobuf messages。
- Gate example workflow 註冊 local Echo handler，並維持 Login → EnterRoom 前置條件。
- Load client 新增 bounded `-echo-route=game|local` flag，warm-up 與正式測量都使用同一路徑。
- 更新 tests、example README 與驗證文件，使兩條路徑可重複執行及比較。

### 3.2 明確不修改

- `pkg/gatelink` client/server、protobuf 或 unary reply contract。
- `grpc.MaxConcurrentStreams`、多 `ClientConn`、connection pool、retry 或 load balancing 行為。
- Gate/Game 現有 metric 名稱、labels、counter/gauge 語意與計時起訖點。
- Redis/Database pool metrics 或 Echo workflow dependency。
- `/health`、`/ready`、`/metrics` endpoints 與 framework lifecycle。
- Production command IDs、production proto 或 product default modules。
- Load metric 新增 `route` label；不同 route 由分開的 run/config 與 log 辨識，避免不必要的 time series。
- Arbitrary command flag、failure injection、server delay、distributed workers、dashboard、alert rules、tracing
  或永久 pprof endpoint。

---

## 4. 架構與資料流

### 4.1 Game Echo（既有完整路徑）

```text
load client
  → WebSocket write
  → Gate read / route=game
  → gatelink unary Forward
  → Game dispatcher / Echo handler
  → unary ForwardResponse
  → Gate enqueue / writer
  → load client read + response validation
```

### 4.2 Local Echo（新增對照路徑）

```text
load client
  → WebSocket write
  → Gate read / route=local
  → Gate local Echo handler
  → Gate enqueue / same writer
  → load client read + response validation
```

兩條路徑共用：

- 相同 WebSocket implementation 與 binary header；
- 相同 connection 數、Login、EnterRoom 與 connection state；
- 相同 `EchoRequest`／`EchoResponse` schema、payload size、warm-up 與 closed-loop admission；
- 相同 Gate writer queue、socket write、client read 與 client-side E2E metrics。

兩條路徑唯一刻意差異是 local 模式移除 `gatelink`、Game gRPC ingress、Game dispatcher 與 unary reply。
因此 local/game 的容量差異可用來隔離 subsystem，而不是比較兩個不同 workload。

### 4.3 判讀決策

| 結果 | 可支持的判斷 | 不可直接宣稱 |
|---|---|---|
| Local 與 Game RPS 平台接近 | 瓶頸較可能在 load client、WebSocket、Gate 共用路徑或 host | gRPC 完全沒有成本 |
| Local 明顯高於 Game，且 load client 未飽和 | 容量限制位於 gRPC/Game 被移除的區段 | 已定位到 grpc-go 某個 function |
| Load CPU 接近可用 CPU，兩模式都平台 | Load generator 可能先飽和，server 結論無效 | Server 容量就是目前 RPS |
| Game handler 穩定、Gate gRPC 上升 | 等待位於 handler 外的 unary round trip | 單向網路 latency 或 MaxConcurrentStreams |
| Delivery/write/queue 同時上升 | Gate outbound/slow client 路徑有壓力 | Game handler 是瓶頸 |

「明顯」不使用固定百分比寫死在程式；同設定至少三輪，以 median 與分布一致性判斷。第一次實際比較可先以
差異大於 run-to-run variability 作為成立條件，之後有正式 performance budget 再另案定義 threshold。

---

## 5. 核心資料模型與介面

### 5.1 Example command contract

在 `examples/metrics/internal/protocol/command.go` 增加兩個 example-local IDs：

```go
const (
	LocalEchoRequestCommandID  uint32 = 0xF1000021
	LocalEchoResponseCommandID uint32 = 0xF1000022
)
```

Local Echo 沿用既有 `EchoRequest` 與 `EchoResponse`，不新增 `.proto` 或 generated code。ID 必須加入既有
protocol contract test，驗證非零、固定值且不與 Login、EnterRoom、Game Echo IDs 重複。

### 5.2 Load route

Load CLI 新增：

```text
-echo-route=game|local
```

- 預設為 `game`，維持既有命令與測試行為。
- `game`、`local` 以外的值（包含明確傳入空字串）在建立 listener/connection 前回傳明確錯誤。
- Route 在 startup 時只解析一次，轉成 package-private command pair；hot path 不重複解析字串。
- Selected request/response command IDs 傳入 setup warm-up 與 measured Echo round trip。
- 結束 log 增加 `echo_route`，讓沒有 Prometheus external labels 的單次結果仍可辨識。
- 不把 route 加入 load metric labels；每個 process 一次只測一種 route。

實作者可使用小型 package-private enum 或 struct 保存 request/response IDs，但不建立通用 command registry、
scenario framework 或 plugin interface。

### 5.3 Gate local Echo handler

`workflow.GateModule()` 除 Login、EnterRoom 外，註冊 `LocalEchoRequestCommandID`：

1. Unmarshal `EchoRequest`。
2. 由 `WebSocketRequestContext` 取得 current session。
3. 從既有 `SessionRegistry.State` 驗證 login 與 room；未登入回 `ErrLoginRequired`，未進房回
   `ErrRoomRequired`。
4. Marshal payload 相同的 `EchoResponse`。
5. 使用既有 `sendResponse` enqueue `LocalEchoResponseCommandID`。

Handler 不存取 Redis、Database、Game client 或 server-send sender，不建立額外 goroutine。Local Echo 使用
不同 request ID，因此不影響既有 Game Echo 仍由 Gate forward 的行為。

---

## 6. Metrics 修改

### 6.1 Load runtime/process collectors

`newLoadObserver` 建立 private registry 後，先註冊：

```go
collectors.NewGoCollector()
collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})
```

再註冊現有三個 load metrics。任何 registration error 都以 wrapped error 返回，禁止 `MustRegister` panic。
所有 registration 在 `net.Listen` 前完成，錯誤時不遺留 listener。Load observer 仍只提供 `/metrics`，不加入
`/health`、`/ready` 或 framework DI。

這會使 load `/metrics` 同時提供：

- `process_cpu_seconds_total`、`process_resident_memory_bytes` 等 process metrics；
- `go_goroutines`、GC、heap、scheduler 與 `go_sched_gomaxprocs_threads` 等 Go metrics；
- 原有 Echo rate、latency 與 in-flight metrics。

### 6.2 Histogram buckets

基準壓測已觀察到約 `2µs` handler、`6µs` write、`10µs` delivery、`1ms`～`7ms` unary/client latency；
`prometheus.DefBuckets` 的 `5ms` 起點已被證明不適用。只替換既有 Histogram 的 `Buckets`，不新增 metric、
label 或 observation。

Request/E2E 類 buckets：

```go
[]float64{
	0.0001, 0.00025, 0.0005,
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
	0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}
```

套用至：

- `gaming_core_gate_websocket_command_duration_seconds`
- `gaming_core_gate_game_grpc_duration_seconds`
- `gaming_core_example_load_echo_round_trip_duration_seconds`

Fine-grained handler/write 類 buckets：

```go
[]float64{
	0.000001, 0.0000025, 0.000005, 0.00001,
	0.000025, 0.00005, 0.0001, 0.00025, 0.0005,
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
	0.1, 0.25, 0.5, 1, 2.5, 5,
}
```

套用至：

- `gaming_core_game_gate_command_duration_seconds`
- `gaming_core_gate_websocket_write_duration_seconds`
- `gaming_core_gate_server_send_delivery_duration_seconds`

Bucket slices 維持 package-private，分別由 Gate、Game 與 load package 擁有；不為共用常數新增 public API
或新的 internal package。Histogram buckets 是 time-series schema 的一部分，tests 應鎖定完整 boundary list，
未來若依 SLO 調整必須是明確變更。

這些 buckets 比 defaults 增加 time series 數量，但 labels 已 bounded，且新增解析度直接修正現有 p95
不可用問題。更新操作仍由 `client_golang` Histogram 完成，不加入自製 percentile engine。

### 6.3 不新增的 metrics

- 不新增 `grpc_stream_wait_seconds`：grpc-go public API 沒有提供可直接且正確量測該 queue 的穩定 contract。
- 不新增單向 Gate → Game／Game → Gate duration：跨 process timestamp 需要 clock assumptions，且無法由
  Histogram quantile 相減取得。
- 不新增 host CPU metrics：application process 不應假裝擁有 node-level 資源；使用 node exporter。
- 不新增 `route` label 到 load metrics：分開執行即可比較，新增 label 不增加診斷能力。
- 不新增 Redis metrics：Echo steady-state 不使用 Redis，既有 pool metrics 已足夠確認 setup traffic。

---

## 7. 錯誤處理與 lifecycle

| 邊界 | 必要行為 |
|---|---|
| Runtime/process collector registration 失敗 | `newLoadObserver` 回傳 wrapped error；尚未 bind listener |
| `-echo-route` 非 `game`/`local` | 啟動失敗，不建立 connections，不產生 partial load |
| Local Echo malformed protobuf | Gate handler error，沿用既有 `handler_error` close contract |
| Local Echo 未登入 | `login_required` 並關閉 connection |
| Local Echo 未進房 | `room_required` 並關閉 connection |
| Local Echo enqueue/write failure | 沿用既有 queue/write result 與 terminal close reason |
| Load request timeout/cancellation | 沿用既有一筆 request 一個 terminal result 的契約 |

新增 collectors 與 route selection 不改變 shutdown 順序。Load observer 仍在 connections 清理後 graceful
shutdown；Gate local handler 只使用 request context 與既有 session lifecycle。

---

## 8. 具體檔案修改

### 8.1 修改

- `examples/metrics/load/metrics.go`
  - 註冊 Go/process collectors。
  - 替換 Echo round-trip Histogram buckets。
- `examples/metrics/load/metrics_test.go`
  - 驗證 runtime collector 存在；process collector 依平台能力驗證。
  - 驗證新的 bucket boundaries、既有 terminal result 與 listener lifecycle。
- `examples/metrics/load/main.go`
  - 增加並驗證 `-echo-route`。
  - 將 selected command pair 傳給 warm-up 與 measured round trip。
  - 完成 log 加入 `echo_route`。
- `examples/metrics/load/main_test.go`
  - 驗證預設 game、明確 local、非法 route、兩種 command pairs、warm-up/measurement route 一致。
- `examples/metrics/internal/protocol/command.go`
  - 增加 local Echo request/response IDs。
- `examples/metrics/internal/protocol/protocol_contract_test.go`
  - 鎖定兩個 IDs 與全 example command ID 唯一性。
- `examples/metrics/internal/workflow/gate.go`
  - 註冊 local Echo handler並重用既有 state、packet encode 與 response enqueue contract。
- `examples/metrics/flow_contract_test.go`
  - 驗證 local Echo payload、state enforcement、Gate local route metrics，且 Game Echo counter 不增加。
  - 保留既有 Game full-flow contract。
- `products/gateproduct/metrics.go` 與既有 metrics contract tests
  - 只替換並驗證 Gate Histograms 的 buckets。
- `products/gameproduct/metrics.go` 與既有 metrics contract tests
  - 只替換並驗證 Game handler Histogram buckets。
- `examples/metrics/README.md`
  - 文件化 `game`/`local` 兩輪相同設定的執行方式。
- `ACTIONABLE_METRICS_EXAMPLE_VALIDATION.md`
  - 補上 load runtime/process saturation queries、兩路對照矩陣與正確 bucket 判讀。

### 8.2 不新增或刪除

- 不新增 proto/generated files、production package、service、listener 或 configuration section。
- 不刪除任何現有 metric、test case、workflow 或 full-flow assertion。
- 不修改 `ACTIONABLE_METRICS_DESIGN.md` 已完成的歷史設計；本文件明確記錄 buckets 決策的後續修正。

---

## 9. 測試策略與完成條件

### 9.1 Unit/contract tests

1. Load registry 同時包含既有三個 Echo metrics 與 Go runtime collector。
2. Process collector 在支援的平台暴露 `process_cpu_seconds_total`；測試沿用既有 observability package 的
   platform-aware 判斷，避免不支援平台假失敗。
3. 六個既有 Histogram 使用本文件指定 buckets，`_count` 與 terminal result 契約不變。
4. Route parser 只接受 `game`、`local`，default 為 `game`，非法值不啟動 observer 或 connection。
5. Warm-up 與 measured requests 使用同一 selected route，warm-up 仍不寫入 load-private Echo metrics。
6. Local Echo 必須 login 且 enter room；成功回傳相同 payload，未呼叫 Game gRPC handler。
7. Game Echo 的原始 command IDs、unary reply 與 metrics correctness 完全不變。

### 9.2 實際驗證矩陣

每個 connection level 以完全相同設定分別執行：

```text
game:  -echo-route=game
local: -echo-route=local
```

第一輪使用 `100、200、400` connections、30 秒、每 connection 一筆 warm-up、相同 payload 與
`GOMAXPROCS=4`。每個組合至少三次，順序交錯，例如：

```text
game → local → local → game → game → local
```

避免所有 local 或 game 固定先跑造成熱機、GC 或系統背景負載偏差。每輪都保存 load/Gate/Game 測試前、
測試中與收尾後 snapshots，並記錄：

- completed RPS、client E2E average/p50/p95 與 errors；
- load/Gate/Game process CPU、RSS、goroutines、GC 與 `GOMAXPROCS`；
- Gate command/gRPC、Game handler、delivery/write latency；
- all in-flight、queue depth/capacity、queue-full 與 terminal close reasons。

Prometheus quantile 只能在各 Histogram 自身計算，不跨 metrics 相減。`_sum / _count` 可提供同一 series 的
平均值；local/game 比較必須使用相同時間窗口與測試設定。

### 9.3 Build/test 門檻

```text
go test ./...
go test -race ./examples/metrics/... ./products/gateproduct ./products/gameproduct
go vet ./...
```

實作階段依使用者要求可先只確認編譯與 tests 可執行，再由使用者確認後進行實際 pressure run；未執行前
不得宣稱已確認新瓶頸。

---

## 10. 關鍵決策與取捨

### 10.1 Local Echo 是必要對照，不是虛構 production API

只有完整 Game Echo 時，所有等待都會反映在 Gate unary duration，無法判定共用 WebSocket/load path 的
容量。Local Echo 保留除 gRPC/Game 外的相同路徑，能回答明確的 subsystem 問題；放在 example-local
protocol/workflow 可避免污染正式 product API。

### 10.2 不直接新增 gRPC 連線或 concurrency 設定

`MaxConcurrentStreams` 假設已被 grpc-go source 與 A/B 結果排除。多 `ClientConn` 可能改變 throughput，
但在根因未確認前加入會改變 production topology，且可能只掩蓋單一 transport、CPU 或 load generator
問題。本次只建立足以決定是否需要後續 gRPC 專項分析的證據。

### 10.3 調整 buckets，而不是增加 timestamp metrics

現有平均值已能顯示量級，但 defaults 讓低延遲 quantile 失真。調整 bucket schema能直接修復既有 metric，
不需要跨 process clocks、request IDs 或 tracing。增加的 series 數量是 bounded 且可預期的必要成本。

### 10.4 Profiling 不成為 product contract

若 local/game 對照把問題縮小到 gRPC/Game，應在測試環境使用 CPU profile 或 OS profiler定位 hot function。
永久 pprof endpoint 需要額外的 access-control 與 deployment policy，超出本次最小 metrics/validation 範圍。

---

## 11. 已知限制與後續方向

- Local/game 對照只能隔離 subsystem，不能單獨證明 protobuf、HTTP/2 writer、scheduler、metrics update 或
  kernel 中哪一個 function 是根因。
- Process collector 不包含整機 CPU、kernel、network interface 或 container quota；正式環境仍需 node/
  container exporter。
- 一秒 scrape 可能錯過非常短的 instantaneous gauge peak；queue-full/error counters 與 latency distribution
  應一起判讀。
- Process CPU 是所有 threads 的總量，平均低於 `GOMAXPROCS` 不代表沒有單一 goroutine、lock 或 writer
  serialization point。
- Buckets 以目前 Echo baseline 為依據；未來若正式 SLO 或 payload range 改變，可明確修訂 bucket contract，
  但本次不加入 runtime bucket configuration。
- Bucket schema 更新後，既有 Prometheus 歷史 series 不會被重寫；rolling deployment 完成並經過完整 query
  window 前，不應把新舊版本聚合結果用於容量比較。
- 若 local 顯著高於 game，下一階段才評估 gRPC-only benchmark、CPU profile 或 controlled multi-connection
  A/B；本次不預先實作。

---

## 12. Self review

### 12.1 需求覆蓋

| 需求 | 設計對應 | 結果 |
|---|---|---|
| 看出 load client 是否飽和 | Load private registry 加 Go/process collectors | 符合 |
| 看出 latency 位於哪個既有 stage | 修正六個現有 Histograms 的低延遲 buckets | 符合 |
| 分辨 WebSocket/Gate 與 gRPC/Game | 相同 workflow 的 local/game Echo 對照 | 符合 |
| 不污染正式業務 API | IDs、handler 與 flag 全在 `examples/metrics` | 符合 |
| 不增加不必要 cardinality | 不新增 metric labels 或動態 values | 符合 |
| 保留現有完整鏈路 | Default route 仍為 game，Game flow contracts 不刪除 | 符合 |
| 可直接實作與測試 | 列出檔案、介面、錯誤、tests 與矩陣 | 符合 |

### 12.2 必要性 review 後保留

- Load Go/process collectors：沒有它們，無法判斷壓測端是否先達容量，任何 server capacity 結論都可能
  無效。
- Histogram bucket 修正：實測 latency 明確低於 defaults 第一個 bucket，既有 p50/p95 不可用；這不是
  預先最佳化。
- Local Echo 對照：沒有移除 gRPC/Game 的同等路徑，就只能知道等待落在 broad unary interval，不能隔離
  subsystem。
- Route log 與 bounded flag：同一 binary 支援兩條路徑時，必須能重現並辨識 run；不需要新增 metric label。
- 對應 tests/docs：新增 CLI、wire IDs、handler 與 bucket schema若沒有 contract tests，容易產生 silent
  routing 或 metric regression。

### 12.3 Review 後拒絕的非必要項目

- `MaxConcurrentStreams=1024`：預設不是 100 streams cap，且目前 concurrency 未達 1024。
- gRPC connection pool／multi-connection production implementation：尚無證據支持，會提前改變 topology。
- 新增 server-side gRPC duration：現有 Game handler與 Gate unary duration 已足以先做 subsystem 對照；
  server method timer仍無法涵蓋完整 response transport。
- Cross-process timestamps、Histogram quantile subtraction 或 request IDs：無法以 Prometheus aggregation
  正確重建單筆 latency。
- Load `route` label：每次只跑一種 route，external job/run metadata 已足夠。
- Host metrics 放入 application：ownership 錯誤，應由 node/container exporter 提供。
- Redis/DB、Player/Broadcast 或未使用 API metrics：不在 Echo steady-state 路徑。
- pprof HTTP endpoint、dashboard、alerts、failure knobs、通用 benchmark framework：不是完成本次隔離所需。

### 12.4 最終範圍判定

Review 後沒有刪除任何既有 metric、驗證路徑或 production contract，也沒有把診斷方便性擴張成 framework
抽象。本設計只修正一個已量測到的 bucket 缺陷、補齊一個缺失的 process owner，並增加一條 example-only
對照路徑。三項分別回答 latency precision、load saturation 與 subsystem isolation，彼此不重複；再刪除
任一項都會留下目前瓶頸結論中的已知盲點。

因此，本文件內容為目前需求的必要最小集合，沒有過度刪減、冗餘或超出需求。

---

## 13. 建議實作順序

1. 先加入 load Go/process collectors 與 package contract tests。
2. 替換既有 Histogram buckets並鎖定 schema tests。
3. 增加 local Echo command IDs、Gate handler 與 state/full-flow tests。
4. 增加 load route selection，更新 warm-up/measured tests 與完成 log。
5. 更新 example README 與 validation 文件。
6. 執行 compile/test/race/vet；使用者確認後才執行實際 local/game pressure matrix。

若實作時發現需要修改 `pkg/gatelink`、production proto、framework lifecycle 或新增 metric label，表示範圍已
超出本設計；應先更新文件說明不可避免的實際問題與取捨，不得直接擴張實作。
