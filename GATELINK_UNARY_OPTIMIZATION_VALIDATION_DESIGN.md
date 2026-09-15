# GateLink Unary 成本最佳化與實驗驗證設計

## 1. 結論

本設計只處理目前 GateLink `Forward` Unary RPC 已有數據支持的兩項成本，並為第三項候選成本保存足以判斷的 allocation 證據：

1. 將 Game server hot path 對 incoming metadata 的三次完整 `MD` copy 降為一次完整 copy，加上三個 bounded key lookup；不改 metadata contract、不移動欄位，也不移除 W3C `traceparent`。
2. 在共用 product gRPC server 增加 optional `stream_workers` 建立期設定，讓 Game example 可對 grpc-go `NumStreamWorkers(0/4/8)` 做 controlled A/B。`0` 保留 grpc-go default；實驗前不改 framework default。
3. 不新增 custom buffer pool。grpc-go v1.72.0 的 client/server 已使用 `mem.DefaultBufferPool()`，而目前 32-byte Echo message 低於 grpc-go 1 KiB pooling threshold；本次只收集 `B/request`、`allocs/request` 與 alloc profile，供報告判斷是否需要另案。

實驗固定為 direct `grpcload → Game`，兩個 process 都使用 `GOMAXPROCS=4`、concurrency 400、1 個 `ClientConn`、32-byte application payload、30 秒 measured admission。每個正式條件使用 fresh process 執行三次有效 attempt 並取中位數。CPU/alloc profiles 另開 profile-only run，不混入正式 RPS。

本設計不宣稱三項技術都會改善。目前證據只支持 metadata copy 的必要修正，以及 `NumStreamWorkers` 的 A/B；custom buffer pool 沒有實作依據。

---

## 2. 需求理解與已知證據

### 2.1 要回答的問題

完成實作與實驗後，報告必須能回答：

1. `NumStreamWorkers` 是否在目前極短 Unary handler 下，重複且可量測地降低 Game CPU/request 或提高 RPS/core。
2. worker candidate 是否同時維持 correctness、error rate、p95/p99、GC 與 scheduler latency，不只是提高單次偶然 RPS。
3. metadata bounded lookup 是否在不改 transport contract 的前提下降低 CPU 與 allocation。
4. 現有 default buffer pool 下，Game／grpcload 的主要 allocation stack 是 gRPC buffers、metadata/context、protobuf、trace，還是其他路徑。
5. 是否有足夠證據另案評估 custom buffer pool；沒有證據時必須明確停止，不增加設定與抽象。

### 2.2 已確認事實

- 專案使用 grpc-go `v1.72.0`。
- `grpcserver.New` 目前沒有傳入 `grpc.NumStreamWorkers`，因此使用 grpc-go default `0`，每個 incoming stream 走 `go f()`。
- grpc-go v1.72.0 的 worker channel 是 non-blocking dispatch；所有 worker busy 時 fallback 到 `go f()`。`NumStreamWorkers` 不是 concurrency limit、queue 或 backpressure。
- grpc-go v1.72.0 client/server 預設使用 `mem.DefaultBufferPool()`；custom pool API 與 `NumStreamWorkers` 都是 experimental API。
- grpc-go 對 `<= 1 KiB` buffer 刻意不進行底層 byte-slice pooling；目前測試的 application payload 是 32 bytes。
- 既有 20 秒 direct Game CPU profile 共取得 54.91 CPU 秒 samples，其中：
  - `runtime.morestack` cumulative 3.96 秒／7.21%；
  - `runtime.newproc.func1` cumulative 0.69 秒／1.26%；
  - `runtime.newproc1` cumulative 0.31 秒／0.56%；
  - `metadata.FromIncomingContext` cumulative 0.82 秒／1.49%；
  - HTTP/2 HPACK decoder cumulative 約 1.88 秒／3.42%；
  - `logging.fillRandom` cumulative 1.54 秒／2.80%。
- 上述原始證據為
  `artifacts/echo-performance/manual-two-runs-20260914T145415Z/direct-grpc/profiles/game-cpu-20s.pb.gz`
  與同目錄的 `game-cpu-top-cum.txt`；它只用來建立本設計的候選假設，不取代新 campaign 的正式 A/B。
- cumulative profile frames 會互相重疊，以上比例不可相加，也不可視為最佳化後必然可完全省下的 CPU。
- Game server 每筆 network Unary 目前會在以下位置完整 copy incoming `metadata.MD`：
  1. logging Unary interceptor 讀取 `traceparent`；
  2. `GateRequestService.Forward` 判斷 incoming metadata 是否存在；
  3. `withIncomingRequestContext` 讀取 connection ID 與 optional Gate ID。
- outgoing 路徑的 logging 與 GateLink interceptor 也會各自 copy metadata，但它們目前依賴「覆寫 reserved key」語意；本次沒有足夠理由改成會 append duplicate values 的做法。

### 2.3 固定假設

- 本次驗證的是目前 GateLink Unary transport/runtime cost，不是完整 WebSocket E2E 容量；因此不啟動 Gate 或 WebSocket load。
- Game example 仍依既有 startup contract 使用 Redis/OrbStack；Redis 不在 measured Echo per-request hot path，不因本實驗停止 OrbStack。
- worker A/B 使用現有 Echo protobuf、dispatcher、handler 與 Unary 原路 reply，不建立 noop service，避免測到不同 workload。
- W3C trace interceptor、structured logging contract 與 metrics interceptor 保持啟用；不得用關閉正式功能換取較漂亮的 benchmark。
- grpc-go 的 `EnableTracing` 與本專案 W3C trace 是不同功能。專案沒有開啟 grpc-go legacy tracing，本次也不新增其開關。

---

## 3. 範圍與非目標

### 3.1 必要程式修改

- 最佳化 incoming metadata 的 bounded key extraction，保留所有既有 validation/error contract。
- 在 `pkg/grpcserver.Config` 增加 `stream_workers`，正值時傳給 `grpc.NumStreamWorkers`。
- 在 Gate/Game example config 與 product example config 顯示 optional `stream_workers: 0`、預設語意與 experimental 性質。
- 為 `examples/metrics/grpcload` 增加 optional orchestration markers，讓 script 能在 warm-up 後取得 baseline，並在 metrics listener 關閉前取得 final snapshot。
- 新增本實驗專用 script、deterministic script tests，以及 metadata extraction benchmark／contract tests。
- 將本實驗產生的 `artifacts/unary-optimization/` 加入 `.gitignore`；raw artifacts 仍保留在本機供報告與稽核使用，不進入 Git staging。
- 實驗後建立 `GATELINK_UNARY_OPTIMIZATION_VALIDATION_REPORT.md`，同時保存 machine-readable summary 與 raw evidence。

### 3.2 本次不做

- 不實作或設定 custom `mem.BufferPool`。
- 不改 protobuf schema、`GateRequest` envelope、command ID、opaque payload 或 Unary reply contract。
- 不把 connection ID、Gate ID 或 `traceparent` 從 metadata 移入 payload。
- 不移除 W3C trace、不更換 entropy source，也不把 trace 成本混稱為 metadata 成本。
- 不最佳化 outgoing metadata；`Set` 改為 `AppendToOutgoingContext` 會改變 reserved key 的覆寫／duplicate 行為，超出本次低風險修正。
- 不調整 `MaxConcurrentStreams`、connections per host、write buffer、flow-control window、keepalive、retry 或 resolver。
- 不新增 per-worker、per-connection 或 metadata key Prometheus labels。
- 不增加 worker fallback counter。grpc-go 沒有公開此 hook；為一次實驗 fork/wrap grpc-go 屬於過度設計。
- 不建立通用 benchmark framework、distributed load 或 production dashboard。

---

## 4. 架構與資料流

### 4.1 測試路徑

```text
grpcload：400 個 closed-loop workers
  → 1 個 gatelink.Client / 1 個 grpc.ClientConn
  → logging Unary client interceptor
  → GateLink request-context metadata interceptor
  → HTTP/2 Unary stream
  → Game product grpcserver
      → optional reusable stream worker
      → logging Unary server interceptor
      → GateRequestService.Forward
      → Game dispatcher / Echo handler
      → SetForwardReply
  → Unary response 原路回 grpcload
  → grpcload 驗證 command ID 與 Echo protobuf payload
```

### 4.2 元件責任

| 元件 | 本次責任 | 不負責 |
|---|---|---|
| `pkg/grpcserver` | 建立 product gRPC server；依 config 選擇是否傳入 `NumStreamWorkers` | 決定 worker 數、提供 backpressure |
| `pkg/logging` | 只讀取 `traceparent` 對應 values；維持 duplicate/malformed fallback | 移除 tracing、讀取 GateLink metadata |
| `pkg/gatelink` | 驗證並讀取 connection/Gate identity；保留 direct service-call compatibility | 解碼 business payload、改變 envelope |
| `examples/metrics/grpcload` | 提供 direct Unary workload、correctness 與 orchestration boundary | 成為通用 gRPC benchmark client |
| validation script | fresh-process A/B、scrape、profile、artifact 與 validity 判斷 | 修改 tracked config、改變 Git staging |
| report | 從保存的 raw evidence計算結果並限制結論層級 | 以主觀觀察或單次結果下結論 |

---

## 5. 必要實作設計

### 5.1 Incoming metadata bounded lookup

#### logging interceptor

將：

```go
values, ok := metadata.FromIncomingContext(ctx)
parents := values.Get("traceparent")
```

改為：

```go
parents := metadata.ValueFromIncomingContext(ctx, "traceparent")
```

contract 保持不變：

- 恰好一個值才嘗試 continue remote trace；
- 缺少、空值、格式錯誤或 duplicate values 都建立新的 root；
- metadata 問題不使合法 business RPC 失敗；
- `ValueFromIncomingContext` 仍會 copy matching value slice，但不 copy 整份 `MD`。

#### GateLink request context

保留 `GateRequestService.Forward` 現有的一次 `metadata.FromIncomingContext`，因為它同時維持以下既有語意：

- 真實 network gRPC context 有 incoming metadata 時，必須驗證 required connection ID；
- 既有直接呼叫 service method 的 contract tests 可使用 `context.Background()`，不被誤判為缺少 transport metadata。

只將 `withIncomingRequestContext` 內第二次完整 copy 改成兩次 bounded lookup：

```go
connectionIDs := metadata.ValueFromIncomingContext(ctx, connectionIDMetadataKey)
gateIDs := metadata.ValueFromIncomingContext(ctx, gateIDMetadataKey)
```

抽出只接受 `[]string` 的 validation helper，維持：

- connection ID 必須恰好一個且 trim 後非空；
- Gate ID optional，但存在時不得 duplicate；
- error message 與 gRPC status code 不變；
- 回傳值仍寫入唯一的 `GateRequestContext` context value。

這項設計刻意只將三次完整 `MD` copy 降為一次，而不是用 `peer.FromContext`、unsafe context access 或改變 direct invocation contract 追求零 copy。

### 5.2 gRPC stream worker config

修改 `pkg/grpcserver.Config`：

```go
type Config struct {
    ListenAddr          string `config:"listen_addr" yaml:"listen_addr"`
    MaxConcurrentStreams uint32 `config:"max_concurrent_streams" yaml:"max_concurrent_streams"`
    WriteBufferSizeBytes int    `config:"write_buffer_size_bytes" yaml:"write_buffer_size_bytes"`
    StreamWorkers       uint32 `config:"stream_workers" yaml:"stream_workers"`
}
```

建立 server options 時：

```go
if cfg.StreamWorkers > 0 {
    serverOptions = append(serverOptions, grpc.NumStreamWorkers(cfg.StreamWorkers))
}
```

contract：

1. `0` 不傳 option，保留 grpc-go default，也就是停用 reusable workers。
2. `>0` 原值傳給 grpc-go；它只建立 reusable workers，不限制 concurrency，也不建立 request queue。
3. 此設定在 `grpc.NewServer` 前生效，runtime 修改必須重啟 process。
4. 本次只用 `0/4/8`。不根據 concurrency 設為 400，也不拿它取代 `MaxConcurrentStreams`。
5. API 是 grpc-go experimental；framework config key 保持 optional，文件不得承諾 grpc-go 永久維持相同實作。

範例設定只增加註解欄位：

```yaml
grpc:
  server:
    # 0 保留 grpc-go default；正值啟用 experimental reusable stream workers。
    # stream_workers 不限制 RPC concurrency。
    stream_workers: 0
```

更新範圍：

- `examples/metrics/configs/game.yaml`
- `examples/metrics/configs/gate.yaml`
- `configs/examples/gameproduct.yaml`
- `configs/examples/gateproduct.yaml`

Gate 也顯示此欄位，是因為 `grpcserver.Config` 是共用 product server contract；本次實驗只調整 Game。

### 5.3 grpcload orchestration boundary

目前 grpcload 結束後會立即關閉 metrics listener，script 可能只能拿到最後一次 periodic scrape，無法保證完整 measured counters。因此增加 optional：

```text
-orchestration-dir string  empty 表示停用，保留目前 standalone 行為
```

每次 attempt 使用新的 empty directory。Marker 都以 temporary file 加 atomic rename 寫入，內容為 JSON，至少包含相同 `run_id` 與 RFC3339Nano UTC timestamp：

| Marker | 寫入者 | 時點／內容 |
|---|---|---|
| `grpc-ready.json` | grpcload | client 已 Start、warm-up 完成，尚未進入 measured window |
| `grpc-start.json` | script | Game/grpcload baseline metrics 已保存，允許開始測量 |
| `grpc-measured.json` | grpcload | 所有 admitted request 已 terminal；包含時間、success、failed workers |
| `grpc-final-scraped.json` | script | Game/grpcload final metrics 已保存，允許 grpcload shutdown |

時序固定：

1. grpcload 啟動 pprof/metrics、建立一個 client、完成 warm-up。
2. grpcload 寫 `grpc-ready.json` 並 bounded 等待 `grpc-start.json`。
3. script 保存兩個 process baseline metrics，再寫 start marker。
4. grpcload 才設定 `measurement_start/admission_end` 並放行 400 workers。
5. admission end 停止發新 request；已發出的 request等待 terminal result或 10 秒 request timeout。
6. grpcload 寫 measured marker並保持 metrics listener。
7. script 等待 Game/grpcload in-flight 歸零，保存 final metrics與 logs，再寫 final-scraped marker。
8. grpcload shutdown並以 workload 結果決定 exit code。

所有 marker wait timeout 固定 `2m`；context cancellation 立即中斷。Marker 缺少、JSON 不合法、run ID 不同或 timeout 都使 attempt invalid。

`grpc-measured.json` 固定包含：

```text
run_id
measurement_start
admission_end
measurement_end
admission_duration_seconds
measured_duration_seconds
terminal_drain_duration_seconds
successful_requests
failed_workers
```

RPS 一律使用 `successful_requests / admission_duration_seconds`，不可用名義 30 秒或 process lifetime。

### 5.4 Metadata benchmark

在既有 package tests 增加 isolated benchmark，同一個 test binary 比較：

- `legacy_full_md_copy`：test-only reference，重現原本完整 `FromIncomingContext + Get`；
- `bounded_value_lookup`：呼叫 production helper。

benchmark input 固定包含 grpc-go 常見 headers、`traceparent`、connection ID 與 Gate ID，並以 `ReportAllocs` 輸出 `ns/op`、`B/op`、`allocs/op`。

test-only legacy function不得進入 production file、不得由 runtime config 切換，也不得被 production code呼叫。這避免為一次 A/B 永久保留慢路徑。

---

## 6. 測試策略

### 6.1 Unit／contract tests

必要測試：

1. logging incoming metadata：valid、missing、malformed、duplicate `traceparent` 的行為與現況相同。
2. GateLink metadata：required connection ID、optional Gate ID、empty、duplicate、trim 與 error text 保持相同。
3. `Forward(context.Background(), ...)` 的 direct invocation contract 保持可用。
4. network gRPC 缺少 connection ID 仍回 `InvalidArgument`。
5. `StreamWorkers=0/1/4` 都能建立、啟動、停止 server並完成 Unary call。
6. blocking handler 占用一個 worker 時，下一個 RPC 仍能進入，證明 config 不是 hard concurrency limit。
7. strict config 可 bind `stream_workers`，未知 key 仍拒絕。
8. orchestration marker success、timeout、cancellation、malformed JSON、wrong run ID 與 listener-until-final-scrape。
9. validation script 的參數、artifact layout、valid/invalid classification 與 cleanup。

必要命令：

```text
go test ./pkg/logging ./pkg/gatelink ./pkg/grpcserver ./products/gateproduct ./products/gameproduct ./examples/metrics/grpcload
go test -race ./pkg/logging ./pkg/gatelink ./pkg/grpcserver ./examples/metrics/grpcload
bash -n examples/metrics/scripts/run-unary-optimization-validation.sh
bash examples/metrics/scripts/run-unary-optimization-validation-test.sh
git diff --check
```

測試與 script 不可執行 `git add/reset/checkout/restore/commit`，不得改變 staging。

### 6.2 錯誤處理

- Game readiness、grpcload startup或 orchestration failure：保留當次 artifacts，標記 invalid，停止本次 PID。
- 任一 request error、timeout、payload mismatch、counter mismatch：attempt invalid，不用更高 RPS 掩蓋 correctness failure。
- metrics/profile scrape failure：容量 attempt invalid；profile-only run只將對應 profile標為缺失，不偽造結果。
- cleanup 只處理 script 保存的 PID；禁止 `pkill` 或 process-name glob。
- 任一 attempt invalid 時 script 不自動無限重試；報告列出理由，由執行者修正環境後明確重跑。

---

## 7. 實驗方法

### 7.1 固定條件

| 項目 | 固定值 |
|---|---:|
| 路徑 | direct `grpcload → Game GateLink Forward` |
| GOMAXPROCS | grpcload=`4`、Game=`4` |
| total concurrency | `400` closed-loop workers |
| ClientConn | `1` |
| Game endpoint | `1` |
| application payload | `32 bytes` |
| measured admission | `30s` |
| warm-up | 每個 ClientConn `1` 筆，不計 measured metrics |
| request timeout | `10s` |
| stream worker variants | `0 / 4 / 8` |
| MaxConcurrentStreams | `0`，保留 grpc-go default |
| write buffer | client/server 都為 `0`，保留 grpc-go default |
| metrics scrape | 每 `1s` |
| 正式 attempts | 每個 variant `3` 次 valid attempt，取 median |

除 `stream_workers` 外不得同時改變任何 gRPC tuning。測試途中若修改 code、dependency或 fixed config，必須建立新 campaign，不能補進舊 summary。

### 7.2 Preflight 與 build-once

1. 確認 Redis可用，但不停止 OrbStack。
2. 確認 Game、grpcload metrics/pprof ports 沒有舊 listener。
3. 記錄 host logical CPU、OS、Go/grpc-go version、Git HEAD與 dirty state。
4. build Game與grpcload各一次至 campaign directory，保存 SHA-256。
5. build 完成後才啟動 service；process 運作期間禁止編譯。
6. 整個正式 A/B 使用相同 binaries；`stream_workers` 只透過 Game config environment override 改變。

### 7.3 Correctness gate

先執行：

```text
stream_workers=0
concurrency=1
duration=10s
```

必須通過第 8 節所有適用 correctness 條件才可執行正式矩陣；若 gate 失敗，script 只保存該次 artifacts 並停止，不進入正式矩陣。這筆資料不納入 A/B median。

### 7.4 正式 worker A/B

執行順序採 round-robin，降低 host 時間漂移造成的偏差：

```text
round 1: workers 0 → 4 → 8
round 2: workers 4 → 8 → 0
round 3: workers 8 → 0 → 4
```

每一格都使用 fresh Game與grpcload process；上一格 PID結束、ports釋放後才進下一格。結果以每個 variant 的三次 valid attempts中位數比較，並同時列 min–max與 MAD。

### 7.5 Profile-only runs

正式矩陣結束後：

1. 對 workers=`0` 跑一次 fresh-process profile-only run。
2. 若 `4` 或 `8` 有符合第 10 節 candidate門檻，選 CPU/request較低者再跑一次；若都不符合，選正式矩陣 CPU/request較低者只作診斷對照，不稱為 winner。
3. 每次在 steady measured window抓 Game與grpcload 20 秒 CPU profile。
4. measured 結束、final scrape前保存：
   - `/debug/pprof/allocs` raw profile；
   - `/debug/pprof/goroutine?debug=2`；
   - Game/負載 final metrics。
5. 由相同 binary產生：

```text
go tool pprof -top -cum <binary> <cpu-profile>
go tool pprof -top -sample_index=alloc_space <binary> <alloc-profile>
go tool pprof -top -sample_index=alloc_objects <binary> <alloc-profile>
```

profile-only RPS只記錄 observer effect，不納入正式 median。這輪不抓 Go trace；worker問題已有 CPU stack、scheduler metrics與 goroutine dump，額外 trace不是必要資料。

### 7.6 Metadata benchmark

在相同 Go version執行至少五次：

```text
go test ./pkg/logging ./pkg/gatelink \
  -run '^$' \
  -bench 'IncomingMetadata' \
  -benchmem \
  -count 5
```

保存完整 stdout，不只抄平均值。報告列出 legacy/optimized 的 `ns/op`、`B/op`、`allocs/op` 中位數及相對差異。

---

## 8. Attempt 有效性與 correctness

正式 attempt 只有全部成立才是 valid：

1. Game `/ready` 成功，grpcload exit status為 `0`。
2. 四個 orchestration markers存在、JSON合法且 run ID一致。
3. baseline取得於 warm-up後；baseline→final成功 counter delta等於 measured marker的 `successful_requests`，且 `successful_requests` 必須大於零，避免零流量結果被誤判為 valid。
4. grpcload success total與duration histogram count delta相等；`error/cancelled` delta為零，`failed_workers=0`。
5. Game Echo command success、request-player success及其 histogram count delta都等於 grpcload成功數。
6. Game gRPC/command/server-send error delta為零。
7. final Game與grpcload in-flight都為零。
8. 兩個 metrics endpoint的 `go_sched_gomaxprocs_threads` 都為 `4`。
9. metadata validation contract tests已通過；不能只用效能結果取代 correctness。
10. baseline/final/periodic metrics、process/host samples與兩份 logs都存在且非空。

Histogram quantile以 baseline→final bucket delta計算，報告預設寫 bucket upper bound；mean使用 `_sum delta / _count delta`。不得以不同 histogram彼此相減推導不存在的 per-request stage latency。

---

## 9. Artifact 與系統分析資料 contract

每次 campaign 使用新目錄：

```text
artifacts/unary-optimization/<UTC-run-id>/
  manifest.txt
  binaries.sha256
  campaign-status.json
  summary.tsv
  metadata-benchmark.txt
  correctness/attempt-01/...
  workers-000/attempt-01/...
  workers-004/attempt-01/...
  workers-008/attempt-01/...
  profiles/workers-000/...
  profiles/workers-<candidate>/...
```

每個 attempt：

```text
metadata.json
run-status.json
orchestration/*.json
logs/{game,grpcload}.log
metrics/{game,grpcload}-baseline.prom
metrics/{game,grpcload}-samples.tsv
metrics/{game,grpcload}-final.prom
host/processes.tsv
host/top.txt
profiles/（只存在於 profile-only run）
```

`metadata.json` 至少包含：run ID、attempt、worker value、兩個 GOMAXPROCS、concurrency、ClientConn、payload、warm-up、request timeout、實際 UTC起迄、binary checksums、grpc-go version、MaxConcurrentStreams與write-buffer值。

`run-status.json` 使用 bounded reasons：

```text
startup_failed
readiness_failed
orchestration_failed
load_exit_failed
request_error
counter_mismatch
terminal_gauge_nonzero
gomaxprocs_mismatch
metrics_missing
host_sample_missing
profile_missing
external_host_load
```

`summary.tsv` 每個 attempt一列，至少包含：

```text
variant
attempt
valid
invalid_reason
successful_requests
admission_seconds
rps
latency_mean_seconds
latency_p50_upper_seconds
latency_p95_upper_seconds
latency_p99_upper_seconds
client_cpu_seconds
game_cpu_seconds
client_cpu_seconds_per_request
game_cpu_seconds_per_request
client_alloc_bytes_per_request
game_alloc_bytes_per_request
client_allocs_per_request
game_allocs_per_request
client_gc_seconds
game_gc_seconds
client_sched_p99_seconds
game_sched_p99_seconds
client_max_goroutines
game_max_goroutines
host_idle_min_percent
```

raw artifacts 不加入 Git staging。Script執行前後保存 `git status --short` 供稽核，但不得執行會改變 staging的 Git操作。

---

## 10. 計算與判定規則

### 10.1 基本公式

設 measured成功數為 `N`：

```text
RPS = N / admission_duration_seconds
process CPU/request = (final process_cpu_seconds_total - baseline) / N
B/request = (final go_memstats_alloc_bytes_total - baseline) / N
allocs/request = (final go_memstats_mallocs_total - baseline) / N
GC CPU/request = (final go_gc_duration_seconds_sum - baseline) / N
```

以上均分別對 Game與grpcload計算。Process-wide allocation包含同窗口背景工作，因此只能在相同 topology與固定條件下做相對比較；不得宣稱全部 bytes都是單筆 RPC直接配置。

### 10.2 Worker candidate門檻

worker=`4` 或 `8` 只有全部成立才可稱為有效 candidate：

1. 三次 valid attempts齊全。
2. Game CPU/request中位數相對 workers=`0` 降低至少 5%，且差異大於 baseline MAD／median。
3. RPS中位數不下降超過 2%。若 RPS提高至少 5%，可作為額外正向證據。
4. p95/p99 bucket upper bound不惡化；error與correctness mismatch仍為零。
5. Game B/request、allocs/request、GC與scheduler p99沒有明顯退化。
6. profile-only run的 `runtime.morestack/newproc` cumulative比例方向一致下降；此項只作機制佐證，不拿 profile RPS作容量判定。

若 4與8都通過，選 Game CPU/request較低者；若差異小於兩者測試波動，選較小值4。若都未通過，不將 reusable workers設為framework default。

即使 candidate通過，本次也只保留 optional config且範例值維持0；單一 synthetic Echo實驗不足以替所有未來可能 blocking的product handler決定production default。

若正式實驗證明所有 worker值都沒有超過波動的效益，`stream_workers` 只為本實驗存在且沒有實際使用理由，最終合併前應移除該 production config與相關範例，報告保留否定結果。這是防止無效 tuning knob 永久留在公開 API 的必要收斂規則。

若 CPU/request 改善低於 5%，但穩定高於 baseline relative MAD，則不得稱為 production candidate，也不得改變 default；可保留 default=`0` 的 optional experimental seam，唯一用途是讓不同 product workload 依相同方法重驗。若後續 workload 也無法證明超過波動的效益，才依前述規則移除該 seam與範例。

### 10.3 Metadata修正門檻

metadata最佳化保留的條件：

- 所有既有與新增 contract tests通過；
- optimized benchmark的 `B/op` 或 `allocs/op` 必須低於 legacy reference，且 `ns/op`不得穩定退化；
- after profile不應再出現原本兩個已替換 call site的完整 `MD` copy；剩餘一次來自 `Forward` presence/compatibility contract是預期行為。

它不要求整體 RPS提高5%才保留，因為這是已定位、行為等價且縮小配置量的局部修正；報告仍不得誇大為主要吞吐改善。

### 10.4 Buffer pool後續門檻

本次報告只有同時滿足以下條件，才能建議另開 custom buffer pool設計：

1. alloc profile顯示 grpc-go receive/marshal buffer stack是 Game或client的主要 allocation來源之一；
2. 該 stack對 `alloc_space` 或 `alloc_objects` 有足以超過測試噪音的占比；
3. 實際production-size payload分布顯示 default tiers不適合，而不是只根據32-byte Echo推測；
4. 已排除 protobuf payload copy、metadata/context與trace allocation是更直接的主要來源。

本次不設定任意10%等固定比例作為跨workload真理；報告必須列原始top stack與絕對bytes/objects，再決定是否值得另案。證據不足時結論固定為「沿用grpc-go default pool」。

---

## 11. 報告格式與結論層級

實驗完成後新增：

```text
GATELINK_UNARY_OPTIMIZATION_VALIDATION_REPORT.md
```

報告至少包含：

1. 執行摘要：哪些 candidate成立、哪些被否定、是否留下production config。
2. 版本與環境：Git state、binary hashes、Go/grpc-go、OS、logical CPU、GOMAXPROCS與全部固定參數。
3. Attempt validity：逐筆列出valid/invalid、理由與對應`run-status.json`。
4. 正式矩陣：0/4/8的三次原值、median、min–max、MAD。
5. RPS／latency／error表。
6. client/Game CPU/request、B/request、allocs/request、GC、scheduler、goroutines與host負載表。
7. Metadata benchmark：五次原始結果及中位數差異。
8. CPU profile：baseline/candidate的絕對CPU sample秒數與top cumulative stacks；不得只列百分比。
9. Allocation profile：`alloc_space`與`alloc_objects`的絕對值、主要stack與buffer pool判斷。
10. 證據鏈：每個結論連到raw artifact，並分開寫「支持的推論」與「不能推論的事」。
11. 最終決策：worker config保留/移除、metadata修正保留/回退、custom pool不做/另案。
12. 已知限制：單機loopback、synthetic Echo、shared host與experimental grpc-go API。

必要結論用語：

| 證據 | 可以下的結論 | 不可下的結論 |
|---|---|---|
| worker candidate降低CPU/request且profile stack同步下降 | reusable workers對目前極短Unary有效 | 所有blocking handler都應使用相同值 |
| RPS變化在MAD內、CPU/request未改善 | 本次workload未證明worker效益 | grpc-go worker永遠無效 |
| bounded lookup benchmark減少allocation | metadata完整copy是可消除的局部成本 | HPACK成本已消失 |
| HPACK仍在profile | W3C與source metadata仍有transport header成本 | 應移除trace或改protobuf contract |
| gRPC buffer stack不是主要allocation | 目前不需custom pool | default pool對所有future payload永遠最佳 |

Markdown報告是給人閱讀；`summary.tsv`、JSON status、raw Prometheus與pprof才是後續系統分析的資料基礎。報告中的每個關鍵數字必須能追到檔案與metric/profile欄位。

---

## 12. 實作順序

1. 修改 metadata bounded extraction並補齊 contract tests/benchmark。
2. 增加 `grpcserver.Config.StreamWorkers`、server option mapping與config tests。
3. 更新四個Gate/Game example config註解。
4. 為 grpcload增加optional orchestration markers與tests。
5. 新增專用 validation script及deterministic script test。
6. 只執行編譯、unit/contract/race與script tests，交由使用者檢視實作。
7. 使用者確認後才執行實際 correctness、0/4/8正式矩陣、profile-only與metadata benchmark。
8. 產生報告並依第10節決定是否保留worker config；不得在數據前改framework default。

---

## 13. 已知限制與擴充方向

- `NumStreamWorkers`是grpc-go experimental API；dependency升級時必須重新編譯與執行contract/performance validation。
- 400 concurrency、1 connection與32-byte Echo代表高QPS短handler，不代表DB/Redis/blocking handler；production值需由實際workload另行驗證。
- 單機loopback的client與server共享host，CPU/request比RPS更適合做同campaign相對比較，但不能直接外推production capacity。
- Prometheus process-wide allocation counters包含同窗口背景工作；alloc profile用來定位stack，兩者需一起判讀。
- W3C span ID entropy成本已在profile中可見，但它不是本次三項範圍；若成為主要成本，需另案且不得降低trace ID安全性。
- 未來若production payload分布與allocation profile共同證明default pool不適合，再設計buffer pool；本次不預留interface或config。

---

## 14. Self review：必要性與需求符合度

### 14.1 需求對照

| 需求 | 設計回應 | 判定 |
|---|---|---|
| 判斷三項對目前Unary是否有幫助 | worker做A/B、metadata做局部修正與benchmark、buffer只收證據 | 符合 |
| 固定GOMAXPROCS=4 | Game與grpcload固定4，validity由metrics驗證 | 符合 |
| 實驗步驟明確 | 固定矩陣、round-robin、fresh process、三次median、profile分離 | 符合 |
| 可供系統分析 | raw prom/pprof/log、JSON status、TSV summary與公式固定 | 符合 |
| 報告可追溯 | 每個結論要求artifact來源與證據限制 | 符合 |
| 只做必要修改 | 不改protocol、不做custom pool、不加高cardinality metrics、不建通用framework | 符合 |

### 14.2 必要性審查

- metadata bounded lookup是必要的：CPU profile已有`FromIncomingContext`成本，現況同一RPC做三次完整copy；設計只移除兩次且保留相容性所需的一次。
- `stream_workers`是完成實際product server A/B所需的最小建立期seam；它不是預設效能修正。若沒有超過波動的效益必須移除；若只低於5% candidate門檻但仍穩定超過波動，則僅保留default=`0`的實驗用途。
- grpcload orchestration是必要的：沒有baseline/final barrier，就無法精確計算counter、CPU、allocation delta，且load listener可能在final scrape前關閉。
- 專用script是必要的：三個variant、fresh process、三次attempt與profile/artifact規則靠人工容易產生不可比較結果。
- metadata microbenchmark是必要的：production不應為A/B保留legacy runtime flag；test-only reference可用最小成本提供同binary allocation比較。
- allocation evidence是必要的：沒有`B/request`、`allocs/request`與alloc profile，就無法對custom pool做數據判斷。
- Go trace、Channelz、worker fallback metric與custom pool目前都不是必要的；設計已排除。

### 14.3 最終review結論

本設計符合目前需求，所有列入的production修改都有直接用途或明確的實驗後移除條件。沒有為了觀測而新增不可採取行動的metrics，沒有改變Unary業務contract，也沒有先實作缺乏證據的custom buffer pool。內容已足以讓實作者按固定順序完成程式修改、測試、實驗、資料保存與報告，不需要自行猜測測試條件或成功標準。
