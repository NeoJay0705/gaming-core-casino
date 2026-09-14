# Client → Gate → Game Echo 效能驗證設計

## 1. 結論

本設計定義一次可重複、可判讀且可追溯原始證據的完整 Echo 效能實驗，路徑固定為：

```text
load Client
  → Gate WebSocket read / dispatcher
  → Gate gRPC client
  → Game gRPC server / dispatcher / Echo handler
  → Forward unary response 原路回 Gate
  → Gate WebSocket write queue / writer
  → load Client read
```

每條 WebSocket connection 先完成 `Login → EnterRoom`，所有 connection ready 後完成 warm-up，
最後才進入 measured window。Load、Gate 與 Game 三個 Go process 都必須以 `GOMAXPROCS=2`
啟動；Redis 因 Gate session ownership 與 service startup 而保持可用，但 Echo hot path 不對 Redis
進行 per-request 存取。

正式容量結果來自未開啟 pprof/trace 的三次有效測試中位數。只在容量平台出現後，
才於同一個壓力點執行獨立的 profile-only run；profile 用來解釋成本，不與正式 RPS
結果混合。

本次不增加 gRPC write-buffer runtime metrics。只增加 Gate→Game client 與共用 gRPC server
兩個彼此獨立的 optional config，保留在瓶頸證據指向 gRPC writer/flush/syscall 時進行
controlled A/B 的能力。不先將調大 buffer 當成效能修正。

本文沿用 `ACTIONABLE_METRICS_EXAMPLE_VALIDATION.md` 的 metric 與 Echo correctness contract；
本次 campaign 的 `GOMAXPROCS=2`、orchestration、重複次數、artifact 與報告規則以本文為準。
這只取代舊文件中 `GOMAXPROCS=4` 的過去實驗條件，不改變既有 metrics 語意。

---

## 2. 需求理解與固定假設

### 2.1 要回答的問題

1. 在當前開發機、三個 process 各 `GOMAXPROCS=2` 的條件下，完整 Echo 路徑的 RPS、
   latency、error 與容量平台是什麼。
2. 首個容量平台出現時，是 load client、Gate、Game、gRPC transport、WebSocket writer
   或 shared host 先出現壓力。
3. 若 gRPC write/flush/syscall 為有證據的候選成本，單獨提高 Gate client 或 Game server
   `WriteBufferSize` 是否有超過測試波動的改善。

### 2.2 本輪基準的固定條件

| 項目 | 固定值 |
|---|---|
| Echo route | `game` |
| Go processes | load、Gate、Game |
| GOMAXPROCS | 三者都為 `2` |
| WebSocket connections | `100、200、400、800、1600、3200`，依停止條件提前終止 |
| admission model | 每條 WebSocket 一個 closed-loop worker，同時最多一筆 Echo in flight |
| payload | `32 bytes` |
| measured admission duration | `30s` |
| warm-up | 每條 connection `1` 筆 Echo，不納入 measured counters |
| setup concurrency | `32` |
| setup timeout | `2m` |
| request timeout | `10s` |
| Gate→Game connections per host | `1` |
| MaxConcurrentStreams | `0`，不傳 option，保留 grpc-go default |
| gRPC write buffer | client/server 皆為 `0`，不傳 option，保留 grpc-go default |
| scrape interval | `1s` |
| 正式嘗試數 | 每個 connection level `3` 次有效嘗試，取中位數 |

grpc-go v1.72.0 的 write buffer default 為 32 KiB，但 config 的 contract 是「`0` 不傳 option」，
而不是將 32 KiB 複製成 framework default。報告必須記錄實驗 binary 使用的 grpc-go version，
避免未來 dependency 升級後仍誤認 default 永遠是 32 KiB。

### 2.3 環境邊界

- 測試以當前 macOS 單機 loopback 執行，load、Gate、Game、Redis/OrbStack 與 kernel
  共用 host CPU。結果是此 topology 的容量，不是 production Game 單機容量。
- OrbStack 與 Redis 不要為了測試而停止，否則 framework startup 與 session ownership 無法保持
  與真實 example 一致。
- setup 期間的 WebSocket handshake、Login、EnterRoom 與 session ownership Redis traffic 不納入 Echo
  RPS/latency。測量期間仍可能出現 ownership renewal，需在報告標示，不得宣稱整個
  process 完全沒有 Redis traffic。

---

## 3. 範圍與非目標

### 3.1 必要實作

- 為 Echo load 增加最小 orchestration barrier，保證 baseline 取在 warm-up 之後，並在
  load metrics listener 關閉前取得 final snapshot。
- 增加一支 example-only 自動化腳本，負責 build-once、fresh-process 啟停、三次嘗試、
  metrics/host 取樣、有效性判定與 artifact 保存。
- 增加 Gate→Game gRPC client 與共用 gRPC server 的獨立 `write_buffer_size_bytes` config。
- 增加 orchestration、config mapping/validation 與腳本分類邏輯的 deterministic tests。
- 建立獨立報告，只從本設計要求的 artifacts 推導結論。

### 3.2 本次不做

- 不新增 WriteBufferSize、per-connection、flush bytes 或 syscall bytes Prometheus metrics。
- 不使用 connection ID、login name、request ID 或 remote address 作 metric label。
- 不修改 Echo protocol、dispatcher、unary reply、WebSocket framing 或 server-send 路徑。
- 不替 `pkg/serversend.GRPCTransport` 增加 write-buffer config；它不在本次 Echo hot path。
- 不同時調整 `connections_per_host`、`MaxConcurrentStreams`、flow-control window、keepalive、
  retry、WebSocket buffer 或 queue capacity。
- 不將 local Echo 加入每個 connection level。只有 profile 無法判定瓶頸是共用 WebSocket/Gate
  路徑或 gRPC/Game 區段時，才在首個平台點執行條件式 local control。
- 不將 pprof/trace run 的 RPS 合併到正式容量表。
- 不建立通用 benchmark framework、dashboard、alert 或 distributed load system。

---

## 4. 必要程式修改

### 4.1 gRPC client write-buffer config

修改 `pkg/gatelink.ClientConfig`：

```go
type ClientConfig struct {
    Target               string        `config:"target" yaml:"target"`
    Timeout              time.Duration `config:"timeout" yaml:"timeout"`
    DNSRefreshInterval   time.Duration `config:"dns_refresh_interval" yaml:"dns_refresh_interval"`
    ConnectionsPerHost   int           `config:"connections_per_host" yaml:"connections_per_host"`
    WriteBufferSizeBytes int           `config:"write_buffer_size_bytes" yaml:"write_buffer_size_bytes"`
}
```

contract：

1. `< 0` 在 `NewClient` 回傳包含 `write_buffer_size_bytes` 的 validation error。
2. `0` 不加入 `grpc.WithWriteBufferSize`，保留 grpc-go default。
3. `> 0` 在每個 resolved endpoint 的每條 `ClientConn` 加入
   `grpc.WithWriteBufferSize(value)`。
4. DNS refresh 新建的 connection 與 static target 使用同一個 config，不允許 runtime
   對既有 connection 原地變更。

修改 `products/gateproduct` 的 `grpc.clients.game` bind model、validation 與
`gatelink.ClientConfig` mapping；這是完整 Echo 路徑中唯一需要修改的 gRPC client。

### 4.2 gRPC server write-buffer config

修改 `pkg/grpcserver.Config`：

```go
type Config struct {
    ListenAddr          string `config:"listen_addr" yaml:"listen_addr"`
    MaxConcurrentStreams uint32 `config:"max_concurrent_streams" yaml:"max_concurrent_streams"`
    WriteBufferSizeBytes int    `config:"write_buffer_size_bytes" yaml:"write_buffer_size_bytes"`
}
```

contract：

1. `< 0` 在 `grpcserver.New` 回傳包含 `write_buffer_size_bytes` 的 validation error。
2. `0` 不加入 `grpc.WriteBufferSize`，保留 grpc-go default。
3. `> 0` 將 `grpc.WriteBufferSize(value)` 與既有 server options 一起傳給
   `grpc.NewServer`。
4. 設定是 listener 啟動前的 immutable config；變更值必須重啟 process。

`grpcserver.Config` 為 product-level 共用 server config，因此 Gate 與 Game 都會支援這個鍵；
本次 Echo A/B 只調整 Game server，因為 Gate product gRPC server 不在這條 request/reply 路徑。

### 4.3 Config 名稱與範例

Gate：

```yaml
grpc:
  clients:
    game:
      target: "127.0.0.1:19090"
      connections_per_host: 1
      # 0 代表不傳 gRPC option，保留 grpc-go default。
      write_buffer_size_bytes: 0
```

Game：

```yaml
grpc:
  server:
    listen_addr: "127.0.0.1:19090"
    # 0 代表不傳 gRPC option，保留 grpc-go default。
    write_buffer_size_bytes: 0
```

更新範圍限於：

- `examples/metrics/configs/gate.yaml`
- `examples/metrics/configs/game.yaml`
- `configs/examples/gateproduct.yaml`
- `configs/examples/gameproduct.yaml`

註解必須寫「grpc-go default」，不將當前 32 KiB 寫成 framework 保證的 default。

### 4.4 Echo orchestration barrier

既有 load metrics listener 在 `runLoad` 結束時立即 shutdown，而 warm-up 後立即進入 measured
window。單靠 `curl` 輪詢無法保證 baseline 與 final snapshot 的邊界，因此必須為
`workload=echo` 加入 example-only barrier。

重用既有 `-orchestration-dir` flag，將 help text 改為同時適用 Echo 與 push validation。
未提供 directory 時，Echo 行為與現在完全相同。每次嘗試使用新的 empty directory，
不刪除或覆寫舊 marker。

Echo markers 使用 JSON，每個 marker 都包含 `run_id` 與 RFC3339Nano UTC timestamp：

| Marker | 寫入者 | 語意 |
|---|---|---|
| `echo-ready.json` | load | 全部 connection 已 Login、EnterRoom 並完成 warm-up |
| `echo-start.json` | script | baseline snapshots 已保存，可開始 measured admission |
| `echo-measured.json` | load | measured requests 已 terminal，包含起迄時間、success 與 failures |
| `echo-final-scraped.json` | script | final snapshots 已保存，load 可關閉 metrics listener 並結束 |

 `echo-measured.json` 的欄位固定包含 `measurement_start`、`admission_end`、`measurement_end`、
 `admission_duration_seconds`、`measured_duration_seconds`、`terminal_drain_duration_seconds`、
 `successful_echo_requests` 與 `connection_failures`。數值 duration 由 load 使用同一組 monotonic
 clock 計算；報告的 RPS 使用 `successful_echo_requests / admission_duration_seconds`，不可用
 名義 duration 取代。若 request error 使所有 worker 在 admission end 前提早 terminal，仍須寫出
 marker；此時 `measurement_end` 可早於 `admission_end`，`terminal_drain_duration_seconds` 固定為
 `0`，由 script 依 counter/error contract 將 attempt 判為 invalid。

時序固定為：

1. load 啟動 metrics listener、建立全部 WebSocket、Login、EnterRoom 與 warm-up。
2. load 以 atomic rename 寫入 `echo-ready.json`，等待同 `run_id` 的 `echo-start.json`。
3. script 保存 load/Gate/Game baseline metrics，然後寫入 `echo-start.json`。
4. load 收到 start 後才取 `measurement_start`、計算 `admission_end` 並放行 workers。
5. admission end 只停止新 request；已發出 request 仍以 `request-timeout` 取得 terminal result。
6. workers 完成後，load 關閉自己的 WebSocket connections，以 atomic rename 寫入
   `echo-measured.json`，但保持 load metrics listener。
7. script 在 bounded timeout 內等待 Gate terminal gauges 回到零，保存 final metrics，再寫入
   `echo-final-scraped.json`。
8. load 收到 marker 後才 shutdown metrics listener 並以 success/failure status 離開。

等待 marker 必須有 `2m` bounded timeout，context cancellation 必須立即中斷等待。Marker 缺失、
JSON 不合法、`run_id` 不同或 timeout 都使該 attempt invalid，不可繼續當成容量樣本。

### 4.5 自動化腳本

新增：

```text
examples/metrics/scripts/run-echo-performance-validation.sh
examples/metrics/scripts/run-echo-performance-validation-test.sh
```

腳本只負責本實驗，不與 server-send validation 腳本共用通用 framework。可複用既有
小型 shell helper 寫法，但不為了去重建立跨腳本 library。

腳本必須：

1. 在 campaign 開始前 build Gate、Game、load 各一次，後續不在 process 運行期間編譯。
2. 記錄 `go version`、grpc-go module version、OS、logical CPU、Git HEAD、dirty state 與三個
   binary SHA-256。
3. 每次 attempt 都啟動 fresh Gate、Game、load；Redis 可持續運行。
4. 只用本次啟動後保存的 PID 停止 process，不使用 `pkill`、process-name glob 或影響
   staging 的 Git 操作。
5. 開始前檢查 Redis，以及 `18080/18081/19080/19090/22081` 沒有舊 listener。
6. 以 `GOMAXPROCS=2` 啟動三個 binary，等待 Game ready 後再啟 Gate，Gate ready 後再啟
   load。
7. 等待 `echo-ready.json`，保存 baseline metrics，再透過 `echo-start.json` 放行測量。
8. measured window 期間每 `1s` 保存 load/Gate/Game `/metrics` 與 host/process sample。
9. 保存 `echo-measured.json`、final metrics、所有 logs 與 attempt status。
10. 正常停止 load，再依反向 lifecycle 停止 Gate 與 Game；等待 PID 結束並確認 ports
    釋放後才進入下一次。
11. 任一主要步驟失敗時仍保留 artifacts，將 attempt 標記 invalid 並停止本次 processes。
12. 腳本不自動重試 invalid attempt，避免無限執行；報告須列出 invalid 理由，由
    執行者處理環境後明確重跑。

腳本參數只保留本實驗需要的 override：

```text
--artifact-dir
--connections              # 單點重跑時使用；預設 staircase
--attempts                 # 預設 3
--duration                 # 預設 30s
--payload-bytes            # 預設 32
--gate-client-write-buffer-bytes  # 預設 0
--game-server-write-buffer-bytes  # 預設 0
--profile                  # 預設 false，只用於獨立 profile-only run
```

腳本對 write buffer 參數使用既有 environment override，不產生或改寫 tracked YAML：

```text
CORE_CASINO_METRICS_GATE__GRPC__CLIENTS__GAME__WRITE_BUFFER_SIZE_BYTES
CORE_CASINO_METRICS_GAME__GRPC__SERVER__WRITE_BUFFER_SIZE_BYTES
```

`GOMAXPROCS=2`、route=game、connections_per_host=1、setup/warm-up/request timeout 是實驗 contract，
不提供 CLI override，避免產生外觀相同但無法比較的 run。要變更這些 contract 時必須
修訂設計與報告版本。

---

## 5. Artifact contract

每次 campaign 使用獨立目錄：

```text
artifacts/echo-performance/<UTC-run-id>/
  manifest.txt
  binaries.sha256
  campaign-status.json
  summary.tsv
  host/preflight-top.txt
  connections-0100/
    attempt-01/
      metadata.json
      run-status.json
      orchestration/
      control/
      logs/{game,gate,load}.log
      metrics/
        {game,gate,load}-baseline.prom
        {game,gate,load}-samples.tsv
        {game,gate,load}-final.prom
      host/top.txt
      host/processes.tsv
      host/socket-queues.tsv
      host/nettop.csv
    attempt-02/...
    attempt-03/...
  profiles/
    <selected-condition>/...
```

`metadata.json` 至少包含：

- run ID、connection level、attempt number、route、payload、duration、warm-up、timeouts；
- 三個 GOMAXPROCS；
- Gate client/Game server write-buffer config；
- connections_per_host 與 MaxConcurrentStreams；
- binary checksums 與測試起迄 UTC timestamps。

`run-status.json` 必須保留機器可判讀的 `valid` boolean 與 bounded reasons，不只寫自由文字。
可用 reasons：

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
external_host_load
```

raw artifacts 是報告證據，不需加入 Git staging。報告必須寫出 artifact root 與每個關鍵結論
對應的相對檔案，不只寫「已觀察」。

---

## 6. 基準實驗執行流程

### 6.1 Preflight 與 build-once

1. 確認 Redis ping 成功。
2. 確認本次 ports 無舊 listener。
3. 確認 host 沒有已知的高 CPU 工作；記錄 preflight host CPU，不自動停止
   OrbStack 或任何使用者 process。
4. 編譯三個 binary 至 campaign artifact directory，保存 checksum。
5. 從此刻起到 campaign 結束不再編譯；若 code/config 修改，必須建立新 campaign。

### 6.2 Correctness run

先執行 `connections=1`、`duration=15s`、一次 attempt。必須滿足第 7 節所有 correctness
條件才可開始 staircase。這筆不納入容量報告中位數。

### 6.3 Capacity staircase

依序執行 `100 → 200 → 400 → 800 → 1600 → 3200` connections。每個 level：

1. 執行三次 fresh-process attempt。
2. 只從 valid attempts 計算中位數與 min–max；少於三次 valid 就不產生該 level
   容量結論。
3. 完成三次後套用停止條件，符合就不進入更高 level。

不在單次 attempt 中 ramp concurrency；每次只有一個 connection level，讓 metrics、process 與 host
樣本有明確對應。

### 6.4 容量停止條件

令當前 level 與前一 level 的三次中位數為 `RPS_current/RPS_previous` 與
`P95_current/P95_previous`。任一項成立即停止增加 connections：

1. 任一 attempt 出現 request error、timeout、unexpected close、write queue full 或 terminal
   counter mismatch；先判定 correctness/root cause，不用更大壓力掩蓋問題。
2. 當前 RPS 中位數成長 `<= 5%`，且當前 p95 中位數增加 `>= 10%`。
3. 當前 RPS 中位數成長 `<= 5%`，且同窗口有一個 saturation evidence：
   - load、Gate 或 Game process CPU 持續接近其 `GOMAXPROCS=2` 可用 CPU；
   - host idle 持續接近零；
   - scheduler latency 相對前一 level 顯著惡化；
   - gRPC/WebSocket in-flight 或 write queue 隨壓力上升且不回落。
4. 環境限制無法建立下一 level 的全部 WebSocket connections。這是 setup/host 限制，
   不可記成 Echo RPS 平台。

「持續」代表 measured window 內至少連續 5 個 1s samples；不用單點 spike 宣稱飽和。
若 RPS 成長 `<=5%` 但 latency 與所有資源訊號都穩定，允許再執行下一 level 一次；
若 RPS 仍無成長，停止並將平台標為「尚未定位」。

---

## 7. Attempt 有效性與 correctness contract

一次 attempt 只有全部成立才是 valid：

1. Game、Gate readiness 在 timeout 內成功，load exit status 為 `0`。
2. `echo-ready`、`echo-start`、`echo-measured`、`echo-final-scraped` 的 run ID 完全相同。
3. baseline 的 load Echo counters 為零；warm-up 不可進入 load-private measured metrics。
4. `echo-measured.json.connection_failures=0`，且 load `error/cancelled` delta 為零。
5. 三個 metrics endpoint 的 `go_sched_gomaxprocs_threads` 都為 `2`。
6. 完成 drain 後，下列 measured success counter delta 必須等於 load completion log/marker 的 `N`：

```text
load Echo success
  = Gate websocket forward success
  = Gate gRPC code=OK
  = Game Echo command success
  = Game request_player success
  = Gate connection enqueue queued
  = Gate server_send WebSocket write success
```

7. 對應 success histogram `_count` delta 也必須等於 `N`。
8. final snapshot 的 Gate/Game command、gRPC、WebSocket write in-flight 都為零；Gate write queue
   messages 為零，WebSocket connections 在 load 關閉後於 bounded drain 內回到零。
9. Gate gRPC non-OK、command error、Game handler/server-send error、WebSocket write error、queue full
   與非預期 close-reason counters 的 delta 都為零。Load 在 final drain 以正常 WebSocket close
   關閉每條已接受 connection，因此 `gaming_core_gate_websocket_connection_closes_total{reason="client_closed"}`
   的 delta 必須等於 connection level，其他 reason 的 delta 必須為零。
10. baseline/final/periodic metrics、host sample 與三份 log 都存在且非空。

Histogram 間只比較各自的 quantile 與 mean，不以「client p95 減 Gate p95」推導某一段
latency。不同 histogram 的 samples 並非同一筆 request 的 pairwise timestamps。

---

## 8. 必須收集的數據

### 8.1 Load/client

- completion marker：successful requests、connection failures、measurement start、admission end、
  measurement end 與 actual elapsed。
- `gaming_core_example_load_echo_round_trips_total{result}`
- `gaming_core_example_load_echo_round_trip_duration_seconds`
- `gaming_core_example_load_echo_round_trips_in_flight`
- process CPU、RSS、goroutines、threads、heap、GC、scheduler latency 與 GOMAXPROCS。

RPS 使用 `successful_requests / (admission_end - measurement_start)`，不使用名義 30 秒或
`measurement_end` 取代 admission window。`measurement_end - admission_end` 另外報告為 terminal drain
overrun。

### 8.2 Gate

- WebSocket active/accepted/close reason、command rate/error/duration/in-flight。
- Gate→Game gRPC code/rate/duration/in-flight。
- WebSocket write rate/error/duration/in-flight。
- write queue messages/capacity/full total。
- server-send connection enqueue 與 receive-to-write terminal duration。
- Redis pool total/idle/pending/wait/wait seconds/timeouts，用來識別 session renewal 或依賴壓力，
  不宣稱 Echo request 使用 Redis。
- process/runtime metrics：CPU、RSS、goroutines、threads、heap、GC、scheduler latency、GOMAXPROCS。

### 8.3 Game

- Echo command success/error/duration/in-flight。
- request-player success/error/duration/in-flight；此 Echo reply 走 unary reply slot，不應增加 async
  player/broadcast queue、dependency 或 fallback counters。
- process/runtime metrics：CPU、RSS、goroutines、threads、heap、GC、scheduler latency、GOMAXPROCS。

### 8.4 Host/OS

- 每秒 host CPU user/system/idle。
- load、Gate、Game 的 process CPU、threads、RSS、context switches、syscall counters；macOS 以
  `top` 的 `cpu`、`threads`、`mem`、`csw`、`sysbsd`、`sysmach` 欄位保存原始樣本，
  `processes.tsv` 也保留同欄位供逐秒比對。
- loopback TCP connection count、bytes、RTT 與可用的 retransmission 欄位。
- 平台點的 socket Send-Q/Recv-Q 快照；單一快照只是候選證據，不可宣稱 sustained
  backpressure。

macOS `top` process `%CPU` 的 100% 約代表一個 logical CPU；報告必須同時提供
raw `%CPU` 與以 host logical CPU 數正規化的比率，不得將 process 200% 寫成佔整機
200%。

---

## 9. 瓶頸診斷流程

在首個符合第 6.4 節的容量平台點，依序進行：

1. 先用 baseline metrics 與 host samples 判斷是否已有單一明確 saturation signal。
2. 在同 connection level 另跑一次 profile-only run，同時取 load/Gate/Game 20s CPU profile、
   goroutine dump 與 5s Go trace。Profile run 不納入三次中位數。
3. 先看 on-CPU top stacks，再用 trace 區分 runnable scheduler delay、network poll、stream admission、
   writer/flush 與 GC/assist。
4. 只有 Game route 與 profile 仍無法區分「WebSocket/Gate 共用路徑」與「gRPC/Game 區段」時，
   在同 connection level 執行 local Echo 三次對照。
5. 一旦兩種以上獨立證據指向同一邊界，就停止加入診斷點，開始寫報告。

Profile-only run 使用 Gate `127.0.0.1:18182`、Game `127.0.0.1:19182`、load
`127.0.0.1:22083` 三個 loopback pprof listeners。三份 20s CPU profile 必須並行抓取，才是同一
load window；5s trace 另開一次 profile-only run 並並行抓取，不與 CPU profile 同時進行。

結論用詞必須與證據層級相符：

| 證據 | 可以下的結論 | 不可下的結論 |
|---|---|---|
| RPS 平台 + process 接近 2 cores + host idle 低 | 本次 topology 受 CPU/shared-host 容量限制 | 實體機器或 grpc-go 有 defect |
| Game handler 平穩，Gate gRPC 與 E2E 同時惡化 | 等待在 Gate gRPC call 至 response 區段 | 一定是 write buffer |
| CPU profile writer/flush/syscall 顯著 + syscall/context switch 高 | write path 是可驗證候選 | 調大 buffer 一定有效 |
| socket queue 連續堆積 + writer 停在 network write | kernel/receiver backpressure 候選 | 只是 buffer 太小 |

---

## 10. WriteBufferSize 條件式 A/B

### 10.1 觸發條件

只有同時滿足下列條件才執行：

1. 已找到第 6.4 節定義的容量平台；
2. Gate 或 Game CPU profile 的 top cumulative stacks 出現 grpc-go `loopyWriter`、`bufWriter.Flush`、
   `flushKeepBuffer`、`net.Conn.Write` 或 syscall write 路徑；
3. 同期 host/process samples 顯示 CPU、syscall 或 context-switch 壓力；
4. Echo correctness 無 error，避免用 tuning 掩蓋功能問題。

若 profile 主要成本在 handler、protobuf、GC 或 WebSocket writer，就不執行 WriteBuffer A/B。

### 10.2 為什麼 client/server 分開

同一條 HTTP/2/TCP connection 的兩端各自擁有 writer buffer：

- Gate client config 影響 Gate → Game request frames。
- Game server config 影響 Game → Gate unary response frames。

同時調整會失去因果歸屬，所以不允許作為第一個 controlled run。

### 10.3 固定矩陣

在首個平台點，每個條件各三次 fresh-process valid attempts：

| 條件 | Gate client | Game server | 目的 |
|---|---:|---:|---|
| A | default (`0`) | default (`0`) | 同日、同 binary baseline |
| B | 64 KiB | default | 單獨驗證 request writer |
| C | default | 64 KiB | 單獨驗證 response writer |
| D | 64 KiB | 64 KiB | 只有本節下方的組合觸發條件成立才執行 |

不直接把舊 campaign baseline 當 A，因為 host background load 與 thermal state 可能已變化。A/B/C
必須使用同一組 binary，只透過 config/env override 改變一個值。

B 或 C 只有同時滿足下列條件才算有效：

- RPS 中位數相對 A 提高至少 `5%`；
- RPS 相對改善幅度大於 A 的 `MAD_RPS / median_RPS` 相對波動；
- client p95 不惡化超過 `5%`；
- error、timeout、counter mismatch 仍為零；
- process/host 證據至少一項符合預期，例如相同 RPS 下 CPU、syscall 或 context switches 下降。

若 B 與 C 都不成立，立即停止，不執行 D 或 128 KiB。若只有一側成立，先將該側
單獨提高至 128 KiB 驗證收益是否繼續；另一側保持 default。只有 B/C 兩側皆有
效，或單側有效且 profile 顯示另一方向仍是明確成本時，才執行 D。

`WriteBufferSize` 是當次 userspace batching capacity，不是「實際每次送出 bytes」。本輪不包裝
`net.Conn`、不量 per-write bytes；以 controlled outcome 與 profile/syscall 證據回答「提高是否有效」。

---

## 11. 報告 contract

新增：

```text
CLIENT_GATE_GAME_ECHO_PERFORMANCE_VALIDATION_REPORT.md
```

報告必須依以下順序，不得只貼 console log：

### 11.1 執行摘要

- 先用 3–5 句回答容量平台、RPS、p95/p99、error 與最有證據的瓶頸邊界。
- 明確寫出是「已定位」、「縮小至某邊界」或「證據不足」。
- 若沒有觸發 WriteBuffer A/B，寫明觸發條件為何不成立；不留空白章節。

### 11.2 版本、環境與 topology

- artifact root、UTC 起迄、Git/binary identity、Go/grpc-go version、OS/logical CPU。
- 三個 `GOMAXPROCS=2` 的 metrics 證據。
- 完整 request/reply 路徑，並說明 Redis 不在 per-Echo hot path。
- 是同機 loopback 實驗，不將結果外推為 production capacity。

### 11.3 Attempt validity

列出每個 connection level 的 3 次 attempt status、invalid reason 與 `run-status.json` 路徑。
只有 3/3 valid 的 level 進入中位數表。

### 11.4 容量結果

每個 valid level 至少報告：

| 欄位 | 必要內容 |
|---|---|
| RPS | 三次原值、中位數、min–max、相對前一 level 成長 |
| client latency | mean、p50、p95、p99，三次中位數 |
| server stages | Gate command、Gate gRPC、Game handler、Gate delivery、WebSocket write 各自 p95 |
| correctness | success delta equality、errors、timeouts、unexpected closes |
| saturation | load/Gate/Game CPU、RSS、goroutines、GC、scheduler p99、in-flight/queue max |
| host | CPU idle min/median、context switches、syscall delta、socket observation |

Histogram quantile 使用每次 attempt baseline→final bucket delta 計算，不使用 process 啟動以來的
cumulative buckets。Process CPU 以 measured window 前後 `process_cpu_seconds_total` delta / actual elapsed
計算，不包含 setup/warm-up/drain。

### 11.5 瓶頸證據鏈

用表格將每個結論連到數據與原始檔案：

| 問題 | 關鍵數據 | 原始證據 | 可支持的推論 | 不能推論的事 |
|---|---|---|---|---|

範例：RPS 平台、Game handler 穩定、Gate gRPC p95 上升，只能將問題縮小到
Gate gRPC call/Game transport 區段；除非 profile/trace/host 另有證據，不能直接寫「grpc-go 或 OS
是根因」。

### 11.6 Profile-only 與 WriteBuffer A/B

- 列出 profile condition、observer effect 邊界、CPU top stacks、trace waiting 與原始 profile 路徑。
- 若執行 WriteBuffer A/B，提供 A/B/C（與條件式 D）各三次 RPS/latency/CPU/syscall
  數據、中位數、MAD 與判定。
- 結論必須指明是 Gate client、Game server、兩者，或兩者皆無可重複收益。

### 11.7 結論、限制與唯一下一步

報告最後只列一個由當前證據觸發的下一步。若證據已足夠，寫「不需要調整」；
不同時建議增加 connection、調 OS、改 protocol、加 metrics 與改 buffer。

---

## 12. 測試策略

### 12.1 Unit/contract tests

- `pkg/gatelink`：`0` 與 positive write buffer 合法，negative 失敗；驗證 ClientConfig
  normalization 與 connection option branch。
- `pkg/grpcserver`：`0` 與 positive 合法，negative 失敗；現有 interceptor 與
  MaxConcurrentStreams 組成不受影響。
- `products/gateproduct`：YAML bind/model 完整映射 `write_buffer_size_bytes`，negative error
  包含完整 config path。
- `examples/metrics/load`：Echo orchestration 的 marker 順序、run ID、timeout、cancellation、
  atomic write 與未設 directory 時的舊行為。
- shell contract test：以 fixture metrics/markers 驗證 counter equality、GOMAXPROCS、terminal gauges、
  plateau 計算與 invalid reason；不在 shell test 啟動真實壓測。

### 12.2 Compile/test 驗證

實作完成、使用者審閱後才執行實際 campaign。實際壓測前至少通過：

```sh
go test ./pkg/gatelink ./pkg/grpcserver ./products/gateproduct ./products/gameproduct ./examples/metrics/load
bash examples/metrics/scripts/run-echo-performance-validation-test.sh
bash -n examples/metrics/scripts/run-echo-performance-validation.sh
go test ./...
go vet ./...
git diff --check
```

再以 race detector 覆蓋本次有 concurrency 修改的 packages：

```sh
go test -race ./pkg/gatelink ./pkg/grpcserver ./examples/metrics/load
```

---

## 13. 錯誤處理與 lifecycle

- config error 在 bind/constructor 期間失敗，不啟動 partial gRPC topology。
- orchestration 是 example-only；任一 marker failure 只使 attempt invalid，不改 framework readiness
  或 production lifecycle。
- 腳本收到 interrupt 時停止本次 load、Gate、Game 並保留已產生 artifacts。
- 停止 process 有 bounded grace period；超時才對已保存 PID 強制終止，並將 attempt
  標記 invalid。
- 不清除 Redis database、不停止 OrbStack、不刪除舊 artifacts，不執行會變更 Git staging
  的操作。

---

## 14. 關鍵決策與取捨

### 14.1 基準使用 GOMAXPROCS=2

當前 host 同時執行 load、Gate、Game、Redis 與 OrbStack，過去三個 Go process 各使用 4 P 會
超過可穩定隔離的 CPU 預算。全部改為 2 P 使資源條件明確，但也代表新結果
不可與過去 GOMAXPROCS=4 數字直接當成 regression comparison。

### 14.2 三次中位數而非單次最高值

單機 loopback benchmark 會受 scheduler、thermal 與 background process 影響。三次是辨識單次
outlier 的最小數量；中位數不被單一極端值主導。

### 14.3 不增加 WriteBuffer metrics

grpc-go 沒有公開每條 connection buffer occupancy/flush-size metric。包裝 `net.Conn` 會引入
診斷 instrumentation 與額外 hot-path 成本；本次的問題是「調高是否改善」，用條件式
A/B 可以直接回答，無需把 grpc-go internal detail 變成 framework metric contract。

### 14.4 Client/server 不同時調整

單獨調整才能判斷 request 或 response writer 的收益。代價是最多多兩組實驗，但可避免
得到「一起改有效」卻無法建立可維運 config 的結論。

### 14.5 Fresh process 而非重用長時間 service

每次 attempt 重啟三個 process，可避免 cumulative counters、舊 connection pool、過去 GC state
與前一輪 session 影響判讀。Build-once 又確保三次使用完全相同 binary。

---

## 15. 已知限制與後續擴充

- closed-loop workload 固定 in-flight 上限，沒有量測 open-loop overload 或 queueing SLO。
- 32-byte Echo 代表小型 unary message，不可推導大 payload 或 streaming RPC 的 write-buffer 效果。
- 同機 loopback 無法分離 load、service 與 kernel 的獨立 production capacity。只有需要這個數字時，
  才於後續將 load 與 services 拆到不同 host。
- WriteBuffer 只能在建立 connection/server 時設定，不支援 hot reload。
- 若未來需要回答實際 `net.Conn.Write` size distribution，可另案增加 example-only dialer/listener
  wrapper；本次不預先建立。

---

## 16. 修改檔案清單與實作順序

### 16.1 Production/framework 必要修改

- `pkg/gatelink/client.go`：client config validation 與 conditional dial option。
- `pkg/gatelink/*_test.go`：client write-buffer config contracts。
- `pkg/grpcserver/server.go`：server config validation 與 conditional server option。
- `pkg/grpcserver/*_test.go`：server write-buffer config contracts。
- `products/gateproduct/grpc.go`：Gate `grpc.clients.game` bind/mapping/validation。
- `products/gateproduct/grpc_contract_test.go`：product config path 與 mapping contracts。
- 四個第 4.3 節列出的 YAML：完整 config 註解。

### 16.2 Example validation 必要修改

- `examples/metrics/load/main.go`：將 orchestration dir 傳入 Echo path，並保持未設定時的
  現有行為。
- `examples/metrics/load/echo_orchestration.go`：新增 bounded marker protocol。
- `examples/metrics/load/*_test.go`：orchestration contract tests。
- `examples/metrics/scripts/run-echo-performance-validation.sh`：campaign runner 與 artifact collector。
- `examples/metrics/scripts/run-echo-performance-validation-test.sh`：fixture-based shell tests。
- `.gitignore`：忽略本實驗產生的 raw artifacts，不將測試輸出誤加入 staging。
- `CLIENT_GATE_GAME_ECHO_PERFORMANCE_VALIDATION_REPORT.md`：執行實驗後建立，不在無數據時
  預填結論。

### 16.3 順序

1. 實作並測試 client/server write-buffer config，預設行為必須不變。
2. 實作 Echo orchestration 與 Go contract tests。
3. 實作 campaign script 與 fixture-based shell tests。
4. 完成 compile、unit、race、vet 與 diff checks，交由使用者審閱。
5. 審閱後執行 correctness run 與 capacity staircase。
6. 於首個平台點執行 profile-only run，依觸發條件決定是否執行 WriteBuffer A/B。
7. 從 raw artifacts 產生報告與唯一下一步。

---

## 17. Self review：必要性與無過度設計

| 需求 | 對應設計 | 必要性 |
|---|---|---|
| 完整 Client→Gate→Game→Gate→Client Echo | 固定 `echo-route=game` 與既有 protocol | 必要，未新增業務 API |
| 資源不足時使用 2 P | 三個 process 固定 `GOMAXPROCS=2` 並用 metrics 驗證 | 必要 |
| 實作方不得誤解測量邊界 | orchestration baseline/final barriers、validity contract、artifact schema | 必要，解決現有 race |
| RPS/latency/error/saturation | load/Gate/Game metrics 與 host samples | 必要，全部使用現有 metrics |
| 找出瓶頸 | staircase → profile-only → conditional local control | 必要且有停止條件 |
| 保留 WriteBuffer 調整能力 | client/server 獨立 optional config | 必要，符合兩個獨立 writer |
| 判斷調大是否有效 | 條件式 A/B/C/D 與收益門檻 | 必要，不先調 production default |
| 完整報告 | 固定章節、原始證據對應、結論邊界 | 必要 |

已刻意排除 WriteBuffer runtime metrics、per-connection labels、server-send client config、預設 buffer
tuning、全矩陣 local Echo、通用 benchmark framework、dashboard/alerts 與線上 hot reload。
這些都不是回答本次效能與瓶頸問題的必要條件。

最小不可再刪減的核心是：

1. warm-up 後 baseline 與 final-scrape barrier；
2. fresh-process、build-once、三次中位數的 staircase；
3. load/Gate/Game/host 同窗口證據與 attempt validity；
4. 平台點的 profile-only 診斷；
5. client/server 獨立且條件式的 WriteBuffer A/B；
6. 可對應 raw artifact 的報告 contract。

因此本設計已剛好覆蓋需求，並沒有把診斷手段擴張為無關的 production
observability 或 transport 重構。
