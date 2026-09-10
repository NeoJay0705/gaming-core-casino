# Gate → Game → Gate Metrics 驗證方法

## 1. 目的與結論

本文件定義如何使用 `examples/metrics` 驗證進房後持續執行 Echo 時，Gate → Game → Gate 的 metrics
是否正確，並把觀測責任分成兩側：

- load client 觀測玩家實際感受到的 Echo round-trip latency、完成率與 request rate；
- Gate、Game 與 Redis 暴露 server-side metrics，用來定位 latency、error 與 saturation 發生在哪一段。

兩側資料只在相同測試時間窗口內綜合分析。不同 process 的 timestamp 或 Histogram quantile 不可直接
相減，也不把 client-side metrics 加入 framework product。

現有 Gate／Game metrics 已足以觀測 server-side 路徑，不需要新增 product metrics。Load example
使用獨立 Prometheus registry 記錄第 3 節的 Echo round-trip，並保留結束時的精確完成總數。

## 2. 測量邊界

測試流程固定為每條 WebSocket connection 先完成 Login → EnterRoom，之後只重複 Echo。Login 與
EnterRoom 是 setup traffic，不納入 Echo latency 與 Echo request 數量。

### 2.1 Client-side round trip

每筆 Echo 的計時範圍：

1. 在 load client 呼叫 `WriteMessage` 送出 Echo request 之前取得 `startedAt`。
2. 等待同一條 connection 收到預期的 Echo response frame。
3. frame header 與 command ID 驗證成功後，記錄 `success` 與 `time.Since(startedAt)`。
4. write、read、frame validation 或 connection error 時，記錄同一筆 request 為 `error` 並結束該
   connection；若錯誤只因全域測試 duration 到期，則記為 `cancelled`。一筆 request 只能有一個
   terminal result。

此數值包含 client write、網路、Gate ingress、Gate-to-Game gRPC、Game handler、server-send、Gate
WebSocket write 與 client read，是玩家角度的 end-to-end latency。

### 2.2 Server-side stages

同一筆成功 Echo 應依序形成下列 samples：

| 階段 | Metric 與必要 labels |
|---|---|
| Gate 接收並 forward | `gaming_core_gate_websocket_commands_total{route="game",command="forward",result="success"}` |
| Gate command latency | `gaming_core_gate_websocket_command_duration_seconds{route="game",command="forward",result="success"}` |
| Gate → Game gRPC | `gaming_core_gate_game_grpc_requests_total{code="OK"}`、`gaming_core_gate_game_grpc_duration_seconds{code="OK"}` |
| Game Echo handler | `gaming_core_game_gate_commands_total{command="4043309073",result="success"}`、`gaming_core_game_gate_command_duration_seconds` |
| Game request-player | `gaming_core_game_server_send_requests_total{operation="request_player",result="success"}`、`gaming_core_game_server_send_duration_seconds` |
| Gate 收到 server-send | `gaming_core_gate_server_send_requests_total{target="connection",result="queued"}` |
| Gate 寫回 client | `gaming_core_gate_websocket_writes_total{source="server_send",result="success"}` |
| Gate receive 至 write terminal | `gaming_core_gate_server_send_delivery_duration_seconds{target="connection",result="success"}` |

Echo request ID `0xF1000011` 的十進位值是 `4043309073`。Gate 使用 bounded label `forward` 是刻意的；
只有能確認 command 已註冊的 Game 才使用 numeric command label。

## 3. Load client 的最小觀測資料

為了讓觀測端自行監測，load example 應使用自己的 Prometheus registry，不能重用 Gate 或 Game registry。
只需要以下三個 example-local metrics：

| Metric | Type / labels | 用途 |
|---|---|---|
| `gaming_core_example_load_echo_round_trips_total` | Counter `{result}` | Echo terminal rate/error；`result` 只允許 `success`、`error`、`cancelled` |
| `gaming_core_example_load_echo_round_trip_duration_seconds` | Histogram `{result}` | Client-side end-to-end latency |
| `gaming_core_example_load_echo_round_trips_in_flight` | Gauge | 已送出但尚未收到 terminal result 的 Echo 數量 |

只增加一個 loopback metrics listener flag `-metrics-addr 127.0.0.1:22081`，暴露
`/metrics`。load client 不是 framework product，不需要 `/health`、`/ready`、DI module 或通用 HTTP
server abstraction。Histogram 第一版沿用 `prometheus.DefBuckets`，待基準壓測顯示 buckets 不適用時再調整。

現有結束 log 的 `echo_requests` 必須保留，並繼續代表成功收到的 Echo response 數；它是單次執行的
精確完成數。Prometheus time series 用於觀察測試期間的 rate 與分布，不能因 scrape timing 取代最終 log。

不得加入 connection ID、login name、room ID、sequence、payload、error text 或 run ID labels，避免
cardinality 隨壓測規模成長。

## 4. 執行方式

### 4.1 啟動依賴與 services

先啟動本機 Redis，再從 repository root 分別啟動 Game 與 Gate：

```sh
go run ./examples/metrics/game
go run ./examples/metrics/gate
```

確認 Gate／Game 的 readiness 與 metrics endpoints 可取得資料：

```sh
curl --fail http://127.0.0.1:19080/ready
curl --fail http://127.0.0.1:18081/ready
curl --fail http://127.0.0.1:19080/metrics
curl --fail http://127.0.0.1:18081/metrics
```

固定 endpoints：

- Game metrics：`http://127.0.0.1:19080/metrics`
- Gate metrics：`http://127.0.0.1:18081/metrics`
- load metrics：`http://127.0.0.1:22081/metrics`

### 4.2 Prometheus scrape

使用同一個 Prometheus instance scrape 三個 targets，建議第一輪使用 `1s` scrape interval，以免短測試
只有少量 samples：

```yaml
scrape_configs:
  - job_name: metrics-example-gate
    scrape_interval: 1s
    static_configs:
      - targets: ["127.0.0.1:18081"]

  - job_name: metrics-example-game
    scrape_interval: 1s
    static_configs:
      - targets: ["127.0.0.1:19080"]

  - job_name: metrics-example-load
    scrape_interval: 1s
    static_configs:
      - targets: ["127.0.0.1:22081"]
```

若 Prometheus 跑在 container 內，targets 必須改成 container 可連到 host 的位址，不能沿用 container
自己的 `127.0.0.1`。

### 4.3 先做 correctness run

先以低 concurrency 執行，目的是核對 counter 與 terminal state，不用來判斷容量：

```sh
go run ./examples/metrics/load \
  -connections 1 \
  -duration 15s \
  -payload-bytes 32 \
  -metrics-addr 127.0.0.1:22081
```

執行前後各保存一次 Gate 與 Game `/metrics` response，以 counter delta 驗證第 5 節。測試結束後至少再
等待兩個 scrape intervals，讓 Gate／Game 的 terminal gauges 與最後一筆 counter 被收集；Gate 與 Game
在取完 final snapshot 前保持運行。

### 4.4 再做 pressure run

correctness run 通過後才逐步增加 connections 或 payload，例如：

```sh
go run ./examples/metrics/load \
  -connections 8 \
  -duration 60s \
  -payload-bytes 256 \
  -metrics-addr 127.0.0.1:22081
```

每次只改一個主要變因，並記錄 connections、duration、payload bytes、程式版本及測試起訖時間。
測試環境允許時，load client 與 services 分開執行，避免 load generator 的 CPU 或 file descriptor 壓力
和 server saturation 混在同一台機器上。

## 5. Correctness 驗收

以測試前後 counter snapshot 的差值 `Δ` 驗證。若 load 結束 log 的 `echo_requests=N` 且
`failures=0`，穩定狀態下以下成功 counter delta 都必須等於 `N`：

```text
load successful Echo round trips
  = Gate forward success
  = Gate gRPC OK
  = Game Echo handler success
  = Game request-player success
  = Gate server-send queued
  = Gate server-send WebSocket write success
```

相對應 success Histogram 的 `_count` delta 也必須等於 `N`。不要比較 `_sum` 或單筆 duration 的精確值；
duration 只要求非負且 observation 數量正確。

Load observer 的 contract test 必須確認
`gaming_core_example_load_echo_round_trips_in_flight` 在所有 terminal results 後回到 `0`。短生命週期
load process 結束時會同步關閉 `/metrics`，因此不要求 Prometheus 恰好 scrape 到最後的零值。

測試停止並完成收尾後，從仍在運行的 Gate／Game final snapshot 確認下列 gauges 回到 `0`：

- `gaming_core_gate_websocket_commands_in_flight`
- `gaming_core_gate_game_grpc_in_flight`
- `gaming_core_game_gate_commands_in_flight`
- `gaming_core_game_server_send_in_flight{operation="request_player"}`
- `gaming_core_gate_websocket_writes_in_flight`
- `gaming_core_gate_websocket_write_queue_messages`
- `gaming_core_gate_websocket_connections`

另外確認以下 error signals 沒有增加：

- Gate command `result="error"`
- Gate gRPC `code!="OK"`
- Game command或 server-send `result="error"`
- Gate WebSocket write `result="error"`
- Gate write queue full
- 非預期 connection close reasons

全域 duration 到期時，每條 connection 最多可能有一筆 client `result="cancelled"`；這是正常停止，
不算壓測錯誤。`result="error"` 才表示非預期的 client-side terminal failure。

正常結束應記為 `client_closed`；若出現 `read_error`、`forward_error`、`write_error`、
`write_queue_full` 或 `panic`，本次 correctness run 不通過。

Counter delta 的相等關係只用於停止送流並完成收尾後的 final snapshots。測試進行中因 scrape 時序及
server-send／WebSocket write 是非同步的，短暫不相等是正常現象。

## 6. 綜合分析方式

### 6.1 Rate 與 Error

比較相同窗口的 client completion rate 與各 server stage rate：

```promql
rate(gaming_core_example_load_echo_round_trips_total{result="success"}[1m])
rate(gaming_core_gate_websocket_commands_total{route="game",command="forward",result="success"}[1m])
rate(gaming_core_game_gate_commands_total{command="4043309073",result="success"}[1m])
rate(gaming_core_gate_websocket_writes_total{source="server_send",result="success"}[1m])
```

若前段 rate 高於後段，沿路檢查 bounded error counters、gRPC code、queue full 與 connection close
reason。不要用 log error text 建立新 label。

### 6.2 Latency

各 Histogram 獨立計算 p95，例如 client round trip：

```promql
histogram_quantile(
  0.95,
  sum by (le) (
    rate(gaming_core_example_load_echo_round_trip_duration_seconds_bucket{result="success"}[1m])
  )
)
```

對 Gate gRPC、Game handler、Game server-send、Gate server-send delivery 與 Gate WebSocket write 使用
相同形式查詢。判讀原則：

- client p95 與 Gate command／gRPC 同時升高：優先檢查 Gate-to-Game 或 Game handler；
- Game handler 平穩，但 Game server-send 升高：檢查 Redis routing、Gate receiver 或 gRPC transport；
- server-send delivery 或 WebSocket write 升高：檢查 write queue、slow client 與 socket write；
- 所有 server stages 平穩，只有 client round trip 升高：檢查 client、網路或 load generator saturation。

Histogram quantile 是不同事件集合的統計結果，不能用「client p95 減 Gate p95」推導網路 latency。

### 6.3 Saturation

在 latency 或 error 上升的同一窗口檢查：

- Gate／Game in-flight；
- Gate write queue messages 相對於 write queue capacity；
- `gaming_core_gate_websocket_write_queue_full_total`；
- Redis pending requests、wait total、wait seconds 與 timeouts；
- Go goroutines、heap／GC、process CPU、resident memory 與 file descriptors；
- load observer 的 in-flight 是否長時間等於 configured connections。

Echo workflow 不使用 Database，不應為這個測試虛構 DB 流量。Database pool metrics 由其獨立 contract
或實際使用 Database 的業務流程驗證。

## 7. 範圍限制

本方法不新增 distributed tracing、跨 process request ID、Grafana dashboard、alert threshold、failure
injection、distributed load workers 或通用 benchmark framework。第一輪目標是確認 metric contract 與
觀測鏈完整；容量上限、SLO 與 Histogram buckets 必須根據後續基準資料決定。
