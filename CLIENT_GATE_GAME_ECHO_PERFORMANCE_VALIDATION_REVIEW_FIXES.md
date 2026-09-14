# Client → Gate → Game Echo 效能驗證必要修正

## 1. 結論

本文件將 `CLIENT_GATE_GAME_ECHO_PERFORMANCE_VALIDATION_DESIGN.md` 與目前 staged changes
的 review 結果整理成可直接實作的必要修正。修正後的功能邊界不變：

- 不修改 Echo protocol、dispatcher、unary reply 或 WebSocket data path。
- 不新增 Prometheus metrics、WriteBuffer runtime instrumentation 或通用 benchmark framework。
- 不調整 gRPC、WebSocket、queue 或 Redis 的 production default。
- 不修改已實作的 WriteBuffer config 與 Echo orchestration Go code。
- 程式修改限於 Echo validation runner 與其 fixture test。

必要修正共五項：Prometheus label selector、RPS 分母、p95 失敗處理、
profile artifact 驗證，以及 periodic metrics/host sample 完整性驗證。若不修正，
真實 campaign 可能全部被誤判為 `counter_mismatch`，或把缺少 latency、profile、
saturation 證據的 attempt 誤判為 valid。

---

## 2. 修正一：使 Prometheus label selector 符合真實 exposition

### 2.1 問題

`metric_sum_or_zero` 將傳入的 labels 當成連續字串比對。client_golang 輸出的
label 順序為名稱字典序，但 runner 內數個 selector 使用了不同順序。例如
真實輸出是：

```text
gaming_core_gate_websocket_commands_total{command="forward",result="success",route="game"}
gaming_core_gate_server_send_requests_total{result="queued",target="connection"}
gaming_core_gate_websocket_writes_total{result="success",source="server_send"}
```

現有 fixture 同樣使用錯誤順序，所以無法暴露這個問題。

### 2.2 具體修正

在 `examples/metrics/scripts/run-echo-performance-validation.sh` 只替換下列 selector：

| 目前值 | 必須改為 |
|---|---|
| `route="game",command="forward",result="success"` | `command="forward",result="success",route="game"` |
| `target="connection",result="queued"` | `result="queued",target="connection"` |
| `source="server_send",result="success"` | `result="success",source="server_send"` |
| `target="connection",result="success"` | `result="success",target="connection"` |
| `target="connection",result="error"` | `result="error",target="connection"` |
| `source="server_send",result="error"` | `result="error",source="server_send"` |

相同 selector 在 counter 與 histogram `_count` 各自出現時都必須修正。不新增
Prometheus parser；本實驗的 metric families 與 labels 已是固定 contract，修正為
client_golang 的 canonical order 是當前最小作法。

同步將 `run-echo-performance-validation-test.sh` 的 fixture lines 改成上述真實順序。
現有 `validate_counter_contract` 成功與 mismatch 測試即可驗證修正，不需要
再建立另一套 parser test。

### 2.3 驗收

- canonical-order fixture 必須通過 `validate_counter_contract`。
- 任一 success counter 少一筆時必須失敗。
- error selector 在 non-zero 時必須失敗。

---

## 3. 修正二：RPS 只使用 admission duration

### 3.1 問題

Runner 目前讀取 `measured_duration_seconds`，並以它作為 RPS 分母。該值包含
admission end 後等待已發出 request 取得 terminal result 的時間，會把 terminal drain
錯誤當成 admission window，使 RPS 被低估。

### 3.2 具體修正

在 `run_attempt` 做下列最小調整：

1. 從 `echo-measured.json` 讀取 `admission_duration_seconds`，不再用
   `measured_duration_seconds` 計算 RPS。
2. `admission_duration_seconds` 缺失、非數字或 `<= 0` 時，attempt 使用
   `orchestration_failed`。
3. 同時讀取 `connection_failures`；非 `0` 時使用既有 `request_error`。
4. 計算公式固定為：

```text
rps = successful_echo_requests / admission_duration_seconds
```

5. `measured_duration_seconds` 與 `terminal_drain_duration_seconds` 仍保留在 marker 作為後續
   報告證據，但不參與 RPS 計算。

為了讓 shell fixture 直接驗證分母，可新增一個只做除法與正值檢查的
`calculate_rps <success> <admission-seconds>` helper；不要建立通用 statistics module。

### 3.3 測試案例

- `success=1000, admission=10, measured=12` 的 RPS 必須是 `100`，不是
  `83.333...`。
- admission 為 `0`、負數或非數字時必須失敗。
- `connection_failures=1` 必須使 attempt 成為 `request_error`。

---

## 4. 修正三：不可將無法計算的 p95 偽裝成零

### 4.1 問題

目前 `histogram_quantile_delta` 失敗時以 `0` 取代，結果為 `+Inf` 時也改寫為
`0`。這會把「latency 未知或超出最高 finite bucket」誤寫為「零延遲」，並污染
capacity plateau 判斷。

### 4.2 具體修正

1. `histogram_quantile_delta` 沒有 positive bucket delta 時回傳 non-zero。
2. quantile 落在 `+Inf` 時回傳 non-zero；不輸出 `0`。
3. 輸出必須是 finite positive number，否則回傳 non-zero。
4. `run_attempt` 使用 `if p95=$(...); then ... else ... fi` 接收結果；失敗時將
   reason 設為既有 `metrics_missing`。
5. 刪除 `|| printf '0'` 與 `+Inf -> 0` 兩個 fallback。

本次不改變現有 bucket-delta quantile 算法，也不導入 PromQL engine。必要修正只是
防止「無法觀測」被當成「零延遲」。

### 4.3 測試案例

- 現有 finite histogram fixture 仍必須取得 finite p95。
- histogram family 缺失時必須失敗。
- 只有 `+Inf` bucket 達到 quantile 時必須失敗。
- baseline/final bucket delta 為零時必須失敗。

---

## 5. 修正四：profile-only run 必須驗證實際 artifacts

### 5.1 問題

`capture_profiles` 目前只將 `curl` 結果寫入 `status.tsv`，但 function 最後仍回傳
success；`run_attempt` 等待 background profile process 時也忽略 exit status。因此 CPU
profile 或 trace 全數失敗時，profile campaign 仍可能被標記為 valid。

### 5.2 具體修正

在同一支 runner 新增小型 `validate_profile_artifacts <directory> <kind>` helper：

- `kind=cpu` 必須同時存在且非空：
  - `game-cpu-20s.pb.gz`
  - `gate-cpu-20s.pb.gz`
  - `load-cpu-20s.pb.gz`
  - `game-goroutine.txt`
  - `gate-goroutine.txt`
  - `load-goroutine.txt`
- `kind=trace` 必須同時存在且非空：
  - `game-trace-5s.out`
  - `gate-trace-5s.out`
  - `load-trace-5s.out`
- `status.tsv` 中對應 target/event 必須各有一筆 `status=ok`，不可只以檔名
  存在當成成功。

`capture_profiles` 在所有 child jobs 結束後呼叫該 helper，驗證失敗就回傳
non-zero。`run_attempt` 等待 `PROFILE_PID` 時必須保留 exit status：

```text
profile capture failed -> reason=metrics_missing
```

仍要先寫出 `echo-final-scraped.json` 讓 load 正常離開，之後再將 attempt 判為
invalid；不可因 profile 失敗而讓 load 額外等待兩分鐘。

同一個 fail-safe 也適用於任何已先設定 invalid reason 的路徑（例如 measured marker
欄位錯誤、terminal gauge timeout 或 final metrics 缺失）：runner 仍嘗試保存 final
metrics 並發布 `echo-final-scraped.json`。若 final marker 無法發布，必須停止本次已保存
的 load PID，避免 `wait LOAD_PID` 無限等待。

### 5.3 測試案例

Fixture 不啟動 pprof server，只建立小型檔案樹驗證：

- CPU 六個檔案與六筆 `ok` status 完整時通過。
- Trace 三個檔案與三筆 `ok` status 完整時通過。
- 任一檔案缺失、空檔或 status 為 `error` 時失敗。

---

## 6. 修正五：valid attempt 必須有足以判斷 saturation 的連續樣本

### 6.1 問題

`validate_attempt_artifacts` 目前只檢查 `*-samples.tsv` 非空，只有 header 也會
通過。`validate_host_samples` 則只要 Game、Gate、load 各有一筆完整列就通過。
這不足以支持設計中「至少連續 5 個 1s samples」的 sustained saturation 判斷。

### 6.2 Periodic metrics 驗證

新增或擴充一個只驗證本實驗檔案的 helper：

1. `game-samples.tsv`、`gate-samples.tsv`、`load-samples.tsv` 各至少要有 `5`
   筆 data rows。
2. 所有 data row 的 status 必須是 `200`；有任一 `error` 就使 attempt
   成為 `metrics_missing`。
3. 每筆 row 第三欄指向的 `.prom` 檔必須存在且非空。
4. baseline 與 final metrics 仍沿用現有檢查，不改變 counter delta 邊界。

最小五筆是為了讓既有「連續 5 秒」飽和定義可被實際判斷，不是新的
performance threshold。

### 6.3 Host process 驗證

擴充 `validate_host_samples`：

1. Game、Gate、load 各至少有 `5` 筆完整 rows。
2. required fields 維持現有 contract：`pid`、`pcpu`、`threads`、`context_switches`、
   `sysbsd`、`sysmach` 必須是數字，`memory` 必須非空。
3. 任一 service 出現 fallback `NA` row 時將 attempt 標記為 `host_sample_missing`；
   不可忽略壞 row 後只用其他一筆好 row 通過。
4. `top.txt`、`nettop.csv` 與 `socket-queues.tsv` 的現有非空檢查保留。

本次不增加 80%、95% 等設計未定義的 sample coverage threshold，也不新增
sampling metrics。滿足五筆連續樣本是與目前 saturation contract 對齊的最小修正。

### 6.4 測試案例

- 三個 service 各有五筆 `200` 且 referenced file 非空時通過。
- header-only、少於五筆、任一 `error` 或 referenced file 缺失時失敗。
- 三個 process 各有五筆完整 rows 時通過。
- 任一 process 少於五筆或出現 `NA` 時失敗。

---

## 7. 修改檔案與預估行數

### 7.1 程式檔

- `examples/metrics/scripts/run-echo-performance-validation.sh`
  - 修正 label selectors、RPS 分母與 p95 error handling。
  - 新增 profile 與 periodic sample 的小型 validation helpers。
  - 預估淨修改 `45–70` 行。
- `examples/metrics/scripts/run-echo-performance-validation-test.sh`
  - 使用 canonical labels，增加 RPS、p95、profile 與 sample completeness fixtures。
  - 預估淨修改 `30–40` 行。

總計預估 `75–110` 行，不包含純排版變動。

### 7.2 不需要修改

- `pkg/gatelink`、`pkg/grpcserver`、`products/gateproduct` 與四個 YAML config。
- `examples/metrics/load/main.go` 與 `echo_orchestration.go`。
- Echo protocol、metrics definitions 與 production lifecycle。
- `CLIENT_GATE_GAME_ECHO_PERFORMANCE_VALIDATION_DESIGN.md` 的架構與實驗矩陣。

---

## 8. 實作順序

1. 先修正 fixture label 順序，確認現有 runner test 因 selector 錯誤而失敗。
2. 修正 runner selectors，讓 counter/histogram equality fixture 恢復通過。
3. 改用 admission duration 計算 RPS，加入 duration 與 `connection_failures` 檢查。
4. 移除 p95 的 zero fallback，加入 missing/無限 bucket fixtures。
5. 加入 profile artifact validation，並將 failure 回傳 `run_attempt`。
6. 加入 periodic metrics 與 host process 最小五筆樣本驗證。
7. 執行第 9 節驗收；本階段不啟動實際 campaign。

---

## 9. 驗收方式

```sh
bash -n examples/metrics/scripts/run-echo-performance-validation.sh
bash -n examples/metrics/scripts/run-echo-performance-validation-test.sh
bash examples/metrics/scripts/run-echo-performance-validation-test.sh
go test ./pkg/gatelink ./pkg/grpcserver ./products/gateproduct ./products/gameproduct ./examples/metrics/load
go test -race ./pkg/gatelink ./pkg/grpcserver ./examples/metrics/load
go vet ./...
git diff --check
```

這些驗收只編譯程式與執行 deterministic fixtures，不啟動 Redis、Gate、Game
或真實壓測。實際 correctness run 與 staircase 仍要在使用者審閱完修正後才執行。

---

## 10. Self review：必要性與範圍

| 修正 | 為什麼必要 | 為什麼沒有超出需求 |
|---|---|---|
| Canonical label order | 否則真實 success delta 會被當成零，campaign 無法生成 valid attempt | 只修正既有 metric contract |
| Admission-duration RPS | 否則 terminal drain 會污染 throughput | 只落實原設計公式 |
| p95 fail closed | 否則缺失 latency 會被誤報為零 | 不改 histogram 與 buckets |
| Profile validation | 否則無 profile 的 run 也可被誤報成功 | 只驗證設計已要求的檔案 |
| Sample completeness | 否則無法判斷連續飽和 | 五筆樣本直接對齊既有 5s contract |

不加入 cosmetic shell refactor、未觸發的 tuning matrix、新 metrics、新 reason taxonomy、
dashboard 或通用 artifact library。因此上述五項都是實驗 correctness 或證據完整性
的必要修正，沒有過路刪減、冗餘或超出 Echo 效能驗證需求。
