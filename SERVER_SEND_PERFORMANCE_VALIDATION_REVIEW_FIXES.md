# Server-send 效能驗證必要修正

## 1. 文件目的

本文件依目前 unstaged changes、`SERVER_SEND_PERFORMANCE_VALIDATION_DESIGN.md` 與
`SERVER_SEND_PERFORMANCE_VALIDATION_REPORT.md` 的實測證據，定義下一次實作只能進行的必要修正。

目標不是保證單一 30 秒 sample 能證明最底層 root cause；這在存在 host noise、罕見 long-tail 與 profiler
observer effect 時並不可靠。目標是讓一次完整 validation campaign 能做到：

1. correctness、warm-up、measured 與 evidence collection 使用同一份可驗證 binary snapshot；
2. 壓力在 warm-up 出現時仍能完成 measured window，不因失敗訊號本身中止診斷；
3. 從同一輪資料判定第一個失守的 application boundary；
4. 收集足以排除 Gate queue、client delivery、Redis server 與 process／host saturation 的資料；
5. 保留三輪重複測量來判斷可重現性，不把單次偶發值寫成穩定容量或 root cause。

本次不新增一般化 benchmark framework、不改正式 server-send API、不加入 OpenTelemetry、不增加高 cardinality
label，也不為了診斷先拆解 `BatchPlayerSender` 的每個內部 phase。現有證據已足以定位第一個失守邊界；只有在
後續確實需要區分 Redis lookup、batch preparation 與 gRPC transport 時，才另行設計 production-actionable
dependency metrics。

## 2. Review 後的正確判斷

目前第一個可確認的失守邊界不是籠統的「shared host」，而是 Game 呼叫的同步 server-send critical path：

```text
example fixed-rate runner
  -> BroadcastSender.Broadcast / PlayerSender.SendToPlayers
  -> call 尚未返回且跨過下一個 scheduled tick
  -> runner 記錄 missed_ticks
```

`examples/metrics/internal/workflow/push.go` 只會在前一個 sender call 的 `lastCallEnd` 晚於下一個
`scheduledAt` 時增加 `missed_ticks`。因此 Gate queue、WebSocket writer 或 load reader 可能透過 backpressure
影響 sender，但不是目前觀測到的第一個失守 boundary。

Player 16ms profile-only run 另有直接 tail evidence：1,860 個成功 calls 中，1,859 個在 100ms 內，1 個落在
`(100ms, 250ms]`，同一輪記錄 15 個 missed ticks。這足以支持「Game synchronous PlayerSender 出現罕見長尾並
破壞 16ms cadence」，但尚不足以在 Redis client、gRPC 或 host scheduling 三者之間宣稱 root cause。

報告必須使用上述精度：明確指出第一個失守 boundary，同時保留 root cause 尚未證實的限制。

## 3. 必要修正總覽

| 項目 | 處理 | 理由 |
|---|---|---|
| warm-up cadence failure | 不阻止 measured；先完成 bounded drain 與 baseline | 目前 12 輪只有 3 輪進入 measured，失去主要診斷資料 |
| correctness provenance | correctness 與 matrix 共用相同 binary checksum | dirty worktree 下相同 commit SHA 不代表相同實作 |
| bottleneck report | 改寫為 Game synchronous sender boundary，補 long-tail evidence | 現有報告把已知邊界退化成過寬候選 |
| Redis server sampling | measured window 每秒收集 Redis CPU／commands | 現有 before／after 無法計算 active-window peak |
| invalid retry | 設定總 attempt 上限並以非零狀態結束 | 避免永久環境錯誤無限重跑與持續佔用磁碟 |
| skip-warmup workaround | 刪除 | warm-up failure 可繼續 measured 後不再需要 |
| production zero-series preinitialization | 刪除，改用穩定 family 做 preflight | 這些 production series 只為測試 preflight 而建立，超出必要範圍 |
| raw artifacts | 保持未追蹤並加入 ignore；保留 manifest | 約 2.3 GiB raw evidence 不應進入 Git source history |

## 4. Warm-up 不得阻斷壓力診斷

### 4.1 修正語意

Warm-up 的責任是讓連線、Redis、gRPC 與 runtime 進入穩定狀態，並確認下一階段開始前沒有殘留 in-flight 或
queued work。它不是 offered load 的 pass gate。

Warm-up 出現以下 workload 結果時仍應進入 measured：

- `missed > 0`；
- sender `partial > 0` 或 `error > 0`，但 Game／Gate／load process 仍存活；
- client delivery 少於 theoretical planned，但已能 bounded drain 且沒有 protocol corruption。

只有以下情況不得進入 measured，並標為 `environment_invalid` 或 correctness invalid：

- control response／run ID 無效；
- 任一必要 process 提前退出；
- reader failure、decode invalid，或無法建立 1000 connections；
- collectors 沒有覆蓋必要 phase；
- terminal in-flight／Gate write queue 在 drain timeout 內無法歸零；
- warm-up completion／drain orchestration marker 缺失或不一致。

Warm-up 的 cadence、sender 與 delivery failure 必須保存在 `run-status.json` 的獨立欄位，不能因 measured 最後
成功就消失；attempt 的主要 `workload_status` 仍以最早失守 boundary 決定。

### 4.2 最小 orchestration protocol

保留現有 filesystem barrier，不增加 HTTP control server或 production API。新增兩個 example-only marker：

```text
warmup-result.json
warmup-drained.json
```

流程固定為：

```text
Game completion log
  -> script 寫 warmup-result.json
  -> load 讀取 result，等待 reader quiet period
  -> load 寫 warmup-drained.json
  -> script 驗證 terminal gauges / Gate queue 歸零
  -> script 保存 baseline metrics 與 Redis snapshot
  -> script 寫既有 baseline-ready.json
  -> load 提交 measured control
```

`warmup-result.json` 至少包含：

```json
{
  "run_id": "...",
  "planned": 312,
  "attempted": 311,
  "success": 311,
  "partial": 0,
  "error": 0,
  "missed": 1,
  "timestamp": "..."
}
```

所有 marker 使用現有 atomic temporary-file + rename 寫法，並驗證 `run_id`。不建立第三套 IPC。

### 4.3 Load 修改

修改 `examples/metrics/load/orchestration.go`：

- 增加 bounded `waitForWarmupResult`，解析並驗證上述數值與 run ID；
- 增加 `writeWarmupDrained`；
- marker timeout 沿用既有 drain timeout，不增加新 config；
- 共用既有 `writeJSON`／polling helper，不複製 filesystem loop。

修改 `examples/metrics/load/push.go`：

- orchestration 模式不再使用 `waitForWarmup(planned)` 等待理論訊息數；因為 producer missed 後該數字永遠不
  可能到達；
- 改為等待 `warmup-result.json`，之後執行 `waitForQuiet(interval)`；
- 在切換 measured run ID 前輸出 warm-up client summary，包含 received、duplicate、gap、invalid 與 reader
  failures，供 script 分類；
- 寫入 `warmup-drained.json` 後等待既有 `baseline-ready.json`；
- 非 orchestration 的手動 example 可保留原本簡單等待，但不得產生正式 baseline 結論。

`waitForWarmup` 若只剩手動模式使用可保留；若沒有 caller 則刪除，不留下死碼。

### 4.4 Script 修改

修改 `examples/metrics/scripts/run-server-send-validation.sh`：

- `capture_baseline_when_warmup_completes` 不再因 `attempted != planned`、`missed != 0`、partial 或 error 立即返回；
- 解析 Game completion 後 atomic 寫入 `warmup-result.json`；
- 等待相同 run ID 的 `warmup-drained.json`；
- 驗證 process 存活、terminal in-flight gauges 與 Gate write queue 歸零；
- 保存 baseline 後照常建立 `baseline-ready.json`；
- warm-up workload failure 與 measured workload failure分開保存，最後依最早 boundary 分類；
- 若 drain 或 lifecycle validation 失敗才停止 measured。

不要用容忍比例（例如 missed 小於 1% 就當成功）掩蓋資料；`missed > 0` 仍代表該 offered cadence 沒有完整
達標，只是不再阻止後續測量。

## 5. 刪除 skip-warmup workaround

Warm-up workload failure 可繼續 measured 後，profile-only 不需要略過 warm-up。刪除：

- load CLI `-push-skip-warmup`；
- `runPushLoad` 的 `skipWarmup` 參數與條件分支；
- script `--diagnostic-without-warmup`／`SKIP_WARMUP`；
- `profile_without_complete_warmup` status 欄位與特殊 validation；
- README、design 與 tests 中只服務這個 workaround 的內容。

`--profile-target game|gate|load` 與 optional `--trace` 保留。Profile run 仍是獨立 attempt，因 CPU profile／trace
有 observer effect，不能混入 baseline median；但它會走同一套 warm-up、drain、baseline 與 measured phase，
不再需要第二種 lifecycle。

這項刪除降低狀態分支，不影響 production pprof 或 framework lifecycle。

## 6. Correctness 與 matrix 必須共用 binary snapshot

### 6.1 目前問題

現有 correctness 與 matrix 雖記錄相同 Git commit，但三個 binary checksum 均不同。工作樹包含 staged、unstaged
與 untracked source 時，commit SHA 不能識別實際 build input，因此報告不得宣稱 correctness gate 已替 matrix
驗證。

### 6.2 修正方式

在一次 validation campaign 開始時 build Game、Gate、load 一次，放在 campaign root：

```text
artifacts/server-send/<campaign>/bin/{game,gate,load}
artifacts/server-send/<campaign>/binary-checksums.txt
artifacts/server-send/<campaign>/source-state.txt
```

後續 correctness 與所有 matrix attempts：

- 只複製或直接執行 campaign root binaries，不重新從可能改變的 working tree build；
- 每個 attempt metadata 都保存相同三個 checksum；
- attempt 啟動前重新計算檔案 checksum，不一致立即標為 `environment_invalid`；
- `source-state.txt` 保存 commit SHA、`git status --short` 與 tracked diff checksum；untracked executable source
  另列檔名與 checksum；
- validation 必須比較 correctness manifest 與 matrix checksums，任何一個不同都不能宣稱 correctness gate pass。

「每次重啟服務」保留；改成不必每個 attempt 重新 build。這既減少 build noise，也使 provenance 可驗證。

現有 correctness artifacts 應在報告中暫時改稱 historical smoke evidence。完成上述修正後，以 campaign binaries
重跑 Broadcast／Player 各一次 10 connections、33ms、5 秒 correctness，再替換報告連結。

## 7. Redis server active-window sampling

不新增 Redis application metrics，也不依賴 OrbStack host PID。由 orchestration 增加一個與其他 collector 相同
生命週期的 Redis INFO loop，每秒保存：

```text
timestamp
used_cpu_sys
used_cpu_user
total_commands_processed
instantaneous_ops_per_sec
connected_clients
blocked_clients
rejected_connections
```

原始輸出可使用 `redis.tsv`；`commands.txt` 必須記錄 address 與實際命令，但不可保存 credential。Redis 使用
非預設 authentication 時，沿用既有安全的 dependency configuration方式，不把 secret 展開到命令紀錄。

Validation 要求：

- collector 在 warm-up 前啟動，final snapshot 後停止；
- measured start 前至少一筆、measured window 中至少兩筆、measured end 後至少一筆；
- timestamp 嚴格遞增，數值可解析；
- collector failure 使 Redis saturation evidence unavailable，但不偽造 0。

報告推導：

```text
Redis CPU one-core % =
  delta(used_cpu_sys + used_cpu_user) / delta(wall_time_seconds) * 100

Redis commands/s =
  delta(total_commands_processed) / delta(wall_time_seconds)
```

報告列 measured window 的 Redis CPU average／max interval、commands/s average／max、blocked clients與 rejected
connections delta。`redis-before.txt`／`redis-after.txt` 可繼續保留作完整 raw snapshot，但不能用包含 drain 的
delta 代替 measured peak。

## 8. Metric preflight 不應改變 production series

刪除以下只為 preflight 預先建立零值 child 的程式碼與對應 tests：

- `products/gameproduct/metrics.go` 中 server-send operation/result 的 eager `WithLabelValues`；
- `products/gateproduct/metrics.go` 中 target/result 的 eager `WithLabelValues`；
- 只驗證「尚未 observation 即存在 family」的 contract assertions。

Preflight 改檢查每個 endpoint 可 scrape、runtime／process collectors存在，以及不需 traffic 就存在的穩定
application metric：

```text
Game: gaming_core_game_gate_commands_in_flight
Gate: gaming_core_gate_websocket_connections
Load: gaming_core_example_load_push_readers
All:  go_sched_latencies_seconds, process_cpu_seconds_total
```

Warm-up 後的 baseline validation 再要求當次 operation 所需的 sender、Gate delivery 與 WebSocket write series
存在。如此仍能發現註冊／wiring 問題，不必讓正式服務為測試工具永久輸出所有未使用 label combinations。

## 9. Attempt 必須有總上限

保留「`environment_invalid` 不佔三個 evidence-valid slots」規則，但總執行次數固定上限為：

```text
max_total_attempts = requested_valid_attempts + 2
```

不增加使用者 config；本次矩陣固定需要三個合法 attempts，因此最多執行五次。超過時：

- 停止 campaign 並回傳非零 exit code；
- 不刪除已收集 artifacts；
- summary 列出每個 invalid attempt 的 `first_failure`；
- 訊息明確要求先修復環境再重新執行，不在同一次 command 中無限循環。

另外將 `stop_pid` 的 `SIGKILL` 放在第二次 `kill -0` 確認後，避免 child 已退出後無條件對舊 PID 發送 signal。

## 10. 報告必要修正

修改 `SERVER_SEND_PERFORMANCE_VALIDATION_REPORT.md`：

1. correctness gate 在相同 checksum 重跑前改為「historical smoke pass；本次 campaign correctness provenance
   invalid」，不得放在 Executive conclusion 當正式 gate；
2. 第一失守 boundary 改為 Game synchronous sender critical path；
3. 補 Player 16ms 的 `(100ms, 250ms]` 單一 long-tail call 與 15 missed ticks 對照；
4. shared-host scheduler／network、Redis client 與 gRPC 保留為造成 sender tail 的候選 root cause，不再與已確認
   boundary 混寫；
5. 第 6.1 節的 warm-up failure 數由 10 修正為 9，與 Executive conclusion及矩陣一致；
6. Redis server CPU 在新增 active-window collector前標為 `not_available`，不得用 command delta 暗示已排除
   Redis saturation；
7. 修正後的實驗若所有 attempts 都進入 measured，列三輪 median、min／max 與 delivery-complete ratio；即使
   missed 很少也不得宣稱 offered load 完整達標。

必要結論格式：

```text
已確認：第一個失守 boundary 是 Game synchronous server-send call 跨過 interval。
已排除：該輪成功 sender calls 在 Gate terminal counters 與 client received 完整對上，沒有 queue full、
         decode、duplicate 或 sequence-gap 證據。
尚未證實：sender long-tail 是 Redis client、gRPC backpressure 或 shared-host scheduling 所致。
```

不要把「尚未證實最底層 root cause」誤寫成「看不出瓶頸」。

## 11. Artifact repository hygiene

目前 `artifacts/server-send` 約 2.3 GiB，包含 binaries、profiles 與每秒 snapshots。必要處理：

- 在 `.gitignore` 加入 `/artifacts/server-send/`；
- 不刪除現有 raw artifacts，也不執行任何影響 staging 的操作；
- campaign root 產生小型 `evidence-manifest.tsv`，保存相對路徑、size 與 SHA-256；
- report 明示 raw evidence 是 local／CI artifact，而不是 repository source；
- 若未來需要 GitHub 可存取的證據，使用 CI artifact／release asset URL，不能直接提交多 GiB raw files。

Report 可繼續以相對路徑引用本機 evidence；在沒有外部 artifact URL 前，不虛構可公開下載的來源。

## 12. 測試與驗收

### 12.1 Unit／contract tests

至少新增或調整：

- warm-up result 含 missed ticks 時，load 仍完成 quiet drain、baseline barrier 並送出 measured control；
- warm-up result 的 stale run ID、invalid JSON、負值／不合理 counters會失敗；
- reader failure、queue drain timeout 或 process exit 不進入 measured；
- `warmup-drained.json` 與 `baseline-ready.json` 使用相同 run ID；
- 刪除 skip-warmup 後沒有死 flag、死欄位或只服務該分支的 tests；
- Redis INFO parser能處理正常輸出、缺欄位與 command failure；
- total attempts 達上限後停止；
- correctness 與 matrix checksum 不同時 validation 失敗；
- metric preflight 使用穩定 families，warm-up baseline才檢查 operation families。

### 12.2 Compile／static checks

實作後先執行：

```sh
gofmt -w <本次修改的 Go files>
go test ./...
go test -race ./...
go vet ./...
go build ./examples/metrics/game ./examples/metrics/gate ./examples/metrics/load
bash -n examples/metrics/scripts/run-server-send-validation.sh
git diff --check
```

不得執行 `git add`、`git reset`、`git checkout` 或其他改變 staging 的操作。

### 12.3 實驗驗收

先由使用者檢視實作，之後才實際測試。正式重跑必須確認：

1. correctness 與 matrix 三個 binary checksums 完全相同；
2. 人為或自然造成 warm-up `missed > 0` 時，30 秒 measured 仍執行；
3. 每個 case 三個 evidence-valid attempts 均有 measured window，除非發生明確 environment／lifecycle invalid；
4. baseline／final counters、Gate terminal、client received、queue／in-flight均可從同一 measured phase核對；
5. Redis、Game、Gate、load 與 host collectors完整覆蓋 measured window；
6. report 能指出第一失守 boundary，並將 root cause certainty獨立標示；
7. command 在永久 environment failure 下最多五次即停止；
8. raw artifacts 保持 unstaged。

## 13. 修改範圍與估算

必要修改預估：

| 範圍 | 預估 touched lines |
|---|---:|
| load orchestration、warm-up lifecycle與 tests | 55–80 |
| shell barrier、Redis collector、checksum與 retry | 45–70 |
| 刪除 skip-warmup／profile特殊分支 | 25–45（以刪除為主） |
| 刪除 production metric eager children與調整 tests/preflight | 35–55（以刪除為主） |
| design、README與 report修正 | 30–45 |
| `.gitignore`／artifact manifest | 5–15 |

合計約 `170–240` 行 touched；因同時刪除 workaround與 eager metric children，淨程式碼增加量會明顯低於
touched lines。這個範圍不包含後續可能的 Redis／gRPC phase metrics，因目前需求只要求先找到第一失守
boundary，現有 aggregate sender histogram與 missed ticks已足夠，不應在證據出現前擴張 production API。

## 14. Self-review

本修正集合皆直接對應已發生的問題：measured 被 warm-up 擋住、provenance 不一致、報告誤判 boundary、Redis
active-window saturation缺證據、無限 retry與大型 artifacts風險。

刻意不做的項目：

- 不修改正式 Broadcast／Player sender transport semantics；
- 不增加新的 framework public interface；
- 不新增每 player／room／endpoint label；
- 不同時 profile所有 process；
- 不用單次 profile取代三輪 baseline；
- 不因一筆 long-tail 就宣稱 OS、Redis或gRPC root cause；
- 不重寫現有 orchestration，只在同一 filesystem protocol補上 warm-up result／drain handoff；
- 不提交或刪除 raw artifacts。

因此這些調整是 review 後維持實驗有效性與最小診斷能力所需，沒有保留純 workaround，也沒有超出本次
server-send效能驗證需求。
