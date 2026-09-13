# Server-send 效能驗證後續必要修正

## 1. 文件目的

本文件記錄對目前 unstaged changes 的後續 review 結果，只修正會影響實驗有效性、證據可重驗性或明確造成
冗餘的項目。既有 `SERVER_SEND_PERFORMANCE_VALIDATION_REVIEW_FIXES.md` 仍是主要設計依據；本文件只補上目前
實作尚未完整落實的部分，不重新設計 server-send、metrics 或壓測框架。

本次必要修正共五類：

1. 讓 correctness 與 matrix 真正共用同一份 campaign binary snapshot；
2. 在放行 measured 前完成 baseline operation metrics 與 terminal gauges 驗證；
3. 讓 `--validate-only` 重驗 warm-up marker schema、counter 與 run ID；
4. 核對 control response 與 Game completion summary 的 planned ticks；
5. 刪除 test-only production helper，並修正 invalid attempt 摘要與必要 regression tests。

## 2. 保持不變的範圍

以下現有修改符合需求，不應回退或擴張：

- warm-up `missed`、partial、error 與 theoretical delivery shortage 不阻止 measured；
- connection、decode、duplicate、sequence gap、reader failure、process exit、drain timeout 仍阻止 measured；
- 保留 `warmup-result.json`、`warmup-drained.json`、`baseline-ready.json` 與既有 filesystem barrier；
- 不恢復 `push-skip-warmup` 或 profile 特殊 lifecycle；
- 不恢復 production metric eager child initialization；
- 保留 Redis active-window sampling、bounded retry、source state、binary checksum 與 evidence manifest；
- 不修改正式 Broadcast／Player API、transport、label 或 framework public interface；
- 不新增 OpenTelemetry、一般化 benchmark framework或每個 sender internal phase metrics。

## 3. 修正一：同一 campaign binary snapshot

### 3.1 現況問題

目前 script 每次 invocation 都呼叫 `build_campaign_binaries`，而 campaign root 已存在
`binary-checksums.txt` 時會直接失敗。每次 invocation 也只跑一組 `workload + interval`。因此 correctness、
四個 matrix cases 與 profile 無法在同一 formal campaign 中共用及互相比對 binary checksum。

這與下列既有設計不一致：

- correctness 通過後才執行 matrix；
- correctness 與 matrix 使用相同 Game／Gate／load binaries；
- 四個 matrix cases 以 round 交錯執行；
- dirty worktree 下不能只靠 commit SHA 宣稱使用相同實作。

### 3.2 最小修正方式

修改 `examples/metrics/scripts/run-server-send-validation.sh`，保留現有 `run_attempt` 作為單一 attempt executor，
只在其上增加 campaign orchestration，不另建第二個 runner。

將目前最底部的單案例 loop 拆成：

```text
main
  -> build_campaign_binaries（每個 formal campaign 恰好一次）
  -> run_correctness_gate
  -> run_matrix_campaign
       -> run_case_round
            -> run_attempt（沿用現有實作）
  -> write_evidence_manifest
```

新增 `--full-campaign` 作為正式 baseline 唯一入口：

- 使用固定 correctness：Broadcast／Player、10 connections、33ms、5s、各一個 evidence-valid attempt；
- correctness 必須是 `delivery_complete`，否則停止並回傳非零；
- correctness 通過後執行既有四個 1000-connection cases；
- correctness 每個 workload 固定只執行 `run-1`；`run-1` 無效即停止，不套用 matrix 的 retry；
- round 1 順序為 Broadcast33、Player33、Broadcast16、Player16；
- round 2 使用反向順序；
- round 3 回到 round 1 順序；
- 每個 case 仍允許最多兩個額外 `environment_invalid` attempts；workload failure 是 evidence-valid，不補跑；
- 每個 attempt 啟動前仍由 `copy_campaign_binaries` 重算並核對三個 checksum；
- correctness 與 matrix metadata 都引用 campaign root 的同一份 `binary-checksums.txt` 與
  `source-state.txt`。

現有單案例參數保留給 smoke、debug 與 profile-only，README 必須明確標示它不構成 formal correctness + matrix
campaign。不要新增可任意編排 cases 的 YAML、DSL 或 generic suite abstraction。

### 3.3 Artifact 路徑

避免 10 connections correctness 與 1000 connections matrix 使用相同 `CASE_NAME` 而互相覆寫：

```text
<campaign>/correctness/broadcast-33/run-1/
<campaign>/correctness/player-33/run-1/
<campaign>/matrix/broadcast-33/run-1/
<campaign>/matrix/player-33/run-1/
<campaign>/matrix/broadcast-16/run-1/
<campaign>/matrix/player-16/run-1/
```

Profile-only 放在 `<campaign>/profile/<case>/run-1/`，不計入 matrix median，也不要求 correctness gate 在同一
profile command 內重跑。它仍必須保存自己的 binary snapshot及完整 lifecycle evidence。

`validate_attempt` 不應再用固定 `../..` 推測 campaign root。每個 attempt metadata 新增
`campaign_root=<absolute path>` 與 `campaign_phase=correctness|matrix|profile|single`，validator 讀取並檢查該
路徑位於 attempt 的祖先目錄，再取得 root checksums。這是支援上述目錄所需的最小 metadata，不做通用路徑
resolver。

### 3.4 Correctness gate 判定

新增小型 `validate_correctness_gate`，只讀取兩個 correctness `run-status.json`，要求：

- `evidence_status == evidence_valid`；
- `workload_status == delivery_complete`；
- `connections == 10`、`interval == 33ms`；
- Broadcast 與 Player 的三個 `binary_sha256_*` 都等於 campaign root manifest；
- `client_received`、sender success與Gate/WebSocket terminal deltas已由既有 attempt validator核對。

不另外發明 correctness metrics 或第二套 equality 判斷。成功後原子寫入
`correctness-gate.json`，內容只需 timestamp、兩個 attempt相對路徑及三個 checksum；matrix 開始前驗證此檔。

## 4. 修正二：baseline 必須在 measured 前驗證

### 4.1 現況問題

目前 `capture_baseline_when_warmup_completes` 在保存 baseline metrics 後直接寫入
`baseline-ready.json`；`validate_operation_metrics` 到整輪結束後才由 `validate_attempt` 呼叫。若 operation
metrics wiring 遺失，load 仍會執行 measured，最後才把整輪判為無效。

### 4.2 最小修正方式

在同一 script 新增 private helper：

```text
validate_baseline_before_measured <attempt_dir>
```

此 helper 只驗證剛保存的 baseline files：

1. 呼叫既有 `validate_operation_metrics`；
2. 使用既有 `metric_values_zero` 驗證 Game：
   - `gaming_core_game_gate_commands_in_flight`；
   - `gaming_core_game_server_send_in_flight`；
3. 使用既有 `validate_gate_terminal_gauges` 驗證 Gate baseline；
4. 三份 baseline metrics檔皆存在且非空。

`capture_baseline_when_warmup_completes` 的順序改為：

```text
capture_metrics_snapshot baseline
  -> validate_baseline_before_measured
  -> capture Redis/process baseline
  -> record measured_baseline events
  -> write baseline-ready.json
```

驗證失敗時建立 `control/baseline-metrics-invalid`，不寫 `baseline-ready.json`。Load 會依既有 bounded timeout退出；
`classify_attempt` 將此 marker 分類為 `environment_invalid`，`first_failure=measured/baseline_metrics`。

不要新增新的 production metric；此修正只使用已存在的穩定及 operation families。

## 5. 修正三：`--validate-only` 重驗 marker

### 5.1 共用 warm-up result validator

目前 runtime writer會檢查 counters，但 `validate_attempt` 只比較 run ID。將 shell 內 counter檢查抽成：

```text
validate_warmup_result_line <json-line>
```

它必須驗證：

- JSON 是 script 固定輸出的單行 bounded schema；
- `timestamp`、`run_id`存在；
- planned、attempted、success、partial、error、missed 全是非負整數；
- `planned > 0`；
- `success + partial + error == attempted`；
- `attempted + missed == planned`。

`write_warmup_result` 應先產生 temporary file，再以同一 helper驗證該內容，成功後才 rename；不要保留 writer與
post-hoc validator兩套不同算式。

### 5.2 `validate_attempt` 修改

在核對三個 warm-up run ID 前後，額外執行：

```text
validate_warmup_result_line "$(cat control/warmup-result.json)"
validate_warmup_drained "$(cat control/warmup-drained.json)"
```

任一失敗回報 `environment_invalid: invalid warm-up result/drained marker`。`validate_warmup_drained` 沿用既有
規則：duplicate、sequence gap、invalid、reader failure 必須為零；`missing` 與 received shortage仍是 workload
evidence，不因 theoretical shortage使 post-hoc validation失敗。

同時驗證：

- `baseline-ready.json` 的 run ID 等於 warm-up run ID；
- `measured-started.json`、`final-ready.json`、`final-scrape.json` 使用同一 measured run ID；
- marker缺失、空檔、schema錯誤或 run ID不一致都不可通過 `--validate-only`。

不導入 `jq` 或新的 JSON dependency；marker schema固定且欄位 bounded，沿用目前 shell parsing即可。

## 6. 修正四：planned ticks 一致性

修改 `examples/metrics/load/orchestration.go`：

- 將 `waitForWarmupResult` 增加 `expectedPlanned uint64` 參數；
- `expectedPlanned == 0` 直接失敗；
- marker完成既有 counter驗證後，再要求 `marker.Planned == expectedPlanned`。

修改 `examples/metrics/load/push.go`：

- 將 `client.startRun` 回傳的 `warmupPlanned` 傳入 `waitForWarmupResult`；
- mismatch時在 quiet drain及 measured control前返回錯誤。

這個核對確保 StartPushResponse與Game completion summary描述的是同一個 schedule；不新增 marker欄位或新的
control message。

## 7. 修正五：刪除冗餘與修正摘要

### 7.1 刪除 test-only writer

刪除 `examples/metrics/load/orchestration.go` 的 `writeWarmupResult`。正式流程的 warm-up result writer是
orchestration script；Go load只負責讀取，因此 production package不應保留只被test呼叫的方法。

`orchestration_test.go` 改為直接用固定JSON fixture寫入 `warmup-result.json`，再測
`waitForWarmupResult`。這同時更接近真實的shell-to-Go contract。

### 7.2 Invalid attempt摘要

campaign達到 retry上限時，列舉 `run-status.json` 前先檢查：

```text
"evidence_status":"environment_invalid"
```

只為符合條件的attempt輸出 `invalid_attempt=... first_failure=...`；已成功取得的 evidence-valid attempts不得被
錯標為 invalid。

### 7.3 小型冗餘

刪除 `validate_redis_tsv` 中只被賦值、未被使用的 `first`／`last` locals與assignment。不要藉此重寫Redis
collector或引入通用TSV parser。

## 8. 必要測試

### 8.1 Go tests

調整 `examples/metrics/load/orchestration_test.go`：

- valid marker含 missed／partial／error仍可讀取；
- marker planned與StartPushResponse planned不同時失敗；
- stale run ID、invalid JSON、負值、counter不守恆仍失敗；
- test fixture由檔案模擬script writer，不呼叫已刪除的Go writer。

既有產品metrics tests只保留「觀測後family存在與bounded labels」；不要恢復eager-series assertions。

### 8.2 Script regression tests

不引入Bats或新test framework。新增一個小型
`examples/metrics/scripts/run-server-send-validation-test.sh`，只測pure validation contracts，不啟動1000條
WebSocket連線：

- valid／invalid warm-up result counters；
- invalid warm-up drained protocol counters；
- campaign binary checksum相同與不相同；
- baseline缺operation family時不建立baseline-ready marker；
- retry在 `requested + 2` 停止；
- failure摘要不列evidence-valid attempts。

為了讓test載入helper，將script最底部執行區包成 `main()`，並使用：

```bash
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
```

不要拆出一般化shell library；目前只有單一script consumer，source同一檔即可避免production與test邏輯分叉。
test只能建立 `mktemp -d` fixture，完成後刪除該temporary directory，不讀寫正式artifacts。

### 8.3 驗證命令

修正後、實際壓測前執行：

```sh
gofmt -w examples/metrics/load/orchestration.go \
  examples/metrics/load/orchestration_test.go \
  examples/metrics/load/push.go
go test ./...
go test -race ./...
go vet ./...
go build ./examples/metrics/game ./examples/metrics/gate ./examples/metrics/load
bash -n examples/metrics/scripts/run-server-send-validation.sh
bash examples/metrics/scripts/run-server-send-validation-test.sh
git diff --check
git diff --cached --check
```

不得執行 `git add`、`git reset`、`git checkout` 或其他會改變 staging 的操作。

## 9. 文件同步

只需更新：

- `SERVER_SEND_PERFORMANCE_VALIDATION_DESIGN.md`
  - 正式入口改為 `--full-campaign`；
  - 補 artifact phase目錄、correctness gate marker與baseline pre-validation順序。
- `examples/metrics/README.md`
  - 提供一條正式full campaign命令；
  - 將單case命令清楚標為smoke/debug/profile；
  - 說明correctness失敗不執行matrix。

`SERVER_SEND_PERFORMANCE_VALIDATION_REPORT.md` 在實際重跑前不填入新數據，只需維持目前「舊資料是historical
smoke／pre-fix evidence」的判斷。不要預先寫入尚未產生的checksum、median或Redis active-window結論。

## 10. 預估修改量

| 範圍 | 預估 touched lines |
|---|---:|
| full campaign orchestration、artifact phase與correctness gate | 55–85 |
| baseline pre-validation與marker post-hoc validation | 25–40 |
| planned ticks核對、刪除test-only writer及Go tests | 20–35 |
| script regression tests與`main` guard | 40–65 |
| retry摘要、小型冗餘與文件同步 | 10–20 |
| 合計 | 約150–245 |

若只計production code，不含tests與文件，約80–125 touched lines。大部分修改沿用現有function與filesystem
protocol，不需要新增package、production API或metrics。

## 11. 驗收條件

修正完成必須同時滿足：

1. 一個formal campaign只build一次；correctness與所有matrix attempts的三個binary checksum完全相同；
2. correctness任一case不是`delivery_complete`時，matrix完全不開始；
3. matrix四case依round交錯，單case最多補兩次environment-invalid attempt；
4. baseline operation family或terminal gauge無效時，不建立`baseline-ready.json`且不開始measured；
5. `--validate-only`可拒絕counter損壞、schema錯誤與cross-phase run ID不一致的markers；
6. control response與completion summary的planned ticks不一致時，不開始measured；
7. retry上限摘要只列environment-invalid attempts；
8. 沒有`writeWarmupResult`等test-only production dead code；
9. 所有既有Go/static checks及新增script regression tests通過；
10. staged changes保持不變。

## 12. Self-review

上述每項都直接修正目前可重現的設計落差：binary provenance尚未跨correctness/matrix成立、baseline驗證太晚、
post-hoc validator可接受損壞marker、兩個planned來源未核對，以及test-only dead helper與錯誤摘要。

本文件刻意不要求：

- 新增production metrics或拆分sender internal phase；
- 修改server-send transport與failure semantics；
- 建立通用benchmark suite DSL；
- 增加外部shell test framework；
- 在本次修正同時重跑1000人矩陣或改寫報告數據；
- 提交、刪除或搬移既有raw artifacts。

因此修正範圍只涵蓋讓既有設計可以被正確執行與重驗所需的內容，沒有額外產品功能或診斷擴張。
