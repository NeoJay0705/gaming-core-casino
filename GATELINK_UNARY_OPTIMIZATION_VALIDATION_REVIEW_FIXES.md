# GateLink Unary Optimization Validation 必要修正

## 1. 結論

本輪 review 後只需要處理兩項實質問題，以及一項報告文字校正：

1. 修正 `stream_workers` 的收斂結論：保留 optional、default=`0` 的實驗 seam，但不得宣稱已通過 production 採用門檻，也不得設為 framework default。
2. 補齊 validation script 對 orchestration marker 的 JSON、`run_id`、`timestamp` 與 workload outcome 驗證，避免無效 attempt 被標記為 valid。
3. 將報告中的 staging 敘述限定為「該次測試完成當下」，避免被誤讀為目前 repository 狀態。

不修改 metadata bounded lookup、protobuf、GateLink Unary contract、custom buffer pool、metrics schema、正式實驗數據或 raw artifacts。也不新增 worker fallback metric、Channelz、Go trace或新的 production tuning。

預估約修改 **70～100 行**；主要是 script validator 與其 deterministic tests，production hot path 不再增加程式碼。

---

## 2. 為何不再移除 `stream_workers`

前一輪建議直接移除 `stream_workers`，但完整追蹤依賴後，這不是最小且一致的修正：

- `run-unary-optimization-validation.sh` 必須透過 `stream_workers=0/4/8` 重跑相同 A/B；只移除 `pkg/grpcserver.Config.StreamWorkers` 會使已納入 staged changes 的 validation script 無法使用。
- 設計第 10.2 節的強制移除條件是「所有 worker 值都沒有超過波動的效益」，不是單純「未達 5% candidate門檻」。
- worker=`8` 的 Game CPU/request median 改善為 `3.952%`；workers=`0` 的 baseline relative MAD 約為 `0.481%`，且三輪方向一致。這個結果低於 `5%` 採用門檻，但高於本次量測波動，不能描述成完全無效。
- `stream_workers` 預設仍為 `0`，範例只提供註解，沒有改變任何 product 的預設 runtime 行為。

因此要區分兩種判定：

| 判定 | 本次結果 | 動作 |
|---|---|---|
| 是否成為 production candidate／default | 否；CPU/request 改善未達 5%，profile mechanism 未一致下降 | 不啟用、不推薦 production 值 |
| 是否完全沒有超過波動的效益 | 否；worker=8 改善高於 baseline MAD | 保留 optional experimental seam，供相同方法重驗 |

### 2.1 修改設計文件

修改 `GATELINK_UNARY_OPTIMIZATION_VALIDATION_DESIGN.md` 第 10.2 節，只補清楚缺少的中間分支：

```text
若 CPU/request 改善低於 5%，但穩定高於 baseline relative MAD，則不得稱為
production candidate，也不得改變 default；可保留 default=0 的 optional
experimental seam，唯一用途是讓不同 product workload 依相同方法重驗。
```

原本「未超過波動時移除」的規則保留，不降低門檻，也不把本次 synthetic Echo 結果外推到 blocking handler。

### 2.2 修改報告

修改 `GATELINK_UNARY_OPTIMIZATION_VALIDATION_REPORT.md`：

- 執行摘要不再寫「合併前移除 `stream_workers`」。
- 最終決策改為：

```text
stream_workers 未通過 production candidate門檻，不設為 framework default，也不提供
建議 production值。因 worker=8 的改善穩定高於本次波動，且 validation runner需要此
建立期 seam 才能重現0/4/8 A/B，所以保留 optional experimental config；預設維持0。
```

- 明確保留限制：單機 loopback、32-byte Echo、單一 ClientConn與極短 handler的結果，不能外推為所有 product 都應設定 `8`。

### 2.3 不修改的程式

以下 staged 內容保留，不再刪除：

- `pkg/grpcserver.Config.StreamWorkers` 與 `grpc.NumStreamWorkers` mapping。
- 四份 example config 中 default=`0`、experimental、非 concurrency limit 的註解。
- `StreamWorkers=0/1/4` lifecycle與「不是 hard concurrency limit」contract tests。
- Game product strict config binding test。

保留原因只有可重現驗證；不得因保留 config 而改成預設啟用。

---

## 3. 補齊 orchestration marker validity

### 3.1 問題

目前 `validate_marker()` 只驗證數值欄位；它會接受缺少 `run_id`、缺少 `timestamp` 或來自其他 attempt 的 measured marker。`failed_workers > 0` 也被混成 `orchestration_failed`，無法區分 marker損壞與實際 request失敗。

這會破壞設計第 8 節的 attempt validity與第 9 節的 bounded reason contract。

### 3.2 JSON讀取方式

validation script 已是 macOS/本機診斷工具，並依賴 `curl`、`redis-cli`、`top`、`lsof` 等工具。為避免繼續用 `sed` 假裝解析 JSON，新增明確的 `jq` preflight dependency：

```bash
for command in curl go git jq shasum awk sed mktemp lsof top sysctl redis-cli grep seq sleep date uname; do
    require_command "$command"
done
```

將 `json_string()` 與 `json_number()` 改為 typed lookup：

```bash
json_string() {
    local file=$1 key=$2
    jq -er --arg key "$key" '.[$key] | select(type == "string")' "$file"
}

json_number() {
    local file=$1 key=$2
    jq -er --arg key "$key" '.[$key] | select(type == "number")' "$file"
}
```

這同時拒絕 malformed JSON、缺少欄位與錯誤型別；不新增自製通用 JSON parser。

### 3.3 Marker schema與identity驗證

新增固定的 UTC timestamp pattern；允許 `time.RFC3339Nano` 在整秒時省略小數，也允許 1～9 位小數：

```bash
RFC3339_UTC_PATTERN='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?Z$'
```

先新增只驗證共用 identity欄位的helper：

```bash
validate_identity_marker() {
    local marker=$1 expected_run_id=${2:-}
    jq -e --arg expected "$expected_run_id" --arg timestamp "$RFC3339_UTC_PATTERN" '
        type == "object" and
        (.run_id | type == "string" and length > 0) and
        ($expected == "" or .run_id == $expected) and
        (.timestamp | type == "string" and test($timestamp))
    ' "$marker" >/dev/null
}
```

將 `validate_marker()` 改名為語意明確的 `validate_measured_marker()`，參數固定為：

```text
validate_measured_marker <marker-path> <expected-run-id>
```

具體實作為：

```bash
validate_measured_marker() {
    local marker=$1 expected_run_id=$2
    validate_identity_marker "$marker" "$expected_run_id" || return 1
    jq -e --arg timestamp "$RFC3339_UTC_PATTERN" '
        ([.measurement_start, .admission_end, .measurement_end] |
            all(.[]; type == "string" and test($timestamp))) and
        (.admission_duration_seconds |
            type == "number" and isfinite and . > 0) and
        (.measured_duration_seconds |
            type == "number" and isfinite and . >= 0) and
        (.terminal_drain_duration_seconds |
            type == "number" and isfinite and . >= 0) and
        (.successful_requests |
            type == "number" and isfinite and . > 0 and floor == .) and
        (.failed_workers |
            type == "number" and isfinite and . >= 0 and floor == .)
    ' "$marker" >/dev/null
}
```

依序驗證的contract為：

1. 整份檔案可由 `jq` 解碼，根節點是 object。
2. `run_id` 是非空 string，且等於 ready marker建立的 `expected-run-id`。
3. `timestamp`、`measurement_start`、`admission_end`、`measurement_end` 都是非空 string，且符合現有 producer輸出的 RFC3339 UTC格式；本次只做固定格式檢查，不建立通用日期函式庫。
4. `admission_duration_seconds` 必須大於零。
5. `measured_duration_seconds`、`terminal_drain_duration_seconds`、`successful_requests`、`failed_workers` 必須為非負 number。
6. `successful_requests` 必須是正整數。

`failed_workers` 不在 schema/identity validator中強制為零。Validator只判定 marker可信度，workload成功與否在下一步分類。

在 `run_attempt()` 內改成：

```bash
if ! validate_measured_marker "$measured_marker" "$run_id"; then
    reason=orchestration_failed
else
    expected=$(json_number "$measured_marker" successful_requests)
    failed_workers=$(json_number "$measured_marker" failed_workers)
    if ! awk -v value="$failed_workers" 'BEGIN { exit(value == 0 ? 0 : 1) }'; then
        reason=request_error
    fi
fi
```

分類規則固定為：

- malformed JSON、missing field、wrong run ID、invalid timestamp／duration：`orchestration_failed`。
- marker合法但`failed_workers > 0`：`request_error`。
- grpcload退出碼非零但尚無更精確原因：`load_exit_failed`。
- counter不一致：`counter_mismatch`。

不得因 request失敗而跳過 final metrics scrape；仍先釋放 `grpc-final-scraped.json` barrier並保存 artifacts，再把 attempt標成 invalid。

### 3.4 四個 marker的一致性

ready marker通過 JSON與必要欄位驗證後才能取得 `run_id`：

```bash
ready_marker="$directory/orchestration/$GRPC_READY_MARKER"
if ! validate_identity_marker "$ready_marker"; then
    reason=orchestration_failed
else
    run_id=$(json_string "$ready_marker" run_id)
fi
```

start與final marker由script使用此`run_id`原子寫入；measured marker必須與它相同。

在 attempt結束前，增加一次小型一致性檢查，確認以下四個檔案都存在且`run_id`相同：

```text
grpc-ready.json
grpc-start.json
grpc-measured.json
grpc-final-scraped.json
```

具體使用一個bounded helper，不掃描directory、不接受額外檔名：

```bash
validate_marker_set() {
    local directory=$1 expected_run_id=$2 marker
    for marker in \
        "$GRPC_READY_MARKER" \
        "$GRPC_START_MARKER" \
        "$GRPC_MEASURED_MARKER" \
        "$GRPC_FINAL_MARKER"; do
        validate_identity_marker \
            "$directory/orchestration/$marker" \
            "$expected_run_id" || return 1
    done
}
```

`grpc-final-scraped.json`寫入完成後、停止process以前呼叫`validate_marker_set "$directory" "$run_id"`。任何一個缺少或不一致都使用`orchestration_failed`。不新增第5個marker，也不改Go端marker schema。

### 3.5 Deterministic tests

更新`run-unary-optimization-validation-test.sh`，至少覆蓋：

1. 完整、相同`run_id`的measured marker通過。
2. malformed JSON失敗。
3. 缺少`run_id`失敗。
4. 缺少`timestamp`失敗。
5. wrong `run_id`失敗。
6. 非正數admission duration失敗。
7. `successful_requests=0`失敗。
8. `failed_workers > 0`的marker schema仍合法，但attempt outcome分類為`request_error`。

刪除目前「沒有`run_id`與`timestamp`仍應成功」的fixture期待。測試只使用temporary directory，不啟動Game、Redis或grpcload，也不得執行任何改變staging的Git操作。

---

## 4. 報告稽核文字

將報告最後一句：

```text
最後的 git diff --cached --name-status 為空；測試未改變 staging。
```

改為：

```text
該次測試完成當下，git diff --cached --name-status 為空；測試腳本未改變
staging。此敘述是campaign稽核紀錄，不代表閱讀報告當下的repository狀態。
```

不修改任何既有測試數字、binary checksum、artifact path或效能結論。

---

## 5. 驗證方法

修改完成後執行：

```text
gofmt -w（只針對有修改的Go檔案；本修正預期不需修改Go檔案）
go test ./pkg/logging ./pkg/gatelink ./pkg/grpcserver ./products/gameproduct ./examples/metrics/grpcload
go test -race ./pkg/logging ./pkg/gatelink ./pkg/grpcserver ./examples/metrics/grpcload
bash -n examples/metrics/scripts/run-unary-optimization-validation.sh
bash -n examples/metrics/scripts/run-unary-optimization-validation-test.sh
bash examples/metrics/scripts/run-unary-optimization-validation-test.sh
git diff --check
```

本輪只修 validator、tests與文件，不改 workload、binary hot path或實驗參數，因此不需要重跑完整 0/4/8 效能矩陣。既有 raw artifacts 已另外確認：10個attempt的40份marker都是合法JSON、每個attempt的四個`run_id`一致，且正式與profile-only binaries checksum相同；validator缺口不影響既有報告數據。

如果修改過任何以下項目，才必須建立全新campaign重跑，不得補寫既有結果：

- `pkg/logging`或`pkg/gatelink` production hot path。
- `pkg/grpcserver` server option mapping。
- grpcload request path、payload、concurrency、connection數或duration。
- Game handler、interceptor順序或metrics定義。

---

## 6. Self review

- `stream_workers`維持default=`0`，沒有改變framework或product預設行為。
- 未把低於5%的結果誇大成production candidate；保留它只為可重現驗證。
- marker修正直接封閉已確認的false-valid風險，沒有增加新的runtime abstraction。
- 使用`jq`取代不可靠的`sed` JSON解析，是validation script範圍內最小且可測的作法。
- 不新增metrics、labels、pprof、trace、buffer pool或protocol欄位。
- 不重跑未受影響的完整效能矩陣，也不修改raw evidence。

修正後，staged changes的production內容只包含有benchmark與contract支持的metadata bounded lookup，以及預設停用、用途受限的experiment seam；實驗工具能繼續重現既有A/B，報告文字與實際判定規則一致。
