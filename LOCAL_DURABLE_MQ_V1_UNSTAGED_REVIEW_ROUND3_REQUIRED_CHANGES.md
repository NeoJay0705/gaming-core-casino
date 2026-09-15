# Local Durable MQ V1 Unstaged Review Round 3 必要修正方法

## 1. 文件目的

本文件只記錄第三輪 unstaged review 確認仍需完成的必要調整。設計依據為：

- `LOCAL_DURABLE_MQ_V1_DESIGN.md`；
- `LOCAL_DURABLE_MQ_V1_REQUIRED_CHANGES.md`；
- `LOCAL_DURABLE_MQ_V1_UNSTAGED_REVIEW_REQUIRED_CHANGES.md`；
- `LOCAL_DURABLE_MQ_V1_UNSTAGED_REVIEW_ROUND2_REQUIRED_CHANGES.md`。

目前 reader cursor、worker draining barrier、membership generation 與 retention
fail-closed 的 production 方向皆符合 V1，也沒有新增 public API、config、persisted
format 或需求外產品能力。本輪不重新設計這些元件，只處理：

1. 補足「同一個 reader」跨 retention floor 與 GC namespace 變更的必要測試；
2. 補足 non-ACTIVE PROTECTED group 阻止 whole-lane collection 的必要測試；
3. 移除 canceled worker flush 失敗後同一 scheduler cycle 內的重複 bounded flush。

## 2. 必要修改總覽

| 編號 | 範圍 | Production | Tests | 性質 |
|---|---|---:|---:|---|
| R3-01 | same-reader GC invalidation | 0 | 約 25～40 | release-gate 缺口 |
| R3-02 | retired lane protected barrier | 0 | 約 15～25 | 新 production branch 的直接證據 |
| R3-03 | terminal flush 去除立即重試 | 刪除約 8～11 | 0～5 | network PV 下避免重複等待 |
| 合計 |  | 約 8～11 行刪除 | 約 40～65 行 | 約 50～75 行 material changes |

R3-01 與 R3-02 原則上只需補測試。若新增測試暴露 production bug，才允許針對失敗
invariant 做最小修正；不得預先重構 reader、retention 或 scheduler。

## 3. R3-01：以同一 reader 驗證 floor advance 與 GC rename/unlink

修改檔案：

- `pkg/infra/localmq/unstaged_required_changes_test.go`

### 3.1 問題

`TestRequiredCursorHandlesGrowthRotationAndFloorAdvance` 在 floor advance 與 sealed
segment removal 後重新呼叫 `newLaneReader`。因此測試只能證明新 reader 能從 published
floor 啟動，不能證明已有 segment cache 與 cursor 的同一 reader 能安全處理：

- retention floor 前進；
- sealed segment 被 rename 到 `.gc-staging`；
- sealed segment 已從 `.gc-staging` unlink。

`TestRequiredReaderUsesPublishedFloorDuringStagingRecovery` 也會建立新的 storage/reader，
用途是 restart recovery，不能取代 same-reader invalidation test。

### 3.2 具體修正

新增一個獨立 table-driven test，例如：

```text
TestRequiredCursorInvalidatesAcrossFloorAndGCNamespaceChange
  - staging rename
  - unlink
```

每個 subtest 依序執行：

1. 建立 sequence `0..1` 的 sealed segment，以及 sequence `2` 的 active segment；
2. durable end 設為 `3`，retention floor 初始為 `0`；
3. 建立一次 `laneReader`，先從 sequence `0` 讀一筆，使 cursor/cache 指向舊 sealed
   segment；
4. durable 發布 retention floor `2`；
5. `staging rename` case：
   - 建立 `<topic>/.gc-staging/<lane-id>/`；
   - 將舊 sealed segment rename 到該目錄；
   - 保留 staging file，不立即 unlink；
6. `unlink` case：直接移除舊 sealed segment，代表 rename 與 directory sync 後的清除
   已完成；
7. 使用步驟 3 的同一個 `laneReader` 再讀取，caller coordinate 可保持在舊位置
   `1`，讓 reader 依 published floor clamp 到 `2`；
8. 驗證：
   - read 不回傳 error；
   - `RetentionFloor == 2`；
   - 第一筆且唯一一筆 message sequence 為 `2`；
   - `NextSequence == 3`；
   - 不回傳 sequence `0` 或 `1`；
   - reader 沒有嘗試依賴已 rename/unlink 的舊 path。

測試應直接使用既有 `newRequiredTestStorage`、`writeRequiredSealedSegment`、
`writeRequiredOpenSegment` 與 `writeRequiredFrontier` fixture，不新增 filesystem
abstraction。

原有 `TestRequiredCursorHandlesGrowthRotationAndFloorAdvance` 可保留 active growth 與
rotation 部分，但應移除 floor 測試前的：

```go
reader = newLaneReader(s, topic, laneID)
```

若新增獨立 table test 已完整涵蓋 floor advance，原測試的最後一段可以保留為較簡單的
integration coverage；不需要複製整套 WAL 建立流程到更多測試。

### 3.3 不允許的擴張

- 不加入 persisted offset index；
- 不改 checkpoint coordinate；
- 不加入 reader background watcher；
- 不為測試建立 virtual filesystem；
- 不要求任意 historical replay。

## 4. R3-02：直接測試 non-ACTIVE PROTECTED group 阻止 retired lane collection

修改檔案：

- `pkg/infra/localmq/unstaged_required_changes_test.go`

### 4.1 問題

`maybeCollectRetiredLane` 已新增 `barrier.blocked` 判斷，確保 INITIALIZING 或 DELETING
的 PROTECTED group 即使 checkpoint 已到達 retired final sequence，也不會刪除整條
lane。

現有 `TestRequiredRetentionRevalidatesProtectedStateBeforePublishingFloor` 只驗證 segment
floor publication；`TestRequiredBestEffortBlockedMarkerPreventsRetiredLaneCollection` 驗證的
則是 BEST_EFFORT blocked marker。兩者都沒有直接執行 `maybeCollectRetiredLane` 的
`barrier.blocked` 分支。

### 4.2 具體修正

新增 table-driven test，例如：

```text
TestRequiredNonActiveProtectedGroupPreventsRetiredLaneCollection
  - INITIALIZING
  - DELETING
```

每個 subtest 依序執行：

1. 建立 header-only empty active segment與 durable end `0`；
2. 使用既有 `forceRetireActiveLane` 完成 empty lane retirement；
3. 建立 PROTECTED group，baseline/checkpoint 設為 retired marker 的
   `FinalNextSequence`，排除「checkpoint 落後」造成的其他 barrier；
4. 將 group control 改為該 subtest 的 `INITIALIZING` 或 `DELETING`；
5. 呼叫 `maybeCollectRetiredLaneIfEmpty`；
6. 驗證呼叫安全返回，且以下 durable namespace 仍存在：
   - lane directory；
   - retired marker；
   - retention index；
7. 不要求在同一測試恢復 ACTIVE 後刪除 lane；既有 whole-lane collection tests 已覆蓋
   正常刪除流程。

只需代表性覆蓋 `INITIALIZING` 與 `DELETING`。missing/corrupt control 已由 metadata
讀取錯誤 fail closed，不需為每種 decode error 重複建立相同測試。

### 4.3 Production 判斷

若上述測試通過，不修改 `retention.go`。目前的 `blocked bool` 是 package-private
process state，且同時供 segment retention 與 whole-lane collection 使用，符合最小
設計。

## 5. R3-03：移除同一 scheduler cycle 的第二次 terminal flush

修改檔案：

- `pkg/infra/localmq/consumer.go`

### 5.1 問題

目前 canceled worker 在 `schedulerLoop` 中會先執行一次 `flushForShutdown`。若該次
失敗，`releaseLane` 將 worker 設為非 in-flight 並保留 draining pointer，隨後同一
scheduler goroutine 立即呼叫 `flushDrainingWorkers`，造成第二次 bounded flush。

對 local disk，這通常只是重複 I/O；checkpoint 位於 network PV 時，兩次呼叫可能各自
等待完整 `HandlerTimeout`。draining pointer 已經阻止 replacement，因此立即重試不提供
額外 correctness，只延長 scheduler/Stop 收斂時間。

### 5.2 具體修正

將 scheduler terminal path 簡化為：

```go
delay := worker.step()
if worker.ctx.Err() != nil {
    if err := worker.flushForShutdown(); err != nil {
        c.setError(err)
        delay = c.cfg.ReconcileInterval
    } else {
        delay = -1
    }
}
c.releaseLane(worker, delay)
```

必須移除：

- `flushFailed` 局部變數；
- failure branch 對 `flushFailed = true` 的賦值；
- `releaseLane` 後立即呼叫 `flushDrainingWorkers` 的整個區塊與不再成立的註解。

保留以下語意：

1. flush 成功時以 terminal `delay = -1` 移除同一 worker pointer；
2. flush 失敗時使用非負 delay，讓 `releaseLane` 將 `inFlight` 清除但保留 worker；
3. worker 必須維持 `draining=true`，`claimLane` 不得再次執行 handler；
4. 後續 reconcile、membership timeout 的 `pauseWorkers` 或 Stop 負責再次 bounded
   flush；
5. flush 仍在 `c.mu` 外執行。

`pauseWorkers` 只修改 worker state、不新增或刪除 map entry，因此其中的
`rebuildLaneOrderLocked()` 也應一併移除。lane order 會保留 draining lane，但
`claimLane` 已明確跳過 draining worker；真正移除 worker 時仍由
`flushDrainingWorkers` 或 `releaseLane` rebuild。

### 5.3 測試影響

既有下列測試應足以驗證修改後語意：

- `TestRequiredDrainingWorkerBlocksReplacementUntilFlush`；
- `TestRequiredInflightOwnershipLossDoesNotOverlapReplacement`；
- `TestRequiredStopWaitsForDrainingFlushAndKeepsMembershipOnFailure`。

只有當現有測試依賴「同一 cycle 立即重試」時，才調整該 assertion；不得新增 retry
timer、retry goroutine 或 public retry config。

## 6. 明確不修改

以下內容不屬於必要修正：

- 不移除 ForceRetire interruption matrix 的 `open remove` subtest；它與既有測試雖有
  部分重疊，但保留完整 fault matrix 有助於 review，不構成功能或維護負擔；
- 不為了縮短測試檔案拆 package 或重寫 fixtures；
- 不把 worker draining state 做成通用 state-machine framework；
- 不加入 distributed fencing token；
- 不修改 Redis membership schema；
- 不加入 partition、DLQ、producer dedup、exactly-once、replication、priority、delayed
  delivery或 arbitrary replay；
- 不變更 public API、config、metrics label與 persisted on-disk format。

in-flight ownership test 的 atomic concurrency counter 可以保留。pointer identity assertion
是主要 correctness 證據，counter 是 race run 的輔助 invariant；為少量行數重寫測試沒有
必要。

## 7. 驗收方式

完成修改後依序執行：

```bash
gofmt -w pkg/infra/localmq/consumer.go pkg/infra/localmq/unstaged_required_changes_test.go
go test ./pkg/infra/localmq -run 'TestRequired(CursorInvalidatesAcrossFloorAndGCNamespaceChange|NonActiveProtectedGroupPreventsRetiredLaneCollection|DrainingWorkerBlocksReplacementUntilFlush|InflightOwnershipLossDoesNotOverlapReplacement|StopWaitsForDrainingFlushAndKeepsMembershipOnFailure)$' -count=10
go test ./pkg/infra/localmq -count=10
go test -race ./pkg/infra/localmq -count=1
go test ./...
go vet ./...
git diff --check
```

驗收條件：

1. 同一 reader 在 staging rename與unlink後只從 published floor繼續；
2. non-ACTIVE PROTECTED group 保留 retired lane namespace；
3. canceled worker flush失敗後仍保留 draining barrier，但 scheduler不在同一 cycle做
   第二次 flush；
4. 同 lane最大 handler concurrency仍為1；
5. 所有既有測試與 race detector通過；
6. unstaged production diff沒有新增 public surface或需求外功能。
