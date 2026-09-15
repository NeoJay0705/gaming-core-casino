# Local Durable MQ V1 Unstaged Review 必要修正方法

## 1. 文件目的

本文件只記錄目前 unstaged implementation 經 review 後仍必須完成的修正，作為
`LOCAL_DURABLE_MQ_V1_DESIGN.md` 與
`LOCAL_DURABLE_MQ_V1_REQUIRED_CHANGES.md` 的後續實作指引。

修正範圍只涵蓋會影響下列項目的問題：

- at-least-once 與 checkpoint correctness；
- 同一 lane 的 handler concurrency；
- lifecycle 正確回報；
- ForceRetire crash recovery 與 request idempotency；
- retention fail-closed；
- network PV read amplification；
- 設計已要求的 deterministic fault/concurrency tests。

本輪不加入 partition、DLQ、producer dedup、exactly-once、replication、priority、
delayed delivery、任意 replay、per-Topic quota、HTTP/gRPC admin server或 persisted
offset index。

## 2. Review 結論

目前架構方向符合 V1，沒有發現產品功能超出需求；但仍有 correctness、crash
recovery 與測試證據缺口，因此尚不能宣稱 implementation 已完整符合設計。

以下 UR-01～UR-08 全部是必要修正。第 7 節列出的純整理項目不應混入本輪。

建議修正順序：

1. UR-01 checkpoint clamp 與 permanent-error flush；
2. UR-02 scheduler cancellation、Stop 與 lifecycle；
3. UR-03 AcknowledgeRecords；
4. UR-04 ForceRetire；
5. UR-05 retention fail-closed；
6. UR-06 sequential reader fast path；
7. UR-07 dependency seam補齊；
8. UR-08 regression、fault與concurrency tests。

## 3. Production 必要修正

### UR-01：修正 checkpoint clamp 與 permanent-error flush

修改檔案：`pkg/infra/localmq/consumer.go`

#### 3.1 BEST_EFFORT floor clamp 必須 durable publish

目前 `laneWorker.step` 在 `effective < retention floor` 時，先把
`checkpoint.persisted` 設成 floor，再呼叫 `flush`。`flush` 因
`next <= persisted` 直接返回，實際上沒有寫入 member checkpoint。

必要修改：

1. 保存目前 process 已完成 handler 的 `checkpoint.next`，不得因 floor clamp
   倒退；
2. clamp target 使用：

   ```text
   target = max(checkpoint.next, retention_floor)
   ```

3. 只有 `target > checkpoint.next` 時呼叫 `checkpoint.advance(target)`；
4. 不得直接修改 `checkpoint.persisted`；
5. 無論 target 是 floor 或較高的 memory watermark，都必須呼叫 `flush`；
6. 只有 `flush` 成功後才能讀取下一批或呼叫 handler；
7. skipped metric仍以原始 durable effective checkpoint 到 floor 的差計算，不能把
   process-local、尚未 durable 的 watermark 算成已被 retention skip。

建議最小流程：

```text
durable_effective = effectiveCheckpoint(...)
floor = retention.EarliestRetainedSeq

if durable_effective < floor:
    skipped = floor - durable_effective
    target = max(checkpoint.next, floor)
    if target > checkpoint.next:
        checkpoint.advance(target)
    checkpoint.flush(worker_context)
    durable_effective = floor

if durable_effective > checkpoint.next:
    // 另一 owner 已 durable 前進，memory state可安全跟進。
    checkpoint.next = durable_effective
    checkpoint.persisted = durable_effective
    checkpoint.pendingCount = 0
```

#### 3.2 Permanent error 前無條件 flush pending watermark

目前只有 binary split 找到的本批成功 prefix 大於零時才 flush。若更早 batches
已成功但尚未達 checkpoint threshold，而 poison record位於本批第一筆，blocked
marker會先於既有成功 watermark發布。

必要修改：

1. `prefix > 0` 時只負責 `advance(last_prefix_sequence + 1)`；
2. 將 `checkpoint.flush(worker_context)` 移到條件外；
3. flush失敗時不得寫 blocked marker，維持 `mustFlush=true` 並 bounded retry；
4. flush成功後才 exclusive publish blocked marker。

預估：production 12～22 行，tests 70～110 行。

### UR-02：修正 scheduler cancellation、Stop 與 lifecycle

修改檔案：`pkg/infra/localmq/consumer.go`

#### 3.3 In-flight lane state不得提前移除

`pauseWorkers` 與 `reconcile` 目前會在 handler仍 in-flight 時立刻從 `workers`
移除 lane。若 membership快速恢復或 ownership再次回到同一 instance，新
`laneWorker` 可能在舊 handler尚未返回時啟動，違反同 lane最多一個 in-flight
handler的設計。

必要修改：

1. ownership loss或heartbeat timeout時先 `worker.cancel()`；
2. 若 `worker.inFlight == true`，暫時保留同一 pointer於 `workers`，不得建立
   replacement worker；
3. 若沒有 in-flight，才從 map/round-robin order移除並執行 bounded flush；
4. scheduler在 `step()` 返回後若發現 `worker.ctx.Err() != nil`，強制使用 terminal
   release，不得把 canceled worker重新排入 runnable queue；
5. terminal release確認 map內仍是同一 worker後才移除，避免舊 worker誤刪後來的
   replacement；
6. map/order更新集中到一個 package-private helper，確保 `scheduleAt` 每次都被
   normalize。

不需要加入新的 priority queue、動態 worker數或 public config。

#### 3.4 Stop 必須取得並回報最後 checkpoint flush結果

目前 scheduler可能在 cancellation後移除 lane，`Stop` 等 scheduler結束後便找不到
該 checkpoint tracker；scheduler內的 flush error又沒有回傳，因此 Stop可能錯誤
回 success。

必要修改：

1. Consumer進入 `consumerStateStopping` 後，terminal release只將
   `worker.inFlight=false`，暫時保留 worker state給 Stop收集；
2. scheduler完成後，Stop snapshot所有剩餘 workers並逐一 bounded flush；
3. 使用 `errors.Join` 或等價方式保留所有 flush errors，不只第一個；
4. 任一 pending checkpoint未成功 durable時，不得移除 Redis membership，也不得
   回 success；
5. flush成功後才移除 membership；
6. 已進入 `consumerStateStopped` 的重複 Stop必須回傳保存的 `stopErr`，維持
   idempotent result；
7. Stop context先到期時保留 membership，由 timeout自然失效，不宣稱 graceful
   shutdown完成。

#### 3.5 Start/Stop 的 WaitGroup registration 必須原子化

目前 state先公開為 Started，之後才執行 `schedulerWG.Add`。Concurrent Stop可能在
Add之前完成 Wait並返回。

必要修改：

1. 第二次確認 `consumerStateNew` 後，在同一個 `c.mu` critical section內完成：

   ```text
   assign context/membership
   set state Started
   WaitGroup.Add(all runtime goroutines)
   launch scheduler、heartbeat與reconcile goroutines
   unlock
   ```

2. heartbeat與reconcile loops也納入同一 runtime WaitGroup；Stop必須等它們退出後才
   `ZREM`，避免晚到的 heartbeat在 remove後重新加入 membership；
3. reconcile goroutine先執行一次initial reconcile，透過buffered result channel回報
   Start；成功後才進入ticker loop，避免Start與background同時執行兩次reconcile；
4. initial reconcile失敗時沿用同一 Stop path收斂，不建立另一套 cleanup流程；
5. 不嘗試強制中止不遵守 context的 handler；Stop仍受 caller context限制。

可將 `schedulerWG` 改名為 `runtimeWG`，或另加一個 background WaitGroup；選擇一種
即可，不要保留兩套用途重疊的等待機制。

預估：production 35～60 行，tests 100～160 行。

### UR-03：AcknowledgeRecords 必須使用 baseline判斷安全重試

修改檔案：`pkg/infra/localmq/admin_ops.go`

目前 `effective >= target` 分支只確認存在 target以下的 blocked marker，沒有確認
`checkpoint-base` 已到達 target；cleanup也只刪到 request target，而不是實際發布的
新 baseline。

必要修改：

1. 先保存：

   ```text
   baseline = checkpoint_base.next_sequence[lane]
   effective = effective_checkpoint(topic, group, lane)
   ```

2. 第一次操作維持以下條件：

   ```text
   target > effective
   earliest_blocked_at_or_after(effective).sequence < target
   target <= lane_durable_end
   ```

3. 發布值使用 `newBaseline = max(baseline, effective, target)`；
4. `effective >= target` 時只有同時滿足以下條件才是安全重試：

   ```text
   baseline >= target
   exists blocked marker with sequence < target
   ```

5. cleanup coordinate使用 `newBaseline`，不是 request target；這會清除所有已被
   baseline越過的 stale markers，避免它們永久阻止 whole-lane GC；
6. cleanup成功後才寫 audit；既有 audit仍必須核對 operation、topic、group、lane與
   target。

不新增自動 skip、DLQ或 arbitrary checkpoint reset。

預估：production 8～15 行，tests 55～85 行。

### UR-04：ForceRetire 必須補齊 empty-segment crash recovery與request fencing

修改檔案：

- `pkg/infra/localmq/admin_ops.go`
- `pkg/infra/localmq/format.go`
- `pkg/infra/localmq/storage.go`
- `pkg/infra/localmq/wal.go`，只在共用 retired finalization protocol需要同步時修改

#### 3.6 Empty active segment不得在 retired marker前不可逆消失

目前 empty active `.open` 會先被 remove，之後才發布 retired marker。兩者之間
crash會留下 durable frontier，但 authoritative segment已不存在，retry只能
fail closed且無法完成 retire。

必要修改：

1. matching `.open` 經 durable prefix驗證後，若 record count為零：
   - truncate到 `durable_byte_end`；
   - sync file；
   - 暫時保留 header-only `.open`；
2. 先 exclusive publish retired marker；此時 marker與durable frontier的
   `final_next_sequence` 必須一致；
3. marker durable後才移除 empty `.open`、sync segments directory；
4. 最後移除 durable-end並sync lane directory；
5. retry finalization helper必須接受以下兩種狀態：
   - marker + frontier + 已驗證的 header-only matching `.open`；
   - marker + frontier，但 matching empty `.open` 已在先前 attempt移除；
6. helper只在 `frontier.next_sequence == frontier.active_segment_base` 且 marker final
   sequence相同時，才可將 `.open` 視為 empty；若檔案存在仍須驗證完整 header、
   identity、base與沒有額外 bytes；
7. 非空 sealed segment維持現有 protocol，不得重新改寫已驗證的 `.wal`。

這個 finalization順序也應用於 graceful retire的 empty `.open`，避免同一 persisted
format存在兩套互相矛盾的 retired crash state；不增加任何新公開行為。

#### 3.7 Retired marker保存 request identity與evidence hash

在 `retiredMarker` 增加 optional欄位：

```text
retire_request_id
fencing_evidence_hash
```

規則：

1. public ForceRetire一開始計算一次 SHA-256 hash，後續都使用同一值；
2. 新 marker必須保存 request ID與hash，不保存原始 evidence；
3. 已存在 marker時必須核對 Topic、lane、final sequence、request ID與hash；
4. crash發生於 marker後、audit前時，同 request ID與相同hash可繼續 finalization；
5. 同 request ID但不同 evidence hash必須 conflict；
6. 舊格式 marker沒有request欄位時，只能由完整且coordinate/hash相符的既有 audit
   證明是同一 operation，否則 fail closed；
7. audit存在時仍核對 operation、Topic、lane與hash。

新增欄位保持 `omitempty`，不提高 format version；decoder仍可讀舊 marker，但舊資料
必須走上述 fail-closed compatibility規則。

預估：production 40～70 行，tests 90～140 行。

### UR-05：Retention footer fast path 必須核對segment base identity

修改檔案：`pkg/infra/localmq/retention.go`

目前 `readSealedFooter` 只收到 path並核對 Topic/lane，未確認檔名解析出的
`segment.base` 與 header的 `BaseSequence` 一致。錯置或損壞的檔案可能提供錯誤
next sequence與append time，讓 GC錯誤推進 floor。

必要修改：

1. 將 `readSealedFooter` 改為接收完整 `segmentFile`，或額外接收 expected base；
2. 明確拒絕 `.open`；
3. 使用已開啟 handle的 `Stat` 作為後續 footer validation依據，避免 path stat與open
   中間被替換；
4. 驗證：

   ```text
   header.topic == expected topic
   header.lane_id == expected lane
   header.base_sequence == segment.base
   footer.file_length == opened file size
   footer.next_sequence >= segment.base
   footer.next_sequence - segment.base == footer.record_count
   footer.record_count > 0
   footer.max_append_time >= footer.min_append_time
   ```

5. 保持 bounded header/footer `ReadAt`；這條 maintenance fast path不要恢復整檔 data
   CRC。完整 data CRC仍由reader首次開啟、ForceRetire與conformance test負責。

預估：production 12～22 行，tests 30～50 行。

### UR-06：Sequential reader不得每批重掃所有segment boundaries

修改檔案：

- `pkg/infra/localmq/reader.go`
- `pkg/infra/localmq/reader_benchmark_test.go`

目前 `readLaneBatchWithCursor` 每個 batch仍會 `listSegments`，並對所有 segments呼叫
`readSegmentBoundary`。單 segment benchmark可以通過，但多 segment backlog會產生
`batch_count × segment_count` 次 header/footer/open/stat操作，不符合使用network PV
時降低I/O amplification的需求。

必要修改採兩條路徑：

#### Slow locate path

只在以下情況執行：

- reader第一次啟動；
- ownership建立新的 `laneReader`；
- retention floor前進；
- caller的 `from` 與cursor sequence不一致；
- current path發生 `ENOENT`/`ESTALE`；
- inode/file identity、open/sealed狀態或durable frontier無法與cache一致。

流程：

```text
read retention floor and durable frontier/retired marker
→ list segments once
→ 由最舊segment依next_sequence定位包含 from 的segment
→ 只掃描一次到 from 的byte offset
→ 初始化current segment cache與cursor
```

Slow path必須驗證經過的segment coverage連續，不能猜測gap。

#### Cursor fast path

`laneReader` 至少保存：

```text
observed_retention_floor
observed_durable_end
current segment base/path/open
file identity
header/data end/footer summary
byte offset/next sequence
record count/append range
sealed data CRC already validated
```

每批流程：

1. 重讀小型 retention index與durable-end/retired metadata；
2. floor與cache相同、`from == cursor.sequence` 時直接開啟current path，不列舉舊
   segments；
3. sealed segment只在第一次取得該immutable file identity時完整驗證data CRC；後續
   batches沿用cached boundary與validation flag；
4. active segment沿用cached header，只把最新 durable byte end視為可解析上限，從
   cursor offset讀新增records；
5. 到達segment end時才刷新segments directory，尋找
   `next.base == current.next_sequence`；
6. active path被rename為`.wal`時丟棄current file cache，重新驗證同base sealed file
   footer/data CRC，再進入sealed fast path；
7. 任一identity、sequence、append-time或CRC錯誤都清除cursor並fail closed；不得在
   同一次錯誤中猜測跳到後續segment；
8. cursor保持process-local，不新增persisted index或修改checkpoint coordinate。

Benchmark必須新增多segment case，並分別報告：

```text
bytes_read/message
storage_ops/message
segment_boundary_reads/op
```

驗收不是固定某個本機ns/op，而是當batch數增加時，boundary reads接近
`O(segment_count + batch_count)`，不得接近`O(segment_count × batch_count)`。

預估：production 90～160 行，tests/benchmark 90～150 行。

## 4. Test seam 必要補強

### UR-07：Membership dependency必須可由runtime constructor注入

修改檔案：

- `pkg/infra/localmq/consumer.go`
- `pkg/infra/localmq/maintenance.go`
- `pkg/infra/localmq/membership.go`

目前 `newMembershipWithStore` 只能單獨測試membership wrapper，但 Consumer.Start、
DeleteGroup與checkpoint compaction仍直接建立Redis-backed membership，無法對完整
lifecycle/membership churn做deterministic test。

必要修改：

1. 定義 package-private `membershipFactory` function type；
2. public `NewConsumer` 與 `NewMaintainer` 使用Redis production factory；
3. 增加package-private constructor供tests注入factory，不改public API；
4. Consumer、DeleteGroup與compaction都只能透過factory取得membership；
5. fake store只模擬heartbeat、active members、remove與受控error/block，不建立通用
   Redis emulator；
6. filesystem仍沿用既有 `storageOps`，不增加DI framework。

預估：production 25～45 行，tests共用harness 40～70 行。

## 5. 必要 regression 與 fault tests

### UR-08：補齊設計已列出的測試證據

修改檔案建議：

```text
pkg/infra/localmq/checkpoint_required_test.go
pkg/infra/localmq/scheduler_required_test.go
pkg/infra/localmq/retention_required_test.go
pkg/infra/localmq/force_retire_required_test.go
pkg/infra/localmq/durability_fault_test.go
pkg/infra/localmq/reader_benchmark_test.go
```

可以依責任拆檔；不要建立第二套storage format或production-only test API。

#### Checkpoint/admin

- BEST_EFFORT floor clamp後重新建立storage/client，member checkpoint仍至少為floor；
- memory watermark高於floor時不得倒退；
- checkpoint write持續失敗期間handler call count不增加；
- permanent record位於batch第一筆時，前批pending watermark先durable；
- shutdown flush failure使Stop回error且不ZREM；
- baseline=0、member=100、stale block=50、target=200必須拒絕；
- baseline publication後、marker cleanup前重試成功；
- effective達target但baseline未達時不得冒充上述安全重試；
- cleanup使用新baseline並移除所有baseline以下marker；
- 同request ID改變Topic/group/lane/target必須conflict。

#### Scheduler/lifecycle

- `2*worker_concurrency+1` lanes全部最終取得handler機會；
- hot lane持續有資料時其他lanes仍前進；
- ownership loss後舊in-flight handler未退出前不得建立同lane replacement；
- blocked/backoff lane不佔worker slot；
- concurrent Start/Stop不得發生WaitGroup誤用、晚啟動worker或remove後heartbeat；
- repeated Stop回傳相同result；
- race test中同lane最大handler concurrency固定為1。

#### Retention

- pressure解除只刪一個segment時不得刪第二個；
- 多Topic/lane先刪footer append time全域最舊candidate；
- planning後checkpoint/control改變時不得發布floor；
- BEST_EFFORT blocked marker存在時不得whole-lane collect；
- filename base與header base不同時fail closed且floor不變；
- floor publication、staging rename、unlink各boundary重啟後不delivery floor以下資料；
- 每次physical deletion後重新statfs並於producer watermark停止。

#### ForceRetire

- matching `.open`、matching `.wal`、`.wal + next header-only .open`三種rotation state；
- active non-durable tail只保留frontier prefix；
- empty `.open` 在marker publication、open remove、directory sync、durable-end remove各點
  crash後可用同request完成；
- wrong Topic/lane/base、sequence gap、append regression、footer/data CRC錯誤全部fail
  closed；
- marker後audit前重試相同request/evidence成功；
- 同request ID改變evidence、Topic或lane必須conflict；
- restart assertion使用全新storage/client instance，不沿用memory state或open handle。

#### WAL/storage fault matrix

既有named fault points必須依各自實際protocol覆蓋：

```text
record_append
wal_sync
durable_end_temp_sync
durable_end_rename
durable_end_parent_sync
footer_sync
segment_rename
checkpoint_rename
retention_floor_rename
staging_rename
group_control_rename
```

每個test至少驗證相關invariants：

- API回success的record重啟後存在；
- `DurabilityUnknown` record可以存在或不存在，但storage可恢復且不產生sequence gap；
- `RejectedBeforeWrite` request不得出現在durable frontier；
- checkpoint不得高於已成功handler watermark；
- retention floor不倒退；
- corrupt metadata或namespace無法驗證時fail closed；
- crash test必須關閉舊handles並重建instance，不能只斷言注入點回error。

#### Reader

- byte/message limit只回contiguous prefix；
- oversized第一筆仍可前進；
- batch跨segment仍連續；
- active frontier只增加時只讀新增bytes；
- active轉sealed、GC rename/unlink與floor advance會正確invalidate cursor；
- sealed CRC corruption與record sequence corruption仍fail closed；
- 多segment benchmark證明沒有`batches × segments` boundary scan。

測試應共用接受 `testing.TB` 的fixture helpers，避免
`required_changes_test.go` 與benchmark各自維護一套topic/lane/segment建立程式。

預估：新增或實質修改tests/fault harness 550～850 行。這些tests是設計明確要求的
correctness evidence，不是增加產品功能。

## 6. 修改量估算

以 additions加material edits估算：

| 範圍 | Production | Tests/benchmark |
|---|---:|---:|
| UR-01 checkpoint | 12～22 | 70～110 |
| UR-02 scheduler/lifecycle | 35～60 | 100～160 |
| UR-03 acknowledge | 8～15 | 55～85 |
| UR-04 ForceRetire | 40～70 | 90～140 |
| UR-05 retention identity | 12～22 | 30～50 |
| UR-06 reader fast path | 90～160 | 90～150 |
| UR-07 membership seam | 25～45 | 40～70 |
| UR-08剩餘共用fault/concurrency matrix | 0～20 | 350～500 |
| 合計 | 約222～414 | 約825～1,265 |

整體約 1,050～1,680 行 material changes。若先只完成production與每個bug的直接
regression tests，約 600～950 行；但在fault matrix完成前，不得宣稱達到設計定義的
成熟durable queue production gate。

## 7. 明確不列入本輪的整理

以下是可見的redundancy，但不影響本輪correctness，不應為了順手整理擴大diff：

- package-private舊 `readLaneBatch` 目前只被tests呼叫；是否移除應等new cursor tests
  完整後另做dead-code cleanup；
- test與benchmark fixture有重複，可在新增必要tests時改成 `testing.TB` 共用helper，
  但不要重寫production format；
- `retainTopic/retainLane` 的舊pressure boolean path可在pressure heap穩定後另行簡化；
- 不做命名、註解或檔案排列的大規模美化；
- 不調整public API，除非現有API無法表達設計已要求的錯誤結果。

上述項目不計入必要production修正估算；只有fixture共用會隨必要tests一併處理。

## 8. 完成門檻

全部修正後至少執行：

```text
gofmt -w pkg/infra/localmq
go test ./pkg/infra/localmq -count=1
go test -race ./pkg/infra/localmq -count=1
go test ./...
go test -race ./...
go vet ./...
git diff --check
```

並另外確認：

1. 所有named durability fault points都有restart invariant test；
2. multi-segment reader benchmark沒有乘法型boundary scan；
3. Stop只有在pending checkpoint durable且membership成功移除後回success；
4. ForceRetire在每個合法rotation/empty-segment crash state可idempotent完成；
5. retention遇到任何identity、checkpoint或control不一致時floor保持不變；
6. staged與unstaged合併後沒有加入第1節排除的V1外功能。
