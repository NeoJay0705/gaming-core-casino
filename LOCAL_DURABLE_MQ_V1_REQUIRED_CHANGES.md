# Local Durable MQ V1 必要修正實作指引

## 1. 文件目的

本文件記錄 staged implementation 相對於 `LOCAL_DURABLE_MQ_V1_DESIGN.md` 的必要修正。目標是修正會影響 correctness、liveness、故障恢復或 network PV I/O 成本的問題，不擴張 V1 功能。

本文件不是新架構提案。下列既有方向維持不變：

- brokerless、直接寫入 RWX PV；
- per-process/per-topic writer lane；
- segmented WAL、durable frontier 與 at-least-once delivery；
- consumer-owned idempotency；
- Redis 只負責 membership 與 lane assignment；
- PROTECTED/BEST_EFFORT retention；
- ambiguous node failure 必須先 external fencing；
- 不加入 partition、DLQ、producer dedup、exactly-once、replication、global ordering 或 arbitrary replay。

## 2. Review 結論

目前實作可以編譯且既有 tests/race test 通過，但尚不能視為符合設計。必要修正分成三類：

1. 可能造成 record 被跳過或 checkpoint 過度前進；
2. 可能造成 lane 永久無法被處理、retention 失效或故障後無法 retire；
3. 會在 network PV 產生與資料量不成比例的重複讀取，抵銷本功能的主要價值。

以下每一項都直接對應既有需求或設計 invariant。未列出的功能不應順便加入。

## 3. 修正順序

建議依下列順序實作，避免後面的測試建立在錯誤 reader/checkpoint 語意上：

1. RC-01 contiguous batch；
2. RC-02 acknowledge checkpoint safety；
3. RC-03 checkpoint failure gate；
4. RC-04 fair lane scheduling；
5. RC-05 segment age rotation；
6. RC-06 pressure GC；
7. RC-07 ForceRetire crash recovery；
8. RC-08 sequential reader；
9. RC-09 deterministic fault tests；
10. RC-10 移除未使用或無法正確定義的表面能力。

## 4. 必要修正

### RC-01：Batch 必須保持 sequence contiguous

問題位置：`pkg/infra/localmq/reader.go` 的 `readSegment`。

目前某筆 record 會使 batch 超過 `maxBytes` 時，程式只是不加入該筆，然後繼續掃描後續 records。若後續較小 record 被加入，handler 會收到不連續 sequence，成功後 checkpoint 會越過被跳過的 record。

必要修改：

1. 在 `readSegment` 維護 `batchFull` 狀態；
2. 第一筆 record 即使大於 `maxBytes` 仍允許加入，確保 oversized-but-valid record 可以前進；
3. batch 已有資料後，若下一筆會超過 `maxMessages` 或 `maxBytes`，立即將 `batchFull=true`；
4. `batchFull` 後不得再加入任何後續 record；
5. 在 RC-08 完成前，可以繼續掃描剩餘 bytes 做完整性驗證，但回傳資料必須是 contiguous prefix；
6. `Batch.Messages[i+1].Sequence` 必須永遠等於 `Batch.Messages[i].Sequence+1`。

必要測試：

- payload 大小為 `60, 60, 20`、`maxBytes=100` 時，第一批只能包含第一筆；
- 下一次從 checkpoint 讀取時必須先得到第二筆，不可直接得到第三筆；
- 第一筆本身大於 `maxBytes` 時仍單筆 delivery；
- 跨 segment 的 batch 仍維持 sequence continuity。

預估：production 10–20 行，tests 40–70 行。

### RC-02：AcknowledgeRecords 必須以 effective checkpoint 驗證

問題位置：`pkg/infra/localmq/admin_ops.go` 的 `acknowledgeRecords`。

`checkpoint-base` 可能落後 member checkpoint。若使用 baseline 判斷 blocked coordinate，stale blocked marker 可能讓 operator 將 baseline 推過實際尚未處理的 records。

必要修改：

1. 同時讀取 `checkpoint-base` 與 `effectiveCheckpoint(topic, group, lane)`；
2. 第一次執行 request 時，只接受 `sequence >= effectiveCheckpoint` 的最早有效 blocked marker；
3. 必須滿足：

   ```text
   blocked.sequence + 1 <= up_to_sequence <= lane_durable_end
   ```

4. 新 baseline 使用：

   ```text
   max(checkpoint_base[lane], effective_checkpoint, up_to_sequence)
   ```

5. 若 `effectiveCheckpoint >= up_to_sequence`，不得再次前進 checkpoint；只有在 baseline 已經到達 target，且仍存在 target 以下 blocked marker 時，才視為「baseline 已發布、marker 尚未清除」的安全重試；
6. marker cleanup 完成後才回 success；
7. 已存在的 `request_id` 必須核對 operation、topic、group、lane 與 target，不能只核對部分欄位。

必要測試：

- baseline=0、member checkpoint=100、stale block=50、target=200 必須拒絕；
- effective=100、block=120、target=121 必須成功；
- crash 模擬在 baseline publication 後、blocked cleanup 前，重試同 request ID 必須完成 cleanup；
- 同 request ID 改用不同 topic/group/lane/target 必須 conflict。

預估：production 20–35 行，tests 60–90 行。

### RC-03：Checkpoint publication failure 後不得開始新 handler

問題位置：`pkg/infra/localmq/consumer.go` 的 handler success path 與 `checkpointTracker`。

目前 checkpoint flush 失敗後會回到主迴圈，下一輪可能先讀取並執行下一個 batch，再次擴大尚未 durable checkpoint 的 replay window。此外 `pendingCount` 現在計算 batch 次數，而不是 records 數。

必要修改：

1. `checkpointTracker` 增加 `mustFlush bool`；
2. `advance(next)` 以 `next-oldNext` 累加 records，並檢查 uint overflow；
3. flush 失敗時設定 `mustFlush=true`；
4. lane loop 每次準備 `readLaneBatch` 或呼叫 handler 前，若 `mustFlush=true`，只能 retry 相同 watermark；
5. retry 失敗時 bounded backoff，不得讀取下一批；
6. flush 成功後才清除 `mustFlush`、`pendingRecords`，並允許下一個 handler；
7. shutdown、ownership loss 與 permanent error 前，仍使用 bounded context flush pending checkpoint。

必要測試：

- checkpoint write 連續失敗期間，handler call count 不得增加；
- 一個 500-record batch 必須增加 500，而不是 1；
- checkpoint recovery 後從原 memory watermark 繼續，不重複同 process 已成功的下一批；
- shutdown flush failure 不得被回報為成功停止。

預估：production 20–40 行，tests 70–110 行。

### RC-04：所有 owned lanes 都必須公平取得執行機會

問題位置：`pkg/infra/localmq/consumer.go` 的 `reconcile` 與 long-running `laneWorker`。

目前只建立前 `worker_concurrency` 條 lane workers。這些 workers 即使追上 durable end 仍持續輪詢原 lane，因此其餘 owned lanes 可能永久飢餓。

必要修改：使用 bounded、round-robin lane scheduler，不增加新 public config。

建議最小結構：

```text
Consumer
  owned map[laneID]*laneState
  runnable sorted/rotating lane IDs
  fixed worker pool: worker_concurrency

laneState
  checkpoint tracker
  process-local reader cursor
  in_flight
  retry_not_before
  blocked
  ownership context/cancel
```

執行規則：

1. `reconcile` 只更新 owned lane set，不直接為每條 lane建立永久 goroutine；
2. 固定數量 worker 每次向 scheduler 取得一條 runnable lane；
3. 每次 dispatch 最多完成一個 read/handler/checkpoint step，然後把 lane 放回 round-robin 尾端；
4. 同一 lane 同時只能有一個 in-flight step；
5. empty、backoff、blocked lane 設定 `retry_not_before`，不得佔住 worker；
6. ownership loss 時取消該 lane in-flight context、flush 已成功 watermark，之後移除 state；
7. scheduler queue、lane state 與 worker count 都受既有 lane scan bound 和 `worker_concurrency` 限制；
8. handler 前重新驗證 group control、membership lease 與 lane ownership。

這個 scheduler 只解決既有 lane concurrency 語意，不加入 priority、delay queue 或動態 partition。

必要測試：

- 單 consumer、lane 數為 `2*worker_concurrency+1`，每條 lane 最終都被處理；
- hot lane 持續有資料時，其他 lane 仍能前進；
- 同一 lane handler concurrency 永遠為 1；
- membership churn 後舊 owner 不再開始 handler，新 owner由 durable checkpoint 接手；
- blocked lane 不佔用 worker slot。

預估：production 100–170 行，tests 110–170 行。

### RC-05：Segment age 必須以 segment creation time 計算

問題位置：`pkg/infra/localmq/wal.go` 的 `shouldRotate`。

目前使用最後一筆 append time；檢查又發生在 append 後，因此持續寫入時幾乎永遠不會因 age rotation。

必要修改：

1. `writerLane` 保存 `activeCreatedAt`；
2. 建立或開啟 active segment 時，從 segment header 初始化該欄位；
3. age 判斷改為 `clock().Sub(activeCreatedAt) >= SegmentMaxAge`；
4. 若 active segment 已有 records 且在下一批 write 前已超齡，先 seal/open next segment，再寫新 batch；
5. rotation 失敗時 lane 進入 failed，尚未寫入的新 requests 回 `RejectedBeforeWrite`；
6. idle lane 不額外增加 timer goroutine。沒有新 message 時允許 partial segment 延後到下一次 Publish 或 graceful stop 才 seal，因 V1 retention 不是 fixed deletion deadline。

必要測試：

- 持續每隔小於 `segment_max_age` 的時間 append，總 segment age 超過上限時仍會 rotate；
- idle 後下一筆 append 先觸發 rotation；
- empty `.open` 不 seal；
- rotation failure 不得把尚未 write 的 request 分類為 `DurabilityUnknown`。

預估：production 15–30 行，tests 50–80 行。

### RC-06：Pressure GC 只回收到解除壓力，且全域選最舊資料

問題位置：`pkg/infra/localmq/retention.go` 的 `runRetention` 與 `retainLane`。

目前在一次 RunOnce 開始時計算 pressure，之後會按 topic/lane 順序刪除全部 eligible segments。這可能在只缺少少量空間時刪除大量 BEST_EFFORT retained history。

必要修改：將 time GC 與 pressure GC 分開。

Time GC：

1. 保留目前「超過 retention 且通過 PROTECTED barriers」的 contiguous prefix 語意；
2. 不需要跨 topic 排序；
3. 每次 logical floor publication 前重讀 group controls/checkpoints。

Pressure GC：

1. 每輪只收集每條 lane 的「最舊一個」eligible sealed segment；
2. 以 `footer.MaxAppendNanos` 建立跨 topic/lane min-heap；
3. 取出全域最舊 candidate；
4. commit 前重讀 Topic/Group controls、retention floor 與 effective checkpoints；
5. 先 durable publish新 floor，再 rename 到 topic-local staging，並 sync source/staging directories；
6. unlink 後重新 `statfs`；
7. free bytes 已回到 `producer_stop_free_bytes` 時立即停止；
8. 若仍在 pressure，才把同 lane 的下一個 contiguous segment放入 heap；
9. 沒有 eligible candidate 時停止並回傳可觀測的 capacity-blocked 狀態，不可無限 retry。

Whole-lane collection 前必須檢查所有 non-DELETED groups 的 blocked markers，不只 PROTECTED barriers；仍有 blocked/admin reference 時不得刪除 lane directory。

必要測試：

- 只需刪除一個 segment 即解除 pressure 時，不得刪除第二個；
- 多 topic 時先刪全域最舊 candidate，不受 topic 字典順序影響；
- checkpoint 在 candidate planning 後倒退或 group 轉為 barrier 時，commit 必須停止；
- BEST_EFFORT blocked marker 存在時不得 whole-lane collect；
- floor publication、rename、unlink 各 crash point 重啟後仍不得重新 delivery floor 以下資料。

預估：production 100–170 行，tests 110–180 行。

### RC-07：ForceRetireLane 必須涵蓋 rotation 的合法 crash states

問題位置：`pkg/infra/localmq/admin_ops.go` 的 `forceRetireActiveLane`。

必須支援以下 durable namespace：

1. frontier 指向正常 `<base>.open`；
2. `<base>.open` 已 seal/rename 成 `<base>.wal`，但 frontier 尚指向舊 base；
3. 舊 `<base>.wal` 與新 `<next>.open` 都存在，frontier 已指向新 `.open`；
4. 新 `<next>.open` 只有完整 header，frontier 尚未前進；
5. active `.open` 在 durable byte end 後有 incomplete/non-durable tail。

必要修改：

1. 先依 durable-end 決定 authoritative base/next sequence；
2. 若 matching `.open` 存在，驗證 topic、lane、base、record continuity、append-time monotonicity，只 truncate 到 `durable_byte_end`；
3. 若 matching `.open` 不存在但 matching `.wal` 存在，驗證 footer 的 next sequence、file length、record count、append range 與 data checksum；不得重新改寫 sealed file；
4. 只允許一個位於 `frontier.next_sequence`、且內容只有合法 header 的 orphan `.open`；其他 unmatched `.open` fail closed；
5. sync 所有 truncate/seal/remove/rename 對應 directories；
6. 最後 exclusive publish retired marker，再移除 durable-end；
7. 已 retired 時必須驗證 final sequence 與 request audit，不得無條件接受不同 request；
8. fencing evidence 只保存 hash，不能保存原文。

必要測試：

- rotation 的 footer sync、rename、new header sync、durable-end rename 前後逐點 crash；
- active tail 截斷後只保留 durable records；
- wrong topic/lane header、sequence gap、CRC mismatch 全部 fail closed；
- 同 request ID retry成功，不同 coordinate conflict。

預估：production 50–90 行，tests 90–140 行。

### RC-08：Reader 必須使用 process-local sequential cursor

問題位置：`pkg/infra/localmq/reader.go` 與 `retention.go` 的 `readSealedFooter`。

目前每個 batch 都從 segment header 重掃到 durable end，sealed segment 還會重新計算整檔 CRC。對 128 MiB segment 分成數百個 batches 消費時，會產生數十到數百倍 network PV read amplification。

必要修改：新增 package-private `laneReader`，由 RC-04 的 `laneState` 持有，不改 public API、不新增 persisted index。

建議 cursor：

```text
topic
lane_id
segment_base
file_path
byte_offset
next_sequence
validated_file_identity
observed_retention_floor
observed_durable_end
```

讀取規則：

1. worker 第一次從 durable checkpoint 啟動時，定位包含該 sequence 的 segment，最多掃描一次到目標 byte offset；
2. 後續 batch 從原 byte offset 繼續，不從 header 重掃；
3. sealed segment 第一次開啟時完整驗證 header/footer/data CRC，之後同一 immutable file identity 不重複驗證；
4. active segment 每次只重讀 durable-end，並解析上次 cursor 到新 durable byte end 的 records；
5. active segment變成 sealed 時，驗證 footer summary 與之前累計結果一致，再切換下一 segment；
6. floor 前進、`ENOENT`、`ESTALE`、inode/file identity 改變或 ownership change 時丟棄 cursor，重讀 retention-index 與 segments；
7. batch byte accounting 使用累加變數，不可每加入一筆就重新遍歷 `result.Messages`；
8. cursor 只存在 memory；crash/rebalance 後從 durable checkpoint 重新定位，最多造成一次重新掃描和 duplicate，不影響 persisted format。

Retention fast path：

1. eligibility 只以 bounded `ReadAt` 讀 header與固定長度 footer；
2. 驗證 identity、footer CRC、file length、sequence range 與 append range；
3. 不得在每個 maintenance cycle 使用 `os.ReadFile` 載入整個 segment；
4. 完整 data CRC 驗證由 reader首次開啟、ForceRetire、explicit inspection/conformance負責。

必要測試與 benchmark：

- 讀取 N 個 batches 時，segment bytes read 接近 O(segment size)，不可為 O(N × segment size)；
- active durable frontier 前進時只讀新增 bytes；
- rotation、GC rename/unlink、floor advance 時 cursor 正確失效；
- benchmark 輸出 bytes read/message 與 storage operations/message；
- record continuity與 CRC corruption 仍 fail closed。

預估：production 140–240 行，tests/benchmark 120–190 行。

### RC-09：加入 deterministic durability fault injection

問題位置：目前 WAL、metadata、checkpoint 與 retention 直接使用 `os.*`，現有 tests 無法覆蓋設計列出的 durable crash boundaries。

這不是新增產品功能，而是自製 durable queue 上線前必要的 correctness 證據。

必要修改：

1. 新增 package-private `storageOps`，只包含目前實際使用的 file primitives，例如 open/create/read/stat/write/sync/truncate/rename/remove/readDir/statfs；production implementation 直接委派給 `os`/`unix`；
2. file handle 使用最小 interface，包含 `ReadAt`、`Write`、`Seek`、`Truncate`、`Sync`、`Stat`、`Close`；
3. 新增 package-private named fault points，不進 public API：

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

4. clock、membership store 與 filesystem operations 都由 constructor 注入 package-private default；
5. 每個 fault test 在指定 boundary 回傳 error或模擬 process crash，關閉舊 handles後以全新 Client/storage instance重新開啟同一 temp root；
6. restart assertion 只檢查 invariants：success record不遺失、unknown record可以存在或不存在、checkpoint不跳過、floor不倒退、corrupt metadata fail closed；
7. 不建立通用 virtual filesystem，也不加入 production dependency；只包裝 localmq 使用的 primitives。

必要測試：

- Publish 在 first-write 前後的三態分類；
- durable-end publication 每一點 crash；
- segment seal/rotation每一點 crash；
- checkpoint replace與baseline compaction race；
- retention floor、staging rename、unlink crash；
- Create/Delete Group control publication；
- concurrent Publish/Stop、dual owner checkpoint與 reader/GC race。

預估：test seam production 180–300 行，fault/concurrency tests 300–500 行。這部分和 RC-01～RC-08 的 tests 有重疊，不應重複建立兩套 harness。

### RC-10：移除未使用或無法正確定義的表面能力

這一項只做一致性清理，不增加功能。

必要修改：

1. `MaintainerConfig.CleanupGrace` 目前沒有被使用。若 V1 接受 baseline durable 後立即刪除 Redis 已判定 inactive 的 member directory，則從 config、example與設計移除 `cleanup_grace`；不要保留無效設定；
2. `laneWorker.terminal`、未被等待的 `done` channels 等 dead state，在 RC-04 scheduler 完成後移除；
3. `localmq_orphan_lanes` 無法只靠 filesystem 正確判定，因 unretired lane 可能仍有暫停中的合法 writer。V1 不新增 producer heartbeat；移除該 metric 的強制承諾，使用 `InspectLane.Retired=false` 提供候選資料，由 external fencing流程判斷 orphan；
4. `localmq_corruption_total` 若保留，必須只在 typed corruption boundary 增加，不能只註冊永遠為零的 collector；否則先從 V1 required metrics移除，待 typed corruption classification完成再加入；
5. 修正 `Client.RegisterMetrics` 註解：Topic/Group 是允許的 durable bounded labels，禁止的是 `message_id`、`lane_id` 與 error text。

預估：production/design/example 淨修改 20–45 行，tests 20–40 行。

## 5. 設計文件只需補充的內容

`LOCAL_DURABLE_MQ_V1_DESIGN.md` 不需要重寫架構，只需加入以下明確語意：

1. `worker_concurrency` 限制同時 handler steps，不限制 owned lane 數；所有 owned lanes 使用 bounded fair scheduler；
2. batch 因 byte/message limit 截止時只能回傳 contiguous prefix；
3. checkpoint publication 一旦失敗，在同 watermark durable 前不得開始下一個 handler；
4. pressure GC 每次只刪全域最舊 eligible segment，並在解除 producer pressure 後停止；
5. `segment_max_age` 以 active segment creation time 計算；idle partial segment不是固定期限刪除保證；
6. process-local reader cursor不是 persisted format，也不改 checkpoint coordinate；
7. ForceRetire 必須接受設計 protocol可能產生的中間 durable namespace；
8. retention eligibility fast path不重複掃描整個 immutable segment，完整 checksum驗證由 reader/recovery/conformance負責。

預估設計文件修改 20–35 行。

## 6. 明確不做的修改

為避免超出需求，本輪不得加入：

- logical partition 或 writer shards；
- persisted offset index；
- producer-side dedup；
- DLQ、自動 skip、priority 或 delayed delivery；
- exactly-once、transaction或跨 lane ordering；
- Raft、WAL replication或 maintenance leader election；
- per-Topic quota；
- arbitrary reset、timestamp replay或 generation；
- 新的 HTTP/gRPC admin server；
- 通用 filesystem framework或新的 DI framework。

若 sequential cursor仍無法達到 benchmark gate，才另開 ADR 評估 sparse persisted index；不得在沒有 benchmark證據前加入。

## 7. 修改量與完成門檻

預估修改量以 additions + material edits 計算：

| 範圍 | 預估行數 |
|---|---:|
| RC-01～RC-08 production correctness/performance | 450–750 |
| 對應 regression/concurrency tests | 600–950 |
| RC-09 共用 fault injection seam與剩餘 crash tests | 450–750 |
| Design與dead-surface cleanup | 40–80 |
| 合計 | 約 1,500–2,400 |

若先做第一個可 review batch，建議只完成 RC-01、RC-02、RC-03 與其 tests，約 220–365 行。這一批消除已知資料跳過與 checkpoint safety 問題，且不需要先重構 scheduler/reader。

全部必要修改完成後，至少必須通過：

```text
go test ./pkg/infra/localmq
go test -race ./pkg/infra/localmq
go test ./...
go test -race ./...
go vet ./...
```

此外，RC-09 的 fault matrix必須全部通過，RC-08 benchmark必須證明 read amplification接近線性，才可宣稱 staged implementation符合設計的成熟開源專案品質標準。
