# Local Durable MQ V1 Unstaged Review Round 2 必要修正方法

## 1. 文件目的

本文件只記錄第二輪 unstaged review 後仍需完成的必要調整。設計依據為：

- `LOCAL_DURABLE_MQ_V1_DESIGN.md`；
- `LOCAL_DURABLE_MQ_V1_REQUIRED_CHANGES.md`；
- `LOCAL_DURABLE_MQ_V1_UNSTAGED_REVIEW_REQUIRED_CHANGES.md`。

本輪不重新設計 Local Durable MQ，也不重做已正確完成的 UR-01～UR-07。
修正僅處理：

1. sequential cursor 跨 sealed segment 時可能在同一 batch 重複 record；
2. membership timeout 或 ownership loss 時，舊 worker checkpoint flush 尚未完成便可能建立 replacement worker；
3. 補上直接保護上述修正，以及既有 UR-08 production gate 尚缺的最小測試證據。

不得加入 partition、DLQ、producer dedup、exactly-once、replication、persisted
offset index、priority、delayed delivery、任意 replay或新的 public API。

## 2. Review 結論

目前 unstaged production changes 的架構方向符合 V1，沒有產品功能超出需求。
checkpoint clamp、permanent-error flush、Stop flush result、ForceRetire request
identity、retention footer identity、membership factory、group initialization recovery
與 blocked filename parsing 都是必要調整，不應回退。

仍需完成 R2-01 與 R2-02。R2-03 只補足設計已承諾的 correctness evidence，
不是新增產品能力。

## 3. Production 必要修正

### R2-01：修正 cursor fast path 跨 sealed segment 的 batch 重複

修改檔案：

- `pkg/infra/localmq/reader.go`
- `pkg/infra/localmq/reader_benchmark_test.go`
- reader regression test file

#### 3.1 問題

`readLaneBatchWithCursor` 在 cursor fast path 讀到 current sealed segment 尾端時，
會先把讀到的 messages加入 `result`。若 batch 尚未達限制且 durable end 位於後續
segment，現行流程會清除 cursor，然後以呼叫進來時的原始 `from` 進入 slow path。

因此 slow path 會重新定位到剛讀完的 current segment，再次加入其尾端 records。
例如每個 segment 有 100 筆、batch size 為 99：

```text
call 1: 0..98
call 2 fast path: 99
call 2 slow path: 99, 100..196
```

第二個 batch 含兩筆 sequence 99，違反「同一 batch 是同 lane、sequence
contiguous prefix」的設計 contract。

#### 3.2 必要修改

只調整 process-local cursor 流程，不改 persisted format或 checkpoint coordinate。

1. 將 `batchBytes` 初始化移到 fast path 之前，使 fast path與後續 segment共用同一個
   batch budget。
2. fast path成功後：
   - append `segmentResult.messages`；
   - 將 `segmentResult.bytes` 納入 `batchBytes`；
   - 若 `batchFull`，維持現有方式立即回傳；
   - 若已到整條 lane durable end，立即回傳；
   - 若 `segmentAtEnd` 且後面仍有資料，將 continuation coordinate設為
     `reader.cursor.sequence`，不得保留原始 `from`。
3. fast path成功抵達 segment end時，不得無條件清空 cursor。後續定位應利用現有
   cached boundaries選擇 `next.base == cursor.sequence` 的下一個 segment。
4. 只有 fast path發生錯誤、file identity失效或 boundary不可用時，才清除 current
   cursor並進入既有 fail-closed/slow-locate流程。
5. 進入下一個 segment後，`maxMessages` 與 `maxBytes` 必須扣除 fast path已加入的
   records/bytes。
6. 最終 `NextSequence` 必須等於回傳 batch最後一筆 sequence加一；不得只因跨
   segment而跳過或重複 sequence。

建議以一個局部 boolean（例如 `continueFromCursor`）區分快速路徑已成功前進與真正
需要重新定位的情況；不需要新增 reader abstraction或 persisted index。

#### 3.3 必要測試

新增 table-driven regression test，至少涵蓋：

1. 兩個以上 sealed segments；
2. batch size與 records-per-segment不整除；
3. 第一個 call停在 segment尾端前一筆，下一個 call跨過 boundary；
4. 每個 batch內相鄰 sequence固定差一；
5. 所有 calls串接後，每個 expected sequence只出現一次；
6. `NextSequence` 每次嚴格前進，最後等於 durable end；
7. byte limit造成的跨 segment boundary也維持 contiguous prefix。

調整 benchmark：

- 將 `batchSize` 改為不等於且不整除 `recordsPerSegment`，例如 97；
- benchmark loop除了檢查 `NextSequence`前進，也逐筆確認回傳 sequence等於當次
  expected coordinate；
- 保留既有 additive `segment_opens/op`上限，確認修正沒有恢復
  `segments × batches`掃描。

預估：production 12～22 行，tests/benchmark 35～60 行。

### R2-02：禁止 canceled worker flush期間建立同 lane replacement

修改檔案：

- `pkg/infra/localmq/consumer.go`
- consumer lifecycle/concurrency regression test file

#### 3.4 問題

`pauseWorkers`與 ownership-loss reconcile 對沒有 in-flight handler的 worker，會先從
`workers` map移除，再於 `c.mu`外執行 checkpoint flush。heartbeat loop與reconcile
loop是不同 goroutine，因此另一個 reconcile可能在舊 flush完成前看到 lane不存在，
建立同 instance、同 lane的新 worker。

在 network PV checkpoint write較慢時，可能形成：

```text
old worker removed from map
→ old checkpoint flush blocked
→ reconcile creates replacement worker
→ replacement reads/handles/flushes same instance/lane
→ old lower checkpoint publication finishes last
```

結果最多通常是 replay，但會破壞同一 instance/lane checkpoint publication單調性。
若 stale reconcile plan跨越 membership timeout套用，也可能在明確 pause後重新建立
worker。

#### 3.5 Worker draining barrier

採最小 package-private state，不新增 public config。

1. 在 `laneWorker`加入 `draining bool`，表示 worker已取消且尚未完成 terminal flush。
2. ownership loss或 membership pause時：
   - 先呼叫 `worker.cancel()`；
   - 設定 `worker.draining = true`；
   - in-flight worker維持目前做法，保留同一 pointer直到 scheduler step返回；
   - idle worker也暫時保留在 `workers` map，不能在 flush前移除。
3. `claimLane`必須跳過 `draining`、context已取消或仍 in-flight的 worker。
4. idle worker在 lock外完成 bounded flush後，再取得 `c.mu`，只有 map內仍是同一
   pointer且沒有 in-flight時才移除。
5. 移除後呼叫既有 `rebuildLaneOrderLocked`，再通知 scheduler；不得維護第二套
   lane order。
6. in-flight worker由 scheduler完成 step與terminal flush後，透過既有
   pointer-identity check移除；Consumer處於 Stopping時仍保留給 Stop做最後收斂。
7. replacement只能在舊 worker已完成 terminal flush並從 map移除後，由後續
   reconcile建立。

不得以持有 `c.mu`執行 filesystem flush作為修正，避免 network PV syscall阻塞所有
scheduler與lifecycle state transition。

#### 3.6 Membership plan generation

為避免 timeout前取得的 reconcile結果在 timeout後套用，增加最小的 process-local
membership狀態：

```text
membershipReady bool
membershipGeneration uint64
```

規則：

1. initial heartbeat成功、Start commit時設定 `membershipReady=true`。
2. heartbeat失敗且已超過 `membership_timeout`時，在 `c.mu`內：
   - 若原本 ready，改為 false；
   - 遞增 generation；
   - 再執行 worker pause/drain。
3. heartbeat重新成功時，在 `c.mu`內更新 `lastBeat`；若由 paused恢復，設定
   ready並遞增 generation。
4. reconcile開始規劃前保存 generation；若 `membershipReady=false`，不得建立
   worker。
5. reconcile完成 Redis/storage reads、準備套用 ownership前，再次取得 `c.mu`並確認：
   - state仍為 Started；
   - consumer context未取消；
   - membershipReady仍為 true；
   - generation與規劃開始時相同。
6. 任一條件不符時丟棄該次 plan，不修改 worker map；下一次 reconcile重新取得
   members與lanes。

generation只存在 memory，不寫入 Redis或PV；它只防止本 process套用 stale async
result，不是 distributed fencing token。

#### 3.7 必要測試

使用現有 package-private `membershipFactory`與fake `membershipStore`，不建立通用
Redis emulator。

至少新增：

1. idle worker checkpoint flush被 channel阻塞時，concurrent reconcile不得建立同
   lane replacement；解除 flush後才允許後續 reconcile建立。
2. in-flight handler ownership loss後，舊 handler返回前同 lane最大 concurrency為1。
3. reconcile已取得 active members、尚未 commit plan時觸發 heartbeat timeout；該
   stale plan不得建立worker。
4. timeout後即使 scheduler被喚醒，也不得開始新 handler attempt。
5. heartbeat重新成功後，使用新 generation的 reconcile可恢復worker。
6. Stop與draining flush同時發生時，Stop仍收集pending checkpoint結果；flush失敗
   不得 `ZREM`。
7. 上述測試納入 `go test -race`，並使用 atomic counter記錄同 lane最大 handler
   concurrency。

預估：production 35～55 行，tests 70～110 行。

## 4. UR-08 尚缺的最小 release-gate tests

以下測試不要求修改 production architecture，但若要宣稱完整符合既有
`LOCAL_DURABLE_MQ_V1_UNSTAGED_REVIEW_REQUIRED_CHANGES.md`，仍必須補齊。

### R2-03A：Reader與scheduler

- R2-01的非整除跨 segment測試；
- hot lane持續有資料時，其他owned lanes仍前進；
- `2*worker_concurrency+1` lanes經真實scheduler loop後全部取得handler機會；
- ownership churn與race test中同 lane最大 handler concurrency固定為1；
- 同一 reader遇到active rotation、floor advance及GC rename/unlink後可依設計重新定位。

### R2-03B：Retention

- candidate planning後PROTECTED checkpoint倒退或control轉為非ACTIVE時，不發布floor；
- BEST_EFFORT blocked marker仍存在時，不做whole-lane collection；
- 跨Topic/lane的實際pressure GC確實先刪全域最舊candidate，不只測heap comparator；
- floor已發布後，在staging rename與unlink recovery state中重新建立storage/reader，
  不delivery floor以下資料。

### R2-03C：ForceRetire與durability restart

- empty `.open`在retired marker publication、open remove、segments directory sync及
  durable-end remove後中斷，均可由同request idempotent完成；
- wrong Topic/lane/base、sequence gap、append-time regression、footer/data CRC錯誤均
  fail closed；
- marker durable但audit尚未寫入時，相同request/evidence成功，不同evidence conflict；
- publish durability fault tests在建立restart instance前明確關閉舊writer/file
  handles，避免同process open handle冒充crash recovery；
- checkpoint fault test重建storage後確認checkpoint不高於handler成功watermark。

這些tests應優先重用現有接受 `testing.TB`的fixtures。可以依責任拆檔，但拆檔不是
correctness必要條件，不應為了檔案美化重寫production code。

預估：額外 tests/fault harness 250～450 行。

## 5. 修改量估算

| 範圍 | Production | Tests/benchmark |
|---|---:|---:|
| R2-01 cursor boundary | 12～22 | 35～60 |
| R2-02 worker/membership race | 35～55 | 70～110 |
| R2-03 release-gate evidence | 0～10 | 250～450 |
| 合計 | 約47～87 | 約355～620 |

若先只修正兩個 runtime問題並加入直接 regression tests，約 **150～250 行**。
若完成本文件全部 release gate，約 **400～700 行 material changes**。其中大多數是
deterministic tests，不增加production功能。

## 6. 明確不修改

本輪不做：

- public API、config或persisted coordinate變更；
- persisted segment index；
- Redis fencing token或distributed lock；
- DLQ、自動skip或任意checkpoint reset；
- reader整體重寫；
- test檔案純粹為縮短行數而拆分；
- package、命名、註解與既有文件的大規模整理；
- 回退 blocked UUID filename、group crash recovery或ForceRetire evidence hash等已完成
  且必要的修正。

## 7. 完成門檻

至少執行：

```text
gofmt -w pkg/infra/localmq
go test ./pkg/infra/localmq -count=10
go test -race ./pkg/infra/localmq -count=1
go test ./...
go vet ./...
git diff --check
go test ./pkg/infra/localmq -run '^$' \
  -bench BenchmarkRequiredCursorReadAmplification -benchtime=1x
```

並人工確認：

1. 非整除batch跨segment時，每個batch內sequence嚴格連續；
2. canceled/draining worker尚未flush完成前，不存在同lane replacement；
3. membership timeout之後，舊generation reconcile plan不能建立worker；
4. checkpoint publication failure最多造成replay，不會造成skip或低值覆寫已完成的
   同instance高值publication；
5. 所有新增state與constructor保持package-private；
6. staged與unstaged合併後仍未加入任何V1排除功能。
