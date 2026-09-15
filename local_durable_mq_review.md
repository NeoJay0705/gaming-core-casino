# Local Durable MQ 需求文件審查報告

- 審查對象：`local_durable_mq.md`（28 節，1641 行）
- 審查日期：2026-09-09
- 審查重點：併發正確性、fencing、liveness（活性）、shared filesystem 實際語意、可擴展性、文件一致性

---

## 0. 結論摘要

這份設計在 **durability protocol、fail-closed 原則、checkpoint 單調性（`max()`）、generation 隔離、missing-coordinate 規則** 上非常嚴謹，大多數常見的 crash-point 與 dual-consumer 問題都已被明確處理。真正需要注意的問題集中在四類：

| 面向 | 評分 | 說明 |
|---|---|---|
| WAL durability / crash recovery | ★★★★★ | flush 順序、durable-end、rotation crash point、CRC 與 identity 驗證都完整 |
| 併發正確性（safety） | ★★★★☆ | 大部分靠 `max()` + generation 化解；**缺少 lock-lease 到期後的 fencing**，以及 GC 決策讀取與 `gc.lock` 的時序綁定 |
| 活性（liveness） | ★★☆☆☆ | 有三條可自我鎖死的路徑：filesystem reserve 死鎖、poison record 拖垮 Publish、Reset/Delete 等不到 batch boundary |
| 效能 / 可擴展性 | ★★☆☆☆ | per-stream durable-end 與 per-batch checkpoint publication 在 shared FS 上會造成嚴重的 fsync + rename 放大；retired lane 累積導致目錄爆炸 |
| 可運維性 | ★★★☆☆ | inspection 與 metrics 齊全，但 blocked state 不 durable、BEST_EFFORT 出界即需人工 reset、無 skip 工具 |
| 文件一致性 | ★★★★☆ | 少數命名、排版與語意歧義（見 §4） |

**最優先要處理的五件事（🔴）**：

1. Advisory lock 因 lease 到期被回收時沒有 fencing，舊 writer 可能繼續 append 到已 seal 的 segment（§2 R1）。
2. 到達 `filesystem_reserved_bytes` 後 consumer 不得開始新 batch，PROTECTED group 無法推進，GC 無法釋放空間，系統自我鎖死（R2）。
3. Retention GC 的「讀取 group control / checkpoint」與「持有 `gc.lock`」之間的時序未綁定，與 Reset 交錯時可能刪除 PROTECTED 資料（R3）。
4. 一筆 poison record + PROTECTED barrier + 無 DLQ + hard limit，最終會讓整個 Topic 的 Publish 被拒（R4）。
5. Reset / Delete 依賴 worker 在「batch boundary」讀到 RESETTING，但 worker 卡在 transient retry 或 hung handler 時永遠不會到 boundary，且持續 heartbeat，管理操作永久掛起（R5）。

---

## 1. 做得好的地方（不需改動）

- **Publish 三態結果**（SUCCESS / REJECTED_BEFORE_WRITE / DURABILITY_UNKNOWN）與 ENOSPC 只能回 UNKNOWN 的規定，避免了「宣稱沒寫」的錯誤。
- **durable-end 先於 delivery** 的設計，杜絕 consumer 處理到 producer 從未 ack 的「幽靈訊息」。
- **effective_offset = max(snapshot, records)**：dual consumer 較晚寫入較小值不會倒退，checkpoint 正確性不依賴 ownership。
- **Generation 用 UUIDv7 隔離 reset 前後 checkpoint**，舊 generation 的遲到 commit 天然無效。
- **Missing-coordinate 統一規則**（新 stream 一律從 `earliest_retained_sequence` 起，且先寫 durable checkpoint 才 deliver），讓 LATEST 不會漏掉初始化期間出現的新 lane。
- **Retention 以 logical floor（retention-index）為權威**，physical unlink 任一 crash point 都不影響正確性。
- **Fail-closed 原則貫徹**：storage_id、fingerprint、CRC、identity 不符一律拒絕，不猜測、不採用本地預設。
- **鎖順序有部分規範**（Topic gc.lock → group lock），且 GC 決策明確不依賴 Redis。

---

## 2. 🔴 高風險：併發正確性與活性

### R1. Cross-node advisory lock 的 lease 到期沒有 fencing，會產生 split-brain

**設計依賴**：§3、§14.2、§25 都以「成功取得 lane-level exclusive advisory lock」作為「原 writer 已不存在」的唯一判準。

**實際行為**：NFSv4、CephFS、EFS 等 shared FS 的 lock 都是 lease-based。當 writer 所在節點與 storage server 發生網路分割、節點 freeze、長時間 I/O stall 或 VM 暫停，server 會在 lease 到期（NFSv4 預設 90 秒）後回收 lock，**但 writer process 仍然活著且不知道 lock 已丟失**。Linux NFS client 對 lost lock 的通報是 `SIGLOST` 或根本沒有通報；§25 要求的「lock lost / lease revoked indication」在多數 backend 上並不可靠。

**併發後果**：

```text
t0  writer W 持有 lane lock，.open 已寫到 seq 1200，durable-end=1200
t1  W 節點與 storage 斷線，W 的 fsync 卡住
t2  lease 到期，Recovery Manager R 取得 lock
t3  R 掃描 valid prefix、append footer、rename .open → .wal、發布 retired marker
t4  W 節點恢復，W 的 fd 仍指向同一 inode（已是 .wal）
t5  W 繼續 append seq 1201.. 到 footer 之後，並覆寫 durable-end 指向新位置
```

結果：sealed `.wal` 尾端出現 footer 之後的 record bytes，`durable-end` 與 retired marker 互相矛盾。若 reader 以「檔尾往前讀 footer」定位，會把 record bytes 當 footer 解析而 block stream；若 reader 信任 durable-end，可能讀到 footer 之後的內容。無論如何都是 corruption incident，而非設計宣稱的「只會 replay」。

Checkpoint lane（§14.2）與 Compactor 的 `compact.lock`、Retention Manager 的 `gc.lock` 都有同樣的問題，**gc.lock split-brain 尤其危險**：兩個 Retention Manager 同時執行 unlink 會破壞 retention-index 的單調性。

**建議**：
- 在每個受 lock 保護的 directory 內放一個 **fence epoch 檔**（`fence`，內容為 UUIDv7 epoch）。取得 lock 者先 atomic 寫入新 epoch 再 mutation；writer 在**每次 group flush 前**（不是每筆 Publish）比對 epoch，不符即進入 failed state。這把 split-brain 視窗從「無限」縮到「一個 group commit」。
- 對 sealed `.wal` 定義 **footer 定位方式**（固定長度 trailer + magic + 指向 footer 起點的 offset），並規定「檔案長度 ≠ footer 宣告長度」必須判為 corruption 而非猜測。
- Conformance test（§25、§28-20）加入 **lease 到期 split-brain 測試**：SIGSTOP writer 超過 lease 時間、讓 recovery 取得 lock、再 SIGCONT writer，驗證 writer 的後續寫入會被拒絕或被偵測。
- `storage-admin.lock`、`gc.lock`、`group.lock`、`compact.lock` 同樣套用 fence epoch。

### R2. Filesystem reserve 觸發後系統自我鎖死

**設計條文**：§14.2 與 §25 規定到達 `filesystem_reserved_bytes` 後「consumer 不得開始新的 handler batch」，且「不得開始新的 WAL、handler 或 compaction workload」。同時 §25 允許 Topic hard limit **overcommit**。

**鎖死路徑**：

```text
多個 Topic hard limit 總和 > 可用容量（允許 overcommit）
→ WAL 成長到 filesystem reserve
→ consumer 停止開新 batch → PROTECTED group checkpoint 停止推進
→ Retention GC 因 PROTECTED barrier 無法刪除任何 segment
→ 空間永遠不會釋放
→ producer 全部 REJECTED / UNKNOWN，系統靜止，需人工介入
```

Consumer checkpoint 每筆只有幾十 bytes，把它與 WAL 用同一條 reserve 線是反效果：**唯一能釋放空間的機制（消費 + GC）被第一個關掉**。

**建議**：
- 採兩階段水位：`producer_reserve_bytes`（較高，先擋 producer）與 `recovery_reserve_bytes`（較低，只保護 recovery / seal / unlink）。Consumer checkpoint 與 GC 允許用到較低水位。
- 明確規定：**consumer checkpoint 寫入永遠優先於 producer WAL**，只有到達 recovery reserve 才停止 checkpoint。
- 若堅持允許 overcommit，必須新增 metric `filesystem_reserve_pressure` 與告警，並在文件中把「overcommit + reserve = 可能死鎖」列為已知限制。

### R3. Retention GC 的決策讀取未綁定在 `gc.lock` 之內

**設計條文**：§25「Retention Manager 必須取得 `gc.lock` 才能執行 segment/lane deletion」；§21 Reset 也在 `gc.lock` 下切換 generation。

**問題**：條文只要求「刪除」在 lock 內，沒有要求「讀取 group control、判定 generation、計算 effective offset」也在同一個 lock 持有期間內完成並在 unlink 前重新驗證。若實作先在 lock 外做一輪掃描與決策（這是最自然的效能優化寫法），會出現：

```text
RM 讀 control：gen G1 ACTIVE，effective(G1, stream S) = 5000
Reset 取得 gc.lock，切到 G2，snapshot 為 EARLIEST（S = 0），釋放 lock
RM 取得 gc.lock，依舊決策刪除 S 的 [0, 5000) 段
→ PROTECTED group 在 G2 要 replay 的資料已被刪除
```

**建議**：文件明確規定 GC 決策所需的一切讀取（storage-control、Topic control、每個 group 的 control 與 effective offset、retention-index）都必須在 `gc.lock` 持有期間讀取，並在每個 stream 的 unlink 前**再次讀取 group control**，發現 generation 或 state 改變即放棄本輪。

### R4. Poison record 會在無 DLQ 的情況下拖垮整個 Topic 的 Publish

**設計組合**：§13 permanent error → block stream、不 skip；§22 PROTECTED group 落後即為 retention barrier；§22 hard limit → `REJECTED_BEFORE_WRITE`；§26 不做 DLQ。

**後果鏈**：一筆無法處理的 record 讓某 PROTECTED group 在該 stream 停住 → 該 stream 之後的所有 segment 永遠不可 GC → 該 lane 永遠不可 retire-GC → WAL 隨時間累積到 hard limit → **所有 producer 對該 Topic 的 Publish 被拒**。這是慢性、可預期、卻沒有任何內建出口的故障模式。唯一出口是 `ResetConsumerGroup` 帶 EXPLICIT_OFFSETS，但它要求停止整個 group、等待 membership 清空、提供**每一個 stream** 的完整向量（§21），對數千個 stream 的 Topic 不可操作。

**建議**：
- 新增管理操作 `AcknowledgeRecords(topic, group, partition, lane, up_to_sequence)`：以管理身分在 current generation 追加一筆 checkpoint record（可放在專用的 `members/admin-<uuid>/` lane），走既有 `max()` 語意，不需停 group、不需切 generation。這與「不自動 skip」不衝突，是**明確的人工決策**。
- Handler 回報 permanent error 時允許 worker **bisect batch**（將 500 筆二分重試）以定位單筆 poison record，並先 commit poison 之前的連續前綴，讓告警能指出具體 record。
- 新增 metric / 告警：`protected_barrier_age_seconds`（PROTECTED group 最舊 barrier 的存在時間）與 `topic_wal_utilization_ratio`，讓運維在到達 hard limit 前介入。

### R5. Reset / Delete 的等待條件無法保證終止

**設計條文**：§13 worker 只在「batch boundary」重新驗證 control；§21 Reset 需「等待 Redis active membership 為空至少一個 membership_timeout」。

**問題**：
- transient error 走 exponential backoff 持續 retry「同一批」，這不是新 batch boundary，worker 可能數小時停在 retry loop 卻**持續 heartbeat**。
- 沒有 handler timeout 規範，hung handler 亦然。
- 結果：membership 永遠不為空，Reset / Delete 永遠停在 RESETTING / DELETING。而依 §22，RESETTING / DELETING 的 PROTECTED group 是 retention barrier，形成另一條通往 R4 的路。

**建議**：
- control 驗證改為「每次 handler 嘗試前（含 retry）與每次 backoff 醒來時」都執行。
- 新增必填 `handler_timeout`；超時視為 transient error，並在下一次嘗試前重驗 control。
- 讀到非 ACTIVE 時，即使 batch 正在 retry 也必須放棄該 batch（downstream 已 idempotent，放棄是安全的）。
- Reset / Delete 對「等待清空」加上可觀測性：回報還在 membership 內的 `consumer_instance_id`，並允許帶 `--force-after` 參數（仍需 durable 寫入 control 以便 crash 後續行）。

---

## 3. 🟠 中風險

### M1. per-stream `durable-end` 造成 fsync / rename 放大（效能核心問題）

§8.2 要求每次 group flush 對**每一個有變動的 stream** 執行：fsync `.open` → 寫 temp file → fsync temp → rename → fsync directory。一個 lane 若同時寫 3 個 Topic × 64 partitions，一次 group commit 最多觸發 192 × (2 fsync + 1 rename + 1 dirsync)，每 2 ms 一次。在 NFS / EFS / CephFS 上單次 fsync 為毫秒到十毫秒級，這個設計**不可能達到 2 ms group commit**，實際 Publish 延遲會被 stream 數量線性放大，且會壓垮 metadata server。

**建議**：把 durable frontier 提升到 **lane-level**：一個 lane 只有一個 `durable-end`，內容是本次 flush 涵蓋的所有 active stream 的 `(topic, partition, base_sequence, next_sequence, byte_end)` 列表（有上限、有 checksum）。一次 group commit 變成 N 次 `fdatasync` + 1 次 rename + 1 次 dirsync。Reader 的驗證邏輯不變。更進一步可讓 lane 內的 stream `.open` 用 `fallocate` 預配置，減少 metadata journal 壓力。

### M2. Checkpoint 每個 batch 一次 durable publication

§13 + §14.2：每個 partition worker 每批（10 ms）→ append checkpoint record → fsync → temp + rename durable-end → dirsync。一個 consumer 若擁有 64 個 partition，理論上每秒最多 6400 次 fsync + rename。checkpoint lane 已經是「一個 consumer incarnation 一條」，天然適合跨 partition 合併。

**建議**：明文規定 checkpoint 也採 group commit（`checkpoint_group_commit_max_wait`，預設 5–20 ms），多個 partition 的 checkpoint record 合併為一次 append + fsync + 一次 durable-end 發布。worker 在 checkpoint 完成前不得讀該 stream 下一批的規則不變。

### M3. Rebalance 的 dual-consumer 視窗過大，且缺少 handoff / 抖動抑制

- **無 handoff**：新 owner 一看到 membership 變化就 StartWorker，舊 owner 才開始 Drain。每次 rolling deploy、每次 HPA 縮放都會製造一段「兩個 consumer 同時處理相同 record」的期間，長度約 `reconcile_interval + 在途 batch + 舊 owner drain 時間`。文件把這視為可接受的 duplicate，但這會發生在**每一次部署**，不是罕見故障。
- **Crash-loop 成員**：一個啟動即崩潰的 pod 每次都會先 ZADD 註冊，被其他人算進 rendezvous，奪走 partition，再消失。CrashLoopBackOff 期間整個 group 持續 rebalance。
- **Redis failover 後成員集合瞬間縮小**：replica 提升後 ZSET 可能為空或落後，先讀到的 instance 會以「只剩我」計算 assignment 而接管所有 partition，3 秒後其他人 heartbeat 回來再全部搬回。
- **自我 fencing 不明確**：instance 若因 GC pause / CPU 飢餓 超過 `membership_timeout` 沒成功 heartbeat，其他人已接管，它恢復後應主動視自己為已被撤銷，但文件只規定 Redis 不可用時要停。

**建議**：
1. Graceful shutdown 順序固定為：停止開新 batch → 完成在途 batch 與 checkpoint → `ZREM` 自己 → 退出。這讓正常部署的 dual window 接近零。
2. 新成員需連續 heartbeat 達 `join_warmup`（例如 2 × heartbeat_interval）才被其他人視為 active（可用 ZSET 另存 first_seen 或第二個 key）。
3. 單次 reconcile 若 active 成員數下降超過某比例（例如 50%），保留當前 assignment 一個 `membership_timeout` 再套用（hysteresis）。
4. 明文：「若自上次成功 heartbeat 起超過 `membership_timeout`，instance 必須視自己已失去所有 ownership，drain 後重新加入。」
5. 可選強化：利用已經要求的 PV cross-node lock，在 `p=<n>/owner.lock` 上做 per-partition 排他（Redis 決定「誰應該擁有」，PV lock 提供互斥）。這不改變正確性契約，但把 dual consumer 從常態降為僅 lease 到期時發生。

### M4. Retired lane 必須等到整個 retention window 才可 GC，造成目錄爆炸

§23 要求 retired lane 刪除須「每個 segment 都符合 retention policy」。假設 20 個 producer pod、每日部署 3 次、retention 7 天、3 Topics × 64 partitions：

```text
retired lanes ≈ 20 × 3 × 7 = 420
stream directories ≈ 420 × 192 ≈ 80,000
```

每個 partition worker 每次 poll 都要 `readdir` 該 partition 下所有 lane（§13），每個 lane recovery / retire-GC 又要跨 T × P 目錄找該 lane 的 streams（§3、§23）。在 NFS 上這是災難級的 metadata 負載。

**建議**：
- 每個 lane 維護 append-only 的 `streams.log`（durable，列出該 lane 曾建立的 `(topic, partition)`），讓 recovery / retire / GC 以 O(streams) 而非 O(T × P) 枚舉。
- 每個 partition 維護 `lanes.index`（由 Retention Manager 在 `gc.lock` 下更新）或至少讓 worker 對「retired 且已完整消費」的 stream 建立本地快取，永久跳過（retired lane 不會再增長，這個快取是安全的）。
- 把「retention_time」的語意拆開：資料 replay 能力（EARLIEST / AT_TIMESTAMP 可及範圍）與 retired lane 目錄保留期。允許 `retired_lane_grace_seconds` 短於 `retention_time_seconds`，但需在文件明示 replay 範圍會因此縮短。
- 給出 partition count 的實務指引：因為 per-instance lane 已將 stream 數乘上 lane 數，`partition_count` 應以 **consumer 併行度**為主（16–64），而非 Kafka 式的數百。

### M5. Stale dual consumer 讀到已被 GC 的 segment 會被誤判為 correctness incident

§22：「PROTECTED group 在 storage failure、corruption 或明確管理操作以外出現 `OFFSET_OUT_OF_RANGE`，必須視為 correctness incident。」

但以下是設計允許的正常路徑：consumer A（membership view 過期）仍在讀 stream S 的 [500, 1000)，consumer B 已 commit 1500，Retention Manager 依 effective=1500 刪除 segment，A 讀到 ENOENT / ESTALE 或發現 floor > 500。這是 dual consumer 的必然結果，卻會觸發 incident 告警。

**建議**：規定 PROTECTED consumer 遇到 floor 超越自身 local watermark 時，必須先**重新計算 effective offset**；若 effective ≥ floor，視為 stale view，從 effective 繼續、計入 `consumer_stale_view_total`，**不算 incident**。只有 effective < floor 才是 incident。

### M6. retention-index「全部刪除時為 stream durable end」語意歧義

§22：「刪除前先 durable atomic 將 earliest 推進到刪除 prefix 後第一個 retained sequence（全部刪除時為 stream durable end）」。

對 **active stream**（仍有 `.open`），刪除所有 sealed `.wal` 後，第一個 retained sequence 應是 `.open` 的 `base_record_sequence`，**不是** durable end。若實作者按字面把 earliest 設為 durable end，`.open` 中已 SUCCESS 的 records 會低於 floor 而永遠不被 delivery，直接違反 durability contract。

**建議**：改寫為「刪除 prefix 後，earliest = 現存最舊 retained segment（含 `.open`）的 base sequence；若 stream 已 retired 且無任何 retained segment，earliest = 最後一個 sealed footer 的 `next_record_sequence`」。同時明確規定：無 retained segment 的 retired stream，其 durable end 由 retention-index 提供（因 §3 說 end 只存在 footer，而 footer 所在檔案已被刪除）。

### M7. Blocked state 只存在於記憶體

§13 的「將該 stream 標記 blocked」沒有定義存放位置。若只在 process 記憶體：rebalance 後新 owner 會再次對 poison record 重試（重複 downstream side effect、告警 flapping）；dual consumer 時 A 認為 blocked、B 已推進，A 的 blocked 視圖是錯的。

**建議**：blocked 以 durable 檔案記錄於 `groups/<topic>/<group>/checkpoints/<generation>/blocked/<partition>-<lane>`（含 sequence、原因、時間、instance），generation 切換自然清除；worker 啟動先讀 blocked 清單；`InspectGroup` 輸出它。

### M8. BEST_EFFORT 出界即 block 並要求人工 Reset

§22 對 BEST_EFFORT 也套用「block 直到管理者執行 Reset」。BEST_EFFORT 的定義就是可容忍遺失，卻要付出與 PROTECTED 相同的人工成本，且 Reset 要停止整個 group。

**建議**：在 group manifest 增加 `out_of_range_policy: BLOCK | SKIP_TO_FLOOR`（僅 BEST_EFFORT 可選 SKIP_TO_FLOOR），SKIP 時寫入一筆 checkpoint 到 floor、累加 `best_effort_skipped_records_total`。這不違反「不自動套用 EARLIEST」——它是建立 group 時的明確選擇。

### M9. Shared filesystem 的 coherence 行為直接決定端到端延遲與錯誤模式

- NFS 預設 attribute cache（`actimeo` 3–60 秒）會讓另一節點的 reader 看不到 `.open` 新增的 bytes 與 rename 後的新 `durable-end`，端到端延遲下限等於 cache 時間；關掉 cache（`noac`, `lookupcache=none`）又會讓 M1/M4 的 metadata 負載雪上加霜。
- rename 覆蓋後，持有舊 fd 的 reader 在 NFS 上會得到 `ESTALE`；unlink 同理。文件沒有把 `ESTALE` / `ENOENT` 明列為「必須重新 readdir + 重讀 index/snapshot 後重試」而非 corruption。
- §25 conformance test 沒有量測「跨節點可見性延遲」。

**建議**：conformance 新增可見性延遲 SLO（例如 p99 < 200 ms），並在文件列出建議的 mount options；規定 reader 對 `ESTALE` / `ENOENT` 的統一處理：重讀 retention-index（WAL）或 snapshot（checkpoint）→ 重新 readdir → 重試；只有 index / snapshot 本身無法讀取才 block。

### M10. `fcntl` advisory lock 的 close 陷阱

POSIX record lock 是 per-process，且 **process 內任何 fd 對同一檔案的 close 都會釋放該 process 的全部 lock**。Inspection、metrics 或 recovery 程式碼若在同一 process 內順手 open/close 了 `writer.lock`，writer 會在毫無感知下失去 lock。

**建議**：文件規定使用 OFD lock（`F_OFD_SETLK`）或 `flock`，並要求 lock 檔只由 lock holder 這一個 fd 開啟，其他任何路徑不得 open lock 檔。

---

## 4. 🟡 低風險與文件缺口

| # | 位置 | 問題 | 建議 |
|---|---|---|---|
| L1 | §8 | 「size 或 ... age ... rotation。」排版斷裂 | 修正段落 |
| L2 | §10 | 「所有 group instances 必須使用相同 timeout，否則拒絕加入」但沒有規定 timeout 存放於何處供比對 | 寫入 group manifest（durable）或 Redis `mq:config:v1:<topic>:<group>`，join 時比對 |
| L3 | §21、§21.1、§25 | 「group administrative lock」與 `group.lock` 是否同一把鎖未明說；`storage-admin.lock` 與 `gc.lock` 的順序未定義 | 統一命名，並定義全域鎖序：`storage-admin.lock → topics/<t>/gc.lock → group.lock → compact.lock → writer.lock`，禁止反向 |
| L4 | §8.1 | sealed footer 的定位方式（如何從檔尾找到 footer 起點）未定義 | 定義固定長度 trailer（magic + footer_length + footer_crc）並要求長度精確匹配 |
| L5 | §8.2、§14.2 | `durable-end` 是唯一沒有 stream / lane identity 欄位的 metadata，與文件「防止誤搬移」的原則不一致 | 加入 `topic_config_fingerprint`、`partition`、`writer_lane`（或 checkpoint 的 generation、instance） |
| L6 | §14.2 | checkpoint rotation / compaction thresholds 為「必填」，其他參數皆有預設 | 給預設（例如 16 MiB / 10 min / 8 segments / 64 MiB）以保持一致 |
| L7 | §22、§25 | 「當下 Topic allocated WAL bytes」若每次 Publish 都跨所有 lane 計算，成本不可接受 | 每個 lane 維護本地已 allocated bytes；Retention Manager 在 `gc.lock` 下週期性發布 Topic usage 檔；Publish 使用「上次發布值 + 本 lane 增量」，接受有界誤差 |
| L8 | §21 | EXPLICIT_OFFSETS 要求「每個目前存在的 stream」的完整向量，在數千 stream 下不可操作 | 允許部分向量 + `others: KEEP_CURRENT | EARLIEST | LATEST` |
| L9 | §22 | size-based GC「超過 soft limit」時，跨 stream / lane 的刪除順序未定義，可能造成某些 stream 被反覆優先淘汰 | 規定以 footer `max_append_time` 全 Topic 由舊到新淘汰 |
| L10 | §13 | 無 handler timeout；無 per-stream 與 per-partition 的 backoff 隔離規範 | 新增 `handler_timeout`；規定一個 stream 的 backoff 不得阻塞同 partition 的其他 stream |
| L11 | §13 | idle 時的 poll 間隔未定義；`batch_max_wait=10ms` 若被誤用為 poll interval，數千 stream 會每 10 ms readdir 一次 | 定義 adaptive poll（10 ms → 1 s 指數退避，有新資料即重置） |
| L12 | §6、§7 | 未說明 stream 內順序 = append 順序，而非呼叫端 Publish 的呼叫順序；多 goroutine 併發 Publish 同一 key 時順序不可預期 | 明文說明，並建議需要順序的 caller 自行串行化 |
| L13 | §10 | Redis Lua 使用 `TIME` 為非決定性指令，Redis < 5 需 `redis.replicate_commands()`；Redis Functions 需宣告 `no-writes` 以外的 flag | 註明最低 Redis 版本（建議 7.x）與 script flags |
| L14 | §3、§23、§25 | 未定義 Lane Recovery Manager、Retention Manager、Compactor、Janitor 由哪種 process 執行；若每個 pod 都跑，會有 N 倍掃描 | 指定為獨立 maintenance role（單一 Deployment，靠 lock 互斥），或至少規定只有 consumer pods 且以 lock 選出單一執行者 |
| L15 | §14.2、§22 | 讀取 checkpoint / WAL 的順序未規定：應先讀 snapshot（或 retention-index）再枚舉 segment；反過來會在 compaction / GC 併發時得到偏低的 effective（安全但多餘 replay） | 明文「snapshot-first / index-first」與 ENOENT 重試 |
| L16 | §8 | `segment_max_age=10min` 對低流量 stream 會產生大量極小 `.wal`（每 10 分鐘一個檔） | 預設改為 1 小時，或改成「自第一筆 record 起算」並設最小 bytes 門檻 |
| L17 | §17 | Group create 的 loser「必須讀取 winner 已 durable 發布的 manifest」，但 winner 可能尚未發布；因整個建立流程都在 `gc.lock` 下，loser 實際會在 lock 上等待 | 明說：loser 阻塞於 `gc.lock`，取得後依 §17 的 crash-resume 邏輯處理 |
| L18 | §17、§22 | INITIALIZING 的 PROTECTED group 若無人 resume 將永久成為 barrier | 新增 `group_non_active_age_seconds` metric 與告警 |
| L19 | §3 | 範例 `writer-A-001` 與「lane_id 必須是 UUIDv7」矛盾（雖僅為示意） | 範例改用 UUIDv7 或註明為示意 |
| L20 | §7 | `message_id` 同 ID 不同內容為 invariant violation，但未說明 consumer 應 block 還是告警後繼續 | 明定：block stream + 告警（與 poison record 同路徑） |
| L21 | §14.2 | 已死 consumer 的 `members/<instance>/` 目錄在完全被 snapshot 涵蓋後由誰刪除、在哪把鎖下刪除未定義；否則 members 數量隨每次 pod 重啟無界成長，影響 effective offset 計算成本 | Compactor 在 `compact.lock` 下、取得該 lane `writer.lock` 後刪除已完全涵蓋且已 seal 的 lane |
| L22 | §25 | Kubernetes `terminationGracePeriodSeconds` 需涵蓋 producer seal + retired marker 與 consumer drain + checkpoint + ZREM | 給出 sizing 公式與建議值 |

---

## 5. 優化建議（依優先級）

### P0（上線前必須）
1. **Fence epoch + footer trailer + split-brain conformance test**（R1）。
2. **兩階段 reserve 水位，checkpoint 優先於 WAL**（R2）。
3. **GC 決策讀取全部綁定在 `gc.lock` 內並於 unlink 前重驗 control**（R3）。
4. **`AcknowledgeRecords` 管理操作 + batch bisect + barrier age 告警**（R4）。
5. **control 驗證下沉到每次 handler 嘗試；新增 `handler_timeout`**（R5）。
6. **修正 retention-index earliest 語意**（M6）。

### P1（首次效能驗證前）
7. **Lane-level durable-end**（M1）。
8. **Checkpoint group commit**（M2）。
9. **Graceful shutdown 的 drain → ZREM 順序、join warm-up、membership hysteresis、自我 fencing**（M3）。
10. **每 lane `streams.log`、worker 對 retired-fully-consumed stream 的永久跳過、retired lane grace 與 retention 解耦**（M4）。
11. **ESTALE / ENOENT 統一處理與 stale-view 不算 incident**（M5、M9）。
12. **Durable blocked state**（M7）。

### P2（提升可運維性）
13. BEST_EFFORT `out_of_range_policy`（M8）。
14. OFD lock 與 lock 檔開啟規範（M10）。
15. 全域鎖序、命名統一、durable-end identity、預設值一致（L3、L5、L6）。
16. Maintenance role 定義（L14）；members 目錄 GC（L21）。
17. Topic usage 快取（L7）；EXPLICIT_OFFSETS 部分向量（L8）。

### P3（可選強化）
18. PV per-partition owner lock 作為 consumer 互斥（M3-5）。
19. 以 `fallocate` 預配置 `.open`，降低 metadata journal 壓力。
20. Adaptive idle poll（L11）；segment age 預設調整（L16）。

---

## 6. 附錄 A：併發場景推演

| 場景 | 參與者 | 現行文件結果 | 是否安全 | 備註 |
|---|---|---|---|---|
| 兩個 consumer 同時擁有 partition，各自 commit | A、B | `max()` 取較大值 | ✅ 安全（duplicate） | 每次部署都會發生（M3） |
| 舊 owner 在途 batch 完成後 commit 較小值 | A | 被 `max()` 忽略 | ✅ | |
| Reset 切換 generation 時舊 worker 遲到 commit | worker、admin | 寫入舊 generation 目錄，無效 | ✅ | |
| Reset 等待期間 worker 卡在 retry loop | worker、admin | 永遠等不到 membership 清空 | ❌ liveness | R5 |
| 到達 filesystem reserve | producer、consumer、GC | 三方都停 | ❌ liveness | R2 |
| GC 在 lock 外讀 control，Reset 交錯 | RM、admin | 依舊 generation 決策刪除 | ❌ safety | R3（取決於實作是否在 lock 內讀取） |
| Writer lock lease 到期，Recovery 取得 lock 後 writer 復活 | W、R | 舊 writer 繼續寫 sealed 檔 | ❌ safety | R1 |
| Compactor 發布新 snapshot 並刪 segment，同時新 owner 掃描 | Compactor、worker | worker 讀到 ENOENT，若用舊 snapshot 會算出偏低 effective | ⚠️ 安全但多餘 replay | L15 |
| Retention 刪 segment，同時 stale consumer 在讀 | RM、A | A 得到 ESTALE / floor 越界 | ⚠️ 安全但被誤判 incident | M5 |
| 新 lane 在 LATEST 初始化期間出現 | producer、admin | missing-coordinate → 從 0 消費 | ✅ 保守 | 文件已說明 |
| 兩個 process 同時 CreateGroup | X、Y | `gc.lock` + atomic mkdir 單一 winner | ✅ | L17 澄清 loser 行為 |
| Redis failover 後 ZSET 為空 | 所有 consumer | 先讀者接管全部，再搬回 | ⚠️ 安全但 churn 與 duplicate 風暴 | M3 |
| Crash-loop pod 反覆 join | 所有 consumer | 持續 rebalance | ⚠️ | M3 |
| ENOSPC 在 fsync 途中 | producer | DURABILITY_UNKNOWN + writer 停止 | ✅ | |
| 刪除 active stream 全部 sealed segment | RM | 依字面 earliest = durable end → 跳過 `.open` 資料 | ❌ safety（若照字面實作） | M6 |
| Poison record 長期存在 | consumer、RM、producer | barrier → WAL 滿 → Publish 被拒 | ❌ liveness | R4 |

---

## 7. 附錄 B：建議新增的驗收條件（接續 §28）

22. Writer 或 lock holder 被 SIGSTOP 超過 lock lease 時間、Recovery / 第二個 janitor 取得 lock 並完成 mutation 後，原 holder 恢復執行時的任何寫入都必須被 fence epoch 拒絕或被 reader 偵測為 corruption，且不得覆寫 `durable-end`、`retention-index` 或 retired marker。
23. Filesystem 到達 producer reserve 後，consumer 仍可 checkpoint、Retention GC 仍可釋放空間，系統在 downstream 恢復後能自行走出 backpressure，無需人工介入。
24. Retention Manager 在取得 `gc.lock` 之後才讀取 group control 與 effective offset；於決策與 unlink 之間注入 Reset，GC 必須放棄本輪，不得刪除新 generation 需要的 segment。
25. 管理者對 poison record 執行 `AcknowledgeRecords` 後，stream 解除 blocked、PROTECTED barrier 解除、GC 可推進，過程不需停止 group 或切換 generation。
26. Worker 在 transient retry loop 或 hung handler（超過 `handler_timeout`）期間，admin 寫入 RESETTING / DELETING 後，worker 必須在一個 retry 週期內停止並離開 membership，Reset / Delete 在有界時間內完成。
27. 刪除 active stream 的全部 sealed segment 後，`.open` 中已 SUCCESS 的 records 仍可被 consumer 讀取；`earliest_retained_sequence` 等於 `.open` 的 base sequence。
28. Graceful rolling restart 一個 consumer group 的所有 pod，duplicate delivery 數量必須有界（例如 ≤ 在途 batch 數），且 rebalance 次數等於 pod 數而非其倍數。
29. Stale consumer 在 segment 被 GC 後讀取失敗，重新計算 effective offset 後從正確位置繼續，不觸發 correctness incident 告警。
30. 在目標 StorageClass 上量測兩節點間 `.open` append 與 `durable-end` rename 的可見性延遲，p99 必須符合宣告 SLO。
31. 一個 lane 同時寫 N 個 stream 時，單次 group commit 的 fsync 與 rename 次數不隨 N 線性成長（驗證 lane-level durable-end）。
