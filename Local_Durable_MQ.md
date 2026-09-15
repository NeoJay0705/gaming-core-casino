# Local Durable MQ 需求

## 1. 背景

系統中的 Kubernetes stateless services 需要將不同類型的 events 最終寫入 downstream systems，並支援大量 burst write。Producer 對外回覆接受前，部分 events 必須先形成可在 process / pod restart 後恢復的 durability obligation。

為了降低同步 downstream write 壓力，系統需要一個不依賴 Kafka 等外部 broker 的 durable async persistence mechanism。

因此設計一個基於 Persistent Volume + WAL 的 Local Durable MQ。

### 1.1 Scope boundary

Local Durable MQ 是 business-agnostic infrastructure。MQ core 的 correctness contract 始於 `Publish` 呼叫，不理解或協調任何外部系統的 business transaction。

MQ core 必須保證：

```text
Publish success
→ message 已 durable 寫入 WAL

consumer handler success
→ checkpoint 才能推進

consumer handler failure
→ checkpoint 不推進，message 之後會再次 delivery
```

MQ core 不保證：

```text
外部 business operation
與
Publish / consumer checkpoint
之間的原子性
```

外部 side effect、database schema、rollback 與 business state merge 均由 producer / consumer integration layer 負責，不進入 MQ envelope、WAL、ownership、checkpoint 或 retention implementation。

---

## 2. 核心目標

Local Durable MQ 必須提供：

1. Producer 能快速把 event durable 寫入 persistent WAL。
2. Downstream system 可以由背景 consumer 非同步 batch persistence。
3. Consumer 語意為 at-least-once。
4. Consumer crash、pod restart、ownership rebalance 後可以從 checkpoint 繼續。
5. Protected downstream 暫時不可用時，未處理資料保留於 WAL，恢復後繼續處理。
6. 支援不同類型資料。
7. 支援同一份資料由不同 Consumer Group 分別消費。
8. 支援系統運行後新增 Consumer Group。
9. Producer hot path 不因 partition owner RPC、leader election 等 distributed coordination 增加額外 failure surface。
10. 系統 correctness 不依賴 exactly-once delivery，而依賴 at-least-once + idempotent consumer。

---

## 3. Producer / WAL 寫入模型

採用 **per-instance WAL lane**。

每個 producer service instance 都可以直接向 Persistent Volume 寫自己的 WAL，不需要把 message RPC forwarding 到 partition owner。

Instance identity 必須代表一次 process incarnation，而不是單純使用 pod name。

使用 UUIDv7 作為 `lane_id`；同一次 process incarnation 的 producer `instance_id` 與 `lane_id` 相同。UUIDv7 提供足夠唯一性與大致時間排序，舊 ID 永遠不可重用。

例如：

```text
lane_id = 01993e11-8d3a-7c8f-a7ce-2ab21df8d410
```

每個 lane 必須建立 immutable manifest，欄位固定為：

```text
format_version
lane_id
created_at_unix_nano
writer_location
application_version
checksum
```

欄位用途：

```text
format_version
→ 選擇 manifest decoder 並拒絕不支援格式

lane_id
→ 驗證 manifest 與實體路徑相符，防止錯讀或誤搬移

created_at_unix_nano
→ lane lifecycle、inspection 與 incident timeline

writer_location
→ local 使用 hostname；Docker 使用 container ID；Kubernetes 使用 pod UID

application_version
→ corruption / compatibility 告警時定位產生 WAL 的程式版本

checksum
→ 偵測 manifest 截斷或 corruption
```

`writer_location` 與 `application_version` 不參與 routing 或 correctness decision，但 `InspectLane`、startup recovery log 與 corruption alert 必須輸出它們；若實作沒有這些 inspection/alert consumers，則不得持久化這兩個欄位。

Pod/process restart 後產生新的 `lane_id`（同時也是該 process incarnation 的 producer `instance_id`），並建立新的 WAL lane。

例如：

```text
writer-A-001
writer-A-002
writer-B-001
```

舊 instance 的 WAL 不會因 restart 被覆寫或接續寫入。

Writer process 必須在存活期間持有 lane-level cross-node exclusive advisory lock。Graceful shutdown 應 seal 所有 active streams，並發布 durable retired marker；crash recovery 只能在成功取得該 lock 後 truncate incomplete tails、seal streams 並發布 retired marker。舊 `lane_id` 永遠不可再次成為 writer。

Retired marker 欄位固定為：

```text
format_version
lane_id
retired_at_unix_nano
checksum
```

各 stream 的 end sequence 由其最後一個 sealed segment footer 取得，不在 retired marker 重複保存。Retired marker 必須以 temporary file、file sync、atomic rename、directory sync 發布；marker 存在代表該 lane 不會再產生新 stream 或 append records。

Marker 的 `lane_id` 驗證 path，`retired_at_unix_nano` 供 lifecycle/GC inspection，`format_version` 與 checksum 用於 decode 與 corruption detection；不另存可由 segment footer 取得的 end offsets。

Lane Recovery Manager 必須在 startup 與之後週期性掃描沒有 retired marker 的 lanes。只有成功取得 lane-level exclusive advisory lock 後，才能判定原 writer 已不存在，並執行 tail recovery、seal 所有 streams 與發布 retired marker；若 lock 無法取得，該 lane 仍屬 active，Recovery Manager 不得修改其任何檔案。這個判定同時供 active/retired inspection、告警與 lane GC 使用，不能以 process list、pod 狀態或檔案 mtime 取代。

此設計必須維持：

```text
one WAL lane = one writer
```

避免多 process 同時 append 同一檔案。Storage backend 若無法可靠提供 cross-node lock semantics，則不符合本設計的 deployment prerequisites。

---

## 4. Topic

MQ 必須支援不同資料流，因此引入 Topic。

例如：

```text
entity-events
audit-log
player-stat
```

Producer publish 時至少提供：

```text
topic
key
message_id
event_type
schema_version
payload
```

MQ core 不理解 business payload。

Payload 應視為 opaque bytes，例如 Protobuf encoded bytes。

MQ 在 append 時產生 `append_time_unix_nano`：

```text
append_time_unix_nano
→ WAL writer 產生；用於 retention、oldest-unconsumed age 與 AT_TIMESTAMP replay
```

不同 writer lanes 的 wall clocks 可能有 skew，因此 `append_time_unix_nano` 不提供跨 lane total order。`AT_TIMESTAMP` 必須依各 stream 的 stored append time 分別定位。

Business event time 若有需要，由 producer 放在 opaque payload；MQ core 不另存未使用的 producer timestamp。

Publish validation 必須在寫入任何 bytes 前完成：

```text
topic       = non-empty UTF-8，且符合 [a-z0-9][a-z0-9._-]{0,127}
key         = non-empty opaque bytes，最大 4 KiB
message_id  = non-empty opaque bytes，最大 128 bytes；producer 必須保證同 Topic 唯一
event_type  = non-empty UTF-8，最大 128 bytes
schema_version = uint32 且大於 0
payload     = opaque bytes；完整 encoded record_length 不得超過 Topic max_record_bytes
```

MQ core 不以 `message_id` 執行 producer-side deduplication；它必須原樣 delivery，供 consumer 實作 idempotency。

相同 logical message 的 retry 必須沿用相同 `message_id`，且 `topic`、`key`、`event_type`、`schema_version` 與 payload 必須完全相同。Producer 不得以相同 ID 發布不同內容；consumer 發現同 ID 不同內容時必須視為 invariant violation，不可任選一筆覆蓋。

每個 Topic 必須有 durable topic manifest：

```text
format_version
topic
partition_count
partition_hash_algorithm
partition_hash_seed
max_record_bytes
checksum
```

除 checksum 外的所有 semantic fields 都以 `format_version` 指定的 canonical encoding 計算 SHA-256 config fingerprint；checksum 本身只保護 manifest file。Manifest 建立後 immutable；`topic` 驗證路徑、partition 欄位決定 routing、`max_record_bytes` 決定 Publish validation。不同 process 讀到不支援格式、checksum 錯誤或 fingerprint 不一致時必須拒絕 Publish / consume。

Retention / capacity 使用獨立的 durable Topic control，允許管理面 atomic 更新而不改變 routing fingerprint：

```text
format_version
retention_time_seconds
soft_max_wal_bytes
hard_max_wal_bytes
updated_at_unix_nano
checksum
```

Producer 使用 hard limit 做 Publish admission；Retention Manager 使用 retention time / soft limit 做 GC；所有 processes 必須從同一份 durable control 讀取，不得以不同 pod 的本地設定各自決策。Control 更新採 temporary file、file sync、atomic rename、directory sync。

`retention_time_seconds` 必須在 CreateTopic 時明確提供且大於 0；soft/hard byte limits 若省略才使用 §22 的 capacity-derived defaults，並把算出的實際 byte values 寫入 control，runtime 不得每次依當下 PV size 重新推導。

Topic 只能由明確的 `CreateTopic` 管理操作建立，Publish 不得看到缺少的 Topic 就自行套用本地 defaults。建立流程必須在 `storage-admin.lock` 下以 atomic directory create 取得 ownership，durable 發布 matching manifest 與 control 後才可使用；crash 留下的不完整 Topic 只能由相同參數的管理操作繼續完成，任一必要檔案缺失、corrupt 或不一致時 Publish、consume 與 GC 都必須拒絕該 Topic。

---

## 5. Partition

每個 Topic 必須設定固定 partition count。Topic 第一次建立並寫入資料後，partition count 不可動態修改。

例如：

```text
entity-events = 64 partitions
```

`64` 只是上述 Topic 的完整設定範例，不是所有 Topic 的全域預設。建立 Topic 時必須明確提供 partition count；設定值必須介於 `1..65536`，並依 peak publish throughput、consumer concurrency、writer lane 數量與 checkpoint cardinality 決定。Partition count、consumer instance 數、worker concurrency 與 batch size 是獨立參數。

Partition routing：

```text
partition =
xxhash64(seed=0, raw_key_bytes) % partition_count
```

Hash algorithm、seed、key byte encoding 與 partition count 都是 Topic durable metadata 的一部分。所有 producer / consumer 必須驗證相同的 Topic config fingerprint；不一致時拒絕啟動或拒絕 Publish，不可靜默採用本地設定。

例如聚合事件：

```text
key = aggregate_id
```

相同 `aggregate_id` 永遠映射到相同 logical partition。

但因採 per-instance WAL，因此一個 partition 實際上可能包含多個 writer lanes：

```text
topic=entity-events
partition=17

writer-A-001
writer-A-002
writer-B-001
writer-C-003
```

MQ 不保證不同 writer lane 之間具有 single global order。

因此 business correctness 不得依賴 partition-wide strict ordering。

---

## 6. Message ordering semantics

Ordering 的最小單位是 physical stream：

```text
stream = (topic, partition, writer_lane)
```

每個 stream 內必須依 `record_sequence` 維持 append / delivery order。

同一 writer lane 的不同 Topic / partition streams 之間，以及不同 writer lanes 之間，都不提供 global ordering guarantee。

因此 consumer mutation 必須設計為：

```text
idempotent
concurrency-safe
monotonic，或明確處理 event version
commutative where possible
```

例如兩個屬於同一 aggregate 的事件：

```text
EVENT_A
EVENT_B
```

即使 persistence 順序不同，最終資料仍應收斂到相同結果。

可使用只允許單向更新的 projection：

```text
event_a_done:
false → true only

event_b_done:
false → true only
```

不可讓舊 event replay 或 concurrent duplicate 將已完成狀態覆蓋成較舊狀態。

---

## 7. WAL durability

Producer Publish 成功的最低語意為：

```text
message 已 append 到 WAL
並完成所要求的 durable flush
```

例如：

```text
write
→ fdatasync/fsync
→ Publish success
```

允許使用 group commit，提高 throughput，例如在短時間內收集多個 messages 後一次 flush。

Group commit 參數必須可設定，且不改變 durability semantics。未設定時使用：

```text
group_commit_max_messages = 256
group_commit_max_bytes = 1 MiB
group_commit_max_wait = 2 milliseconds
```

三個值都必須大於 0；達到任一條件就執行 flush。

每個 Publish 必須等到包含該 message 的 group flush 成功後才能回傳 success。Publish result 至少區分：

```text
SUCCESS
    record 已完成 durable flush

REJECTED_BEFORE_WRITE
    validation、quota 或 lifecycle check 在任何 record bytes 寫入前拒絕

DURABILITY_UNKNOWN
    write / sync 已嘗試，但呼叫端無法確定 record 是否 durable
```

呼叫端遇到 `DURABILITY_UNKNOWN` 時可以使用相同 `message_id` 重試；MQ 仍只提供 at-least-once，duplicate 由 consumer idempotency 處理。

必須能處理 WAL 尾端 incomplete record。

WAL 使用 little-endian versioned binary format。Segment 內的 record 不重複保存已存在 segment header / physical path 的 Topic、partition、lane ID 與 format version。

每個 record 必須包含：

```text
record_length        uint32
record_sequence      uint64
append_time_unix_nano int64
key_length           uint32
key                  bytes
message_id_length    uint16
message_id           bytes
event_type_length    uint16
event_type           UTF-8 bytes
schema_version       uint32
payload              bytes；長度由 record_length 扣除其他欄位後得到
crc32c               uint32；涵蓋從 record_length 到 payload 的所有 bytes，不包含 crc32c 本身
```

`record_length` 是包含自身與尾端 CRC 的完整 record byte count。它必須同時受 format hard limit 與 Topic `max_record_bytes` 限制，且至少能容納所有 fixed fields；所有 component lengths 必須在配置範圍內且加總完全等於 record boundary，任何整數溢位都必須在配置 memory 前被拒絕。這些 lengths 是 decoder 的 framing / allocation bounds；CRC 用於偵測 header、metadata 與 payload corruption。

`record_sequence` 在每個 stream 從 0 開始，對每個成功形成完整 record 的 append 連續加一，不允許重複或倒退。

Recovery 時若最後一筆未完整寫入，應安全 truncate incomplete tail。

若 CRC corruption 出現在已封存 segment 中段，不可靜默跳過。該 `(topic, partition, writer_lane)` 必須進入 blocked/corrupt 狀態並告警，等待人工修復。

Topic manifest 建立時若未提供 `max_record_bytes`，使用 `1 MiB`；可設定範圍為 `1 KiB..16 MiB`。超過限制必須在寫入前回傳 `REJECTED_BEFORE_WRITE`。

---

## 8. WAL segment

WAL 不可使用單一無限成長檔案。

每個 `(topic, partition, writer_lane)` stream 使用 segmented WAL。Active segment 使用 `.open`，完成 seal 後才改名為 `.wal`：

```text
p=0017/lanes/writer-A-001/
    0000000000000000.wal
    0000000000100000.wal
    0000000000200000.open
    durable-end
```

Segment 依下列任一條件 rotation：

```text
size
或
age
```

rotation。

Rotation 參數必須可設定，未設定時使用：

```text
segment_max_bytes = 128 MiB
segment_max_age = 10 minutes
```

兩個值都必須大於 0，達到任一條件就 rotation。調整參數只改變新 segment 的切分，不改變 persisted format 或 correctness。只有 sealed `.wal` 可以進行 retention GC；active `.open` 必須先 seal / sync / rotate。

### 8.1 Segment header / footer

每個 segment header 必須包含：

```text
magic
format_version
header_length
topic_config_fingerprint
partition
writer_lane
base_record_sequence
created_at_unix_nano
header_crc32c
```

Reader 必須使用 header 驗證檔案格式、Topic config、physical partition/lane path 與起始 sequence；任一不一致都必須 block stream，不可猜測或修正。

Sealed segment footer 必須包含：

```text
next_record_sequence
record_count
min_append_time_unix_nano
max_append_time_unix_nano
segment_data_crc32c
footer_crc32c
```

`next_record_sequence` 與 `record_count` 用於 checkpoint、lag、GC 與下一個 segment continuity validation；append time range 用於 retention、oldest-unconsumed age 與 AT_TIMESTAMP segment pruning；checksums 用於偵測截斷、遺漏及 corruption。

`segment_data_crc32c` 涵蓋完整 segment header 與所有 records，不包含 footer；`header_crc32c` / `footer_crc32c` 分別涵蓋各自 structure 中位於 checksum 前的 bytes。Format version 必須固定各欄位型別與 encoding。不得發布沒有 record 的 sealed segment；retirement / recovery 遇到只有 header 的空 `.open` 時應移除它並同步 parent directory。

### 8.2 Active stream durable frontier

Consumer 不得 delivery 尚未完成 durable flush 的 record。每個 active stream 必須發布 `durable-end`：

```text
format_version
active_segment_base_sequence
next_record_sequence
durable_byte_end
checksum
```

Writer group flush 順序固定為：

```text
append complete records
→ fdatasync/fsync active WAL
→ 以 temporary file + atomic rename 發布新的 durable-end
→ directory sync
→ 對該批 Publish 回覆 SUCCESS
```

`durable-end` 只能在 WAL sync 成功後前進。Consumer 讀取 `.open` 時只能讀到 `durable_byte_end`，並驗證該位置對應 `next_record_sequence`；sealed `.wal` 則以 footer 為 durable end。

若 WAL 已 sync 但 durable-end 發布失敗，Publish 回覆 `DURABILITY_UNKNOWN`。Recovery 取得 lane lock 後掃描 valid record prefix、truncate incomplete tail、重新建立 durable frontier，再 seal segment。Durable-end 遺失或倒退只能造成 replay / 暫時 lag，不得造成 record skip。

Segment rotation 必須依下列順序執行：

```text
確認目前所有已回覆 SUCCESS 的 records 都在 durable-end 內
→ append footer 並 sync `.open`
→ atomic rename `.open` 為同 base sequence 的 `.wal`
→ sync stream directory
→ 建立、sync 下一個 base sequence 的 `.open` header
→ sync stream directory
→ atomic 發布指向新 `.open` header end 的 durable-end
```

Reader 先以 sealed footer 判定 `.wal`，只有 `durable-end.active_segment_base_sequence` 與現存 `.open` base 相符時才讀 active segment。Rotation 任一 crash point 都必須由持有 lane lock 的 recovery 依 header/footer/filename continuity 完成或封存；不得因 stale durable-end 重複解讀 footer，也不得跳過已封存 records。

### 8.3 File publication

建立 lane manifest、建立 segment、seal 或 rename 後，除了同步檔案內容，也必須同步 parent directory metadata。Storage backend 必須通過對應的 crash-recovery 驗證。

---

## 9. Consumer Group

同一 Topic 必須可以被多個 Consumer Group 獨立消費。

例如：

```text
topic: entity-events

group: primary-persistence
group: reporting
group: audit
```

不同 group 各自維護消費進度。

不同 Consumer Group 之間為 fan-out。

同一 Consumer Group 內，多 instance 之間做 partition load balancing。

本設計中一個 Consumer Group 只綁定一個 Topic，所有同 group instances 必須具有相同 subscription 與 handler contract。

Consumer Group 分為：

```text
PROTECTED
    未完成的 offset 會阻止相關 WAL segment 被 GC

BEST_EFFORT
    落後超過 retention window 時允許 OFFSET_OUT_OF_RANGE
```

是否為 `PROTECTED` 是 durable group metadata，建立後不可靜默修改。

---

## 10. Consumer membership

Consumer Group instance 啟動後，需向 Redis 註冊 heartbeat。

推薦使用 ZSET：

```text
key:
mq:members:v1:<topic>:<group>

member:
consumer_instance_id（每次 process incarnation 產生 UUIDv7）

score:
last_heartbeat_unix_milliseconds
```

Heartbeat：

```text
ZADD members now_ms consumer_instance_id
```

取得 active instances：

```text
ZRANGEBYSCORE
members
now_ms-membership_timeout_ms
+inf
```

Heartbeat timestamp 與 active 判斷必須在同一個 Redis Lua script / Function 中使用 Redis `TIME`，避免各 pod clock skew，並原子執行 `TIME` 與 `ZADD`。ZSET score 使用 Unix milliseconds，不使用超出 IEEE-754 exact integer range 的 Unix nanoseconds。

Stale member 可另外使用：

```text
ZREMRANGEBYSCORE
```

進行 GC。

Membership 參數必須可設定，未設定時使用：

```text
heartbeat_interval = 3 seconds
membership_timeout = 15 seconds
reconcile_interval = 5 seconds
```

必須滿足 `membership_timeout >= 3 × heartbeat_interval`，讓單次 heartbeat 延遲不會立即觸發 ownership churn。所有 group instances 必須使用相同 timeout，否則拒絕加入 group。

Redis membership 是 ephemeral coordination state，不是 Consumer Group identity、checkpoint 或 WAL data 的 durable source。

---

## 11. Partition ownership

Redis 正常的前提下，不需要 distributed consensus。

所有 Consumer Group instances：

```text
讀取相同 active membership
↓
使用 deterministic Rendezvous Hash
↓
計算 partition assignment
```

Rendezvous score 必須使用固定 canonical encoding，避免不同 application versions 算出不同 owner：

```text
input =
    uint16_be(len(topic)) || topic_utf8 ||
    uint32_be(partition) ||
    uint16_be(len(member_id)) || member_id_utf8

score = xxhash64(seed=0, input)
owner = score 最大的 member
```

Hash score 相同時，以 `member_id` UTF-8 bytes lexicographically smallest 者勝出。Membership list 的原始 Redis 回傳順序不得影響 assignment。

例如：

```text
owner =
RendezvousHash(
    topic,
    partition,
    activeGroupInstances
)
```

每個 instance 只需要算出：

```text
desiredPartitions
```

Consumer Manager 定期 reconcile：

```text
desired - current
→ StartWorker

current - desired
→ DrainWorker
```

允許 membership view 暫時不同造成短時間 dual consumer。

因為 delivery model 為 at-least-once，因此這種競態只能造成 duplicate processing，不得造成 data corruption。

Consumer instance 數量、每 instance worker concurrency、batch max messages、batch max bytes 與 batch max wait 必須是獨立設定。增加 consumer instance 只影響 partition ownership，不用來控制單批大小。

---

## 12. Redis failure

本設計不提供 Redis fallback ownership，也不混用 Kubernetes Pod list 作為第二套 membership source。

Redis 無法提供可靠 membership 時，consumer 可以在目前 membership lease 內 drain 已取得的 batch，但超過 `membership_timeout` 後必須停止處理並釋放本地 workers。Producer WAL append 不受影響。

因此 Redis outage 的主要結果為：

```text
WAL backlog 增加
consumer lag 增加
downstream processing 延遲
```

Redis 恢復後，consumer instances 重新註冊 membership、重新計算 partition assignment，並從 Persistent Volume 上的 durable checkpoint 繼續處理。

Redis restart、failover 或 membership keys 全部遺失，都不得造成 WAL、Consumer Group metadata 或 checkpoint 遺失，也不得使既有 group 自動重新套用 `EARLIEST` 或 `LATEST`。

---

## 13. Consumer Worker

取得 partition ownership 後啟動 Shard/Partition Worker。

Worker 負責掃描該 partition 下所有 relevant writer lanes。

例如：

```text
partition 17

writer-A-001
writer-A-002
writer-B-001
```

Consumer 依各 writer lane checkpoint 讀取未處理 messages。

流程：

```text
read WAL
→ batch
→ optional consumer-specific coalesce
→ process
→ downstream commit success
→ checkpoint
```

一個 handler batch 只能包含同一 physical stream 中 sequence 連續的 records；不同 streams 可以獨立排程。Handler 只有在整批 records 的 downstream effect 都已 durable 完成時才能回報 success；partial success 必須視為整批失敗並以相同 records retry，因此 downstream 仍需 idempotent。

Checkpoint 必須在 downstream operation 成功後才能推進。

Downstream success 後若 checkpoint durable append 失敗，worker 不得讀取該 stream 的下一批 records；必須 retry 同一 checkpoint，或在 error 無法恢復時 block stream。這會造成 replay，但不可造成 offset 跨越未 durable checkpoint 的 processing gap。

Worker 只能在 durable group control 為 `ACTIVE` 且持有 current generation 時開始新 batch；每個 batch boundary 都必須重新驗證 control。讀到 `INITIALIZING`、`RESETTING`、`DELETING`、`DELETED`、不一致 generation，或無法讀取並驗證 control 時，必須停止取得新 records、drain / abort 尚未開始的 batch、停止 heartbeat 並從 Redis membership 移除自己。已開始 handler 的結果仍可完成 downstream side effect，但只有 generation 仍為 current 時其 checkpoint 才會生效。

錯誤處理：

```text
transient handler error
→ exponential backoff + jitter 後持續 retry

permanent / poison record
→ 將該 stream 標記 blocked、停止推進 checkpoint 並告警
```

本設計不提供 DLQ，因此 permanent error 不可靜默 skip。

Consumer batching 參數必須可設定，未設定時使用：

```text
batch_max_messages = 500
batch_max_bytes = 4 MiB
batch_max_wait = 10 milliseconds
worker_concurrency = min(owned_partitions, GOMAXPROCS)
```

Batch messages / bytes / wait 必須皆大於 0，達到任一條件即可送 handler。調整這些值不得改變 stream order、checkpoint continuity 或 failure semantics。

---

## 14. Durable Group / Offset 模型

因為採 per-instance WAL，一個 partition 沒有單一 global offset。

Consumer Group offset 是 vector：

```text
(group, topic, partition, writer_lane)
→ next_record_sequence
```

例如：

```text
group=persistence
topic=entity-events
partition=17

writer-A-001 = 1000
writer-A-002 = 200
writer-B-001 = 830
```

每個 `(topic, partition, writer_lane)` stream 都有自己的 monotonically increasing `record_sequence`。

`next_record_sequence = 1000` 表示 sequence `< 1000` 都已成功處理，下次由 1000 開始。Checkpoint 不使用 byte position 作為公開語意；segment header 必須記錄起始 sequence，讓 reader 可以定位。

Group 為 `ACTIVE` 時首次發現 snapshot/checkpoint 中不存在的 stream，effective starting offset 是該 stream 當下的 `earliest_retained_sequence`；worker 必須先以此值建立 durable checkpoint，才可開始 delivery。這是唯一的 missing-coordinate 規則，與 group 最初或最近 reset 使用 EARLIEST/LATEST 無關；若建立 checkpoint 前 retention floor 已超過選定值，必須重新讀取 floor，PROTECTED group 則由其 missing coordinate 阻止 GC。

### 14.1 Durable Consumer Group metadata

Consumer Group identity 與 lifecycle metadata 必須存放於 Persistent Volume，而不是 Redis：

```text
manifest（建立後 immutable）:
    format_version
    topic
    group
    group_type: PROTECTED | BEST_EFFORT
    initial_position
    created_at_unix_nano
    checksum

control（以 atomic replace 更新）:
    format_version
    current_generation
    state: INITIALIZING | ACTIVE | RESETTING | DELETING | DELETED
    pending_generation（RESETTING 時必填）
    reset_position（RESETTING 時必填）
    updated_at_unix_nano
    deleted_at_unix_nano（DELETED 時必填）
    checksum
```

欄位用途：Topic / group 驗證 physical path 與 identity；group type 控制 retention barrier；initial position 只用於首次初始化；generation 隔離 reset 前後的 checkpoint；state、pending generation 與 reset position 使 initialization/reset crash 後能繼續同一操作；deleted time 記錄停止 retention protection 的明確邊界；其他 timestamps 供 inspection/audit；format version 與 checksum 用於 decode 及 corruption detection。

Group name 必須符合 Topic name 相同的字元與長度規則。`current_generation` 使用 UUIDv7，不以可重用的整數或 process-local counter 產生。

Group 是否已存在，必須以 durable group manifest 判斷。Redis key 不存在不得被解讀成「新 group」。

同名 group 的首次建立必須使用 filesystem atomic exclusive-create / create-directory primitive；同時建立的 loser 必須讀取 winner 已 durable 發布的 manifest，並驗證設定一致。

### 14.2 Per-consumer checkpoint lanes

為避免 dual consumer 同時覆寫同一 checkpoint file，每個 consumer process incarnation 只 append 自己的 checkpoint lane：

```text
groups/<topic>/<group>/
    manifest
    control
    checkpoints/<generation>/
        snapshot
        members/<consumer_instance_id>/
            writer.lock
            <base-commit-sequence>.checkpoint.wal
            <base-commit-sequence>.checkpoint.open
            durable-end
```

Checkpoint segment header 包含：

```text
magic
format_version
generation
topic
group
consumer_instance_id
base_commit_sequence
created_at_unix_nano
header_crc32c
```

Header 中與 path 重複的 identity fields 必須逐一驗證；它們用來防止合法但被誤搬移的 checkpoint file 被套用到錯誤 group / generation。

Checkpoint record 包含：

```text
record_length
commit_sequence
partition
writer_lane_length
writer_lane
next_record_sequence
committed_at_unix_nano
crc32c
```

Binary types 為 `record_length:uint32`、`commit_sequence:uint64`、`partition:uint32`、`writer_lane_length:uint16`、`next_record_sequence:uint64`、`committed_at_unix_nano:int64`、`crc32c:uint32`，並使用 little-endian。

`record_length` 是包含自身與尾端 CRC 的完整 checkpoint record byte count；decoder 必須先驗證 fixed minimum、lane length、record boundary 與整數溢位，再配置 memory 或套用內容。

`commit_sequence` 在每個 consumer checkpoint lane 內連續增加，用於 incomplete-tail recovery 與 snapshot coverage；partition、writer lane、next record sequence 是 vector coordinate / value；commit time 用於 inspection 與 checkpoint age metrics；length 與 CRC 用於 framing、bounded allocation 及 corruption detection。

`commit_sequence` 從 0 開始且不得重複、倒退或出現 gap。Consumer incarnation 必須在 append 期間持有該 checkpoint lane 的 `writer.lock`，同一 `consumer_instance_id` 永遠不可重用。Graceful shutdown 應 seal active checkpoint segment；crash 後只有成功取得該 lock 的 Checkpoint Recovery Manager 才能 truncate incomplete tail 並 seal `.open`。無法取得 lock 表示原 writer 仍可能 active，不得修改其 lane。

Sealed checkpoint segment footer 包含 `next_commit_sequence`、`record_count`、segment data CRC 與 footer CRC。Checkpoint durable-end 使用 `active_segment_base_commit_sequence`、`next_commit_sequence`、`durable_byte_end` 與 checksum；欄位用途與 message WAL 相同。

每個 checkpoint lane 同樣遵守 `one lane = one writer`、segmentation、checksum、durable flush 與 incomplete-tail recovery。Active checkpoint segment 使用 `.open`，並套用與 message WAL 相同的 durable-end publication protocol。只有 sealed `.wal` 或 durable-end 以前的 records，才能被其他 worker recovery、snapshot compaction 與 retention GC 採信。

有效 checkpoint 是相同 generation 中的 durable snapshot 與所有 durable checkpoint records 的 maximum：

```text
effective_offset(coordinate) =
max(snapshot_offset, durable_checkpoint_records)
```

因此 stale / dual consumer 較晚寫入較小 offset 時不會造成 offset regression，也不需要為 checkpoint correctness 重新確認 ownership。

Consumer 只能 commit 已連續成功處理的 watermark，不可跨過尚未成功的 record gap。Retention GC 只能採信已完成 durable flush 的 checkpoint，不可採信仍在記憶體或尚未 seal 的 checkpoint batch。

Checkpoint logs 必須定期合併為 immutable snapshot。Snapshot 包含：

```text
format_version
topic
group
generation
created_at_unix_nano
entries[]:
    partition
    writer_lane
    next_record_sequence
covered_checkpoint_lanes[]:
    consumer_instance_id
    next_commit_sequence
checksum
```

Entries 保存 compaction 時每個 coordinate 的 maximum；coverage 明確指出 snapshot 已包含每個 checkpoint lane 的哪一段，只有 `end_commit_sequence <= covered next_commit_sequence` 的 sealed checkpoint segments 才能被 GC。

Snapshot entries 必須依 `(partition, writer_lane bytes)` 排序，coverage 必須依 `consumer_instance_id bytes` 排序，確保 canonical encoding、checksum 與重複 recovery 結果一致。

Snapshot 必須以 temporary file、file sync、atomic rename、directory sync 的順序發布。Compactor 必須依序取得 `group.lock` 與 `checkpoints/<generation>/compact.lock` 的 cross-node exclusive locks，並重新確認該 generation 仍是 ACTIVE current generation；它先擷取每個 lane 當下的 durable `next_commit_sequence` 作為固定 coverage boundary，再以舊 snapshot 與 boundaries 以前的 records 計算新 maximum。Boundary 之後 concurrently append 的 records 留給下一次 compaction。新 snapshot 未 durable 發布前不得刪除任何來源 log；新 coverage 必須保留舊 snapshot 已涵蓋但目前無 log directory 的 lanes。Recovery 使用 snapshot 加上所有未被 coverage 涵蓋的 durable checkpoint records 計算 effective offset。

Checkpoint segment rotation 與 compaction thresholds 必須是必填 deployment config，且都大於 0：

```text
checkpoint_segment_max_bytes
checkpoint_segment_max_age
checkpoint_compact_after_segments
checkpoint_compact_after_bytes
```

達到任一 compaction threshold 就必須嘗試 compaction；compaction failure 不影響既有 checkpoint correctness，但必須告警並持續 retry。若 checkpoint growth 使 filesystem free space 到達 §25 的 `filesystem_reserved_bytes` 邊界，consumer 不得開始新的 handler batch；已完成 handler 的 batch 仍可嘗試寫入一次 checkpoint，失敗時依 at-least-once 語意 replay，不能讓無界 checkpoint growth 耗盡 WAL recovery 所需空間。

### 14.3 Generation

Generation 用於隔離 offset reset 或重新初始化前後的 checkpoint：

```text
effective checkpoint
→ 只讀取 control.current_generation 下的 records
```

Group `control` 必須使用 temporary file、file sync、atomic rename、directory sync 的順序切換 current generation。舊 generation worker 的遲到 commit 不得影響新 generation。正常 ownership rebalance 不需要增加 generation。

---

## 15. At-least-once delivery

Consumer delivery semantic 明確定義為：

```text
at-least-once
```

典型情況：

```text
downstream commit success
↓
consumer crash
↓
checkpoint 尚未更新
↓
restart
↓
same messages replay
```

因此 downstream consumer 必須自行保證 concurrency-safe idempotency。

MQ 不承諾 exactly-once processing。

---

## 16. Consumer-specific processing

MQ broker/storage layer 不得進行 business coalescing。

例如：

```text
EVENT_A
EVENT_B
```

WAL 中仍保存兩個 immutable events。

Projection consumer 可以自行：

```text
EVENT_A
EVENT_B
↓
coalesce
↓
PROJECTED_STATE A
```

Reporting consumer 則可以逐 event 處理。

Audit consumer 也可以保留完整歷史。

因此：

```text
coalescing
batch strategy
idempotency
business merge
```

均屬於 Consumer implementation，而不是 MQ core。

---

## 17. 新增 Consumer Group

系統運行中允許新增 Consumer Group。

新 group 第一次啟動時必須設定 Initial Position。

建立 group 必須先 durable 建立 group manifest，並經過：

```text
取得 Topic gc.lock
→ 以 atomic mkdir 取得同名 group create ownership
→ durable 建立 manifest、generation directory 與 state=INITIALIZING 的 control
→ 建立 initial checkpoint snapshot
→ durable atomic 更新 control.state=ACTIVE
→ 釋放 Topic gc.lock
```

同名 group 已存在時，新的 instance 必須讀取既有 manifest，不得以本地 config 覆寫 initial position。

Group create 持有 `gc.lock`，避免 EARLIEST/LATEST snapshot 與 segment deletion 競爭。若 crash 留下 manifest 但 control 缺失或為 `INITIALIZING`，下一個取得 create/group lock 的管理程序必須依 immutable initial position 繼續同一 generation；Retention Manager 必須將缺失、corrupt 或非 ACTIVE control 的 PROTECTED group 視為 barrier，不得假設 group 不存在。

至少支援：

```text
EARLIEST
LATEST
```

---

## 18. EARLIEST

新 Consumer Group 從目前 retained WAL 中最早可用資料開始消費。

EARLIEST 代表：

```text
earliest retained message
```

而不是 Topic 建立以來的第一筆資料。

如果歷史資料已經被 retention GC，就無法再讀取。

初始化 snapshot 對每個當時已知 stream 寫入 `earliest_retained_sequence`；沒有 record 的 stream 寫入其 durable end。初始化後新 stream 依 §14 的 missing-coordinate 規則處理。

---

## 19. LATEST

新 Consumer Group 建立時，以目前所有已存在 streams 的 durable end offset 建立初始 checkpoint。

例如：

```text
writer-A = 1000
writer-B = 850
writer-C = 720
```

初始化：

```text
A = 1000
B = 850
C = 720
```

初始化完成後只需處理 snapshot durable end 之後的新 message。

Group 建立後新出現的 writer lane，依 §14 的 missing-coordinate 規則從該 lane 開始處理，不得因 manifest 的 `initial_position=LATEST` 而跳到新 stream 尾端。

初始化必須只讀取 sealed segment footer 或 active stream durable-end，不得以檔案 size 或尚未 durable 的完整 record 推測 end。Snapshot durable 發布後才能將 group state 切換為 `ACTIVE`。

`LATEST` 採 no-skip / conservative semantics：

```text
不遺漏 group 進入 ACTIVE 後 durable publish 的 message
但可能 delivery 與 group initialization 同時發生的較早 message
```

若要求嚴格線性化的 `LATEST` boundary，必須在管理流程中暫停該 Topic producers；MQ 不提供跨 writer lanes 的自動線性化協調。

---

## 20. Initial Position 只使用一次

Group durable manifest 建立後，consumer restart 不可再次套用 EARLIEST/LATEST。

邏輯：

```text
if durable group manifest does not exist:
    create group and apply initial position exactly once
else if control.state == INITIALIZING:
    resume the existing initialization
else:
    resume from durable checkpoint
```

Consumer instance 是 ephemeral identity。

Consumer Group 是 durable identity。

Pod replacement、Redis restart、Redis failover 或 Redis data loss 都不可使 group identity 或 offset 消失。

---

## 21. Offset Reset / Replay

管理面必須支援：

```text
ResetConsumerGroup
```

可 reset 到：

```text
EARLIEST
LATEST
AT_TIMESTAMP
explicit offsets
```

主要用於：

```text
bug fix 後重新處理
materialized view rebuild
report replay
manual recovery
```

Reset 必須要求 group 處於 stopped / empty 狀態，流程為：

```text
取得 Topic gc.lock
→ 取得 group administrative lock
→ durable 寫入 state=RESETTING、pending_generation、reset_position
→ consumer 在開始下一批前讀到 RESETTING 並停止
→ 等待 Redis active membership 為空至少一個 membership_timeout
→ 寫入新 generation initial checkpoint snapshot
→ durable atomic 更新 current_generation=pending_generation、state=ACTIVE
→ 清除 pending_generation / reset_position
→ 依相反順序釋放 locks
```

Redis 無法可靠確認 membership 為空時，Reset 必須失敗並保留 `RESETTING`，不可逕自完成。Crash 後管理程序依 durable control 繼續相同 reset operation，不得產生另一個 pending generation。

`reset_position` 必須完整持久化可重建相同 snapshot 的 request：position kind、AT_TIMESTAMP 的 timestamp，或 EXPLICIT_OFFSETS 的 canonical complete vector；resume 不得重新讀取呼叫端參數或改用當下 defaults。

舊 generation 的遲到 checkpoint record 必須被忽略。Generation 能防止 offset contamination，但無法撤銷 reset 前已開始的 downstream side effect；因此 consumer integration 仍必須允許 concurrent duplicate，Reset API 不提供 downstream transaction fencing。

Reset position 定義：

```text
EARLIEST
→ 每個 stream 使用 earliest_retained_sequence

LATEST
→ 每個 stream 使用 reset snapshot 時的 durable end

AT_TIMESTAMP(T)
→ 每個 stream 使用 sequence 最小且 append_time_unix_nano >= T 的 retained record；
  若不存在則使用該 stream durable end

EXPLICIT_OFFSETS
→ 必須為每個目前存在、尚未隨 lane GC 移除的 stream 提供 next_record_sequence
```

`AT_TIMESTAMP` 依各 stream 獨立定位，不宣稱跨 lane total order。因 append time 在單一 stream 內也可能受 wall-clock adjustment 影響，reader 必須使用 segment min/max time pruning 後逐 record 檢查，不可假設 timestamp 隨 sequence 單調。

若 stream 曾執行 retention GC，只有在 `T > retention-index.max_deleted_append_time_unix_nano` 時才能安全執行 AT_TIMESTAMP；否則可能符合條件的較小 sequence 已被刪除，必須回傳 `OFFSET_OUT_OF_RANGE`。

任何目標 offset 小於 `earliest_retained_sequence`、大於 durable end、缺少目前存在的 stream coordinate，或指向 corrupt / blocked stream 時，整個 Reset 必須回傳 `OFFSET_OUT_OF_RANGE` / validation error，current generation 不得切換。Reset snapshot 是 all-or-nothing durable publication。

### 21.1 Delete Consumer Group

停止使用的 PROTECTED group 若永遠保留，會永久阻止 WAL GC，因此管理面必須提供明確且不可逆的 `DeleteConsumerGroup`：

```text
取得 Topic gc.lock
→ 取得 group administrative lock
→ durable 寫入 state=DELETING 使 workers 停止
→ 等待 Redis active membership 為空至少一個 membership_timeout
→ durable atomic 寫入 state=DELETED、deleted_at_unix_nano
→ 依相反順序釋放 locks
```

Redis 不可用時不得完成 Delete。只有 durable `DELETED` state 可以使 retention GC 忽略該 group；刪除 Redis membership、停止 deployment 或缺少 heartbeat 都不具有此效果。

Crash 後若 control 為 `DELETING`，管理程序只能繼續等待 membership 清空並完成同一 delete，不得恢復為 ACTIVE。若 control 已是 `DELETED`，重複 Delete 必須 idempotently 回傳已刪除。

Deleted group name 永久保留，不可用同名重建；consumer 必須拒絕加入。Checkpoint data 依下節規則 GC，manifest/control 必須保留以維持名稱 tombstone。

所有需要同時取得 Topic GC lock 與 group administrative lock 的操作，都必須固定先取得 Topic lock 再取得 group lock，避免管理操作彼此 deadlock。

### 21.2 Checkpoint generation GC

只有不再是 `control.current_generation` 的 generation，或 group 已 durable `DELETED`，才可刪除 checkpoint generation。Janitor 必須先取得 `group.lock`、再取得該 generation 的 `compact.lock`，重新讀取 control 驗證 eligibility，接著以 `consumer_instance_id` byte order 取得該 generation 所有 checkpoint `writer.lock`；任一 lock 無法取得就整次退出，不可部分刪除。

驗證完成後，generation directory 必須 atomic rename 到 root-level checkpoint GC staging directory 並同步 parent directory，再釋放 locks 及遞迴刪除；crash recovery 可重入完成 staging deletion。Group manifest、control 與 group lock 不得隨 checkpoint generation 刪除。

---

## 22. Retention

WAL 不能無限保存。

Retention 至少需要支援：

```text
time based
size based
```

Closed WAL segment 只有在下列條件成立時才可刪除：

```text
所有 PROTECTED groups
都已將 durable checkpoint 推進超過 segment end

AND

segment 超過 retention time
或 wal_total_bytes 超過 soft size limit
```

某 PROTECTED group 若缺少該 stream coordinate、checkpoint 無法驗證或 effective offset 小於 segment footer 的 `next_record_sequence`，條件即不成立；Retention Manager 不得把 missing coordinate 當成 LATEST 或已消費。

`INITIALIZING`、`RESETTING` 或 `DELETING` 中的 PROTECTED group 必須視為 retention barrier；只有 `ACTIVE` group 的 durable checkpoint 或 durable `DELETED` state 可以參與 GC decision。

BEST_EFFORT group 不會阻止 retention GC。其 lag 超過 retained WAL 時，可以出現：

```text
OFFSET_OUT_OF_RANGE
```

MQ 不保證被 GC 的資料可以恢復。

每個建立過的 stream 在 lane 被整體 GC 前都必須保存 durable `retention-index`，即使目前沒有 retained segment：

```text
format_version
earliest_retained_sequence
max_deleted_append_time_unix_nano（尚未刪除過 record 時為 null）
updated_at_unix_nano
checksum
```

Stream 建立時 index 的 `earliest_retained_sequence=0`、deleted-time 為 null。Retention Manager 只能依 sequence 刪除最舊的 contiguous sealed segment prefix。刪除前先 durable atomic 將 earliest 推進到刪除 prefix 後第一個 retained sequence（全部刪除時為 stream durable end），並將 deleted-time 更新為舊值與本次刪除 records append time 的 maximum；該更新是資料不再可 replay 的 logical boundary。之後即使 segment file 因 crash 尚未完成 physical unlink，consumer 也不得讀取低於 `earliest_retained_sequence` 的 records，recovery janitor 只需完成刪除。

`earliest_retained_sequence` 用於 EARLIEST、offset validation 與 OFFSET_OUT_OF_RANGE；`max_deleted_append_time_unix_nano` 是所有已 logical deleted records 的 append-time maximum，用於安全判定 AT_TIMESTAMP；updated time 供 inspection；format/checksum 用於 decode 與 corruption detection。Index 缺失、checksum 錯誤或 floor 倒退時必須 block stream 與 GC 並告警，不得僅從仍存在的 files 猜測較小 floor。

發生 `OFFSET_OUT_OF_RANGE` 後，對應 group/partition 必須進入 blocked state 並停止自動消費，直到管理者執行 Reset；不得自動套用 EARLIEST 或 LATEST。PROTECTED group 在 storage failure、corruption 或明確管理操作以外出現此錯誤，必須視為 correctness incident。

PROTECTED group 尚未完成的資料不得因 time-based 或 size-based retention 被刪除。Time-based age 使用 sealed segment footer 的 `max_append_time_unix_nano` 計算；size-based usage 使用實際 allocated WAL bytes，不使用 record count 估算。

Retention / capacity config 至少包含：

```text
Topic control:
retention_time_seconds
soft_max_wal_bytes
hard_max_wal_bytes

Root storage control:
filesystem_reserved_bytes
```

必須滿足 `0 < soft_max_wal_bytes < hard_max_wal_bytes`，並在 filesystem 中保留 `filesystem_reserved_bytes` 供 in-flight flush、seal 與 recovery / GC metadata 使用。

Publish 加入 group-commit batch 前，若當下 Topic allocated WAL bytes 加該 encoded record 會超過 hard limit，回傳 `REJECTED_BEFORE_WRITE`。多 writer capacity check 存在競爭；若 write / sync 已開始後才遇到 `ENOSPC`、quota 或 I/O error，必須回傳 `DURABILITY_UNKNOWN`，停止該 writer 接受新 Publish 並告警，不可錯誤宣稱「沒有寫入任何 bytes」。

未設定 byte limits 時，可由 PV usable capacity 扣除 reserved bytes 後產生下列預設水位：

```text
85% = soft_max_wal_bytes
90% = hard_max_wal_bytes
```

部署所選 WAL capacity 必須至少滿足：

```text
peak_wal_bytes_per_second
× maximum_supported_downstream_outage_seconds
× 2 safety factor
```

---

## 23. Writer lane GC

Instance restart 後會建立新 writer lane。

舊 writer lane 不再寫入，稱為 retired lane。

當：

```text
durable retired marker 已發布

AND

對 lane 下每一個 (topic, partition) stream：
所有 PROTECTED Consumer Groups
都已 checkpoint 到最後 sealed segment footer 的 next_record_sequence

AND

每個 segment 都符合 retention policy
```

才可以刪除整個 retired lane。Lane 沒有單一 scalar end offset，GC 必須逐 stream 驗證，不得只比較 lane 建立時間或目錄 mtime。

Active lane 中已 sealed 且符合 §22 條件的舊 segments 可以逐 segment GC，不必等待整個 lane retired。Retired lane deletion 必須先 atomic rename 到 GC staging directory、sync parent directory，再遞迴刪除；crash recovery 必須能安全繼續 staging directory 中斷的刪除。

因此正常情況下 writer lane 數量應維持為：

```text
active instance lanes
+
尚未完成 retention/consumer 的 retired lanes
```

而不是永久累積。

---

## 24. Consumer Lag Metrics

由於沒有單一 partition offset，lag 必須跨 writer lanes aggregate。

例如：

```text
lag_messages =
Σ(
    stream_durable_end_next_sequence
    -
    group_effective_next_sequence
)
```

每一項最小為 0；不存在的 coordinate 必須先依 group initial/new-stream policy 建立有效起點，不可把 missing checkpoint 默認為 LATEST。`lag_bytes` 由 effective offset 到 durable end 間的 bounded record lengths / segment sizes 加總，不以平均 record size 推估。

至少監控：

```text
consumer_lag_messages
consumer_lag_bytes
oldest_unconsumed_age_seconds
wal_total_bytes
wal_retained_bytes
active_writer_lanes
retired_writer_lanes
consumer_rebalance_count
handler_retry_total
blocked_streams
wal_corruption_total
checkpoint_corruption_total
publish_rejected_total
publish_durability_unknown_total
checkpoint_compaction_failures_total
```

Metric 計算來源：lag messages/bytes 使用 stream durable-end、sealed footer 與 effective checkpoint；oldest age 使用 checkpoint 指向 record 的 stored append time；active/retired lanes 使用 retired marker。MQ core 無法在 crash 後精確判斷某 record 是否曾被 downstream 執行，因此不提供宣稱精確的 duplicate/replay counter；duplicate observation 應由 downstream 使用 `message_id` 統計。

Prometheus labels 不得包含 `message_id` 或未受控的 `writer_lane`。Writer-lane 維度只出現在 bounded admin inspection；metrics 預設依 Topic、Consumer Group、partition 與 result class aggregate。

### 24.1 Admin inspection

維運面必須提供 bounded、read-only inspection（CLI 或 library API 均可）：

```text
InspectTopic
→ topic manifest/control、allocated bytes、stream/lane counts

InspectLane(lane_id)
→ lifecycle、writer_location、application_version、streams、durable ends、corruption

InspectGroup(topic, group)
→ manifest/control、generation、effective offsets、lag、blocked coordinates

InspectStream(topic, partition, lane_id)
→ retention floor、segments、durable end、checksum/recovery state
```

所有查詢必須有明確 Topic/group/lane filter 與最大輸出筆數，不提供無限制 message payload dump。這些 inspection consumers 是 lane/group manifest diagnostic fields 與 timestamps 的實際使用者。

---

## 25. Storage / Kubernetes PV

MQ storage 使用 Kubernetes Persistent Volume。

Storage root 必須由明確的 `InitStorage` 管理操作建立，runtime process 不得在 `storage-control` 缺失時自動初始化空白 storage。Control 欄位固定為：

```text
format_version
storage_id（UUIDv7，建立後 immutable）
filesystem_reserved_bytes
updated_at_unix_nano
checksum
```

每個 deployment 必須設定預期的 `storage_id`；mount 到空白、錯誤或 checksum 不合法的 root 時必須拒絕 Publish、consume 與 GC，避免把換錯 PV 誤判為資料已消失。除 Topic fingerprint 明確使用 SHA-256 外，本文件所有 metadata `checksum` 都是對 `format_version` 所定義 canonical bytes（不含 checksum 欄位本身）計算 CRC32C；reader 必須限制 metadata file size、拒絕 trailing bytes 與不支援版本。同一 `format_version` 不得持久化本文件未列出且沒有 reader、decision 或 inspection consumer 的欄位；新增欄位必須升級 format 並同時定義用途與相容行為。

Storage topology 明確要求：

```text
所有 producer 與 consumer pods
掛載同一個支援 multi-node ReadWriteMany 的 shared filesystem

filesystem / CSI backend
必須提供本設計所需的 directory visibility、atomic rename、exclusive create、cross-node advisory lock、file sync 與 directory sync semantics
```

Kubernetes access mode 不替 application enforce `one lane = one writer`。MQ 必須使用全域唯一 lane ID、exclusive create 與 lane ownership validation 防止兩個 process append 同一 physical stream。Writer / compactor 持有 lock 期間若收到 storage I/O error、lock lost 或 lease revoked indication，必須停止 mutation、進入 failed state 並要求 process restart，不得假設仍有 ownership。

每種 StorageClass / CSI / mount-option 組合上線前都必須由兩個不同 nodes 的 pods 通過 conformance test：exclusive create 只能有一個 winner；同一路徑 advisory lock 互斥且 holder kill 後可回收；directory entry 對另一節點可見；rename 的 reader 只能看到完整舊版或新版；file sync + directory sync 後的檔案在 pod/node 強制終止與 backend 支援的 remount/failover 測試後仍存在且內容完整。任一測試不成立即不得部署本設計，不能只根據 `ReadWriteMany` access mode 推定語意。

實體布局：

```text
<root>/v1/
    storage-control
    storage-admin.lock
    lane-gc-staging/
    checkpoint-gc-staging/
    lanes/<writer_lane>/
        manifest
        writer.lock
        retired（retired 後存在）
    topics/<topic>/
        topic-manifest
        topic-control
        gc.lock
        p=<partition>/
            lanes/<writer_lane>/
                <base-record-sequence>.wal
                <base-record-sequence>.open
                durable-end
                retention-index
    groups/<topic>/<group>/
        manifest
        control
        group.lock
        checkpoints/<generation>/
            snapshot
            compact.lock
            members/<consumer-instance>/
                writer.lock
                <base-commit-sequence>.checkpoint.wal
                <base-commit-sequence>.checkpoint.open
                durable-end
```

`storage-control` 只保存所有 process 必須一致使用的上述 root-level 欄位，並以 temporary file、file sync、atomic rename、directory sync 更新；更新不得改變 `storage_id`。Topic 的 logical hard limits 可以 overcommit；真正的全 filesystem safety boundary 由 runtime `statfs` 與 reserved bytes 決定，因此不得宣稱把各 Topic hard limit 加總就能預留實體容量。

Producer 在形成新 group-commit batch 前、consumer 在開始新 handler batch 前，都必須以該操作的最大 bounded additional bytes 計算 projected free space，確認完成後仍高於 `filesystem_reserved_bytes`，不足時套用 backpressure。檢查後仍可能因多 writer 競爭或 backend quota 遇到 `ENOSPC`；Publish 依 §22 回傳 `DURABILITY_UNKNOWN`，checkpoint 失敗則不得推進 offset。到達 reserve 後，只允許完成已開始的 bounded flush/checkpoint，以及 recovery、seal、retention unlink 等能恢復一致性或釋放空間的操作；不得開始新的 WAL、handler 或 compaction workload。

`filesystem_reserved_bytes` 必須大於 0，且部署 sizing 至少涵蓋最大 replica/concurrency 設定下所有已開始 WAL batches、checkpoint records、segment seal 與必要 recovery metadata 的 worst-case bytes，再加 storage backend 所需安全餘裕；若無法給出這個上界，該部署不可宣稱有 hard ENOSPC protection。

每個 Topic 的 Retention Manager 必須取得 `gc.lock` 才能執行 segment/lane deletion。同時執行的第二個 janitor 必須退出；lock loss 或 storage error 時立即停止 GC。GC correctness 只依賴 PV 上的 Topic control、group control、durable checkpoint、segment footer 與 retired marker，不依賴 Redis membership。

每個 partition worker 只掃描自己 partition 下的 lanes，不得要求每個 worker 全量掃描其他 partitions 的 records。

但 MQ 不假設：

```text
PV = automatically replicated
```

Durability 實際取決於：

```text
StorageClass
CSI Driver
cloud storage backend
replication scope
```

部署時必須確認：

```text
single disk failure
node failure
rack failure
AZ failure
```

哪些 failure domain 被 storage backend 保護。

MQ application layer 本身不實作 WAL replication protocol。

如果部署環境只能提供 per-pod RWO volume，則不符合此架構前提；要支援該拓撲必須另行設計 remote WAL reader / replication protocol。

---

## 26. 不做的功能

本設計明確不實作：

```text
exactly-once
broker replication
cross-region replication
distributed transaction
transactional producer
Publish 與外部 business operation 的原子性
strict global ordering
multiple partition leader protocol
remote partition appender
producer RPC forwarding
Raft/Paxos data replication
priority queue
delayed messages
DLQ
message compaction
dynamic partition resizing
```

目標是保持為：

```text
lightweight
WAL-backed
at-least-once
partitioned
multi-consumer-group
durable local/shared-storage MQ
```

而不是重新實作 Kafka。

---

## 27. 最終核心模型

```text
Producer Instance
       │
       │ Publish
       ▼
Per-instance WAL Lane
       │
       ├── Topic
       │     └── Partition
       │
       ▼
Persistent Volume ───────────────────────────┐
       │                                     │
       │ WAL                                 │ durable group manifests,
       │                                     │ generations and checkpoints
       ├─────────────────────┐               │
       ▼                     ▼               │
Consumer Group A       Consumer Group B ◄────┘
       │                     │
partition ownership     partition ownership
via Redis membership    via Redis membership
       │                     │
       ▼                     ▼
Downstream A            Downstream B
```

系統最重要的 correctness contract 為：

```text
Publish success
→ message durable in WAL

Consumer
→ at-least-once

Duplicate
→ expected and allowed

Downstream
→ must be idempotent

Redis ownership
→ performance/load-balancing coordination
→ not source of data correctness

Persistent Volume
→ durable source for WAL, Consumer Group identity and checkpoints

Checkpoint
→ only advance after successful processing

Redis restart / data loss
→ rebuild membership and ownership
→ resume from PV checkpoint

Business integration
→ outside MQ core contract
```

---

## 28. 驗收條件

至少必須以 automated tests / fault-injection 驗證：

1. Publish 回覆 `SUCCESS` 後在每個 flush/publication 邊界立即 kill process 或 remount storage，record recovery 後仍可讀取；尚未進入 durable-end 的 record 不得被 live consumer delivery。
2. Write 中途 kill 只 truncate incomplete tail；WAL 已 sync 但 durable-end 尚未成功發布時回覆 `DURABILITY_UNKNOWN`，recovery 可保留該 record，但不得遺失任何已回覆 SUCCESS 的 record。
3. Segment footer append、`.open` rename、directory sync、下一個 `.open` create 與 durable-end replace 的每個 crash point，recovery 都維持 sequence continuity，且 stale durable-end 不造成 skip 或把 footer 當 record。
4. Segment header/path identity、Topic fingerprint、record/footer CRC 或前後 segment sequence 任一不一致時，只 block 對應 stream 並告警，不會猜測修正或靜默 skip。
5. 兩個 writer 無法同時取得同一 WAL/checkpoint lane lock；holder crash 後 Recovery Manager 才能取得 lock、修復 tail 並 seal。Process restart 一律建立新 UUID lane，retired marker durable 前不得做 whole-lane GC。
6. 單一 physical stream 的 delivery 與 checkpoint 依 sequence 連續；不同 lanes 並行不建立虛假的 partition-wide order，也不允許 checkpoint 跨過 failed/poison record。
7. Consumer 在 downstream success、checkpoint durable 前 crash 或 checkpoint append 失敗時 message 會 replay；worker 不會先處理同 stream 下一批。Permanent error 會 block 而非自動 skip。
8. Dual consumer concurrent processing 最多造成 duplicate；較晚寫入的較小 checkpoint 不使 effective offset 倒退，所有生效 checkpoint 都是各自已連續成功的 watermark。
9. Active checkpoint lane 只有 durable-end 以前的 records 可採信；checkpoint tail recovery、segment rotation 與 snapshot compaction 的每個 crash point 都不會使 effective offset 倒退或超前。Snapshot coverage 之後的 concurrent commits 必須由 log recovery 保留；non-current generation GC 必須等所有 writer locks 並可從 staging crash 繼續。
10. Group concurrent create 只有一個 durable winner；在 manifest、INITIALIZING control、initial snapshot 與 ACTIVE publication 各點 crash，都只能繼續原 generation/initial position，不會另建 group。
11. Redis restart、failover、keys 清空或短暫 membership view 分歧後，group 從 PV 的 current generation/checkpoint 恢復，不重新套用 Initial Position；Redis outage 超過 timeout 時 consumer 停止，Producer 不受影響。
12. 不同 process/language 對相同 canonical membership input 算出相同 Rendezvous owner；Redis 回傳順序、heartbeat client clock skew 與 hash collision tie 都不改變結果。
13. EARLIEST、LATEST、AT_TIMESTAMP 與 EXPLICIT_OFFSETS reset 都建立完整新 generation snapshot；缺 stream、越界、corruption 或 timestamp 不大於 deleted-time boundary 時整體失敗，current generation 不切換。切換後舊 generation commit 永遠無效。
14. RESETTING / DELETING 時 workers 停止加入新 batch；Redis 無法確認 empty membership 時操作不完成。Delete crash 可繼續且 idempotent，只有 durable DELETED 才解除 PROTECTED retention barrier，同名 group 無法重建。
15. PROTECTED group 落後、缺 coordinate、control 不可驗證或正在 INITIALIZING/RESETTING/DELETING 時，retention GC 不刪除可能尚未消費的 segment；BEST_EFFORT 落後超過 floor 時 block 並回傳 `OFFSET_OUT_OF_RANGE`，不自動套用 EARLIEST/LATEST。
16. Retention-index durable 更新前後與 physical unlink/staging deletion 的每個 crash point，都維持 monotonic logical floor；已 logical deleted records 即使檔案仍存在也不可 replay，index 缺失/corrupt 不得從殘留檔案推測較小 floor。
17. Active lane sealed-prefix GC 與 retired whole-lane GC 都逐 stream 驗證 footer、protected checkpoints 及 retention；whole-lane rename 到 staging 前後 crash，可重入完成且不刪錯 active lane。
18. Topic hard limit 在尚未寫入該 Publish 的 record bytes 前命中時必須回覆 `REJECTED_BEFORE_WRITE`；多 writer race、quota 或 `ENOSPC` 發生在 write/sync 開始後只能回覆 `DURABILITY_UNKNOWN`。到達 filesystem reserve 時不開始新 WAL/handler/compaction workload，既有 protected data 不被容量策略刪除。
19. 缺少、corrupt 或不符 expected `storage_id` 的 storage root，以及缺少/corrupt/mismatched Topic manifest/control，都會 fail closed，不會自動初始化或採用 pod-local defaults。
20. 每個實際 StorageClass / CSI / mount-option 組合都通過 §25 的雙節點 lock、visibility、atomic rename、sync/remount conformance test，否則部署驗收失敗。
21. `InspectTopic`、`InspectLane`、`InspectGroup`、`InspectStream` 能在 bounded output 中呈現各自承諾的 diagnostic fields；corruption log/alert 包含 lane 的 writer location 與 application version，證明這些 persisted fields 有實際 consumer。

效能 benchmark 必須至少產出：

```text
Publish throughput
Publish p50 / p95 / p99 latency
group commit batch distribution
consumer throughput
checkpoint write amplification
checkpoint recovery time
backlog catch-up rate
WAL bytes per message
writer lane / partition 數量對 scan 與 memory 的影響
```

Segment size、group commit wait、partition count、worker concurrency、batch size 與容量水位，只有在上述 correctness tests 通過後才能依 benchmark 調整。
