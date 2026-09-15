# Local Durable MQ V1 系統設計

## 1. 文件狀態

本文件是 `Local_Durable_MQ.md` 的 V1 收斂設計，也是後續實作的主要依據。原文件保留作為完整構想與風險分析來源；兩者衝突時，以本文件為準。

本次只保留下列需求必須的能力：

- Kubernetes service 面對短期 burst logging/event traffic 時，不把待處理資料無界保留在 process memory；
- Producer 不經 remote broker，直接將 message durable 寫入共用 Persistent Volume；
- Consumer 非同步、批次處理資料；
- `Publish` success 後，process/pod restart 不遺失該 message；
- delivery 為 at-least-once，consumer 必須能處理 concurrent duplicate；
- 支援多 Topic、同一 Topic 的多 Consumer Group，以及運行後建立新 Group；
- downstream、Redis、pod 或 node 短暫不正常時可以安全停止並恢復；
- WAL 空間有界，已符合條件的過期資料可安全移除；
- 提供足以操作 blocked data、orphan lane 與容量事件的 inspection/admin 能力。

本文件不把尚未取得的流量、延遲及容量數字硬編碼為架構常數。這些值是 deployment sizing input，必須在目標 StorageClass benchmark 後定案。

關鍵字 `必須`、`不得` 表示 correctness contract；`建議` 表示可依 benchmark 調整，但不得破壞 contract。

---

## 2. 需求理解與合理假設

### 2.1 問題

事件目前可能先累積在 application memory。短期流量高於 downstream throughput 時，memory 使用量會隨 backlog 增長，最終造成 latency、GC pressure 或 OOM。

傳統 MQ 可以提供 durable buffering，但會增加 broker data path、broker cluster 維運與容量管理。其 storage 也可能位於 network PV，因此本設計不宣稱「不使用網路」；目標是移除 Producer 到 broker 的額外 hop，讓 application 內的 writer 直接形成 durability obligation。

### 2.2 假設

- 專案使用 Go 1.24、Linux 與 Kubernetes；
- 所有參與者掛載同一個 RWX filesystem；
- storage backend 通過本文件定義的 conformance tests；
- Redis 已是現有 infrastructure，只用於 consumer membership/load balancing；
- consumer side effect 支援 `(topic, message_id)` idempotency，並可安全面對同一 message 的 concurrent execution；
- 不要求不同 producer lanes 之間的 global order；
- storage backend 本身負責 disk/node/AZ durability，本 package 不做 WAL replication；
- V1 的 retention 是 operational retention，不是法規要求的 secure erase 或固定刪除 deadline。

### 2.3 名稱

`Local` 表示 writer embedded in application process，沒有 remote broker hop；不表示使用 node-local disk。

---

## 3. V1 範圍與必要性審查

### 3.1 保留

| 能力 | 保留理由 |
|---|---|
| Per-topic writer lane | 讓每個 producer 直接 sequential append，吸收 burst |
| Segmented WAL、group commit、durable frontier | durability、bounded files 與批次 fsync 必要 |
| Topic | 隔離不同資料類型、retention 與容量觀測 |
| Consumer Group | 同一資料供不同 downstream 獨立處理 |
| Redis membership + Rendezvous ownership | consumer 水平擴展；Redis 不承擔 durable correctness |
| Durable checkpoint | crash/rebalance 後恢復且不跳過未完成資料 |
| PROTECTED / BEST_EFFORT | 同時支援不可遺失與可犧牲的日誌類型 |
| Retention index + staging deletion | crash-safe logical delete 與 physical cleanup |
| 兩階段 capacity reserve | 避免 filesystem 滿時 producer、consumer、GC 自我鎖死 |
| Blocked record 與人工 acknowledge | 沒有 DLQ 時仍要有可稽核的操作出口 |
| Inspection、metrics、fault injection | production operation 與公開專案品質必要 |

### 3.2 從 V1 移除或延後

以下能力不是目前需求的必要條件，且會顯著增加 correctness state space：

- logical partition、partition hash 與 partition resize；
- `(topic, partition, writer_lane)` 的 stream matrix；
- offset generation、任意 Reset、AT_TIMESTAMP、EXPLICIT_OFFSETS；
- checkpoint append WAL、checkpoint segment rotation 與 coverage compaction protocol；
- strict `LATEST` linearization；
- producer-side deduplication；
- DLQ、自動 skip、priority、delay、exactly-once、global ordering；
- broker replication、cross-region replication、Raft/Paxos；
- 動態變更 persisted format、Topic rename/delete；
- 跨語言 writer/reader compatibility；V1 只保證同一 Go major implementation 的 versioned format compatibility。

建立新 Group 的 `EARLIEST`/`LATEST` 已涵蓋目前需要的 retained history replay。未來若確認需要任意 replay，再以獨立 ADR 引入 generation；不得先把 generation 複雜度放進 V1。

### 3.3 為何移除 partition

原設計每個 producer 對每個 Topic/partition 維護獨立 `.open`、`durable-end` 與 checkpoint coordinate。隨機 key 的一個 group commit 可能同時 sync 多個 stream，且 lane、partition、group 相乘會造成大量目錄與 metadata I/O。

V1 的資料是 logging/event burst，沒有 partition-wide ordering 需求。Consumer 改以 writer lane 為 ownership 單位：

```text
stream = (topic, writer_lane)
checkpoint = (topic, group, writer_lane) -> next_sequence
```

一個 producer process 對每個使用中的 Topic 建立一條 lane。Consumer concurrency 由同 Topic 的 active/retired lanes 提供。若 benchmark 證明單 producer/單 lane 限制 consumer throughput，V2 才加入明確的 `writer_shards_per_topic`；不得直接恢復 partition × lane matrix。

---

## 4. Correctness invariants

實作與 review 必須優先維持以下 invariants：

1. `Publish` 回覆 success 前，record bytes 與對應 durable frontier 都已完成要求的 sync。
2. 一條 lane 在正常生命週期只有建立它的 process 可以 append。
3. Recovery 不得只因 advisory lock 可取得，就推論曾失聯的 writer 永遠不會恢復。
4. Consumer 只能 delivery sealed segment，或 active segment durable frontier 以前的完整 records。
5. Checkpoint 只能來自 handler 已完成 durable side effect 的連續 sequence。
6. Checkpoint 遺失或倒退最多造成 replay，不得造成 skip。
7. Retention floor 只能單調前進，且只能越過所有 PROTECTED groups 已成功處理的資料。
8. Logical retention floor durable 前，不得 physical delete 對應 segment。
9. Redis state 遺失最多造成暫停、rebalance 或 duplicate，不得造成 WAL/checkpoint 遺失。
10. 任何無法驗證的 metadata、checksum、identity 或 sequence continuity 都 fail closed。
11. 到達 producer capacity watermark 後，consumer checkpoint 與 GC 仍有保留空間可以推進。
12. 所有 background queue、batch 與 scan 都有明確上限。

---

## 5. 系統架構

```text
Product code
    │
    │ Publish(ctx, Message)
    ▼
localmq Publisher
    │ bounded admission queue
    │ per-topic group commit
    ▼
RWX Persistent Volume
    │
    ├── Topic/Lane WAL ───────────────┐
    ├── Group manifests/checkpoints  │
    └── Retention metadata           │
                                      ▼
Redis membership ─────────────► localmq Consumer
    load balancing only               │
                                      │ bounded batch
                                      ▼
                                 Product Handler
                                      │
                                      ▼
                                  Downstream

Maintenance Controller
    ├── retention / pressure GC
    ├── checkpoint baseline compaction
    ├── orphan/block inspection
    └── serialized admin operations
```

### 5.1 元件責任

`Client`

- 驗證 config 與 storage identity；
- 建立 process/topic writer lanes；
- 提供 `Publish`；
- framework lifecycle stop 時停止 admission、drain group commit、seal lanes；
- 不自動建立 Topic 或 Consumer Group。

`Consumer`

- 註冊 Redis membership；
- 以 Rendezvous Hash 計算 lane ownership；
- 以 bounded、round-robin scheduler 在所有 owned lanes 間分派最多
  `worker_concurrency` 個同時執行中的 handler steps；
- 依 durable checkpoint 讀取同一 lane 內 sequence contiguous 的 batch；
- 呼叫 product handler；
- 批次發布 checkpoint；
- permanent error 時定位並 durable block record。

`Maintenance Controller`

- 執行 retention、capacity-pressure cleanup 與 checkpoint baseline compaction；
- 執行 CreateTopic、CreateGroup、DeleteGroup、AcknowledgeRecords；
- 不參與 Publish hot path；
- V1 同一 storage root 同時只能有一個 externally fenced maintenance writer。

`Inspector`

- read-only、bounded 查詢；
- 不取得 writer ownership，也不修改 metadata。

### 5.2 Maintenance singleton

Generic RWX advisory lock 不足以對曾 network-partition 的舊 process 提供 fencing。因此 V1 不用 filesystem lock 宣稱 maintenance leader election correctness。

部署必須保證：

- Maintenance Controller 使用單 replica；
- replacement 前，舊 pod/node 必須已停止或由 infrastructure fence；
- controller 無法確認前任已停止時，只能 read-only inspection，不得執行 metadata mutation；
- `maintenance.lock` 只防止誤啟動兩個健康 process，不是 node fencing 證明。

Maintainer 暫停時，Publish/consume 可以繼續；GC、group admin 與 checkpoint compaction 暫停。這是刻意以 liveness 換 safety。

---

## 6. 專案組織

沿用 repository 現有 framework/config/observability，不新增 DI 或 logging framework。

```text
pkg/infra/localmq/
    doc.go                  package contract
    config.go               runtime config 與 validation
    errors.go               typed public errors
    message.go              public Message/Receipt/Handler types
    client.go               lifecycle 與 public facade
    publisher.go            bounded admission、group commit
    wal.go                  append/flush/rotation/recovery reader
    format.go               versioned encoding、CRC、bounded decode
    storage.go              layout、atomic publication、sync primitives
    consumer.go             lane reconcile、batch loop
    membership.go           Redis ZSET/Function adapter
    checkpoint.go           member checkpoints 與 baseline
    group.go                durable group lifecycle
    retention.go            logical floor、staging、pressure GC
    blocked.go              durable blocked record 與 acknowledge
    maintenance.go          serialized maintenance operations
    inspect.go              bounded inspection API
    metrics.go              Prometheus collectors

pkg/infra/module.go         lazy framework-managed Client registration
configs/examples/infra.yaml local_mq 範例與繁體中文說明

cmd/localmqctl/             storage init、conformance、offline inspection；
                            mutation command 只能連到 active Maintainer，或在確認
                            Maintainer 已停止且前任 node 已 fenced 後執行
```

第一版維持單一 Go package，以 package-private interfaces 注入 filesystem、clock、Redis membership 與 fault points。只有出現第二個 production consumer 時才拆 subpackage，避免預先抽象。

---

## 7. Public API

以下為語意 contract，實作者可以調整非必要命名，但不得改變結果分類。

```go
type Message struct {
    Topic         string
    MessageID     []byte
    EventType     string
    SchemaVersion uint32
    Payload       []byte
}

type Receipt struct {
    Topic    string
    LaneID   string
    Sequence uint64
}

type PublishErrorKind uint8

const (
    RejectedBeforeWrite PublishErrorKind = iota + 1
    DurabilityUnknown
)

type Publisher interface {
    Publish(context.Context, Message) (Receipt, error)
}

type Handler interface {
    Handle(context.Context, Batch) error
}

type Batch struct {
    Topic    string
    LaneID   string
    Messages []DeliveredMessage
}

type DeliveredMessage struct {
    Sequence      uint64
    AppendTime    time.Time
    MessageID     []byte
    EventType     string
    SchemaVersion uint32
    Payload       []byte
}

type Subscription struct {
    Topic string
    Group string
}

func NewConsumer(
    client *Client,
    redisClient *redis.Client,
    subscription Subscription,
    handler Handler,
) (*Consumer, error)

func NewMaintainer(
    client *Client,
    redisClient *redis.Client,
    config MaintainerConfig,
) (*Maintainer, error)
```

`Client` 實作 `framework.ManagedResource`。`Consumer` 由有 handler 的 product module 建立並以 `PhaseService` managed resource 啟停；`Maintainer` 只由指定 maintenance deployment 建立。Public API 不暴露 WAL file descriptor、codec 或可繞過 durability boundary 的 append method。

Maintainer 對外提供 typed admin methods；HTTP/gRPC/CLI 只是 deployment adapter，不進入 core package：

```go
InitStorage(context.Context, InitStorageRequest) error
CreateTopic(context.Context, CreateTopicRequest) error
CreateGroup(context.Context, CreateGroupRequest) error
DeleteGroup(context.Context, DeleteGroupRequest) error
AcknowledgeRecords(context.Context, AcknowledgeRequest) error
ForceRetireLane(context.Context, ForceRetireLaneRequest) error
```

### 7.1 Publish result

`nil error`

- record 與 durable frontier 已 sync；
- `Receipt` 有效。

`RejectedBeforeWrite`

- validation、lifecycle、queue capacity 或 projected free-space check 在任何 record byte 寫入前拒絕；
- caller 可安全使用同一 `message_id` retry。

`DurabilityUnknown`

- write/sync 已開始，或 context 在開始寫入後取消；
- record 可能在 recovery 後出現；
- caller retry 必須沿用完全相同的 `message_id` 與內容。

不得把 context cancellation 一律映射成 before-write rejection。Batcher 必須記錄每個 request 是否已越過 first-write boundary。

### 7.2 Handler error

- 一般 error 預設為 transient，使用 bounded exponential backoff + jitter；
- product 可使用 `localmq.Permanent(err)` 明確分類 poison/permanent error；
- handler timeout 視為 transient，但 operation 可能在 downstream 繼續，因此 consumer 仍必須 idempotent；
- 每次 handler attempt 前及 retry wake-up 後都必須重讀 group control；
- 一條 lane 的 backoff 不得阻塞其他 lanes。

---

## 8. 核心資料模型

### 8.1 Topic

Topic manifest 建立後 immutable：

```text
format_version
topic
max_record_bytes
created_at_unix_nano
checksum
```

Topic control 可由 Maintainer atomic replace：

```text
format_version
retention_time_seconds
updated_at_unix_nano
checksum
```

Topic name 必須符合 `[a-z0-9][a-z0-9._-]{0,127}`。Topic 只能由 `CreateTopic` 建立，Publish 不得自動建立或採用 process-local default。

### 8.2 Lane

一條 lane 是 `(topic, lane_id)` 的單一 ordered WAL stream。每個 process 對每個實際 Publish 的 Topic lazy 建立一條 UUIDv7 lane。

Lane manifest：

```text
format_version
storage_id
topic
lane_id
producer_instance_id
writer_location        // Kubernetes 使用 pod UID
application_version
created_at_unix_nano
checksum
```

Retired marker：

```text
format_version
topic
lane_id
final_next_sequence
retired_at_unix_nano
retire_reason           // graceful | externally_fenced_recovery
retire_request_id      // force-retire request identity when externally fenced
checksum
```

Retired marker 只能 exclusive publish；存在後該 lane 不得再 append。

Active durable frontier：

```text
format_version
storage_id
topic
lane_id
active_segment_base_sequence
next_sequence
durable_byte_end
updated_at_unix_nano
checksum
```

Reader 必須逐一驗證 identity、active segment base、byte boundary 與 next sequence。`durable-end` 只能在 active segment sync 成功後前進；identity 不符或 floor/durable frontier 倒退時 block lane。

### 8.3 Record

Persisted record 必須包含：

```text
record_length
record_sequence
append_time_unix_nano
message_id_length + message_id
event_type_length + event_type
schema_version
payload
crc32c
```

- `record_sequence` 在 lane 內由 0 連續增加；
- `append_time` 由 writer 產生，只用於 retention/lag/inspection，不提供跨 lane ordering；
- `message_id` 在同 Topic 內代表 logical message identity；
- decoder 必須先驗證所有長度、上限與整數溢位，再配置 memory；
- WAL 使用簡單 versioned binary framing；metadata 使用 length-bounded versioned binary encoding。V1 不需要把 encoding library 暴露成 public API。

### 8.4 Consumer Group

Manifest immutable：

```text
format_version
topic
group
group_type              // PROTECTED | BEST_EFFORT
initial_position        // EARLIEST | LATEST
created_at_unix_nano
checksum
```

Control：

```text
format_version
state                   // INITIALIZING | ACTIVE | DELETING | DELETED
updated_at_unix_nano
deleted_at_unix_nano    // DELETED only
checksum
```

V1 沒有 generation，因為不提供 arbitrary reset。同名 DELETED group 不可重建，避免舊 consumer incarnation 誤加入新 group。

### 8.5 Checkpoint

```text
(topic, group, lane_id) -> next_sequence
```

`next_sequence=N` 表示 `< N` 都已由該 Group 的 handler durable 完成。

每個 consumer incarnation 只更新自己的 checkpoint files：

```text
members/<consumer_instance_id>/<lane_id>.checkpoint
```

單一 checkpoint file 使用 temporary file、file sync、atomic rename、directory sync 發布，且同一 instance/lane 內只能單調前進。

Group 另有 `checkpoint-base` snapshot。有效值為：

```text
effective_checkpoint(lane) =
    max(checkpoint_base[lane], all valid member checkpoint files)
```

Dual consumer、stale member 或 compaction race 即使遺失較新的 member file，也只會回到較低 baseline 造成 replay；不得製造高於已成功 handler watermark 的值。

### 8.6 Retention index

每條 lane 保存：

```text
format_version
topic
lane_id
earliest_retained_sequence
updated_at_unix_nano
checksum
```

Floor 只能增加。刪除 sealed prefix 後：

- 尚有 retained `.wal` 或 `.open`：floor 等於最舊 retained segment 的 base sequence；
- retired 且已無 segment：floor 等於 retired marker 的 `final_next_sequence`。

不得把 active `.open` 的 durable end 當成 floor，否則會跳過仍 retained 的 records。

---

## 9. Storage layout

所有 lane data 必須位於同一 Topic subtree，讓 staging rename 真正可執行：

```text
<root>/v1/
    storage-control
    maintenance.lock
    topics/<topic>/
        topic-manifest
        topic-control
        .gc-staging/
        lanes/<lane-id>/
            manifest
            retired                 // retired 後存在
            durable-end             // active 時存在
            retention-index
            segments/
                <base-sequence>.wal
                <base-sequence>.open
    groups/<topic>/<group>/
        manifest
        control
        checkpoint-base
        blocked/
            <lane-id>/
                <sequence>-<consumer-instance-id>.blocked
        members/<consumer-instance-id>/
            <lane-id>.checkpoint
```

這個 layout 取代原本將 lane metadata 與 Topic stream data 分散在不同 subtree 的做法。V1 whole-lane GC 只需把 `topics/<topic>/lanes/<lane-id>` rename 到相同 Topic 的 `.gc-staging`。

所有 replace/publication 固定使用：

```text
write temporary file in same directory
→ sync temporary file
→ atomic rename
→ sync parent directory
```

不得跨 filesystem rename，也不得使用 copy+delete 冒充 atomic publication。

---

## 10. Producer 與 WAL protocol

### 10.1 Bounded admission

每條 lane 有 bounded request queue。Queue full 時 `Publish` 等待 caller context；context 結束且尚未開始 write 時回 `RejectedBeforeWrite`。不得建立 unbounded goroutine 或 slice 吸收 burst。

Topic 的第一個 concurrent Publish 只能由一個 process-local initializer 建立 lane。建立流程使用同目錄 temporary directory，完整寫入並 sync manifest、retention-index、第一個 `.open` header 與 sequence 0 的 durable-end，再 atomic rename 為 `<lane-id>` 並 sync `lanes/` parent。Consumer/Maintainer 忽略 temporary directory；crash 殘留由 externally fenced maintenance cleanup。

### 10.2 Group commit

Batch 達到任一條件後 flush：

```text
group_commit_max_messages
group_commit_max_bytes
group_commit_max_wait
```

流程固定為：

```text
validate all records
→ encode complete records
→ append to one active segment
→ fdatasync/fsync segment
→ atomic publish durable-end
→ sync lane directory
→ return success for included requests
```

一條 lane 只有一個 active segment與 durable frontier，因此一次 group commit 不會因 Topic partition 數量增加 metadata publications。

### 10.3 Segment rotation

依 `segment_max_bytes` 或 active segment 的 creation time 加上
`segment_max_age` rotation。空 `.open` 不 seal；idle partial segment 可延後
到下一次 Publish 或 graceful stop，不提供固定期限刪除保證。

```text
flush all accepted records
→ append footer and sync
→ rename .open to .wal
→ sync segments directory
→ create/sync next .open header
→ publish durable-end for new .open
```

Footer 至少包含 next sequence、record count、append-time range、data CRC 與固定長度 trailer。Trailer 必須能由檔尾定位 footer，且 footer 宣告檔案長度必須與實際長度完全一致。

### 10.4 Graceful stop

```text
stop new admission
→ drain already admitted requests within Stop context
→ flush
→ seal non-empty active segment
→ remove empty .open
→ exclusive publish retired marker
```

Stop context 到期時，不得宣稱尚在 write path 的 Publish 未寫入；相關 call 回 `DurabilityUnknown`。

---

## 11. Consumer protocol

### 11.1 Membership 與 ownership

Redis ZSET member 為每次 process incarnation 的 UUIDv7，timestamp 必須由 Redis `TIME` 產生。建議預設：

```text
heartbeat_interval = 3s
membership_timeout = 15s
reconcile_interval = 5s
```

所有 active members 對每個 `lane_id` 計算 deterministic Rendezvous owner。Redis 回傳順序不得影響結果。

若距離上次成功 heartbeat 已超過 `membership_timeout`，instance 必須停止開始新 handler attempt、drain/abandon local batches，並重新加入；不得繼續把舊 ownership 當成有效。

正常 shutdown：

```text
stop new handler attempts
→ finish or cancel in-flight attempts
→ publish pending checkpoints
→ ZREM self
→ stop
```

### 11.2 Starting position

Group create 對當下 lanes 建立 `checkpoint-base`：

- `EARLIEST`：使用每條 lane 的 retention floor；
- `LATEST`：使用每條 lane 的 durable end。

Group ACTIVE 後首次發現缺少 coordinate 的新 lane，一律由該 lane 當下 retention floor 開始，並先 durable checkpoint 才 delivery。這是 conservative no-skip policy；`LATEST` 不會使未來新 lane 的既有 records 被跳過。

### 11.3 Delivery 與 checkpoint batching

同一 batch 只包含同一 lane 的連續 records。`worker_concurrency` 限制同時
執行中的 handler steps，不限制 owned lane 數量；所有 owned lanes 由 bounded、
round-robin scheduler 公平取得執行機會。同一 lane 同時最多一個 in-flight step。
Batch 因 `batch_max_messages` 或 `batch_max_bytes` 截止時，只回傳目前已取得的
contiguous prefix，不得跳過超限 record 再加入後續 record。

Handler success 後先推進 memory watermark。達到任一條件才 durable publish member checkpoint：

```text
checkpoint_max_records
checkpoint_max_delay
worker drain/stop
permanent error 前的成功 prefix
```

Checkpoint 尚未發布前允許處理後續 batch，因為 crash 只會 replay 已成功但尚未
checkpoint 的 records。Checkpoint publication failure 後，該 lane 必須設定
flush gate；在相同 memory watermark durable 成功前，不得開始下一個 handler，
只能以 bounded backoff retry 或停止該 lane，避免 replay window 無界增長。

### 11.4 Permanent error

當 batch 回 permanent error：

1. 以 binary split 重試，找出最早 permanent record；
2. 每次 split retry 仍可能重複 side effect，因此 idempotency contract 不變；
3. durable checkpoint poison record 之前的成功 prefix；
4. atomic publish blocked record；
5. 停止該 lane，其他 lanes 繼續；
6. 告警並等待管理者處置。

相同 `message_id` 但不同 payload/content hash 由 consumer integration 視為 permanent invariant violation。

Blocked marker 是 immutable、unique file，不以單一 shared file互相覆寫。Worker 計算 blocked 狀態時，只採用 `sequence >= effective_checkpoint` 的最小有效 marker。若另一個 dual owner 已成功處理並把 effective checkpoint 推過該 sequence，該 marker自動成為 stale，由 Maintainer 清除，不再阻止 lane。

---

## 12. Checkpoint baseline compaction

Member checkpoint 數量隨 consumer restart 增長，但不需要 checkpoint WAL。

Maintainer 定期：

```text
read current checkpoint-base
→ read all valid member checkpoint files
→ per lane take max
→ atomic publish new checkpoint-base
→ re-read ACTIVE Redis members
→ delete inactive member directories
```

Compaction 與 member update 的 race 最多遺失尚未納入 baseline 的較高 checkpoint，結果是 duplicate replay；不能產生較高的虛假 checkpoint。Active member directory 不主動刪除。

Inactive member directory 只在新 baseline durable publish 後刪除；V1 不提供
額外的 inactive-member grace 設定。刪除失敗留待下一次 compaction，不影響 durable
checkpoint correctness。

Whole-lane GC 完成後，Maintainer 下一次 compaction 必須從 baseline 移除已不存在 lane 的 coordinate，並清除 inactive members 對該 lane 的 checkpoint。Active stale member 後續重建已刪 lane checkpoint時，reader忽略不存在的 lane，cleanup留待下一輪；不得因此重新建立 WAL lane。

Snapshot decoder 必須限制 lanes 數與 file size。超過設定上限時停止 compaction並告警，不可無界配置 memory。

---

## 13. Node、process 與 storage failure

### 13.1 Process/pod crash

- 新 process 建立新 lane，不沿用舊 lane ID；
- consumer 可讀舊 `.open` 到 durable-end；
- incomplete tail 不 delivery；
- 未 checkpoint 的成功 handler records replay；
- 舊 lane 標記為 orphan，等待 external fencing 後 retire。

### 13.2 Node freeze/network partition

只有 advisory lock 可取得不足以 retire lane。當舊 node 可能恢復時：

- 不修改舊 lane；
- replacement producer 使用新 lane；
- 舊 writer若恢復且 lane 尚未 retired，可繼續自己的 lane；
- 若 infrastructure 已啟動 replacement，business layer 仍須避免兩個 service incarnation 同時接受相同 request，或以相同 message ID deduplicate；
- 只有確認舊 node/process 已 fenced，才能 `ForceRetireLane`。

這項策略避免 recovery 與復活 writer 同時 append/seal 同一 inode。

### 13.3 ForceRetireLane

必要參數：

```text
lane_id
fencing_evidence
operator
reason
request_id
```

流程：

```text
confirm external fencing and durable frontier
→ accept the valid rotation namespaces (.open/.wal and header-only next .open)
→ scan valid prefix only to durable-end
→ truncate bytes after durable-end/incomplete tail
→ seal non-empty active segment
→ publish retired marker(final_next_sequence) exclusively
→ remove durable-end
```

若 matching `.open` 已在 rotation 中被 rename 為 `.wal`，不得要求 frontier
先被改寫；若只留下 frontier 指向的 `.wal` 與下一個只有完整 header 的
`.open`，兩者也屬合法 crash intermediate state。其他 unmatched active
namespace、identity、sequence 或 checksum 無法驗證時一律 fail closed。已存在
的 retired marker 必須核對 final sequence 與 force-retire request identity；
fencing evidence 只保存 hash。

操作必須 idempotent並寫 structured audit log。Package 無法自行驗證 cloud VM/node fencing；這是 deployment integration responsibility。

### 13.4 Redis outage

- Producer 不受影響；
- consumer 在最後成功 membership lease 內只 drain 已開始 attempt；
- timeout 後停止；
- Redis 恢復後重新註冊、重新 assignment、由 durable checkpoint 繼續；
- Redis keys 全失不得重建 Group 或重新套用 Initial Position。

### 13.5 Storage outage

- 新 Publish 停止或回 typed error；
- write 已開始者只能回 `DurabilityUnknown`；
- consumer 不開始無法保證 checkpoint capacity 的新 handler；
- kernel hard-mount I/O 可能不可立即取消，V1 不承諾 storage partition 時的 bounded syscall latency；
- watchdog 必須使 readiness 失敗並告警，但不得用 process restart 冒充 I/O cancellation。

---

## 14. Retention 與過期資料移除

### 14.1 Eligibility

只有 sealed `.wal` 可以刪除。Segment 必須同時符合：

```text
所有 ACTIVE PROTECTED groups 的 effective checkpoint >= segment.next_sequence

AND

(segment 已超過 retention_time
 OR filesystem 進入 producer pressure)
```

INITIALIZING/DELETING、missing/corrupt control 或 missing checkpoint 的 PROTECTED group 一律是 barrier。DELETED group 不再參與。

BEST_EFFORT 不阻止 GC。其 checkpoint 落後 retention floor 時，worker durable 將 checkpoint clamp 到 floor、記錄 skipped messages/bytes metric，再繼續；不要求人工 Reset。

### 14.2 Time semantics

Writer lane 內的 `append_time` 必須 clamp 為 non-decreasing：

```text
append_time = max(system_wall_clock, previous_append_time)
```

部署必須維持明確的最大 clock skew。Time eligibility 使用：

```text
maintainer_now >= segment.max_append_time + retention_time + 2*max_clock_skew
```

若 clock health 無法符合 deployment contract，暫停 time-based GC 並告警；pressure GC 仍依 group barriers運作。Retention 表示正常情況的目標保留時間，不提供 secure erase 或 no-later-than deletion guarantee。

### 14.3 Crash-safe deletion

Time GC 每條 lane 一次只刪除最舊 contiguous sealed prefix：

```text
Maintenance Controller serialize operation
→ 重新讀 Topic/Group controls 與 effective checkpoints
→ 選定 prefix
→ atomic publish higher retention-index floor
→ rename segments to Topic-local .gc-staging
→ sync directories
→ asynchronous unlink staging contents
```

Pressure GC 使用相同的 barrier 與 floor publication protocol，但每條 lane
每輪只提出一個最舊 eligible sealed segment，依 footer 的 `max_append_time`
建立跨 Topic/lane min-heap，一次只 commit 全域最舊 candidate。每次 unlink
後重新檢查 filesystem free bytes；回到 `producer_stop_free_bytes` 立即停止。
沒有 eligible candidate 時回報 capacity-blocked，不無限重試。

Floor publication 是 logical deletion boundary。之後即使 staging files 仍存在，reader 也不得讀取 floor 以下 records。

Retired lane 的所有 segments 都已 logical deleted且沒有 blocked/admin reference 後，可把整個 lane directory rename 到 Topic-local staging，再刪除。因 lane data 全在同一 subtree，此 rename 是可實作的。

### 14.4 Reader 與 concurrent GC

Reader 遇到 `ENOENT`/`ESTALE`：

1. 重讀 retention floor；
2. 重列 lane/segment；
3. 若 effective checkpoint 或 floor 已越過 local position，由新的有效位置繼續；
4. 只有 metadata 無法驗證、floor 倒退，或 ACTIVE PROTECTED group 的 effective checkpoint 仍低於 floor 時才回 correctness incident。

---

## 15. Capacity 與 backpressure

Root storage control：

```text
format_version
storage_id
producer_stop_free_bytes
recovery_reserve_bytes
max_clock_skew_seconds
updated_at_unix_nano
checksum
```

必須滿足：

```text
producer_stop_free_bytes > recovery_reserve_bytes > 0
```

### 15.1 水位行為

`NORMAL`

- producer、consumer、checkpoint、maintenance 正常。

`PRODUCER_STOP`

- projected free space 低於 `producer_stop_free_bytes` 時，新 Publish 在 write 前拒絕；
- consumer、checkpoint、seal、recovery與 GC 繼續。

`RECOVERY_ONLY`

- projected free space 低於 `recovery_reserve_bytes` 時，不開始新 handler；
- 只允許完成已開始的 bounded checkpoint、seal、retention-index publication 與 unlink；
- 若已完成 handler 的 checkpoint 仍失敗，該 lane 停止並於恢復後 replay。

### 15.2 Sizing

```text
recovery_reserve_bytes >=
    2 * worst_case(
        all in-flight checkpoint publications
        + all segment seals
        + retention metadata/staging operations
        + storage backend safety margin)

producer_stop_free_bytes >=
    recovery_reserve_bytes
    + worst_case(all producer group commits already admitted)
```

不能算出 bounded upper limit 的 deployment 不得宣稱有 hard ENOSPC protection。

V1 不實作 per-Topic hard quota；filesystem pressure 依「最舊且可刪除的 segment」跨 Topic處理。Noisy-neighbor isolation 是已知限制，只有實際需求出現後才增加 per-Topic quota/accounting。

---

## 16. Group 與 blocked-data 管理

### 16.1 CreateGroup

由 Maintenance Controller serialized 執行：

```text
exclusive create group directory
→ publish immutable manifest
→ publish INITIALIZING control
→ enumerate current lanes/floors/durable ends
→ publish checkpoint-base
→ re-read floors and clamp if GC advanced
→ publish ACTIVE control
```

同時建立同名 Group 只有一個 winner。Loser 驗證 manifest 相同後回 already exists；參數不同則回 conflict。

### 16.2 DeleteGroup

```text
publish DELETING
→ workers在下一次 attempt/retry boundary 停止 heartbeat
→ 等待 membership empty 一個 membership_timeout
→ publish DELETED tombstone
```

Redis 無法驗證 empty 時不得完成。只有 durable DELETED 才解除 PROTECTED barrier。

### 16.3 AcknowledgeRecords

只用於人工處理 poison/block：

```text
topic, group, lane_id, up_to_sequence
operator, reason, request_id
```

Maintainer 必須驗證：

- Group ACTIVE；
- 以 `effective_checkpoint` 找到目前 coordinate 之後的最早有效 blocked marker；
- target 不小於 blocked sequence + 1，且不大於 durable end；
- request ID 重試 idempotent；
- 在 current `checkpoint-base`、effective checkpoint 與 target 中取最大值，先 durable發布新 baseline，再移除低於新 baseline 的 blocked markers；
- audit log 不得包含 payload，只包含 message ID、sequence 與 hash。

若 baseline 已發布後 crash、blocked marker 尚未移除，重試會由同一 request ID讀到 baseline 已越過 target，安全完成 marker cleanup並回 success。

這是明確資料處置，不是自動 DLQ/skip。

---

## 17. Consumer idempotency contract

MQ core 不做 deduplication。每個接入 consumer 必須通過以下 contract tests：

1. 相同 `(topic, message_id)` sequential duplicate 不重複產生不可逆 side effect；
2. 相同 message concurrent duplicate 結果與執行一次相同；
3. timeout 後原 attempt 延遲完成，再次 delivery 仍安全；
4. 不同 lanes 的舊事件晚到時，不會覆蓋較新 business version；
5. deduplication state 保存時間至少涵蓋最大 retained/replay window；
6. 相同 ID、不同 content hash 被視為 invariant violation，而非任選一筆覆蓋。

Handler 必須遵守傳入 context；若第三方 SDK 無法取消，integration layer 必須限制其 concurrency與resource lifetime。MQ 的 timeout只能停止等待和阻止新 attempt，不能強制終止任意 Go goroutine或撤銷已送出的 downstream operation。

若 consumer 無法滿足，該資料流不得使用本 MQ；不得以降低 duplicate 機率取代 correctness contract。

---

## 18. Configuration

Runtime config 沿用 `config.SourceSnapshot` strict binding。建議欄位：

```yaml
local_mq:
  root_path: /var/lib/gaming-core/local-mq
  storage_id: "019..."

  publish_queue_capacity: 4096
  group_commit_max_messages: 256
  group_commit_max_bytes: 1048576 # bytes
  group_commit_max_wait: 2ms

  segment_max_bytes: 134217728 # bytes
  segment_max_age: 1h
  producer_stop_free_bytes: 1073741824 # bytes
  recovery_reserve_bytes: 536870912 # bytes
  max_clock_skew: 1s

  consumer:
    heartbeat_interval: 3s
    membership_timeout: 15s
    reconcile_interval: 5s
    worker_concurrency: 8
    batch_max_messages: 500
    batch_max_bytes: 4194304 # bytes
    batch_max_wait: 10ms
    handler_timeout: 30s
    checkpoint_max_records: 5000
    checkpoint_max_delay: 100ms
```

數值只是開發預設，不是 production sizing。Durable Topic/Group/root controls 不得由不同 pod 的 local YAML 靜默覆寫。

`Client` 維持 lazy framework-managed resource；沒有 product 依賴時不建立 lane、不 mount-check、不註冊 collectors。Consumer由 product module 建構為 `PhaseService` managed resource，handler 保持 business-owned。

---

## 19. Observability 與開發者體驗

### 19.1 Metrics

至少提供：

```text
localmq_publish_total{topic,result}
localmq_publish_latency_seconds{topic}
localmq_publish_queue_depth{topic}
localmq_group_commit_messages{topic}
localmq_group_commit_bytes{topic}
localmq_wal_bytes{topic}
localmq_filesystem_free_bytes
localmq_consumer_lag_messages{topic,group}
localmq_oldest_unconsumed_age_seconds{topic,group}
localmq_handler_attempt_total{topic,group,result}
localmq_blocked_lanes{topic,group}
localmq_rebalance_total{topic,group}
localmq_checkpoint_publish_failures_total{topic,group}
localmq_gc_bytes_total{topic,reason}
```

不得使用 `message_id`、`lane_id` 或 error text 作 Prometheus label。

### 19.2 Inspection

```text
InspectStorage()
InspectTopic(topic, limit)
InspectLane(topic, lane_id)
InspectGroup(topic, group, limit)
InspectBlocked(topic, group, limit)
```

所有輸出有 filter、limit 與 payload redaction。CLI/API error 必須包含可定位的 Topic/group/lane/sequence，但不得輸出 payload 或 secrets。

### 19.3 Documentation

公開專案至少附：

- package example：Publish、Consumer handler、graceful lifecycle；
- storage prerequisites 與 conformance 操作；
- capacity sizing worksheet；
- duplicate/idempotency integration guide；
- poison record runbook；
- node fencing/ForceRetireLane runbook；
- format compatibility policy與 upgrade guide。

---

## 20. Error handling

錯誤分為：

| 類別 | 行為 |
|---|---|
| Validation/config | fail fast；未寫入 |
| Admission/backpressure | `RejectedBeforeWrite`；可 retry |
| Write/sync ambiguous | `DurabilityUnknown`；writer failed；同 ID retry |
| WAL tail incomplete | 只讀 durable frontier；fenced recovery 後 truncate |
| Sealed corruption | block lane、告警；不得 skip/猜測修復 |
| Transient handler | bounded backoff + jitter |
| Permanent handler | bisect、checkpoint prefix、durable block |
| Checkpoint failure | 停止該 lane後續 handler，retry checkpoint |
| Redis failure | lease 內 drain，timeout 後停止 consumer |
| Metadata mismatch | fail closed 對應 Topic/Group/Lane |
| ENOENT/ESTALE during GC | 重讀 floor與目錄後分類，不立即判 corruption |

Background goroutine 的 terminal error 必須傳回 owner，使 readiness 失敗並觸發 structured log/metric；不得只 log 後永久退出。

---

## 21. Storage prerequisites 與 conformance

每個 StorageClass/CSI/mount-option 組合上線前必須在兩個不同 nodes 測試：

1. same-directory rename 的 reader 只看到完整舊版或新版；
2. file sync + directory sync 後，kill/remount/failover 仍保留完整檔案；
3. active `.open` append 與 durable-end replace 的跨 node可見性及 p99 latency；
4. concurrent create 只有一個 winner；
5. unlink/rename 後 reader 的 `ENOENT`/`ESTALE` 行為符合 reader retry protocol；
6. node network partition、SIGSTOP 超過 lock lease、恢復後不執行自動 lane recovery；
7. hard mount outage/recovery 不造成已回 success record 遺失；
8. 實際 allocated bytes 與 `statfs` 水位行為可被可靠觀測。

RWX access mode 本身不代表上述語意成立。任一必要測試失敗，該 backend 不受支援。

---

## 22. 測試策略

### 22.1 Unit/contract tests

- config defaults、strict unknown fields、cross-field bounds；
- record/header/footer/metadata round trip；
- truncated input、CRC、oversized length、integer overflow fuzzing；
- Publish 三態與 cancellation boundary；
- group commit batching與 bounded queue；
- sequence continuity、rotation與 durable frontier；
- checkpoint effective max及 regression只造成 replay；
- handler transient/permanent/timeout分類；
- retention eligibility與 floor monotonicity；
- capacity state machine；
- lifecycle concurrent Start/Stop 與 idempotent Stop；
- metrics registration不得 panic、不得高 cardinality。

### 22.2 Deterministic fault injection

Filesystem abstraction 只暴露實際需要的 primitives，測試可在每個步驟注入 error/crash：

```text
record append
WAL sync
durable-end temp sync
durable-end rename
directory sync
footer append/sync
segment rename
checkpoint replace
retention-index replace
staging rename/unlink
group control publication
```

每個 crash point restart 後都驗證 invariants，而非只驗證 API error。

### 22.3 Concurrency tests

- 多 goroutine concurrent Publish；
- dual consumers 同時處理/commit同一 lane；
- membership churn 與 rolling shutdown；
- checkpoint compaction 與 member update；
- Create/Delete Group 與 GC serialization；
- reader 與 segment staging deletion；
- `go test -race ./...`。

### 22.4 Integration/e2e

- real Redis restart/failover/key loss；miniredis 只做不依賴 server-time/replication 的 unit tests；
- real target RWX PV two-node conformance；
- SIGKILL producer at every durability boundary；
- SIGSTOP/network partition與 externally fenced ForceRetire；
- downstream outage、recovery與 backlog catch-up；
- ENOSPC/quota fault、producer stop後 consumer+GC 自動恢復；
- poison record acknowledge audit path。

### 22.5 Benchmark gate

至少輸出：

```text
Publish throughput and p50/p95/p99
group commit distribution
network bytes and storage operations/message
consumer throughput
checkpoint write rate
backlog catch-up rate
WAL bytes/message
lane count對 scan、memory、metadata latency 的影響
```

Production gate：correctness tests 全數通過，且目標 topology 的 sustained throughput 至少為需求 peak 的 1.5 倍。絕對 latency、outage與 catch-up門檻在 sizing input 補齊後加入，不以本機 benchmark 外推 production capacity。

---

## 23. 安全性

- WAL payload 可能含敏感日誌；依 StorageClass 提供 at-rest encryption，傳輸使用 backend 支援的 encryption；
- mount path 只授權 producer、consumer與 maintainer service accounts；
- admin mutation 必須驗證 operator identity並記 audit；
- inspection 預設不輸出 payload；
- metadata decoder 對 hostile/corrupt bytes 做 size bounds，避免 OOM；
- V1 不宣稱 cryptographic tamper evidence；CRC32C 只偵測 accidental corruption。

---

## 24. 關鍵取捨

### 24.1 Lane ownership 取代 partition ownership

減少 active files、fsync與 checkpoint cardinality；代價是 consumer parallelism受 lane數限制。這符合 burst logging V1，並保留未來 writer sharding方向。

### 24.2 Checkpoint 可保守倒退

Per-consumer checkpoint + baseline max 不要求 distributed CAS。極端 compaction/dual-owner race可造成 duplicate replay，但在 at-least-once contract 下安全，並顯著簡化 checkpoint protocol。

### 24.3 Ambiguous node failure 不追求自動 recovery

舊 lane保持可讀，新 writer使用新 lane；代價是 orphan lane需 external fencing後清理。這避免 generic advisory lock無法解決的 stale writer corruption。

### 24.4 Single maintenance writer

GC/admin暫停不影響 durable append和既有 checkpoint；代價是 maintenance failover需 external fencing。對 V1 而言，比自製 distributed fencing protocol更符合必要複雜度。

### 24.5 不做 producer dedup

`DurabilityUnknown` retry可能形成 duplicate。由 consumer idempotency處理，避免 writer hot path增加 global index與跨 lane coordination。

---

## 25. 已知限制與擴充方向

已知限制：

- 不保證跨 lane ordering；
- consumer parallelism 受 `worker_concurrency` 與可用 lane 數量限制；
- maintainer/node ambiguous failure需要 external fencing；
- RWX filesystem latency和metadata capacity可能成為瓶頸；
- PROTECTED poison/backlog可阻止資料回收並最終 backpressure producer；
- V1 沒有任意 offset reset或 timestamp replay；
- 沒有 application-level replication，failure domain等同 storage backend；
- 沒有 per-Topic capacity isolation；
- storage hard-mount I/O outage可能造成不可立即取消的 syscall。

只有實際需求或 benchmark證據出現後，才依序考慮：

1. 每 process/topic 多 writer shards；
2. per-Topic quota與容量隔離；
3. generation-based reset/replay；
4. maintenance leader的 linearizable fencing service；
5. checkpoint更低 metadata write amplification；
6. remote replication或改用成熟 broker。

---

## 26. 對原設計的具體修改清單

| 原設計 | V1 必要修改 |
|---|---|
| §3 per-instance lane 跨 Topic/partition散布 | 改為每 process/topic一條完整 lane subtree |
| §5 Partition | V1 移除；ownership改為 lane |
| §6 `(topic,partition,lane)` ordering | 改為 `(topic,lane)` sequence |
| §8 per-stream durable-end | 一條 lane一個 active segment/durable-end |
| §10–11 partition membership/ownership | Redis membership保留，Rendezvous target改為 lane ID |
| §13 partition worker | 改為 lane worker；不同 lanes並行 |
| §14 checkpoint WAL/segments | 改為 per-member atomic checkpoint + group baseline max |
| §14.3 generation | 移除；V1不提供 arbitrary reset |
| §17–20 group create | 保留 EARLIEST/LATEST；missing lane一律由 floor開始 |
| §21 Reset/AT_TIMESTAMP/EXPLICIT | 延後至獨立需求/ADR |
| §21.1 Delete Group | 保留並簡化為無 generation lifecycle |
| §22 retention-index | 修正 active `.open` floor；BEST_EFFORT out-of-range自動 clamp |
| §22 filesystem reserve | 拆成 producer stop與 recovery reserve |
| §23 whole-lane GC | lane data移到同 Topic subtree後才允許 atomic staging |
| §25 advisory lock recovery | 不作 stale writer fencing；ambiguous node需 external fencing |
| §26 無 DLQ | 維持，但新增 durable block + audited AcknowledgeRecords |
| §28 驗收 | 以本文件 §21–22 fault/concurrency/benchmark gates取代 |

未列入本表且仍適用的原則，例如 CRC、bounded decode、fail closed、opaque payload、`DurabilityUnknown`、storage identity與 directory sync，繼續保留。

---

## 27. 最終需求覆蓋與過度設計審查

| 需求 | V1 對應 | 結論 |
|---|---|---|
| Burst 不累積在 memory | bounded Publish queue + direct WAL | 覆蓋 |
| Durable acceptance | group commit + durable-end | 覆蓋 |
| 無 broker hot path | embedded Publisher + RWX PV | 覆蓋 |
| Async batch persistence | lane Consumer + bounded batch | 覆蓋 |
| At-least-once | handler-success watermark + durable checkpoint | 覆蓋 |
| Consumer idempotency | 明確 integration contract/tests | 覆蓋 |
| Concurrent consumers | Redis assignment + dual-owner-safe checkpoint max | 覆蓋 |
| Pod/process restart | new lane + durable frontier/checkpoint | 覆蓋 |
| Node短暫不正常 | 不搶救 ambiguous lane；external fencing後 retire | 覆蓋且保守 |
| Redis/downstream outage | consumer停、WAL累積、恢復續跑 | 覆蓋 |
| 過期資料移除 | monotonic floor + staging GC | 覆蓋 |
| WAL滿時可恢復 | 兩階段 reserve + pressure GC | 覆蓋 |
| Poison record | durable block + audited acknowledge | 覆蓋 |
| 多資料類型/多下游 | Topic + Consumer Group | 覆蓋 |

本設計沒有加入需求以外的 delivery guarantees、replication、ordering、DLQ、priority、transaction或通用 replay。相較原文件，移除 partition matrix、checkpoint WAL/generation及自動 stale-lane recovery，降低 filesystem metadata amplification與併發 state space。

結論：本文件的元件都直接服務已確認需求或 production safety/operation；未發現仍可移除而不破壞需求的主要元件。實作前剩餘輸入只有 throughput、latency、outage、PV capacity與目標 StorageClass，這些影響 sizing與go/no-go，不改變本文件的核心架構。
