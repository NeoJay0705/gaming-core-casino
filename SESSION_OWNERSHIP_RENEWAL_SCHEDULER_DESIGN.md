# Session ownership Redis renewal scheduler 設計

## 1. 文件目的

本文件定義 Gate player session ownership lease 的必要修改：修正現有正常 renewal interval 被 retry
上限截短的問題，並以 process-scoped、單層 bucket scheduler 取代每個 session 各自持有 goroutine 與
timer 的做法。到期 bucket 內的 lease 使用 Redis pipeline 續租，並提供能直接對應維運措施的
Prometheus metrics。

設計基準為每個 Gate process 約 10,000 個同時在線 player，預設 ownership lease TTL 為 5 分鐘。
文件同時作為後續實作依據；除本文件明列的修改外，不改變既有登入、重複登入 fencing、斷線釋放、
Gate endpoint registration、broadcast 或 SendToPlayer 行為。

## 2. 需求理解與合理假設

### 2.1 必須完成

- `session_ownership` 預設且持續啟用，登入成功後仍以 Redis lease 表示目前 authoritative Gate。
- 同一 Gate 約可有 10,000 個 active ownership leases。
- 正常 renewal frequency 與錯誤 retry frequency 必須分離；5 分鐘 TTL 的預設正常 renew interval
  應為 100 秒，而不是目前實際可能發生的 5 秒。
- 每個 Gate 只使用一個 renewal scheduler loop，不再為每個 session 建立 renewal goroutine 與 timer。
- scheduler 將一個 interval 分成固定數量 buckets，每個 tick 只處理到期 bucket。
- bucket 內的 single-key fenced renew commands 使用非 transactional Redis pipeline；必須逐筆判斷結果。
- pipeline 呼叫方式需同時適用目前支援的 standalone 與 Cluster client。
- metrics 必須能回答 renewal 是否失敗、排程是否落後、batch 是否逼近容量，以及 Redis pool 是否飽和。
- lifecycle start、stop、session replacement、disconnect 與 in-flight renewal 必須保持 race-safe。

### 2.2 合理假設

- 所有 session ownership lease 使用同一份 `session_ownership` config，因此可共用一個單層 wheel；本次
  不需要 hierarchical timing wheel。
- player login 時間可能集中，不能依賴登入時間自然分散；使用 opaque connection identity 的 stable hash
  將 lease 均勻分配到 buckets。
- `lease_ttl / 3` 留有足夠 failure retry safety margin；可設定 interval，但不得大於 `lease_ttl / 2`。
- go-redis ClusterClient 會依 key slot 將普通 pipeline commands 分組至不同 master；應用層不自行探索
  Redis topology 或按 node 分批。
- Redis renewal Lua script 只操作一個 player presence key，具備 fencing 且可安全重試。
- Sentinel 尚非目前 `pkg/infra/redis` 支援模式；本次不新增 Sentinel config 或 client construction。未來
  加入 Sentinel 時，scheduler 與 batch renewal contract 不需改變。

## 3. 範圍與非目標

### 3.1 範圍內

- session ownership renewal config 與 validation。
- `PresenceRegistry` 的 batch renewal pipeline。
- Gate process 的單層 renewal bucket scheduler。
- `SessionRegistry` 對 scheduler 的 register／remove 整合。
- framework lifecycle wiring。
- renewal scheduler 的 bounded metrics 與必要 structured logs。
- config、unit、contract、Redis integration 與 race tests。

### 3.2 明確非目標

- Gate gRPC endpoint Redis lease：每個 process 只有一筆，保留既有單一 ticker。
- Redis Pub/Sub subscriber reconnect：保留既有 exponential backoff，這不是 item refresh。
- Gate-to-Game DNS refresh：不是 Redis 行為，不使用本 scheduler。
- room membership、broadcast payload、SendToPlayer lookup 或任何 product business cache refresh。
- Redis Sentinel 支援。
- 多 key Lua script、Redis transaction、`TxPipeline` 或用 hash tag 將所有 player 集中到同一 slot。
- hierarchical timing wheel、通用 framework scheduler、動態自動調參或分散式 scheduler coordination。
- 第一版不提供 renewal worker pool、parallel pipeline 或 concurrency config；Cluster node concurrency 由
  go-redis 負責。
- ownership lost 後主動中斷 WebSocket。維持目前停止該 lease renewal並記錄事件的行為；若要改變產品
  行為需另案定義。

## 4. 現況與必要修正

目前 `products/gateproduct/session_registry.go` 對每個 presence 建立一個 goroutine，並在每輪建立 timer。
正常成功路徑計算如下：

```text
baseDelay = leaseTTL / 3
maxDelay  = min(leaseTTL / 2, 5s)
delay     = jitter(baseDelay, maxDelay)
```

`jitter` 又把結果限制在 `maxDelay`，因此範例 `lease_ttl: 5m` 的正常 delay 由預期的約 100 秒被截成 5 秒：

```text
現況：約 10,000 / 5s   = 2,000 renew commands/s
修正：約 10,000 / 100s =   100 renew commands/s
```

本次不只把函式參數改名，而是移除 per-session loop，由 scheduler 分別保存：

- `renewal.interval`：正常成功後的下一次 renew 時間。
- retry backoff：只在 transient Redis error 後使用，上限維持 5 秒。

兩者不得共用同一個 duration cap。

## 5. 架構與模組邊界

```text
products/gateproduct
  SessionRegistry
    ├─ login/replacement/disconnect 的 local session 狀態
    └─ Schedule／Remove ownership lease
             │
             ▼
  sessionPresenceRenewalScheduler
    ├─ 單一 lifecycle loop
    ├─ buckets、next due、retry state
    ├─ scheduler metrics
    └─ 呼叫 RenewMany
             │
             ▼
pkg/serversend
  GatePresenceRegistry
    └─ 綁定並驗證本 Gate identity
             │
             ▼
  PresenceRegistry.RenewMany
    ├─ single-key fenced Lua command
    ├─ non-transactional pipeline
    └─ 對齊輸入順序的逐筆結果
             │
             ▼
pkg/serversend/redisstore → pkg/infra/redis → go-redis
```

責任邊界：

- `pkg/infra/redis` 只管理 Redis client、pool、連線設定與 pool metrics，不知道 session 或 renewal policy。
- `pkg/serversend` 擁有 presence Redis schema、fencing Lua 與 single／batch Redis operations，不負責排程。
- `products/gateproduct` 擁有 session lifecycle 與 renewal scheduler，因為只有 Gate 知道何時登入、替換及
  斷線。
- 不建立通用 `pkg/timingwheel`。目前只有 session ownership 是高 cardinality periodic work；抽成通用
  framework API 會增加未被需求證明的抽象。

## 6. Config contract

在既有 config 下只增加必要的 renewal block：

```yaml
session_ownership:
  # 必填：Redis ownership lease TTL。
  lease_ttl: "5m"

  # renewal:
  #   # 非必要：正常續租間隔；預設 lease_ttl / 3，且不得大於 lease_ttl / 2。
  #   interval: "100s"

  #   # 非必要：一個 interval 內的分片數；預設 100。
  #   buckets: 100
```

資料模型可由 `serversend.PresenceConfig` 擴充：

```go
type PresenceConfig struct {
    LeaseTTL time.Duration         `config:"lease_ttl" yaml:"lease_ttl"`
    Renewal  PresenceRenewalConfig `config:"renewal" yaml:"renewal"`
}

type PresenceRenewalConfig struct {
    Interval time.Duration `config:"interval" yaml:"interval"`
    Buckets  int           `config:"buckets" yaml:"buckets"`
}
```

Normalization 與 validation：

| 欄位 | Default | 規則 |
|---|---:|---|
| `lease_ttl` | 無 | 必須大於 0 |
| `renewal.interval` | `lease_ttl / 3` | 必須大於 0，且不得大於 `lease_ttl / 2` |
| `renewal.buckets` | `100` | 必須大於 0 |
| derived tick | `interval / buckets` | 必須為正值且不得大於 5 秒；不符合時 startup fail fast |

`interval` 是 correctness／availability knob；`buckets` 是負載平滑 knob。不要再暴露 `tick_interval`，
避免三個互相依賴的參數形成不一致設定：

```text
tick interval = renewal interval / buckets
```

因為第一版只有一個以 derived tick 為週期的 scheduler loop，`interval / buckets` 不得大於 retry backoff
上限 5 秒；否則 retry due time 會被推遲到下一個 tick。此限制由 Gate scheduler constructor 驗證，不新增
公開 `tick_interval` 或 `retry_max` config。由於 bucket 排程會向上 rounding 一個 tick，transient retry 的
logical due time 還必須預留一個 tick；因此正常 ticker 運作下，實際預定 retry 不會超過 5 秒。GC pause、CPU
starvation 或 Redis operation blocking 造成的額外延遲，仍由 scheduler lag metric 反映。
處理 timing wheel 的目前 bucket 前，`nextTick` 會先推進到下一個尚未處理的 tick，確保跨 wheel rotation 的
early bucket requeue 使用一致的時間基準，不會額外延遲一格。

每批最多 256 commands 作為第一版內部 safety limit。超過時按 256 拆成多個 sequential pipelines。
不先暴露 `max_batch_size` config；若實際 metrics 證明 256 本身成為限制，再另案開放。

以 10,000 player、100 秒 interval、100 buckets 為例：

```text
tick interval       = 1s
average bucket size = 100 leases
average Redis rate  = 100 commands/s
pipeline calls      ≈ 1 call/s（Cluster 內部可能再按 node 拆分）
```

## 7. 核心資料模型

### 7.1 Batch renewal result

在 `pkg/serversend` 增加對齊輸入位置的結果：

```go
type PresenceRenewResult struct {
    Presence Presence
    Err      error
}

func (r *PresenceRegistry) RenewMany(
    ctx context.Context,
    presences []Presence,
) []PresenceRenewResult
```

使用 result slice 而非單一 error，原因是 Cluster、failover 或連線中斷可能造成 partial success。呼叫端
必須分別 reschedule success 與 failed lease。空輸入回傳空 slice，不執行 Redis I/O。

`GatePresenceRegistry.RenewMany` 先拒絕不屬於自身 GateID 的 lease，再把合法項目交給底層 registry；
結果仍與輸入一一對齊。

### 7.2 Scheduler entry

具體欄位名稱可於實作微調，但必須表達以下狀態：

```go
type renewalEntry struct {
    presence   serversend.Presence
    generation uint64
    nextDue    time.Time
    attempt    uint32
}
```

- active entries 以 `ConnectionID` 索引；presence 的 epoch／GateID 仍在 Schedule、Remove 與結果 commit 時
  比對。
- `generation` 防止已從 bucket 取出的 stale result 重新排入已被 remove／replace 的 lease。
- `nextDue` 使用含 monotonic component 的 `time.Time`，不可只依 ticker 次數推算正確性。
- bucket 只保存 entry identity／generation，不複製可變 session 或 WebSocket pointer。

### 7.3 Wheel

單層 wheel 保存固定數量 buckets、cursor 與 active entry map。正常 `Schedule` 使用 opaque connection
identity hash 分配第一個 bucket，使同時登入的 10,000 個 session 不會全部落在同一 tick。成功後的下一次
renew 約為一個完整 interval；實作可保留不超出一個 tick 的 rounding。

每次 tick：

1. 依 monotonic current time 推進 cursor。
2. 取出所有 `nextDue <= now` 的 entries；尚未 due 的 entry 留在／移至正確 bucket。
3. 釋放 scheduler lock。
4. 每 256 筆執行一次 `RenewMany`；Redis I/O 期間不得持有 scheduler 或 SessionRegistry lock。
5. 重新取得 scheduler lock，只有 identity 與 generation 仍一致的結果才可 commit。
6. success 清除 retry attempt 並排入下一個正常 interval。
7. transient error 使用 bounded exponential backoff 排入較近 bucket。
8. `ErrPresenceNotOwner` 永久移除該 entry，不再重試。

若 process 因 GC pause 或 Redis call 而錯過 tick，下一次 wake-up 必須依 `nextDue` 處理所有已到期
entries，不能把漏掉的 bucket 延後一整個 wheel revolution；同一 entry 在一次 wake-up 最多 renew 一次。

## 8. Redis pipeline contract

`PresenceRegistry.RenewMany` 使用既有 `renewPresenceScript`，每個 lease 各排一個 `EVAL`：

```text
Pipeline
  EVAL renewPresenceScript presence:alice ...
  EVAL renewPresenceScript presence:bob   ...
  EVAL renewPresenceScript presence:carol ...
```

規則：

- 使用普通 `Pipelined`，不用 `TxPipelined`。
- 每個 Lua command 只包含一個 key；不得建立跨 player key script。
- 先保存每個 `*redis.Cmd`，Exec 後逐筆讀取 result。
- result `1` 是 success；`0` 是 `ErrPresenceNotOwner`；其他值是 store contract error。
- command error 轉為既有 `ErrRouteStoreUnavailable` chain。
- pipeline overall error 只表示至少一個 operation 失敗，不能將所有 command 一律判為失敗；已有成功
  result 的 command 必須視為成功。
- 若 pipeline 在 dispatch 前整體失敗，無法取得個別結果的項目才共同使用 overall error。
- network response 遺失時可以重試，因為 fenced `PEXPIRE` 不會建立不存在的 key，也不會延長其他 epoch。

對不同 Redis deployment：

| 模式 | Scheduler／API | go-redis 行為 | 本次處理 |
|---|---|---|---|
| Standalone | 相同 | 一個 server pipeline | 逐筆結果 |
| Cluster | 相同 | 依 key slot 分組並對各 master 執行 | 逐筆結果，允許 partial success |
| Sentinel（未支援） | 未來相同 | failover client 切換 master | 本次不新增 client config |

## 9. Session lifecycle 資料流

### 9.1 Login

```text
Authenticate login
  → Redis Claim（產生新 epoch）
  → scheduler.Schedule(claimed presence)
  → commit authoritative local session
```

framework 必須先啟動 scheduler，才開放 WebSocket ingress，因此正常 production registration 不會遇到
未啟動 scheduler。若 Schedule 因 validation 或 lifecycle race 失敗，local authoritative mapping 尚未 commit；
Register 必須以 fenced Release best-effort 釋放剛 claim 的 lease，不可留下沒有 renewal 的成功登入，也不應
改動原本的 local mapping。

同一 connection／login 的 idempotent Register 不得重複 schedule。

### 9.2 Replacement

新 connection 先 Claim 新 epoch，再成為 local authoritative session。舊 lease 從 scheduler 移除後才執行
既有 socket close／Release cleanup。即使舊 renew 已經 in flight：

- 發生在新 Claim 前：可能成功，但隨後新 Claim 會覆寫 owner／epoch。
- 發生在新 Claim 後：Lua fencing 回傳 `ErrPresenceNotOwner`。
- 發生在舊 Release 後：key 不存在或 epoch 不符，Lua 不會重建 key。

result commit 還需檢查 generation，確保 stale success／error 不會重新 schedule 舊 lease。

### 9.3 Disconnect、kick 與 shutdown

既有 cleanup ordering 保持「先 detach local mapping，再 close（需要時），最後 fenced Release」。其中
per-lease `cancel + wait` 改為同步 `scheduler.Remove(presence)`；Remove 返回後保證 entry 不會再次被排程，
但不必等待已送到 Redis 的 fenced command，因為上述 Lua 語意保證它不能復活已釋放或已替換的 lease。

scheduler lifecycle 使用 framework managed resource：

- `PhaseService` 啟動，早於 `PhaseIngress` 的 WebSocket listener。
- shutdown 時 WebSocket ingress 先停止並移除 sessions，scheduler 再停止，Redis infrastructure 最後停止。
- `Stop` 停止接收 Schedule、取消 timer／operation context、等待目前 pipeline 返回或 context deadline、清空
  entries；必須 idempotent。

## 10. 錯誤與 retry 策略

| 狀況 | 行為 |
|---|---|
| invalid presence／foreign GateID | Schedule／batch item 拒絕，視為程式或 wiring error |
| batch success | 約一個正常 interval 後再 renew |
| `ErrPresenceNotOwner` | 移除 entry，不重試，增加 bounded metric 並記一筆 bounded summary log |
| Redis transient error | 僅失敗項目 exponential backoff；初始為一個 tick，上限 5 秒 |
| partial pipeline failure | success 正常排程，failed 分別 retry／remove |
| scheduler lag | 不丟 lease；處理所有 due entries並暴露 lag／overdue metrics |
| context cancelled／Stop | 不排新 retry，等待或取消目前 operation |

Redis outage 可能長於 lease TTL，此時 Redis key 會依設計過期；恢復後 renew 取得
`ErrPresenceNotOwner` 並停止。第一版維持既有不主動關閉 WebSocket 的產品行為，但 metrics 必須讓維運立即
發現。未來若定義「lost ownership 必須斷線」，應由 SessionRegistry product policy 另案處理，不能藏在
Redis adapter。

為避免 10,000 個 lease 同時錯誤造成 log storm：

- 不逐 lease 記錄 transient error。
- 每個失敗 batch 最多產生一筆 structured summary log，包含 result category、count、duration；不得加入
  login name、connection ID 或 payload。
- individual outcome 由 counters 表達。

## 11. Actionable metrics

metrics 歸屬 Gate session ownership module，固定使用 `gaming_core_gate_session_ownership_` prefix；不得使用
`login_name`、`connection_id`、`bucket_id`、Redis address 或任意 error string 作 label。

| Metric | Type／labels | 問題判斷與對應措施 |
|---|---|---|
| `..._active_leases` | Gauge | 趨近單 Gate 10k 規劃容量時準備 scale out |
| `..._renewals_total{result}` | Counter；`success`、`error`、`not_owner` | `error` 持續增加立即查 Redis／network；`not_owner` 異常增加查 failover、TTL expiry 或重複登入 |
| `..._renewal_batch_duration_seconds` | Histogram | p95／p99 上升時對照 Redis pool wait；判斷 client pool、network 或 Redis server latency |
| `..._renewal_batch_size` | Histogram | 長期接近 256 時增加 buckets 或降低單 Gate player 容量 |
| `..._scheduler_lag_seconds` | Histogram | lag 上升但 Redis 正常時查 Gate CPU、GC、Go scheduler；與 Redis latency 同升則查 Redis 路徑 |
| `..._overdue_leases` | Gauge | 持續非零表示 scheduler 無法按期處理，需立即處理 |

既有 `gaming_core_redis_pool_pending_requests`、`wait_total`、`wait_seconds_total`、`timeouts_total`、
`total_connections`、`idle_connections` 已提供 Redis pool saturation，不重複建立 ownership 專用 pool
metrics。

判讀關係：

```text
scheduler lag 高 + Redis pool/latency 正常
  → Gate CPU、GC pause 或 scheduler loop 問題

scheduler lag 高 + Redis pool wait/timeouts 高
  → Redis pool saturation

batch duration 高 + pool wait 低
  → Redis server、network、Cluster redirect 或 failover

batch size 長期逼近 256 + 其他指標正常
  → buckets 太少；維運可增加 buckets

active leases 接近容量 + batch/lag 同時成長
  → 優先 scale Gate，不只調參
```

第一版不增加 `inflight_batches`：同步 scheduler 最多只有一個 application-level batch in flight，固定為
0／1，對定位沒有額外價值。不增加 per-bucket occupancy metric：bucket label 雖 bounded，仍難直接形成維運
動作；batch size 與 lag 已能回答負載是否分布不均或處理不及。

## 12. 主要介面與元件責任

### 12.1 `PresenceRegistry.RenewMany`

- 驗證 input。
- 建立 fenced single-key pipeline commands。
- 回傳逐筆 typed result。
- 不決定 retry、下一次 due time、log 或 Gate session 行為。

### 12.2 `GatePresenceRegistry.RenewMany`

- 保證每筆 presence GateID 等於 process GateID。
- 保留輸入／輸出順序。
- 不接觸 buckets 或 WebSocket。

### 12.3 `sessionPresenceRenewalScheduler`

- lifecycle、bucket、hash distribution、due time、retry、batch split。
- 更新 renewal metrics 與 batch summary log。
- 不 Claim、Release、關閉 WebSocket或修改 room state。

建議最小介面：

```go
type sessionPresenceBatchRenewer interface {
    RenewMany(context.Context, []serversend.Presence) []serversend.PresenceRenewResult
}

type sessionPresenceScheduler interface {
    Schedule(serversend.Presence) error
    Remove(serversend.Presence)
}
```

介面保留 package-private，僅供 Gate component 與 contract tests 使用；不提前公開成 framework API。

### 12.4 `SessionRegistry`

- Claim 成功後先 Schedule，再 commit local authoritative session。
- detach 時 Remove，再執行既有 close／Release。
- Schedule failure 時不 commit local mapping，並 Release 剛 Claim 的 lease。
- 不處理 bucket、pipeline 或 retry 細節。

## 13. 測試策略

### 13.1 Config tests

- `lease_ttl: 5m`、省略 renewal 時得到 interval 100 秒、buckets 100、tick 1 秒。
- interval 零值 default；負值、`> lease_ttl / 2` 拒絕。
- buckets 零值 default；負值、導致非正 tick 或 derived tick `> 5s` 的設定拒絕。
- strict config 拒絕未知欄位。
- regression test 明確證明正常 interval 不再被 5 秒 retry max 截短。

### 13.2 Batch Redis contract tests

- 空輸入不執行 pipeline。
- all success、all not-owner、all transient error。
- partial success：overall error 非 nil 時仍保留已成功 command。
- invalid／foreign Gate presence 對齊原輸入回傳錯誤。
- 每個 EVAL 只有一個 Redis key，未使用 transaction。
- miniredis integration 驗證成功續期、stale epoch 不續期及不存在 key 不被重建。

不新增真實 Redis Cluster test dependency。Cluster-specific correctness 由 single-key command contract、fake
partial results 與 go-redis client contract覆蓋；若未來 CI 已有 Cluster fixture，再補 integration test。

### 13.3 Scheduler deterministic tests

透過 package-private clock／tick seam 驅動，不以長時間 `time.Sleep` 驗證：

- 10,000 個 identities 分布到所有 buckets，且每個 entry 只存在一次。
- 每 tick 只處理 due bucket；一次 wake-up 不重複 renew 同一 lease。
- 超過 256 entries 拆成多個 sequential batches。
- success 回正常 interval；transient error 依 retry backoff；not-owner 移除。
- retry due 的 bucket rounding 不會讓正常 ticker 下的預定 retry 超過 5 秒。
- 跨 wheel rotation 的 early bucket requeue 不會額外延遲一個 tick。
- delayed tick 會處理所有 overdue entries，不延後一整輪。
- Remove during in-flight batch 後，stale result 不會重新 schedule。
- replacement epoch／connection identity 不被舊 result 移除。
- duplicate Start 回 typed error；Stop 可重複，concurrent callers 等待同一 termination event；Stop during pipeline。

### 13.4 Session lifecycle tests

更新既有 presence contract tests：

- Register 成功 schedule 一次；idempotent Register 不重複 schedule。
- Claim failure 不 schedule、不改 local authoritative session。
- Schedule failure 回滾 local mapping 並 Release。
- disconnect、kick、room kick、replacement 都 Remove 正確 lease 並保持 fenced Release。
- 不再要求等待 per-session renewal goroutine；改驗證 Remove 返回後 stale batch result 無法 reinsert。
- framework 啟動順序為 Redis → scheduler → WebSocket；停止順序相反。

### 13.5 Metrics 與 concurrency tests

- metric family、type、固定 labels 與 registration failure contract。
- active leases 隨 Schedule／Remove 正確變化。
- success／error／not_owner、batch size、duration、lag、overdue 更新正確。
- 不產生 player、connection、bucket 或 error-string label。
- concurrent Schedule／Remove／tick／Stop 通過 `go test -race`。
- 完成實作後執行 `go test ./...`、focused `go test -race` 與 `go vet ./...`。

## 14. 預計檔案修改

| 檔案 | 必要修改 |
|---|---|
| `pkg/serversend/presence.go` | renewal config、batch result 與 `RenewMany` pipeline |
| `pkg/serversend/gate_presence.go` | Gate-bound `RenewMany` |
| `pkg/serversend/redisstore/store.go` | 僅在既有 `Pipelined` adapter contract 不足時做窄幅調整 |
| `products/gateproduct/session_renewal.go` | 新增單層 bucket scheduler |
| `products/gateproduct/session_registry.go` | 以 Schedule／Remove 取代 per-session goroutine／timer |
| `products/gateproduct/server_send_runtime.go` | config mapping、scheduler constructor 與 logger wiring |
| `products/gateproduct/app.go` | 將 scheduler 註冊為 `PhaseService` managed resource |
| `products/gateproduct/metrics.go` | 加入必要 ownership metrics |
| `configs/examples/gateproduct.yaml` | 顯示 renewal defaults、必要性與用途 |
| 對應 `*_contract_test.go` | config、pipeline、scheduler、lifecycle、metrics、race contracts |

不修改 Game product、Gate endpoint registrar、Redis broadcast、gatelink DNS、WebSocket wire protocol、protobuf
或 Redis key schema。

## 15. 關鍵決策與取捨

### 15.1 單層 buckets，不做完整 timing wheel library

所有 leases 共用一個 interval，單層 wheel 已能把 10k timers 收斂為一個 timer。hierarchical wheel 與
generic scheduler 不會改善目前需求，反而增加取消、round、clock 與 API 維護成本。

### 15.2 Pipeline 降低 round trips，不改 atomicity

每筆 fenced Lua 仍獨立 atomic；pipeline 只批次傳輸。這讓 standalone、Cluster 與未來 Sentinel 可共用
contract，也避免跨 slot script。

### 15.3 Sequential batches，不加入應用層 worker pool

預估平均每秒約 100 commands，小於 256 internal batch limit。單一 scheduler 的行為、ordering、metrics 與
shutdown 最容易驗證；ClusterClient 已會對不同 nodes parallelize。只有實測顯示 batch duration造成 lag，
才需要另案加入 bounded workers。

### 15.4 可調 interval／buckets，但不暴露重複 knobs

維運可用 active leases、batch size 與 lag 調整 buckets，亦可在 TTL safety constraint 內調整正常 interval。
tick 由兩者推導；不再提供 tick、batch、concurrency 等尚未證明必要的 config。

### 15.5 Remove 不等待已發出的 fenced renew

停止等待每個 per-session goroutine是本次降低資源與 cleanup latency 的一部分。single-key epoch fencing 與
generation check 已保證 stale command 不會重建／延長新 owner，也不會在本機重新排程，因此同步等待不會
增加 correctness。

## 16. 已知限制與擴充方向

- Redis Pub/Sub 仍為 best-effort，與 ownership scheduler 無關。
- Redis outage 長於 lease TTL 時 ownership 會過期；本次只觀測，不新增自動 WebSocket disconnect policy。
- 10k 是單 Gate 規劃基準，不是 hard limit；capacity threshold 應由部署告警規則決定。
- bucket hash 只能平滑大量 identities，無法保證每個 bucket 完全等量；batch size metric用於判斷是否需要
  增加 buckets。
- 未來 Sentinel 支援只應擴充 `pkg/infra/redis` config／constructor與 failover integration test。
- 若實測出現 scheduler lag 且 Redis pipeline duration 是主因，可新增小型 bounded worker pool；在有數據前
  不加入。
- 若未來出現另一種高 cardinality、不同 TTL 的 refresh 工作，應先獨立設計其 correctness 與 lifecycle，
  不能直接共用 session ownership wheel。只有至少兩個模組確有相同 contract 時才抽取通用 scheduler。

## 17. Self-review：必要性與需求符合度

| 設計內容 | 保留原因 | 結論 |
|---|---|---|
| 修正正常／retry interval 混用 | 現況把 5m TTL renew 放大為約每 5s，是直接的 Redis 負載問題 | 必要 |
| process-scoped 單層 buckets | 10k leases 下移除 10k renewal goroutines／timers並平滑同時登入 | 必要 |
| single-key pipeline | 將約 100 commands/s 收斂為約 1 pipeline/s，同時保持 fencing／Cluster-safe | 必要 |
| 逐筆結果 | Cluster／failover 可 partial success，錯誤處理正確性所需 | 必要 |
| generation check | Remove／replacement 與 in-flight result race 所需 | 必要 |
| managed lifecycle | 保證 scheduler 早於 ingress、晚於 ingress 停止 | 必要 |
| 六項 bounded metrics | 分別回答容量、錯誤、Redis latency、batch 壓力、scheduler lag與 overdue | 必要且可行動 |
| interval／buckets config | 分別控制 correctness 與負載平滑，使用者已要求可依 runtime data 調整 | 必要 |
| internal batch limit | 防止單 bucket 形成無界 pipeline；不增加公開 config | 必要 safety bound |
| Sentinel、endpoint、Pub/Sub、DNS、room/business refresh | 與本次 high-cardinality ownership renewal 無直接關係 | 排除 |
| worker pool、parallel pipeline、generic timing wheel | 10k／100 commands/s 基準沒有數據證明需要 | 排除 |

Review 結論：本設計完整覆蓋目前兩個實際問題——錯誤的 renewal 頻率與 per-session timer／Redis round-trip
成本；修改邊界只落在 session ownership renewal。設計沒有把低 cardinality endpoint heartbeat、Pub/Sub
重連、DNS refresh 或未定義的業務更新納入，也沒有提前加入 Sentinel、worker pool或通用 scheduler。
在每 Gate 10k player 的已知規模下，內容皆能直接對應 correctness、容量或維運判斷，沒有為未提出需求
保留額外實作。
