# Session ownership renewal scheduler 必要修正

## 1. 文件目的

本文件記錄對目前 staged changes 與 `SESSION_OWNERSHIP_RENEWAL_SCHEDULER_DESIGN.md` 的 review 結果，
作為下一次實作的直接依據。

修正範圍只包含已實作的 session ownership renewal scheduler、Redis fenced batch renewal、相關 metrics、
lifecycle 與 contract tests。本次不新增 worker pool、第二個 scheduler、額外 metrics、公開 framework API，
也不修改 endpoint registration、broadcast、SendToPlayer、WebSocket protocol 或其他 product。

## 2. Review 結論

目前整體方向正確：process-scoped 單層 buckets、single-key Redis pipeline、逐筆 partial result、generation
fencing、framework managed lifecycle 與六項 bounded metrics 都有直接需求依據，應保留。

合併前只需完成以下五組必要調整：

1. 防止 derived tick 使 5 秒 retry 上限失效。
2. 修正 scheduler lag 的取樣時間與零值缺漏。
3. 正確分類 Redis renewal Lua 的非預期回傳值。
4. 讓 concurrent／timeout `Stop` 真正共用同一個終止狀態。
5. 移除只為舊測試存在、但沒有 lifecycle owner 的 production scheduler 路徑與靜態不可能失敗的 assertion。

另需同步設計文件中的 login ordering 與 duplicate `Start` 契約。這些調整都用來修正現有契約或刪除冗餘，
不擴張需求。

## 3. 必要修正一：限制 derived tick 不得大於 retry 上限

### 3.1 問題

目前只驗證：

```text
tick = renewal.interval / renewal.buckets > 0
```

但 scheduler 只有一個以 `tick` 為週期的 ticker，`scheduleAtLocked` 也會將早於 `nextTick` 的 due time推遲至
`nextTick`，並將較晚的 due time 向上 rounding 到 bucket tick。若設定為：

```yaml
lease_ttl: 5m
renewal:
  interval: 100s
  buckets: 1
```

則 `tick=100s`。Redis error 後雖然 `presenceRetryDelay` 算出最多 5 秒，實際仍只能在 100 秒後執行，違反
既有 retry contract，並減少 lease 到期前可重試的次數。即使 tick 已限制為 5 秒，若 due 比 `nextTick`
晚一點點，向上 rounding 仍可能讓實際預定 retry 接近 10 秒。

### 3.2 最小修法

不要增加第二個 retry timer。由 `products/gateproduct/session_renewal.go` 的 production scheduler constructor
在 normalization 後驗證：

```go
tick := normalized.Renewal.Interval / time.Duration(normalized.Renewal.Buckets)
if tick > defaultPresenceRetryMax {
    return nil, fmt.Errorf(
        "gate session: presence renewal tick %s exceeds retry maximum %s",
        tick,
        defaultPresenceRetryMax,
    )
}
```

驗證放在 Gate scheduler constructor，而不是把 retry policy 匯出到 `pkg/serversend`：

- `PresenceConfig` 的基本 default／正值／TTL safety validation 繼續由 `pkg/serversend` 負責。
- 5 秒 retry cap 是 Gate scheduler policy，由實際使用該 policy 的 component 驗證。
- framework 建構 Gate 時必須 resolve scheduler，因此仍會在開放 ingress 前 fail fast。

錯誤應包住 `serversend.ErrDestinationInvalid`，讓 startup 與測試能穩定分類：

```go
return nil, fmt.Errorf(
    "%w: presence renewal tick %s exceeds retry maximum %s",
    serversend.ErrDestinationInvalid,
    tick,
    defaultPresenceRetryMax,
)
```

在 config 範例註解與主設計文件補充：

```text
interval / buckets 必須大於 0 且不得大於 5 秒。
```

不新增 `tick_interval` 或 `retry_max` 公開 config；兩者目前沒有獨立調整需求。

另外，在 transient error 的 `commitResult` 排程 retry 時，先將 logical due 限制在：

```go
latestDue := now.Add(defaultPresenceRetryMax - s.tick)
```

若 backoff due 超過 `latestDue`，使用 `latestDue` 再交給 `scheduleAtLocked`。這是因應 bucket 向上 rounding
所需的最小修正，不新增 timer 或第二套 scheduler；在正常 ticker 下可保留一個 tick 的 rounding 空間。

處理目前 bucket 前，`advanceLocked` 先推進 `nextTick`，再呼叫 `takeBucketLocked`。如此 early bucket requeue
與 `scheduleAtLocked` 使用相同的「下一個 tick」基準；若 retry due 跨越一整圈，不會因基準差一格而多延遲一個 tick。

### 3.3 必要測試

在 `products/gateproduct/session_renewal_contract_test.go` 增加 constructor contract：

- `interval=100s, buckets=20` 得到 `tick=5s`，可以建立。
- `interval=100s, buckets=19` 得到大於 5 秒的 tick，回傳 `ErrDestinationInvalid`。
- default `lease_ttl=5m` 仍得到 `interval=100s, buckets=100, tick=1s`。
- retry due 跨越多 bucket wheel 一整圈且提早落入 bucket 時，仍在 5 秒上限內執行，不因 requeue 多延遲一個 tick。

不需要另建 retry timer integration test。

## 4. 必要修正二：scheduler lag 必須包含零值與 sequential batch 等待

### 4.1 問題

目前一次 `processAt` 只在開頭取得一個 `now`，所有 sequential batches 都使用同一時間計算 lag；若第一個
Redis pipeline 花費一段時間，第二批等待第一批的時間不會反映在 `scheduler_lag_seconds`。

目前也只在 `lag > 0` 時 Observe。這使健康狀態的零值樣本不存在；發生過一次延遲後，Prometheus cumulative
histogram 會長期只保留非零樣本，quantile 無法表示大部分續租準時執行。

### 4.2 最小修法

在每一批建立 input 前重新讀取 monotonic clock，並對該批所有 entries 記錄非負 lag：

```go
batchNow := s.clock()
presences := make([]serversend.Presence, len(batch))
for index, entry := range batch {
    presences[index] = entry.presence
    if s.metrics != nil {
        lag := batchNow.Sub(entry.nextDue)
        if lag < 0 {
            lag = 0
        }
        s.metrics.ObserveSessionOwnershipSchedulerLag(lag)
    }
}
```

`renewal_batch_duration_seconds` 仍應在呼叫 `RenewMany` 的正前方另取 `started := s.clock()`，避免將 input
slice 建立時間混入 Redis pipeline duration。

不新增 queue-wait metric；修正後的既有 scheduler lag 已能回答同一問題。

### 4.3 必要測試

補兩個 deterministic assertion：

- entry 在 due time 準時處理時，lag histogram observer 仍收到一筆 `0`。
- 超過 256 entries 拆成兩批，fake renewer 在第一批後推進 clock；第二批 lag 必須包含第一批耗時。

擴充既有 `renewalMetricsFake` 提供 lag snapshot 即可，不需要 Prometheus sleep-based test。

## 5. 必要修正三：精確分類 Lua renewal result

### 5.1 問題

設計 contract 是：

| Lua result | 分類 | Scheduler 行為 |
|---:|---|---|
| `1` | success | 回正常 interval |
| `0` | `ErrPresenceNotOwner` | 永久移除 entry |
| 其他值 | store contract error | 記 error 並 retry |

目前 `PresenceRegistry.RenewMany` 將所有非 `1` 值都當成 `ErrPresenceNotOwner`。一旦 script／adapter contract
異常，scheduler 會永久停止該 lease，而非將它作為可觀測的 store error 重試。

### 5.2 最小修法

在 `pkg/serversend/presence.go` 增加 package-private classifier，供單筆與 batch renewal 共用：

```go
func presenceRenewResultError(loginName LoginName, updated int64) error {
    switch updated {
    case 1:
        return nil
    case 0:
        return fmt.Errorf("%w: %q", ErrPresenceNotOwner, loginName)
    default:
        return fmt.Errorf(
            "%w: renew player presence %q returned unexpected result %d",
            ErrRouteStoreUnavailable,
            loginName,
            updated,
        )
    }
}
```

`Renew` 與 `RenewMany` 在成功取得 `Int64()` 後都呼叫此 helper，避免兩條路徑日後產生不同分類。

Redis command error 仍使用既有 `routeStoreError`；不要將 command error 與非預期 script value 合併成單一
aggregate error。

### 5.3 必要測試

在 `pkg/serversend/presence_renewal_contract_test.go` 直接測 package-private classifier：

- `1` 回 nil。
- `0` 可由 `errors.Is(..., ErrPresenceNotOwner)` 判斷。
- `2` 可由 `errors.Is(..., ErrRouteStoreUnavailable)` 判斷，且不可判為 `ErrPresenceNotOwner`。

既有 miniredis fenced pipeline test 保留；不需要為無法由目前 Lua 正常產生的回傳值建立複雜 fake
`redis.Pipeliner`。

## 6. 必要修正四：修正 concurrent／timeout Stop 狀態

### 6.1 問題

目前第一個 `Stop` 在 scheduler loop 結束前就設定 `started=false` 並清掉 `done`。若第一個 caller 因 deadline
先返回，第二個 `Stop` 會直接回 nil，即使 Redis pipeline goroutine 尚未退出。這不符合：

- 多個 Stop caller 應觀察同一個 termination event。
- Stop 成功返回表示 scheduler loop 已停止。
- caller timeout 不代表 component 已停止，也不能讓下一次 Stop 假成功。

### 6.2 必要狀態不變量

- `stopped=true`：不再接受 `Schedule`，且不可重新 `Start`。
- `started=true`：loop 仍可能存活；只有 loop exit path 可以改為 false。
- `done`：從 loop 建立到 loop 完成期間保持同一個 channel。
- caller context timeout 只影響該次等待，不清除 entries 或 lifecycle handle。
- loop 真正退出時清空 entries、將 metrics 歸零，再關閉 `done`。

### 6.3 最小修法

`Stop` 只負責一次性設定 stopping、取消 loop 並等待既有 `done`：

```go
func (s *sessionPresenceRenewalScheduler) Stop(ctx context.Context) error {
    if s == nil {
        return nil
    }
    if ctx == nil {
        ctx = context.Background()
    }

    s.mu.Lock()
    if !s.started {
        s.stopped = true
        s.clearLocked()
        s.mu.Unlock()
        return nil
    }
    firstStop := !s.stopped
    s.stopped = true
    cancel, done := s.cancel, s.done
    s.mu.Unlock()

    if firstStop && cancel != nil {
        cancel()
    }
    select {
    case <-done:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

loop 的 defer 成為唯一 completion owner：

```go
func (s *sessionPresenceRenewalScheduler) finishLoop(done chan struct{}) {
    s.mu.Lock()
    s.started = false
    s.cancel = nil
    s.clearLocked()
    close(done)
    s.mu.Unlock()
}
```

`loop` 使用 `defer s.finishLoop(done)`，ticker 仍由 loop defer Stop。完成後可保留已關閉的 `done` 或在
`started=false` 下清成 nil；但不得在 loop exit 前清除。

`Start` 維持 one-shot contract：

- 已 started：回 `errPresenceSchedulerStarted`。
- 已 stopped：回 `errPresenceSchedulerStopped`。

這裡不需要讓 duplicate Start 靜默成功；framework 本身也是 one-shot lifecycle。

### 6.4 必要測試

在 scheduler contract test 增加一個刻意不理會 context、直到 test channel 解鎖才返回的 batch renewer：

1. 啟動 scheduler並讓 pipeline 進入 blocked 狀態。
2. 第一次 `Stop` 使用短 deadline，必須回 `context.DeadlineExceeded`。
3. 第二次 `Stop` 使用有效 context，必須保持等待，不可立即成功。
4. 解鎖 pipeline 後，第二次 `Stop` 回 nil。
5. entries 與 active／overdue gauges 歸零，ticker 只停止一次。

保留現有 duplicate Start error 與 sequential duplicate Stop 測試。

## 7. 必要修正五：移除測試專用 production lifecycle 與冗餘 assertion

### 7.1 問題

`products/gateproduct/session_registry.go` 的 `newSessionRegistryWithLogger` 只為既有 package tests 自動建立並
啟動 scheduler，沒有任何 owner 呼叫其 `Stop`。每個 test registry 因此留下 ticker 與 goroutine；短 TTL
測試還會建立高頻 ticker。

`singlePresenceBatchRenewer` 也只用來維持舊 fake 的單筆 `Renew` 介面，production wiring 實際永遠使用具備
`RenewMany` 的 `*serversend.GatePresenceRegistry`。

另外，`GatePresenceRegistry.presence` 的靜態型別是 `*PresenceRegistry`，它一定有 `RenewMany`；再做
`presenceBatchRenewer` type assertion 與 unsupported branch 是不可到達的冗餘程式。

### 7.2 最小修法

Production code：

- 從 `products/gateproduct/session_registry.go` 移除 `newSessionRegistry` 與
  `newSessionRegistryWithLogger`；production 只保留 framework 使用的 `newSessionRegistryWithScheduler`。
- 將 `sessionPresence` 縮成 SessionRegistry 真正需要的 `Claim`／`Release`；移除不再使用的 `Renew`。
- `newSessionPresenceRenewalScheduler` 直接接受 `sessionPresenceBatchRenewer`，移除
  `singlePresenceBatchRenewer` 與 fallback assertion。
- `pkg/serversend/gate_presence.go` 移除 local `presenceBatchRenewer` interface及 type assertion，直接呼叫：

```go
batchResults := r.presence.RenewMany(ctx, validPresences)
```

Tests：

- 在 `session_presence_contract_test.go` 建立 test-only `sessionPresenceSchedulerFake`。
- test helper 以 `newSessionRegistryWithScheduler` 注入 fake，不啟動真 ticker。
- 第一個 lifecycle test 改驗證 Claim 後只 Schedule 一次，Remove 後只移除同一 presence。
- 刪除舊有「等待定時 Renew」與 `time.Sleep` assertion；scheduler 定時與 in-flight generation 已由
  `session_renewal_contract_test.go` deterministic tests 覆蓋。
- 將舊 blocked single Renew test 改為 Schedule failure rollback contract：Schedule 回錯時，不建立 local
  mapping，並 fenced Release剛 Claim 的 presence。

不要新增 exported constructor 或讓 `SessionRegistry` 自己擁有 scheduler lifecycle；production lifecycle
仍只能由 framework managed resource 管理。

## 8. 主設計文件同步

只修改 `SESSION_OWNERSHIP_RENEWAL_SCHEDULER_DESIGN.md` 的以下內容：

### 8.1 Login ordering

統一為實際且較安全的順序：

```text
Authenticate login
  → Redis Claim（產生新 epoch）
  → scheduler.Schedule(claimed presence)
  → commit authoritative local session
```

Schedule failure 發生在 local commit 前，因此行為是 fenced Release 新 claim，並保留原本 local mapping；
不再描述成「rollback 已 commit 的 local mapping」。

### 8.2 Lifecycle wording

將模糊的 `Start／Stop idempotency` 改為：

```text
duplicate Start 回 typed error；Stop 可重複且 concurrent callers 等待同一 termination event。
```

### 8.3 Config validation

在 derived tick 規則加入 `<= 5s`，說明這是單 ticker 架構履行 retry upper bound 所需的 safety constraint。

## 9. 驗證順序

完成修改後依序執行：

```bash
gofmt -w <本次修改的 Go 檔案>
go test ./pkg/serversend ./products/gateproduct
go test -race ./pkg/serversend ./products/gateproduct
go test ./...
go vet ./...
git diff --check
```

需確認：

- 不新增或變更 Redis key schema。
- 每個 pipeline command 仍只有一個 key，且仍使用普通 `Pipelined`。
- normal interval 沒有再次被 5 秒 cap 截短。
- retry 的計算與實際最早 wake-up 都不超過 5 秒。
- scheduler lag 每筆 renewal 都有一筆樣本，且 sequential wait 可見。
- unexpected Lua result 不會被分類為 `not_owner`。
- Stop timeout 後再次 Stop 不會假成功。
- tests 不留下 scheduler ticker／goroutine。
- staged changes 不被測試或格式化流程改動。

## 10. 預估修改量

| 類別 | 預估 touched lines |
|---|---:|
| scheduler config、lag、Stop | 30～45 |
| Redis result classifier | 8～15 |
| 移除 production test compatibility／冗餘 assertion | 20～35 |
| deterministic contract tests | 55～80 |
| 主設計與 config 註解 | 5～10 |
| 合計 | 約 120～170 |

其中包含刪除舊 constructor、adapter、sleep-based assertion 與不可到達分支；production code 的淨增加量應
很小，甚至可能下降。

## 11. Self-review：必要性與非目標

| 調整 | 必要原因 | 是否擴張需求 |
|---|---|---|
| tick `<= 5s` validation | 否則現有單 ticker 無法履行 retry contract | 否，僅 fail fast 拒絕無法正確執行的設定 |
| 每批即時 lag＋零值樣本 | 否則無法由既有 metric 判斷 pipeline backlog | 否，沒有新增 metric |
| Lua result 精確分類 | 防止 contract error 被誤判後永久停止續租 | 否，修正既有分類 |
| shared Stop completion | lifecycle timeout／concurrent caller correctness | 否，修正既有 lifecycle |
| 移除 test-only scheduler path | 消除無 owner goroutine 與 production 冗餘 | 否，實作範圍縮小 |
| 文件 ordering／Start 語意同步 | 避免設計與實作互相矛盾 | 否，僅修正文義 |

明確不做：

- 不加入 worker pool、parallel pipeline 或可調 batch size。
- 不加入第二個 retry timer或 hierarchical timing wheel。
- 不新增 per-bucket、inflight batch 或 per-player metrics。
- 不在 ownership lost 時新增 WebSocket close policy。
- 不新增 Sentinel、Redis topology discovery 或 Cluster integration fixture。
- 不修改 endpoint refresh、Pub/Sub reconnect、DNS refresh、room 或 broadcast 行為。

完成上述修正後，實作即可符合主設計的 correctness、維運可觀測性與 lifecycle 要求，同時刪除只為舊測試
存在的相容路徑；其餘 staged implementation 不需調整。
