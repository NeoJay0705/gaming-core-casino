# Async Server-send Batching 必要修正

## 1. 文件目的

本文件只描述 staged changes 對照 `ASYNC_SERVER_SEND_BATCHING_DESIGN.md` 後確認的兩項必要修正，作為後續實作者的直接修改依據。

修正範圍只有：

1. Player admission 必須在 defensive clone 前完成整批 queue capacity preflight；
2. Player／Broadcast 的 `Stop(ctx)` 必須在 deadline 到期後返回，不可再無期限等待 worker。

不藉此重構既有 sender、增加 worker pool、flush timer、retry、Redis pipeline、per-Gate queue、公開 queue API、額外 config 或 metrics family。`ASYNC_SERVER_SEND_BATCHING_DESIGN.md` 的架構與對外 contract 不變。

## 2. 必要修正一：clone 前拒絕超過 queue capacity 的 Player input

### 2.1 現況與問題

`AsyncPlayerSender.SendToPlayers` 目前依序執行：

1. 驗證 messages；
2. `clonePlayerMessages(messages)`；
3. 建立 `playerJob` 並計算 bytes；
4. `queue.enqueue` 才判斷整批是否超過 queue capacity。

因此一個必然得到 `ErrEnqueueTooLarge` 的呼叫，仍可能先複製遠大於 queue 上限的所有 payload。這會讓 `queue_capacity_messages`／`queue_capacity_bytes` 無法形成有效的記憶體邊界，與原設計「先計算整批大小、超限立即拒絕、再 clone」不一致。

### 2.2 修改檔案

- `pkg/serversend/async_player.go`
- `pkg/serversend/async_sender_contract_test.go`

不需修改 public interface、config schema或product wiring。

### 2.3 具體修改

在 `async_player.go` 增加 package-private Player queue accounting helper。它只做現有資料的 deterministic accounting，不解析 client protobuf payload：

```go
func playerQueueAccounting(
    messages []PlayerMessage,
    capacityMessages int,
    capacityBytes int,
) (itemBytes []int, totalBytes int, err error)
```

行為固定如下：

1. `len(messages) > capacityMessages` 時直接回傳包裝 `ErrEnqueueTooLarge` 的 error；此時不得配置 `itemBytes`。
2. count 合法後，才配置長度最多為 `capacityMessages` 的 `itemBytes`。
3. 每項使用既有 `playerDeliveryWireSize` 計算 encoded bytes。
4. 單項超過 `DefaultMaxPayloadBytes` 時維持既有 `ErrPayloadTooLarge`。
5. 累加前使用 `itemSize > capacityBytes-totalBytes` 判斷是否超過 queue byte capacity；如此同時避免 `int` overflow。超過時回傳包裝 `ErrEnqueueTooLarge` 的 error。
6. 成功時回傳每項 bytes 與總 bytes，供 `playerJob` 直接使用，不再重算一次。

`SendToPlayers` 的順序調整為：

1. 檢查 sender 是否存在；
2. 執行既有 message validation；
3. normalize nil context 並檢查 `ctx.Err()`；
4. 以已 normalize 且不可變的 `s.config.QueueCapacityMessages`／`QueueCapacityBytes` 執行 `playerQueueAccounting`；
5. 若 accounting 回 `ErrEnqueueTooLarge`，呼叫一次 `s.observer.ObserveQueueRejected("player", "too_large")` 後直接返回；其他 validation error 不記成 queue rejection；
6. accounting 成功後才執行 `clonePlayerMessages`；
7. 使用 clone、預先計算的 `itemBytes`／`totalBytes` 與 detached trace 建立 `playerJob`；
8. 呼叫既有 `queue.enqueue`。它仍必須在 lock 內重查 running state 與當下剩餘容量，處理 concurrent producers；此時不足記為 `full`，不可改成 `too_large`。

`newPlayerJob` 應改為接收已驗證的 accounting result，避免 clone 後再次走訪整批資料。它仍負責 detached trace 與 `acceptedAt`；不要把 queue capacity 或 observer 注入 `playerJob`。

### 2.4 錯誤與 metrics contract

- input 自身超過 configured capacity：`ErrEnqueueTooLarge`、`reason="too_large"`，恰好記錄一次；
- input 合法，但當下 queue 剩餘容量不足：`ErrQueueFull`、`reason="full"`，仍由 `queue.enqueue` 記錄一次；
- 單一 Player protobuf envelope 超過 1 MiB：維持 `ErrPayloadTooLarge`，不是 queue rejection；
- invalid message 或 canceled context：維持原 error，不 enqueue、不 clone、不呼叫 delegate。

不得為了 preflight 增加另一份 payload copy、heap-size estimator或新的 capacity error 類型。

### 2.5 必要測試

在 `async_sender_contract_test.go` 補下列 contract：

1. count 超過 queue capacity 時回 `ErrEnqueueTooLarge`，delegate call 數與 queue snapshot 都為零；
2. encoded bytes 超過 queue capacity 時同樣整批拒絕，沒有 partial admission；
3. 上述每次拒絕只增加一次 `too_large` observation；
4. input 自身可容納、但已存在工作占用剩餘容量時仍回 `ErrQueueFull`，避免 preflight 誤改 concurrent capacity 語意；
5. 正常成功路徑仍驗證 caller 修改原 slice／payload 不會影響 queued job。

測試只鎖定可觀察 contract，不使用脆弱的精確 allocation 次數或 heap bytes 斷言。

## 3. 必要修正二：`Stop(ctx)` deadline 到期後不可繼續阻塞

### 3.1 現況與問題

`waitAsyncPlayerStop` 與 `waitAsyncBroadcastStop` 在收到 `ctx.Done()` 後會先 cancel worker，接著無條件執行 `<-done`。若 custom delegate 沒有正確回應 context cancellation，`Stop(ctx)` 仍可能永久阻塞，使 framework shutdown deadline 失去作用。

這與原設計「deadline 到期時 cancel in-flight dependency，並返回 context error」直接衝突。

### 3.2 修改檔案

- `pkg/serversend/async_queue.go`
- `pkg/serversend/async_player.go`
- `pkg/serversend/async_broadcast.go`
- `pkg/serversend/async_sender_contract_test.go`

### 3.3 共用等待邏輯

在 `async_queue.go` 放置一個 package-private helper，僅共用 Player／Broadcast 相同的等待語意：

```go
func waitAsyncWorker(
    ctx context.Context,
    done <-chan struct{},
    cancel context.CancelFunc,
) error {
    if done == nil {
        return nil
    }
    select {
    case <-done:
        return nil
    case <-ctx.Done():
        if cancel != nil {
            cancel()
        }
        return ctx.Err()
    }
}
```

deadline 分支不得再等待 `<-done`。`context.CancelFunc` 可重複呼叫，因此 concurrent `Stop` 不需要另一層 cancel guard。

這個 helper 不應擴充成 generic lifecycle framework；sender state、queue drain與discard仍由各 sender 負責。

### 3.4 Worker 完成時更新 lifecycle state

Player 與 Broadcast 各自增加 package-private completion method，例如 `finishWorker(done)`，並由 `run` 的單一 defer 呼叫。完成順序必須是：

1. `queue.markStopped()`；
2. 在 sender mutex 下，若 state 是 `stopping`，改成 `stopped`；
3. `close(done)`。

`done` 關閉必須最後發生，確保所有等待者看到完成時，queue 與 sender state 已經一致。

不要在 `Stop(ctx)` deadline 分支把 sender 提前標成 `stopped`。delegate 尚未返回時，正確狀態仍是 `stopping`，新 admission 也會因 queue 已進入 stopping 而得到 `ErrSenderNotRunning`。

### 3.5 `Stop` 狀態流程

Player／Broadcast 的 `Stop` 統一依下列流程：

- `new`：維持既有 no-op；
- `running`：在 lock 下轉成 `stopping`、保存 `done/cancel`、呼叫 `queue.beginStop()`，解鎖後呼叫 `waitAsyncWorker`；
- `stopping`：保存同一組 `done/cancel`，解鎖後呼叫 `waitAsyncWorker`；
- `stopped`：idempotent no-op。

移除 `Stop` 返回後自行寫入 `stopped` 的程式；最終 state 只能由 worker completion method 寫入。如此可處理以下情境：

1. 正常 drain：worker 完成、關閉 `done`，`Stop` 回 nil；
2. deadline：cancel worker並立即回 `ctx.Err()`，state 暫時維持 `stopping`；
3. delegate 稍後返回：worker discard 可確認的 pending jobs、完成 state transition並關閉 `done`；
4. 後續再次 `Stop`：若仍 stopping，可使用新的 context等待同一個 worker；完成後為 no-op。

既有 worker 在 cancellation 後對 pending／channel work 記錄 `shutdown_timeout` discard 的邏輯保留。已交給 delegate 且只得到 partial／error 的 active batch，仍使用 worker result metric；本次不擴張 `discarded_total` 語意，也不新增 delivery ack。

### 3.6 必要測試

為 Player 與 Broadcast 各增加一個 non-cooperative fake delegate：

- 呼叫開始後通知 `started`；
- 故意忽略 `ctx.Done()`；
- 只在測試關閉 `release` channel 後返回。

測試流程：

1. Start sender並 enqueue 一筆，等待 delegate `started`；
2. 使用已取消或極短 deadline context，在 goroutine 呼叫 `Stop`；
3. 透過有上限的 test select 確認 `Stop` 在 delegate 尚未 release 前返回 `context.Canceled`／`DeadlineExceeded`；
4. 確認此時新 admission 得到 `ErrSenderNotRunning`；
5. 關閉 `release`，再以有效 context呼叫 `Stop` 等待 worker完成；
6. 確認後續重複 `Stop` 回 nil。

測試失敗路徑必須先 release fake delegate再結束，避免測試自己遺留 goroutine。不要用長時間 `Sleep` 判斷正確性。

## 4. 驗證順序

完成修正後依序執行：

```bash
gofmt -w pkg/serversend/async_queue.go \
  pkg/serversend/async_player.go \
  pkg/serversend/async_broadcast.go \
  pkg/serversend/async_sender_contract_test.go

go test -race -count=1 ./pkg/serversend
go test -race -count=1 ./products/gameproduct ./products/gateproduct ./examples/metrics/...
go vet ./pkg/serversend ./products/gameproduct ./products/gateproduct ./examples/metrics/...
git diff --check
```

本次修正不需重新產生 protobuf，也不需修改 performance validation script。因修正涉及 admission memory boundary 與 shutdown deadline，不改變正常 transport path；完成 source／contract tests 後，再由使用者決定是否重跑 live performance campaign。

## 5. 修改量估算

| 項目 | 預估修改量 |
|---|---:|
| Player capacity preflight、job construction | 約 35–50 行 |
| Stop deadline、worker completion state | 約 35–45 行 |
| 對應 contract tests 與既有 shutdown 斷言調整 | 約 200–230 行 |
| 合計 | 約 270–325 行 |

估算是 changed lines，不包含 `gofmt` 造成的純排版差異。設計文件本身不需修改。

## 6. 必要性與範圍 Self Review

- capacity preflight 是 bounded queue 真正限制 admission memory 的必要條件，不是提前最佳化；
- deadline 後返回是 `Stop(ctx)` 與 framework shutdown 可控的必要條件，不是假設所有 delegate 都正確的防禦性擴張；
- 測試只覆蓋這兩個過去未被捕捉的 contract gap；
- 沒有增加 public API、config、metric、transport、retry或新的業務語意；
- 沒有要求抽象 Player／Broadcast 全部 lifecycle，避免為消除少量重複碼而擴張設計；
- 沒有納入 config 排版、額外 payload copy、dashboard 或效能調參等非阻斷項目。

結論：以上兩項都是 review 後確認的必要修正；其餘 staged changes 不需藉本次 review 刪減或擴張。
