# Actionable Metrics 後續必要修改（request-player unary reply）

## 1. 目的與範圍

本文件只整理目前 Echo 壓測暴露出的兩項必要修正：

1. `RequestPlayerSender` 回覆原請求 connection 時，將單一 reply 寫入 Gate → Game `Forward` unary response，
   避免每筆 Echo 建立 reverse RPC 或以 Redis `GET` 重新解析 Gate endpoint。
2. load client 必須先建立全部 WebSocket connections，再初始化 Login／EnterRoom、執行 warm-up，最後才開始
   固定 duration 的正式測量，避免 connection establishment 與正式 Echo 數據混在同一窗口。

除此之外不新增 metrics、業務 API、通用壓測框架、連線池、自動調參、dashboard 或部署設定。既有
Gate／Game／load Prometheus metrics 足以分析 latency、RPS、error 與 saturation，不需要再加一組重複指標。

## 2. 現況確認與必要性

### 2.1 Direct reply 現況

原本 direct request-player reply 會由 Game 另發一筆 Game → Gate unary RPC，並依 request route 尋址；這對
「回覆目前這筆 Gate request」不是必要的 network hop。現在 `gatelink.Forward` 回傳可選的
`ForwardResponse`，Game handler 以 request-scoped single reply slot 提供最多一筆 reply，Gate 收到後直接
enqueue 發起該 request 的 session。`PlayerSender` 仍依玩家目前所在 Gate 查 presence/directory，broadcast
行為與 Redis dependency 不變。

### 2.2 Load client 現況

目前每個 goroutine 會自行 Dial → Login → EnterRoom → Echo，而且 `duration` 在 Dial 前就開始計時。大量
connections 同時 Dial 可能填滿 listen backlog；較晚完成 setup 的 connection 也會得到較短的實際 Echo
測量時間。因此目前結果不能公平比較 100、200、400、800、1600、3200 connections。

listen backlog 是尚未完成 accept 的排隊上限，不是可維持 WebSocket connections 的總上限。超過時，新連線
可能延遲、重傳或 timeout；因此 load client 應限制 setup concurrency，而不是調高 framework runtime 的
連線上限或把 backlog 當成服務容量。

## 3. 修改一：request-scoped unary reply

### 3.1 `pkg/gatelink`

`gatelink.proto` 將 `Forward` response 改為 `ForwardResponse`，其中最多有一個 optional `ForwardReply`；
generated files 必須由既有 `protoc` toolchain 同步產生。`gatelink.Server` 在 handler context 建立
single reply slot，handler 成功結束後才把 reply 放入 unary response；`gatelink.Client.Forward` 明確回傳
`(*gatelink.Reply, error)`，no-reply 為 `nil, nil`。

slot 以 mutex 保護，第一次設定成功，duplicate、handler 結束後設定、invalid command 或 payload error 都
不能交付第二筆 reply。handler error／panic 時丟棄 slot。這條新路徑不需要新增 metadata、endpoint 或自訂
codec。

### 3.2 `pkg/serversend` 與 Game composition

`RequestPlayerSender` interface 不變；`DirectRequestPlayerSender` 只保存 payload bound。`SendToRequestPlayer`
先驗證 message，再呼叫 `gatelink.SetForwardReply`，將 missing slot、duplicate 與 payload errors 映射為既有
或新增的 typed server-send errors，成功回傳既有 receipt。它不依賴 `GRPCTransport`、GateResolver、Redis
或任何 network I/O。

`newGameRequestPlayerSender` 只以 `serversend.Config.MaxPayloadBytes` 建立此 sender；`PlayerSender`、
`BroadcastSender` 與它們使用的 Redis directory/reverse transport 維持不變。

### 3.3 Gate enqueue 與 metrics

Gate 在 `Forward` 成功收到 optional reply 後，使用仍持有的原始 WebSocket session；非空
`ExpectedLoginName` 先以 `SessionRegistry.State` 驗證，再沿用 `outboundSourceServerSend`、既有 queue、writer
與 server-send metrics enqueue。Gate 不依 reply 內容重新 routing，也不為 request-player reply 發起 reverse RPC。

request-player 的 Game server-send counter 記錄 slot acceptance result；Gate server-send request/delivery metrics
則從收到 `ForwardResponse` 開始。既有 Player/Broadcast reverse receiver 的 metrics 語意不變。

### 3.4 必要 contract tests

只調整受 contract 變更影響的測試：

- `pkg/gatelink/gatelink_contract_test.go`：no-reply、payload copy、single/duplicate/concurrent slot、handler
  error／after-handler rejection 與 malformed response。
- `pkg/serversend/sender_contract_test.go`：active slot 寫入、missing slot、duplicate、invalid message 與
  payload bound；證明不需要 transport 或 Redis。
- Gate/Game full-flow contract：不啟動 Gate reverse listener，Echo 仍回到原始 connection；no-reply、identity、
  queue 與 handler error 行為沿用既有 policy。

不要新增 Redis command-level metric；移除 reverse dependency 已能在型別層級保證 direct sender 不執行 lookup。

## 4. 修改二：load client 分階段執行

### 4.1 必要 flags

在 `examples/metrics/load/main.go` 只新增：

- `-setup-timeout`：限制 Dial、Login、EnterRoom 與 warm-up 的總時間，預設 `2m`；
- `-setup-concurrency`：限制同時進行的 connection setup 數，預設 `32`，必須大於 `0`；
- `-warmup-requests`：每條 connection 在正式測量前完成的 Echo 次數，預設 `1`，允許 `0`。
- `-request-timeout`：限制單筆 Login／EnterRoom／Echo round trip，預設 `10s`，必須大於 `0`。

`-duration` 改為只表示正式 measured Echo phase，不包含 setup 或 warm-up。不要加入 ramp pattern、target RPS、
open-loop mode 或 percentile 計算。

### 4.2 最小內部資料結構

在 load package 內加入未匯出的 connection state；不建立新 reusable package：

```go
type preparedConnection struct {
	conn     *websocket.Conn
	sequence uint32
	name     string
}
```

將目前 `runConnection` 拆成幾個單一責任的 package-private helpers 即可：

- Dial connection；
- 對既有 connection 執行 Login／EnterRoom；
- 執行指定次數的 warm-up Echo；
- 對既有 prepared connection 執行 measured closed-loop Echo。

不要為四個 phase 建 interface、builder、worker framework 或公開 API。

### 4.3 固定執行順序

load process 必須依序完成：

1. 啟動既有 load `/metrics` observer。
2. 在 `setup-timeout` 內，以 `setup-concurrency` 為上限建立全部 WebSocket connections；此階段不送 application
   request。
3. 全部 Dial 成功後，以相同 concurrency 對每條 connection 依序完成 Login → EnterRoom。
4. 全部 session ready 後，每條 connection 完成 `warmup-requests` 次 closed-loop Echo。
5. 全部 warm-up 完成後記錄共同的 `measurementStart` 與 `admissionEnd = measurementStart + duration`，並同時
   啟動 measured Echo loops。
6. 到達 `admissionEnd` 後不再送出新的 Echo；每條 connection 當下最多一筆 in-flight request，允許它在
   `request-timeout` 內完成。
7. 所有 loops 收尾後記錄 `measurementEnd`、正常關閉 connections、關閉 observer，再輸出 summary。

任一 setup、Login、EnterRoom 或 warm-up 失敗都採 fail-fast：取消 setup、關閉已建立 connections，且不開始
measured phase。這能避免使用少於指定數量的 connections 卻仍把結果標示成該級負載。

setup concurrency 的目的只是在 macOS/Linux listen backlog 可接受的速度內建立 long-lived connections；正式
measured phase 仍同時維持並使用完整的 `-connections` 數量。

每筆 WebSocket round trip 都建立最長為 `request-timeout` 的 operation context，並在 read/write 前設定該
deadline。進入下一個 phase 或下一筆 request 時必須覆寫舊 deadline，不能讓 setup deadline 於正式測量期間
誤觸發。不要另建 timer abstraction；由既有 round-trip helper 在每次操作前設定 deadline 即可。

`duration` 是 request admission window，不是強制取消時間。這能確保已送出的最後一筆 Echo 有唯一 terminal
result，避免 Game 已成功處理但 client 因全域 deadline 記為 `cancelled`，導致跨服務 counter 無法精確對帳。
正常結束時間最多約為 `setup-timeout + duration + request-timeout + observer shutdown timeout`；收到 process
context cancellation 時才將進行中的 operation 記為 `cancelled`。一般 duration 到期不取消 request；真正的
request timeout 應記為 `error`，不是正常 `cancelled`。

### 4.4 Warm-up 與 metrics 邊界

warm-up 呼叫既有 Echo encode/write/read/validation path，但傳入 `nil` load metrics，因此不增加 load
`round_trips_total`、Histogram 或 in-flight Gauge。正式 measured phase 才呼叫既有 `loadMetrics.startEcho`／
`finishEcho`。

warm-up 使用固定 request count，而不是以到期 context 中斷 read。Gorilla WebSocket 在 read timeout 後不應
重用；固定次數可讓每筆 warm-up response 完整結束後再進入 measured phase。

Gate／Game metrics 仍會記錄 warm-up，這是正確的 server 行為。因此：

- correctness counter baseline 在 load process 啟動前取得；已知成功 warm-up 數
  `W = connections × warmup-requests`，server-side final delta 扣除 `W` 後才與 measured client counter 比較；
- Prometheus rate/latency 比較只選取 load summary 輸出的 `measurement_start` 至 `measurement_end`；
- 若任一 warm-up 失敗則不開始 measured phase，因此不會以不完整的 `W` 進行正式驗收。

成功 correctness run 的每個 Echo server stage 必須滿足：

```text
server success counter delta - W = load measured success counter
server success Histogram _count delta - W = load measured success Histogram _count
```

Prometheus `rate`／Histogram bucket rate 有 lookback window，measurement 開頭的圖仍可能包含 warm-up samples。
在 1 秒 scrape interval 下使用短而足夠的 window（例如 `[5s]`），並排除 measurement 開始後第一個完整
lookback window；不要為了清除 warm-up 而重建 framework registry 或重啟 services。

summary 至少輸出 requested/prepared connections、setup duration、warm-up requests、measurement start、
admission end、measurement end、successful Echo requests 與 connection failures。`cancelled` 仍由既有
Prometheus metric 表達，不需要為 log 再維護一份重複 counter。不要把 setup failure 算成 Echo error metric。

### 4.5 必要 load tests

在 `examples/metrics/load/main_test.go` 與既有 metrics tests 補足：

- 全部 WebSocket handshakes 完成前，不會送出 Login；
- 全部 Login／EnterRoom 完成前，不會送出 warm-up Echo；
- 全部 warm-up 完成前，不會開始 measured Echo；
- warm-up Echo 不增加 load metrics，measured Echo 才增加；
- 任一 setup phase 失敗時不開始 measured phase，且已建立 connections 都被關閉；
- `duration` 從 measured phase 開始計算，而且到期只停止新 request，不取消已送出的 request；
- 單筆 request timeout 會終止該 connection 並記為 error；
- measured phase 的 read/write deadline 不沿用 setup deadline；
- `setup-concurrency` 的實際同時工作數不超過設定值。

測試使用 `httptest`／Gorilla WebSocket 與 channel barrier 驗證 phase ordering；timeout 只作為測試終止上限，
不要以任意 sleep 猜測 goroutine 是否已進入某個 phase。

## 5. 文件與驗證指令的必要同步

實作時同步更新既有文件，但不另寫重複教學：

- `ACTIONABLE_METRICS_DESIGN.md`：修正 request-player 改走 unary reply slot，並把 load workflow 改為四階段；
- `ACTIONABLE_METRICS_EXAMPLE_VALIDATION.md`：加入 warm-up delta 扣除方式、measurement window，以及固定測試矩陣；
- `examples/metrics/README.md`：補四個 flags、admission/drain 終止條件與 phase summary 語意。

固定壓測矩陣使用相同 measured duration，且 server process 限制為四個 logical processors：

```sh
GOMAXPROCS=4 go run ./examples/metrics/game -config ./examples/metrics/configs/game.yaml
GOMAXPROCS=4 go run ./examples/metrics/gate -config ./examples/metrics/configs/gate.yaml

go run ./examples/metrics/load \
  -connections <100|200|400|800|1600|3200> \
  -setup-concurrency 32 \
  -setup-timeout 2m \
  -warmup-requests 1 \
  -request-timeout 10s \
  -duration 30s \
  -payload-bytes 32
```

每個級距使用新的 load process，並在開始下一級前確認前一級 connections 與 in-flight gauges 已回到 `0`。
Prometheus 維持既有 scrape 設定，不需要每次壓測後才抓 metrics；counter correctness 以 load 啟動前 baseline
與收尾後 final snapshot 的 delta 扣除已知 warm-up 數驗證，時間序列分析則使用固定 scrape interval 持續抓取。

## 6. 不修改項目

以下都不是解決目前問題的必要條件，本次明確不做：

- 不調整 OS `kern.ipc.somaxconn`、file descriptor 或 TCP sysctl；先由 bounded setup 排除 connection burst。
- 不改 Gate listener/backlog 實作，也不新增 application-level max connections。
- 不新增 Redis/DB command latency metrics；既有 pool saturation metrics 保留。
- 不修改 Gate/Game/load 現有 metric names、labels 或 Histogram buckets。
- 不新增 API/GMS traffic、額外 proto commands、failure injection、distributed workers、dashboard 或 alerts。
- 不把 example load client 移入正式 product/package；它已位於 `examples/metrics/load`，位置符合需求。
- 不改 `PlayerSender`、`BroadcastSender` 或其 route semantics。

## 7. Self review

上述兩組變更都直接對應已觀察到的問題：每筆 direct reply 的非必要 Redis lookup，以及大量 connections 下
setup 與 measured load 混合。unary response contract、single reply slot、sender dependency 與受影響 contract
tests 是移除 lookup 的最小閉環；setup barrier、bounded concurrency、單次 warm-up 與獨立 measured context 是取得可比較壓測結果的
最小閉環。

沒有要求新增 metrics，因現有 client round trip、Gate/Game stage、queue/in-flight、Redis/DB pool 與 Go/process
metrics 已覆蓋 latency、RPS、error、saturation。沒有將 OS tuning 當成修正，因 backlog 只影響 connection burst，
不代表已建立 WebSocket 的容量。也沒有把 request-scoped reply 改動錯誤擴張到 player routing 或 broadcast。

因此，本文件列出的內容皆為完成上述兩項行為 contract 所需的修改；第 6 節項目若加入，會超出目前需求。
