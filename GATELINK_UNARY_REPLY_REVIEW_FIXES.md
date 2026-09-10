# GateLink Unary Reply staged changes 必要修正方法

## 1. 目的與範圍

本文件記錄依據 `GATELINK_UNARY_REPLY_DESIGN.md` review 目前 staged changes 後，仍需完成的最小修正。
修正只處理下列三件事：

1. 移除 unary reply 後已失去 actionability 的 Game request-player duration／in-flight metrics，保留必要的
   request rate 與 error counter。
2. 移除 reply ownership 已確立後的兩次額外 payload copy。
3. 補齊新 unary reply 分支直接需要的最小 contract tests，讓設計宣告的原 connection、identity 與失敗
   lifecycle 有實際保護。

現有 `ForwardResponse`、single reply slot、`RequestPlayerSender` interface、Gate 原 session enqueue、
WebSocket queue/writer，以及 Player／Broadcast reverse transport 的架構均不需重構。

本次不得新增 metric family、label、bucket config、streaming、delivery acknowledgment、retry、queue、
tracing 或另一套 reply abstraction；也不得刪除 `ReplyEndpoint`、Gate reverse listener、PlayerSender 或
BroadcastSender，因為它們仍服務 mixed-version rollout 與一般 server push。

---

## 2. 修正一：移除非必要的本機 request-player duration 與 in-flight

### 2.1 問題

改成 unary reply slot 後，`measuredRequestPlayerSender.SendToRequestPlayer` 只做：

1. message／payload validation；
2. 一次 bounded payload copy；
3. 取得 request-scoped slot；
4. 以 mutex 接受零或一筆 reply。

`gaming_core_game_server_send_duration_seconds{operation="request_player"}` 因此不再量測 Redis、gRPC、
Gate enqueue 或 WebSocket delivery，只是 `gaming_core_game_gate_command_duration_seconds` 中極短的本機子區段。
使用目前 `prometheus.DefBuckets` 時，第一個 bucket 為 5ms，正常 slot acceptance 通常全部落在第一個 bucket，
p95 無法提供有效解析度。為這個本機操作另調 microsecond buckets 也沒有對應的營運處置，會擴大不必要的
metric surface。

`gaming_core_game_server_send_in_flight{operation="request_player"}` 同樣只涵蓋極短的同步函式，正常 scrape
幾乎只會得到 0，不能代表 Game handler、gRPC 或其他有限資源的 saturation。

`gaming_core_game_server_send_requests_total{operation="request_player",result=...}` 仍然必要，因為它可觀察：

- handler 是否呼叫 request-player sender；
- call RPS；
- invalid payload、missing slot、duplicate reply 等 contract error。

### 2.2 `products/gameproduct/metrics.go`

`gameMetrics` 只刪除下列兩個欄位：

```go
serverSendDuration *prometheus.HistogramVec
serverSendInFlight *prometheus.GaugeVec
```

在 `newGameMetrics` 中刪除對應的 Histogram／Gauge 建立與 collector registration，保留：

```go
serverSendRequests *prometheus.CounterVec
```

`measuredRequestPlayerSender.SendToRequestPlayer` 縮減為只記錄 terminal counter：

```go
func (s *measuredRequestPlayerSender) SendToRequestPlayer(
	ctx context.Context,
	message serversend.RequestPlayerMessage,
) (serversend.Receipt, error) {
	if s == nil || s.delegate == nil {
		return serversend.Receipt{}, errors.New("game server send: request player sender is not configured")
	}

	receipt, err := s.delegate.SendToRequestPlayer(ctx, message)
	result := "success"
	if err != nil {
		result = "error"
	}
	if s.metrics != nil {
		s.metrics.serverSendRequests.WithLabelValues("request_player", result).Inc()
	}
	return receipt, err
}
```

不要移除 `measuredRequestPlayerSender`：counter 仍需在 interface decorator 統一記錄成功與錯誤，也不要把
metrics 操作放進 `pkg/serversend`，以免 transport-neutral package 綁定 product metrics。

### 2.3 測試與文件同步

只移除已不存在 metric 的 assertion／敘述：

- `products/gameproduct/metrics_contract_test.go`
  - 保留 request-player success/error counter assertions；
  - 刪除 duration sample count 與 in-flight zero assertions。
- `examples/metrics/flow_contract_test.go`
  - 刪除 `gaming_core_game_server_send_in_flight` assertion；
  - 保留每筆 Echo 恰有一筆 request-player success counter 的 assertion。
- `ACTIONABLE_METRICS_DESIGN.md`
  - Game metrics 表刪除 request-player duration 與 in-flight 兩列；
  - latency/saturation 敘述不再把 slot acceptance 當成獨立瓶頸階段。
- `ACTIONABLE_METRICS_EXAMPLE_VALIDATION.md`
  - Game request-player 只列 `gaming_core_game_server_send_requests_total`；
  - p95 查詢清單刪除 Game server-send；
  - saturation 清單刪除 Game server-send in-flight；
  - 刪除「Game server-send 升高時檢查 unary response transport」的錯誤判讀。Unary response transport 只能由
    Gate 的 `gaming_core_gate_game_grpc_duration_seconds` 與 Game handler 指標綜合分析。
- `ACTIONABLE_METRICS_NEXT_CHANGES.md`
  - 將「Game server-send duration 只涵蓋 slot acceptance」改成「Game request-player counter 記錄本機
    acceptance result」。
- `GATELINK_UNARY_REPLY_DESIGN.md`
  - Metrics 表刪除 Game request-player duration；
  - 明確記錄這次 transport 改變後選擇刪除低價值 Histogram／Gauge，但保留 request counter。

不新增 replacement duration。必要的 latency 與 saturation 已由 Game handler、Gate-to-Game gRPC、Gate
command、write queue、WebSocket write、Go/process 與實際使用的 Redis／DB pool metrics 覆蓋。

### 2.4 預估修改量

- production code：約刪除 18–25 行；
- tests：約刪除 8–12 行；
- 文件：約修改 12–20 行。

---

## 3. 修正二：只保留必要的 payload ownership copies

### 3.1 Ownership contract

payload 只需要在兩個跨 ownership boundary 的位置複製：

1. `SetForwardReply` 寫入 slot 時複製，確保 business caller 後續修改原 slice 不影響 reply。
2. `Client.Forward` 把 protobuf response 映射為公開 `Reply` 時複製，讓 caller 不依賴 protobuf buffer。

slot 中的 payload 已由 `pkg/gatelink` 獨占；handler 返回、`finish` 關閉 slot 後，可以把 ownership 直接移交
給 `ForwardResponse`。不需要在同一 package 內再複製兩次。

### 3.2 `pkg/gatelink/reply.go`

將 `forwardReplySlot.finish` 的成功分支改為移交 pointer：

```go
reply := s.reply
s.reply = nil
return reply
```

取代：

```go
reply := s.reply.clone()
s.reply = nil
return &reply
```

mutex、`closed`、discard 與 duplicate 行為完全不變。

### 3.3 `pkg/gatelink/server.go`

組裝 response 時直接轉移 slot 已擁有的 payload：

```go
response.Reply = &ForwardReply{
	CommandId:         reply.CommandID,
	Payload:           reply.Payload,
	ExpectedLoginName: reply.ExpectedLoginName,
}
```

不得刪除 `SetForwardReply` 與 `Client.Forward` 的 defensive copy。現有 payload mutation tests 應原樣通過；
不為 allocation 數量增加容易受 compiler/runtime 影響的 brittle test 或 benchmark gate。

### 3.4 預估修改量

production code 約修改 3–5 行，不需新增 production abstraction 或測試 helper。

---

## 4. 修正三：補齊最小 unary reply contract tests

### 4.1 `pkg/gatelink`：設定 reply 後 panic 必須丟棄

調整既有 `TestContractRecoversHandlerPanicAndKeepsServerAvailable`：第一次 handler call 先成功執行
`SetForwardReply`，再 panic。第一筆 `Client.Forward` 必須同時符合：

- `reply == nil`；
- gRPC status 為 `Internal`。

第二筆 call 仍成功，證明 panic recovery 後 server 可用。直接擴充既有測試，不新增另一個 server fixture。

同一檔案再增加一個已取消 context 的小型 slot test：已取消的 context 呼叫 `SetForwardReply` 必須回傳
`context.Canceled`，slot 最後不得含 reply。這直接鎖定設計已宣告的 cancellation contract，不建立 timing race。

### 4.2 `pkg/serversend`：補 invalid message 與 adapter copy contract

擴充既有 direct sender contract：

- `CommandID == 0` 必須回 `ErrMessageInvalid`，不得因沒有 slot 而先回 route error；
- 使用可變 `payload` 呼叫 sender，呼叫成功後修改原 slice，最後的 `ForwardResponse` 必須仍保留原內容；
- 繼續驗證 `ExpectedLoginName` 被完整映射。

不重複測試 gatelink client output copy；該 ownership boundary 已由 `pkg/gatelink` contract test 負責。

### 4.3 Gate product：原 connection 與 identity

擴充既有 `TestGateWebSocketContractUnaryGameReplyReturnsToOriginalConnection`，沿用同一組 Game／Gate app：

1. 建立並完成 Login／EnterRoom 的 `alice`、`bob` 兩條 WebSocket connections。
2. 由 `alice` 發出 Echo；handler 回覆 `ExpectedLoginName: "alice"`。
3. 驗證 `alice` 收到正確 reply。
4. 對 `bob` 設短 read deadline，驗證沒有收到該 command；完成 assertion 後不再重用此 connection。
5. 再建立並初始化 `carol` connection，由 handler 回傳不相符的 `ExpectedLoginName`。
6. 驗證 `carol` 沒有收到 payload，且連線依現有 `forward_error` policy 關閉。

handler 可依 request payload 選擇 expected identity；這只是 test control，不加入 production command、config
或 metric label。不要透過 Redis、Gate directory 或 reverse listener驗證 unary reply，因為該測試的目的正是
證明它們不是 request-player dependency。

### 4.4 Gate product：queue-full reason 不得被覆蓋

擴充 `TestWebSocketQueueMetricsContract`，讓造成 queue full 的第二筆 message 使用：

```go
outboundMessage{
	data:       []byte("second"),
	source:     outboundSourceServerSend,
	receivedAt: time.Now(),
	target:     serverSendTargetConnection,
}
```

`sendOutbound` 返回 `errWebSocketWriteQueueFull` 後，再模擬 caller 執行：

```go
_ = connection.closeWithReason(closeReasonForwardError)
```

最終 close reason 必須仍是 `write_queue_full`，並驗證 queue-full counter 的 `source="server_send"`。這個
小型 contract 已足以保護 unary caller 不覆蓋第一個 terminal reason，不需要建立不穩定的 full network
queue saturation test。

### 4.5 已有覆蓋，不重複新增

以下 contract 已由 staged 或既有測試覆蓋，不再增加相同案例：

- handler 不呼叫 sender時，gatelink 回傳 no-reply；
- slot 只接受第一筆 reply，包含 concurrent calls；
- handler error 後 reply 被丟棄；
- malformed response 與 command zero；
- Gate full-flow 不啟動 reverse listener仍可完成 Echo；
- WebSocket writer success/error/dropped terminal metrics；
- PlayerSender／BroadcastSender 仍走既有 reverse transport。

### 4.6 預估修改量

- gatelink／serversend tests：約 15–25 行；
- Gate product tests：約 30–45 行。

---

## 5. 修正後 metrics 邊界

| 觀測值 | 精確範圍 | 是否保留 | 可採取的行動 |
|---|---|---|---|
| Gate command duration | packet 解析完成後進入 dispatch，到 Forward reply enqueue 完成 | 是 | 與 gRPC 比較，判斷 Gate 本機前／後處理是否升高 |
| Gate → Game gRPC duration | Gate 呼叫 `Forward` 到收到並驗證 `ForwardResponse` | 是 | 檢查 Game handler、gRPC transport、deadline與 runtime saturation |
| Game handler duration | Game product handler 進入到 dispatcher 返回 | 是 | 檢查 dispatcher／business handler 與 Game runtime |
| Game request-player requests | sender terminal success/error counter | 是 | 發現 missing slot、duplicate、invalid payload 或 handler call 數異常 |
| Game request-player duration | 本機 validation／copy／mutex | 否 | 與 handler 重疊，無獨立 dependency或有效 p95 resolution |
| Gate server-send request | 收到 logical reply後的 ignored/error/queued counter | 是 | 判斷 identity 或 enqueue failure；它不是 latency metric |
| Server-send delivery duration | Gate 接受 reply 到 write success/error/drop | 是 | 配合 write duration與 queue gauge判斷排隊／slow client |
| WebSocket write duration | writer 執行單次 `WriteMessage` | 是 | 判斷 socket write 或 client read 壓力 |

不得以 Histogram p95 相減推導某段 latency。只有 sample 一對一且測量窗口一致時，才可用 `_sum/_count`
平均值比較 Gate gRPC 與 Game handler，估算 transport／scheduling residual；需要逐筆因果關係時應另案使用
tracing，不在本次加入。

---

## 6. 驗證順序

實作後依序執行：

```sh
gofmt -w pkg/gatelink/reply.go pkg/gatelink/server.go \
  products/gameproduct/metrics.go \
  pkg/gatelink/gatelink_contract_test.go \
  pkg/serversend/sender_contract_test.go \
  products/gameproduct/metrics_contract_test.go \
  products/gateproduct/websocket_contract_test.go \
  products/gateproduct/websocket_metrics_contract_test.go \
  examples/metrics/flow_contract_test.go

go test ./pkg/gatelink ./pkg/serversend
go test ./products/gameproduct ./products/gateproduct
go test ./examples/metrics/...
go test ./...
go test -race ./pkg/gatelink ./pkg/serversend ./products/gameproduct ./products/gateproduct ./examples/metrics/...
go vet ./...
git diff --check
```

若需保留目前 staging，實作與驗證期間不得執行 `git add`、`git reset`、`git restore --staged`、
`git checkout` 或其他會改變 index 的操作。測試通過後先 review unstaged diff，再由使用者決定如何更新 staging。

---

## 7. 修改量總估算

必要調整合計約 **75–115 行變更**：

- production：約 21–30 行，主要是刪除 metrics instrumentation與移除兩次額外 copy；
- tests：約 45–70 行；
- 文件：約 12–20 行。

generated protobuf、public Go interface、proto wire fields、config schema 與 reverse transport均不需再修改。

---

## 8. Self review：必要性與範圍控制

| 檢查項目 | 結論 |
|---|---|
| unary reply 架構是否需重做 | 否；現有 response、slot與原 session enqueue正確 |
| 是否保留 latency／RPS／error／saturation | 是；只移除沒有獨立資源或可行動處置的本機 duration／in-flight |
| 是否仍能觀察 request-player call 與 error | 是；保留 bounded `requests_total{operation,result}` |
| 是否改變一筆 request最多一筆 reply | 否；single slot與 duplicate error不變 |
| 是否降低 payload ownership安全 | 否；保留真正跨 ownership boundary 的兩次 copy |
| 是否新增測試專用 production介面 | 否；只擴充既有 contract tests與 fixture |
| 是否刪除一般 server push能力 | 否；Player／Broadcast與 reverse transport不變 |
| 是否加入未要求的監控能力 | 否；不增加 metric、label、dashboard、alert或 tracing |
| 是否為未來可能需求預留抽象 | 否；不加入 multi-reply、streaming、ack、retry或 queue |

Review 後只有上述三組調整同時符合「直接修正目前設計／實作缺口」與「不擴張需求」。其他可能改動都不是
完成 Gate-to-Game unary 原路回覆及其 actionable metrics 所必需，不應納入本次。
