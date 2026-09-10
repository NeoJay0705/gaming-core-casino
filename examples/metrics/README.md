# Actionable metrics example

這組範本讓 Gate 與 Game 走一個可實際壓測的 example workflow：Login → EnterRoom → Echo。
Echo 由 Gate forward 至 Game，Game 透過 direct request-player server-send 回到原始 Gate，
再由 Gate 的 WebSocket writer 完成 binary write。API/GMS 只啟動 lifecycle 與共用的
`/health`、`/ready`、`/metrics` listener，不虛構業務流量。

先啟動 Redis（預設 `127.0.0.1:6379`），再在 repository root 執行：

```sh
go run ./examples/metrics/game
go run ./examples/metrics/gate
go run ./examples/metrics/load -connections 8 -duration 30s
```

各服務的 `/metrics` endpoint 分別是 Game `http://127.0.0.1:19080/metrics`、Gate
`http://127.0.0.1:18081/metrics`、API `http://127.0.0.1:20081/metrics`、GMS
`http://127.0.0.1:21081/metrics`；同一 listener 也提供 `/health` 與 `/ready`。
Load client 另外在 `http://127.0.0.1:22081/metrics` 暴露 example-local Echo round-trip Counter、
Histogram 與 in-flight Gauge，可用 `-metrics-addr` 調整 listener。這個 endpoint 只觀測壓測端，
不屬於 framework product；完整的 client／Gate／Game 比對方法見 repository root 的
`ACTIONABLE_METRICS_EXAMPLE_VALIDATION.md`。
只要更換 `-config` 即可調整 listener 或 Redis 設定；範例 login 是 in-memory policy，不能
當成正式 authentication。EnterRoom 與 Echo 的 protobuf 與 command IDs 位於
`examples/metrics/internal/protocol`，只屬於本範例，不是 product 的 production API；正式服務應
在自己的 module 註冊實際業務 command。

WebSocket application frame 沿用既有 16-byte big-endian header：`command_id uint32`、
`total_length uint32`、`sequence uint32`、`session uint16`、`version uint16`，後面接 protobuf
payload。範本 command IDs 為 Login request/response `0xC00002`/`0xC00003`、EnterRoom
request/response `0xF1000001`/`0xF1000002`、Echo request/response
`0xF1000011`/`0xF1000012`。

API/GMS：

```sh
go run ./examples/metrics/api
go run ./examples/metrics/gms
```

Load client 是 bounded closed-loop client；`-payload-bytes` 上限保留既有 1 MiB packet 的 framing
空間，不提供任意 delay、
錯誤注入或通用 load framework。
