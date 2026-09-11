# Actionable metrics example

這組範本讓 Gate 與 Game 走一個可實際壓測的 example workflow：Login → EnterRoom → Echo。
Echo 由 Gate forward 至 Game，Game 將 reply 寫入原始 `Forward` unary response，Gate 收到後
enqueue 原始 session，再由 Gate 的 WebSocket writer 完成 binary write。API/GMS 只啟動 lifecycle 與共用的
`/health`、`/ready`、`/metrics` listener，不虛構業務流量。

先啟動 Redis（預設 `127.0.0.1:6379`），再在 repository root 執行：

```sh
go run ./examples/metrics/game
go run ./examples/metrics/gate
go run ./examples/metrics/load \
  -connections 8 \
  -setup-concurrency 32 \
  -setup-timeout 2m \
  -warmup-requests 1 \
  -request-timeout 10s \
  -duration 30s
```

Load 預設使用完整的 `game` Echo 路徑；若要隔離 Gate WebSocket 與 local handler，可用相同設定另跑
`-echo-route=local`。兩種 route 都會先 Login、EnterRoom，再以相同 payload 執行 warm-up 與正式 Echo；
local route 只在 Gate 回覆，不呼叫 Game。每次執行只選一種 route，結束 log 會輸出 `echo_route`，方便與
Prometheus snapshots 對照。

各服務的 `/metrics` endpoint 分別是 Game `http://127.0.0.1:19080/metrics`、Gate
`http://127.0.0.1:18081/metrics`、API `http://127.0.0.1:20081/metrics`、GMS
`http://127.0.0.1:21081/metrics`；同一 listener 也提供 `/health` 與 `/ready`。
四個 framework product 的 production pprof 由 `observability.pprof_listen_addr` 控制；空白值為預設關閉，
且只接受 loopback address。它會使用獨立 listener，不會出現在上述 `/metrics` listener：

```sh
CORE_CASINO_METRICS_GAME__OBSERVABILITY__PPROF_LISTEN_ADDR=127.0.0.1:19082 \
  go run ./examples/metrics/game
```

operator 可透過相同 network namespace 的 SSH tunnel 或 `kubectl port-forward` 存取；不要將 pprof
address 設為 wildcard，也不要把 pprof listener 加入 Prometheus scrape。下方 `-pprof-addr` 只保留給
example/benchmark 的獨立 helper 使用；同一個 process 不要同時以 config 與 flag 綁定同一個 port。
Load client 另外在 `http://127.0.0.1:22081/metrics` 暴露 example-local Echo round-trip Counter、
Histogram、in-flight Gauge，以及 Go runtime/process collectors，可用 `-metrics-addr` 調整 listener。這個 endpoint 只觀測壓測端，
不屬於 framework product；完整的 client／Gate／Game 比對方法見 repository root 的
`ACTIONABLE_METRICS_EXAMPLE_VALIDATION.md`。

若要單獨隔離 GateLink gRPC transport（不經 WebSocket、Login、EnterRoom 或 Gate writer），可啟動 direct
gRPC load client。它固定呼叫本範例的 Echo command，使用固定總 worker 數，只有
`-client-connections` 是 A/B 變因：

```sh
go run ./examples/metrics/game
go run ./examples/metrics/grpcload \
  -game-target 127.0.0.1:19090 \
  -client-connections 1 \
  -concurrency 400 \
  -warmup-requests 1 \
  -duration 30s
```

direct client 預設在 `http://127.0.0.1:22082/metrics` 提供上述 metrics，可用 `-metrics-addr` 調整。

在相同 `-concurrency`、`-payload-bytes`、`-duration` 與 `GOMAXPROCS=4` 下依序比較
`-client-connections 1`、`2`、`4`，每個組合至少重複三次並交錯順序。每輪記錄 grpcload
`completed_rps`，並從 `/metrics` 讀取
`gaming_core_example_grpc_load_round_trips_total`、
`gaming_core_example_grpc_load_round_trip_duration_seconds` 與
`gaming_core_example_grpc_load_round_trips_in_flight`；`result="error"` 或測量結束後
in-flight 非零的 run 不具比較資格。此 direct 結果只代表 loopback 與 plaintext gRPC，不能單獨取代完整
Gate/WebSocket 壓測。

要判斷是否已到達 capacity knee，先固定 `-client-connections 1`，以相同 payload 與 duration
測試 concurrency `100`、`200`、`400`、`800`（每組至少三次）。若提高 concurrency 後 median RPS
已沒有超過 run-to-run variation 的增加，但 p95/p99 latency 持續上升，才表示該組合已接近飽和；
`in_flight` 只是 active unary requests，在 closed-loop 穩態下接近 concurrency 是預期行為，不是
saturation ratio。再於該 concurrency 固定總 workers 比較 `ClientConn=1/2/4`，避免把 offered load
誤認成 connection 效果。

最小 PromQL（以 scrape interval 小於查詢窗口為前提）如下：

```promql
# completed RPS（只計成功）
rate(gaming_core_example_grpc_load_round_trips_total{result="success"}[30s])

# 成功 request 的 p95 latency
histogram_quantile(0.95, sum by (le) (
  rate(gaming_core_example_grpc_load_round_trip_duration_seconds_bucket{result="success"}[30s])
))

# error rate
rate(gaming_core_example_grpc_load_round_trips_total{result="error"}[30s])

# active unary requests；不是 saturation ratio
gaming_core_example_grpc_load_round_trips_in_flight

# grpcload 約略使用的 Go CPU capacity 比例（job label 依 Prometheus 設定調整）
rate(process_cpu_seconds_total{job="grpcload"}[30s])
/
go_sched_gomaxprocs_threads{job="grpcload"}
```

CPU 比例接近 `1` 表示該 process 接近可用 `GOMAXPROCS`；若 CPU 未接近上限但增加
`ClientConn` 可持續改善 RPS，才支持 single-transport contention 候選。CPU、latency knee 與
pprof／trace 必須一起判讀，不能由單一 Gauge 宣稱飽和。

需要 function-level evidence 時，framework product 使用上述 config；standalone example/benchmark
才使用 `-pprof-addr`。不帶該設定時不會建立 listener：

```sh
GOMAXPROCS=4 go run ./examples/metrics/game -pprof-addr 127.0.0.1:19082
GOMAXPROCS=4 go run ./examples/metrics/gate -pprof-addr 127.0.0.1:18082
GOMAXPROCS=4 go run ./examples/metrics/grpcload \
  -client-connections 1 \
  -pprof-addr 127.0.0.1:22083

go tool pprof -http=:0 \
  'http://127.0.0.1:19082/debug/pprof/profile?seconds=20'
curl -o /tmp/game-gatelink.trace \
  'http://127.0.0.1:19082/debug/pprof/trace?seconds=5'
go tool trace /tmp/game-gatelink.trace
```

pprof／trace run 只用來找 CPU、goroutine、network block 或 scheduler hotspot，不納入正式 RPS／latency
比較。只有在未 profile 的 direct `1/2/4 ClientConn` A/B 至少三輪可重現、CPU／runtime saturation 未由
其他 process 先達上限，且 profile stack 與 A/B 方向一致時，才可把結果寫成 root-cause candidate；不能
僅憑 broad Gate unary latency 或單次 pprof 宣稱 `MaxConcurrentStreams`、grpc-go defect 或硬體極限。
只要更換 `-config` 即可調整 listener 或 Redis 設定；範例 login 是 in-memory policy，不能
當成正式 authentication。EnterRoom 與 Echo 的 protobuf 與 command IDs 位於
`examples/metrics/internal/protocol`，只屬於本範例，不是 product 的 production API；正式服務應
在自己的 module 註冊實際業務 command。

WebSocket application frame 沿用既有 16-byte big-endian header：`command_id uint32`、
`total_length uint32`、`sequence uint32`、`session uint16`、`version uint16`，後面接 protobuf
payload。範本 command IDs 為 Login request/response `0xC00002`/`0xC00003`、EnterRoom
request/response `0xF1000001`/`0xF1000002`、Echo request/response
`0xF1000011`/`0xF1000012`；Gate-local Echo request/response `0xF1000021`/`0xF1000022`。

API/GMS：

```sh
go run ./examples/metrics/api
go run ./examples/metrics/gms
```

Load client 是 bounded closed-loop client。它先以 `-setup-concurrency` 建立全部 WebSocket connections，
再完成 Login／EnterRoom 與 `-warmup-requests` 次 Echo，全部 ready 後才進入 `-duration` 的正式測量。
`duration` 到期只停止新的 Echo；最後一筆 in-flight request 由 `-request-timeout` bounded drain。
`-setup-timeout` 限制 setup 與 warm-up 總時間；若任一 setup phase 失敗，不會開始正式測量。
`-payload-bytes` 上限保留既有 1 MiB packet 的 framing 空間，不提供任意 delay、錯誤注入或通用 load framework。
