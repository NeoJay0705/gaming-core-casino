# GateLink gRPC trace 與 OS 邊界診斷執行方案

## 1. 目的與結論

目前 `Concurrency=400`、`ClientConn=1/2/4` 的 CPU profile 已足以排除 Echo handler、dispatcher 與
`GOMAXPROCS=4` CPU 硬飽和是主要原因，但不足以區分 grpc-go transport、Go scheduler 與 macOS socket
write path。

下一步先不修改 production code，也不先設定 `MaxConcurrentStreams=100`。現有 example 已提供獨立的
loopback pprof endpoint，因此只需在未改變 gRPC 設定的情況下，同步收集：

1. grpcload 與 Game 的 5 秒 Go execution trace；
2. 兩個 process 的 CPU、threads、context switches 與 syscall counters；
3. loopback TCP connection、吞吐與 socket queue；
4. trace 前後完整 `/metrics` response 與 completion log。

只有 baseline trace 顯示大量 RPC 疑似停在 stream admission，但仍無法判定是否為 stream quota 時，才新增
`MaxConcurrentStreams=100` positive control。positive control 是刻意製造已知限制來比對 trace signature，
不是效能修正或 production 建議值。

---

## 2. 本輪範圍

### 2.1 必要工作

- 固定 `Concurrency=400`、`GOMAXPROCS=4`、payload `32 bytes`、duration `30s`。
- 分別以 `ClientConn=1` 與 `4` 執行；每輪重新啟動 Game 與 grpcload binary。
- 在穩態同步收集 client/Game Go trace 及 macOS process/network samples。
- 保存完整原始產物，不再只保留 console snapshot。
- 依本文件第 6 節的規則判斷 grpc-go、scheduler 或 kernel/socket 候選。

選擇 `1` 與 `4` 是因為兩者 connection topology 差異最大，但先前 throughput 接近，最適合辨識增加 transport
後 goroutine、syscall 與等待狀態如何改變。`ClientConn=2` 只有在 `1/4` 結果矛盾時才補抓，避免無必要的第三輪
高 overhead trace。

### 2.2 本輪不做

- 不修改 `pkg/gatelink`、product config、protobuf 或 metrics contract。
- 不改 `MaxConcurrentStreams`、flow-control window、buffer、keepalive、retry 或 connection pool。
- 不用 trace run 的 RPS 作正式容量比較；trace 有 observer effect。
- 不把 `syscall.Write`／`rawsyscalln` 的 CPU sample 直接解釋為 OS saturation。
- 不要求安裝完整 Xcode，也不要求關閉 SIP 或使用 privileged DTrace。

---

## 3. 測試前置與固定條件

從 repository root 建立一次 binary；每輪只重啟 process，不在 process 執行中重新編譯：

```sh
diagnosis_dir=/tmp/gaming-core-casino-grpc-trace-$(date +%Y%m%d-%H%M%S)
mkdir -p "$diagnosis_dir"

go build -o "$diagnosis_dir/game" ./examples/metrics/game
go build -o "$diagnosis_dir/grpcload" ./examples/metrics/grpcload
```

每輪必須確認：

- Game gRPC：`127.0.0.1:19090`；
- Game observability：`127.0.0.1:19080`；
- Game pprof：`127.0.0.1:19182`；
- grpcload metrics：`127.0.0.1:22082`；
- grpcload pprof：`127.0.0.1:22083`；
- client 與 Game 都使用 `GOMAXPROCS=4`；
- 測試前上述 ports 沒有前一輪殘留 listener。

若機器同時有高負載工作，該輪作廢。`top` 的 host header 需一併保留，讓報告可確認整台機器的 idle CPU，而非
只看兩個 Go process。

---

## 4. 每輪具體執行方式

以下以 `ClientConn=1` 為例；完成並停止所有 process 後，將 `connection_count=4` 重新執行一次。所有 PID
都取自本輪剛啟動的 process，不以 process name 批次終止。

### 4.1 啟動 fresh Game 與 grpcload

```sh
connection_count=1
run_dir="$diagnosis_dir/conn-$connection_count"
mkdir -p "$run_dir"

GOMAXPROCS=4 "$diagnosis_dir/game" \
  -pprof-addr 127.0.0.1:19182 \
  >"$run_dir/game.log" 2>&1 &
game_pid=$!

until curl -fsS http://127.0.0.1:19080/ready >/dev/null; do
  sleep 0.2
done

GOMAXPROCS=4 "$diagnosis_dir/grpcload" \
  -game-target 127.0.0.1:19090 \
  -client-connections "$connection_count" \
  -concurrency 400 \
  -warmup-requests 1 \
  -duration 30s \
  -payload-bytes 32 \
  -request-timeout 10s \
  -metrics-addr 127.0.0.1:22082 \
  -pprof-addr 127.0.0.1:22083 \
  >"$run_dir/grpcload.log" 2>&1 &
load_pid=$!

until curl -fsS http://127.0.0.1:22082/metrics \
  | grep -q 'gaming_core_example_grpc_load_round_trips_total{result="success"}'; do
  sleep 0.2
done
```

若 Game readiness 或 grpcload success 在合理啟動時間內沒有出現，停止本輪並先檢查 log；不得在服務未 ready
時繼續收集空 trace。

### 4.2 保存 trace 前 metrics

```sh
curl -fsS http://127.0.0.1:22082/metrics \
  -o "$run_dir/client-metrics-before.prom"
curl -fsS http://127.0.0.1:19080/metrics \
  -o "$run_dir/game-metrics-before.prom"
```

### 4.3 同步收集 Go trace 與 host samples

`top` 的 `csw`、`sysbsd`、`sysmach` 是累積 counters；比較連續 samples 的 delta，不比較不同 process 的
絕對起始值。`nettop` 只觀察本輪兩個 PID 的 loopback TCP。

```sh
top -l 10 -s 1 -pid "$load_pid" \
  -stats pid,command,cpu,threads,mem,csw,sysbsd,sysmach \
  >"$run_dir/client-top.txt" &
client_top_pid=$!

top -l 10 -s 1 -pid "$game_pid" \
  -stats pid,command,cpu,threads,mem,csw,sysbsd,sysmach \
  >"$run_dir/game-top.txt" &
game_top_pid=$!

nettop -m tcp -n -t loopback -L 10 -s 1 \
  -p "$load_pid" -p "$game_pid" \
  >"$run_dir/nettop.csv" &
nettop_pid=$!

curl -fsS 'http://127.0.0.1:22083/debug/pprof/trace?seconds=5' \
  -o "$run_dir/client.trace" &
client_trace_pid=$!

curl -fsS 'http://127.0.0.1:19182/debug/pprof/trace?seconds=5' \
  -o "$run_dir/game.trace" &
game_trace_pid=$!

wait "$client_trace_pid" "$game_trace_pid"

curl -fsS 'http://127.0.0.1:22083/debug/pprof/goroutine?debug=2' \
  -o "$run_dir/client-goroutines.txt"
curl -fsS 'http://127.0.0.1:19182/debug/pprof/goroutine?debug=2' \
  -o "$run_dir/game-goroutines.txt"

netstat -anv -p tcp >"$run_dir/socket-snapshot.txt"

wait "$client_top_pid" "$game_top_pid" "$nettop_pid"
```

不另外重抓 CPU pprof；既有 profile 已回答 on-CPU hotspot，本輪新增的必要證據是 off-CPU、scheduler 與 socket
狀態。

### 4.4 保存 trace 後 metrics 並正常結束

```sh
curl -fsS http://127.0.0.1:22082/metrics \
  -o "$run_dir/client-metrics-after.prom"
curl -fsS http://127.0.0.1:19080/metrics \
  -o "$run_dir/game-metrics-after.prom"

wait "$load_pid"
load_status=$?

kill -TERM "$game_pid"
wait "$game_pid"

test "$load_status" -eq 0
```

本輪有效條件：

- grpcload exit status 為 `0`；
- completion log 的 `failed_workers=0`；
- client/Game trace 都可由 `go tool trace` 開啟；
- before/after metrics、兩份 `top`、`nettop` 與 socket snapshot 都存在且非空；
- Game 與 grpcload 在下一輪開始前已停止。

---

## 5. 分析方式

### 5.1 Go trace

```sh
go tool trace "$diagnosis_dir/conn-1/client.trace"
go tool trace "$diagnosis_dir/conn-1/game.trace"
go tool trace "$diagnosis_dir/conn-4/client.trace"
go tool trace "$diagnosis_dir/conn-4/game.trace"
```

依序檢查：

1. goroutine analysis 中 grpc-go transport 相關 goroutine 的主要 blocking state；
2. RPC workers 是否大量停在 stream creation/admission；
3. transport writer 是否長時間 runnable、被 scheduler 延遲，或停在 network poll/write；
4. Game handler goroutine是否快速完成後，response writer 才形成等待；
5. GC pause、assist 或 allocation 是否占據顯著區段。

trace UI 顯示的等待時間必須與 goroutine dump 中的具體 stack 一起判讀，不以單一 function name 定論。

### 5.2 Metrics

從 before/after raw exposition 計算 5 秒 trace 周邊窗口的 counter delta，至少記錄：

- success/error/cancelled request delta；
- histogram `_count`、`_sum` 與 buckets delta；
- process CPU counter delta；
- Go goroutines、GC 與 `GOMAXPROCS`；
- in-flight snapshot。

trace run 的 latency/RPS 只用來確認瓶頸仍可重現，不與先前未 profile 的正式 capacity sample 合併。

### 5.3 macOS host/network

- `top`：比較每秒 CPU、threads、`csw`、`sysbsd`、`sysmach` delta，並確認 host CPU idle。
- `nettop`：比較 connection 數、bytes/packets、RTT 與 retransmission-related columns；以實際輸出的可用欄位為準。
- `socket-snapshot.txt`：只篩選 PID 或 port `19090`，查看 `Recv-Q`／`Send-Q` 是否持續非零。
- 本機 `netstat -s` TCP protocol counters 經預檢顯示為零，因此不列為必要證據，避免保存無法判讀的全機器
  counter。

目前主機的 `xctrace` 因 active developer directory 只有 Command Line Tools 而無法使用，DTrace 也需要額外權限；
兩者都不是完成本輪診斷的前置條件。若無權限工具已足以定位，不再增加工具。

---

## 6. 判讀與停止條件

| 同時出現的證據 | 結論 | 下一步 |
|---|---|---|
| workers 大量停在 grpc-go stream admission/quota；Conn=4 明顯分散等待 | per-transport stream/admission 是候選 | 才進入第 7 節 positive control |
| workers/transport writer 長時間 runnable，host idle 仍充足 | Go scheduler 或單一 transport runnable contention | 用 trace stack 定位；不先調 OS |
| writer 停在 network poll/write，且 socket `Send-Q` 持續堆積 | kernel socket／接收端 backpressure 候選 | 再查接收端 reader 與 host socket 行為 |
| syscall/context-switch 隨 Conn 增加，但 throughput 不增、socket queue 不堆積 | 多 transport 增加 syscall/flush overhead | 排除 connection pool 是改善方向 |
| client/Game host CPU 合計接近整機 capacity、host idle 接近零 | host CPU scheduling 是限制 | 換隔離機器或拆開 client/server 驗證 |
| GC/assist 占比顯著且 metrics 同步顯示高 allocation/GC | Go allocation/runtime 候選 | 回到具體 allocation stack；不歸因 OS |

一旦 trace stack、goroutine dump 與 host sample 能一致指向同一類等待，即停止增加診斷 metrics。沒有證據時不得將
root cause 命名為 OS、grpc-go defect 或 `MaxConcurrentStreams`。

---

## 7. 條件式 `MaxConcurrentStreams=100` positive control

### 7.1 何時才需要

只有第 6 節第一列成立，但 baseline 無法確認是否真的受到 stream quota 時才實作。若 baseline 沒有 stream
admission wait，這個修改沒有診斷價值，應直接省略。

### 7.2 最小必要程式修改

若確定需要，採用現有 `ServerConfig` 的單一 optional field，不建立新的 option abstraction 或 connection
manager：

```go
type ServerConfig struct {
    ListenAddr          string `config:"listen_addr" yaml:"listen_addr"`
    MaxConcurrentStreams uint32 `config:"max_concurrent_streams" yaml:"max_concurrent_streams"`
}
```

在 `pkg/gatelink/server.go` 建立 `grpc.Server` 時：

1. 保留既有 unary interceptors；
2. field 為 `0` 時不加入 option，完全維持 grpc-go default；
3. field 大於 `0` 時才加入 `grpc.MaxConcurrentStreams(value)`。

測試只增加一個 integration contract：設定較小上限，以 blocking handler 證明同一 transport 不會同時進入超過
上限的 handler；釋放第一批 request 後，其餘 request 必須能完成。不要測 grpc-go 內部 function 或 sleep-based
timing。

controlled run 透過 example 專用 environment override 設定，不修改預設 YAML：

```sh
CORE_CASINO_METRICS_GAME__GATE_TO_GAME__MAX_CONCURRENT_STREAMS=100 \
GOMAXPROCS=4 "$diagnosis_dir/game" \
  -pprof-addr 127.0.0.1:19182
```

重新執行 `ClientConn=1/2/4`，固定其餘條件，預期人工 quota 的 waiting signature 隨可用 transports 改變。若
positive-control signature 與 baseline 不同，即可排除 baseline 是同類 stream quota；不得因設定 `100` 後變慢就
反推原本存在限制。

這個條件式實作預估約修改 production code `10–15` 行、contract test `30–50` 行；只有觸發條件成立時才是必要
變更。是否將該 config 長期保留為 public contract，需在取得 trace 證據後另行 review，不能由診斷需求直接決定。

---

## 8. Self review

- baseline 使用既有 endpoint 與 binary，不需要新增 application metrics 或修改 production code。
- `ClientConn=1/4` 已是回答 transport 數量差異所需的最小 trace matrix；`2` 保留為結果矛盾時的補測。
- CPU pprof 已有數據，因此不重複收集；新增 trace 是為了補足 CPU pprof 看不到的 off-CPU waiting。
- `top`、`nettop`、`netstat` 都是目前主機可直接使用且不需額外權限的必要觀測。
- `MaxConcurrentStreams=100` 被隔離為有明確觸發條件的 positive control，不先擴張 production config。
- 未加入 Channelz、OpenTelemetry、通用 benchmark framework、長時間 trace 或 production tuning，沒有超出目前
  根因診斷需求。

因此目前應先執行第 3～6 節；第 7 節不是本輪預設修改內容。
