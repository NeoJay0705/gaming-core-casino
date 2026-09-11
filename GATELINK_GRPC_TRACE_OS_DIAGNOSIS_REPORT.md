# GateLink gRPC trace 與 macOS OS 診斷實測報告

## 1. 執行摘要

本報告記錄依據 GATELINK_GRPC_TRACE_OS_DIAGNOSIS_PLAN.md 完成的容量重複測試與
Go trace／macOS host 觀測。測試期間保留 OrbStack，Redis 維持可用；沒有修改 production
code 的預設行為或 staging。依照本次 positive-control 要求，另外加入可選的
`MaxConcurrentStreams` 設定；只有明確設定大於 0 時才傳給 gRPC server。

容量基準在 Concurrency=400、GOMAXPROCS=4、32-byte Echo、30 秒 closed-loop 下，
分別對 ClientConn=1 與 4 各執行三輪。三輪均 failed_workers=0、error delta=0：

- Conn=1 的 RPS 中位數為 118,301.609，平均 latency 中位數為 3.382284 ms。
- Conn=4 的 RPS 中位數為 119,988.351，平均 latency 中位數為 3.224105 ms。
- Conn=4 相對 Conn=1 僅增加 1.43% RPS、降低 4.68% 平均 latency；這個差異小於單輪波動。

Trace 顯示 client goroutine 在 gRPC HTTP/2 transport 的
http2Client.operateHeaders 喚醒後承受主要 scheduler runnable delay；這不是該函式的 CPU
使用率。CPU／syscall profile 則顯示實際執行成本集中在 HTTP/2 writer／socket write path。
原始 baseline dump 沒有穩定出現可直接命名的 `streamQuota`／`quotaPool` stack；本次以
同條件 default=0 對照 `MaxConcurrentStreams=100`，再驗證已知的 `NewStream` admission
等待 signature。

測試期間 host idle 平均約 8%，且 client／Game 的主要 on-CPU 與 syscall stack 都在 gRPC
HTTP/2 transport。綜合容量、runtime、trace、CPU profile 與 positive control，可以判定本次
同機 benchmark 的容量上限位於「gRPC transport／kernel syscall／shared-host scheduler」邊界，
而不是 Echo handler、Redis、GC、ClientConn 數量或 `MaxConcurrentStreams`。這是本次測試的
明確結論；它不等同於 macOS、grpc-go defect 或 Game 單機 production 容量。只有要進一步拆分
client、Game 與 host 各自的容量占比時，才需要分離到不同 host 或 VM。

## 2. 測試範圍與拓撲

本輪使用 direct gRPC Echo load，路徑如下：

    grpcload workers
      -> gatelink.Client / grpc.ClientConn
      -> Game GateLink Forward
      -> Game dispatcher / Echo handler / unary reply
      -> grpcload

這不是完整 Gate WebSocket 路徑。Echo request 本身不使用 Redis；Redis 是 Game example
啟動所需的外部依賴。每輪都重新啟動 Game 與 grpcload，並在結束後停止本輪 process。

固定條件：

| 項目 | 設定 |
|---|---|
| Concurrency | 400 closed-loop workers |
| ClientConn | 容量基準使用 1、4，各三輪；trace 使用 1、4，各一輪 |
| GOMAXPROCS | client 與 Game 均為 4 |
| payload | Echo 32 bytes |
| warm-up | 每個 ClientConn 1 request |
| measured duration | 30s |
| request timeout | 10s |
| network | macOS loopback、plaintext gRPC |
| gRPC 設定 | baseline 使用 default=0；positive control 使用 `MaxConcurrentStreams=100` |

容量基準不開啟 pprof。trace run 另外開啟 loopback pprof，抓取 5 秒 Go trace；
trace run 的 RPS 不與容量中位數混合。

## 3. 容量基準：三輪中位數

每輪的 latency 是 metrics before／after counter delta 的平均值，不是 p50。

| Conn | RPS 第 1 輪 | RPS 第 2 輪 | RPS 第 3 輪 | RPS 中位數 | 平均 latency 第 1 輪 (ms) | 第 2 輪 (ms) | 第 3 輪 (ms) | latency 中位數 (ms) | error delta | failed_workers |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 105,754.242 | 118,301.609 | 119,943.847 | 118,301.609 | 3.872982 | 3.357770 | 3.382284 | 3.382284 | 0 | 0 |
| 4 | 119,988.351 | 107,766.032 | 135,605.996 | 119,988.351 | 3.224105 | 3.799365 | 2.933021 | 3.224105 | 0 | 0 |

原始 completion log 與 before／after metrics 位於：

    /tmp/gaming-core-casino-grpc-median.kYlWXW/

摘要檔案為：

    /tmp/gaming-core-casino-grpc-median.kYlWXW/capacity-summary.tsv

判讀：

- Conn=1 的 RPS 範圍為 105,754.242–119,943.847。
- Conn=4 的 RPS 範圍為 107,766.032–135,605.996。
- Conn=4 的中位數改善只有 1.43%，不足以證明增加 HTTP/2 transport 能改善容量。
- in_flight 約等於 400 是 closed-loop 負載設計的預期值，不是 saturation ratio。

## 4. Trace run 的 metrics

trace run 只作等待路徑診斷。兩輪 completion log 均成功：

| Conn | successful_requests | failed_workers | trace-run RPS |
|---:|---:|---:|---:|
| 1 | 3,463,712 | 0 | 115,450.746 |
| 4 | 3,968,045 | 0 | 132,262.064 |

trace-run RPS 受到 pprof trace、goroutine dump 與 host scheduler observer effect 影響，
不納入容量比較。

從 trace 前後 client metrics 取得的診斷窗口：

| 指標 | Conn=1 | Conn=4 |
|---|---:|---:|
| success counter delta | 473,846 | 618,180 |
| histogram mean latency | 4.304670 ms | 3.311480 ms |
| histogram p50 bucket | (2.5 ms, 5 ms] | (2.5 ms, 5 ms] |
| histogram p95 bucket | (5 ms, 10 ms] | (5 ms, 10 ms] |
| in_flight before → after | 400 → 400 | 400 → 400 |
| process CPU counter delta | 9.792 s | 12.773 s |
| GC count delta / cumulative GC time | 694 / 0.103 s | 858 / 0.139 s |
| go_goroutines before → after | 415 → 415 | 430 → 430 |
| go_threads before → after | 11 → 11 | 10 → 12 |

Game 的 trace-window process CPU counter delta 分別約為 9.451 秒與 12.502 秒；
Game metrics 沒有新增業務指標，只有既有 process／Go runtime collectors。

## 5. Go trace 與 goroutine evidence

原始 trace 與由 go tool trace -pprof 產生的分析檔位於：

    /tmp/gaming-core-casino-grpc-trace-20260911-113757/

### 5.1 Scheduler 與 synchronization

| Profile | Conn=1 | Conn=4 |
|---|---:|---:|
| client scheduler delay：http2Client.operateHeaders | 91.37% | 90.45% |
| client synchronization delay：runtime.selectgo | 96.63% | 93.34% |
| client goroutine 中 waitOnHeader stack | 370 | 346 |
| 原始 dump 中可直接命名的 `streamQuota`／`quotaPool` stack | 0 | 0 |

http2Client.operateHeaders 的百分比來自 scheduler delay profile，表示 goroutine
可執行但等待被排程的時間，不能解讀成該函式自身占用相同比例的 CPU。
runtime.selectgo 與 waitOnHeader 符合 closed-loop workers 等待 unary response 的模型。

### 5.2 Network 與 syscall

- client／Game net profile 都主要停在 internal/poll.(*FD).Read，亦即 gRPC HTTP/2 reader
  的 network wait。
- client syscall profile 的主要累積路徑是 transport bufWriter.Flush／flushKeepBuffer；
  Game syscall profile 也主要是 server HTTP/2 loopyWriter 的 Flush。
- Game scheduler profile 可見 runtime.systemstack_switch、runtime.selectnbsend、
  runtime.Gosched、controlBuffer 與 processUnaryRPC；原始 baseline 沒有可直接命名的 quota
  admission stack。
- goroutine dump 沒有 streamQuota、waitForStreamQuota、quotaPool。

因此 trace 支持 transport／scheduler／write path 是候選，不能單獨證明 stream limit。

## 6. macOS process、CPU 與 socket observation

兩個 trace run 的 top 每秒樣本：

| Conn | host idle 平均（min–max） | client steady CPU 平均 | Game steady CPU 平均 | client CSW delta | Game CSW delta |
|---:|---:|---:|---:|---:|---:|
| 1 | 8.12% (2.43–15.64%) | 195.2% | 191.5% | 362,072 | 380,454 |
| 4 | 8.58% (0.56–13.80%) | 247.0% | 242.9% | 352,242 | 372,498 |

client／Game 的 sysbsd delta 分別為：

- Conn=1：445,393／588,666。
- Conn=4：503,410／655,946。

host 有 8 logical CPU；兩個 Go process 個別未達各自 GOMAXPROCS=4，但同機測試期間
host idle 接近零，表示 process、kernel work 與其他 host 工作共同造成排程競爭。

nettop loopback 觀測：

| Conn | established rows | RTT 範圍 | retransmission observation |
|---:|---:|---:|---|
| 1 | 20 | 1.22–1.59 ms | 從第二個 sample 起看到固定 16,332 rx_dupe／re-tx，後續沒有增加 |
| 4 | 80 | 1.22–1.47 ms | 0 |

socket snapshot 在單一時間點可見非零 Recv-Q／Send-Q，例如 Conn=1 的 Game endpoint
Send-Q=3,120、client endpoint Recv-Q=4,400；Conn=4 也有短暫非零 queue。這不是連續
sample，不能推導 sustained socket backpressure。macOS netstat -s -p tcp 在本機回報全零，
未作為本輪根因證據。

## 7. 數據如何推導瓶頸

下表把容量結果、app/runtime profile 與 host observation 放在同一條證據鏈中。不同工具的
百分比不可相加；判斷依據是多個獨立觀測是否指向同一層，而不是用單一 profile 命名根因。

| 問題 | 收集到的數據 | 推論 |
|---|---|---|
| 是否已到容量平台 | 未開 profiler 的三輪中位數：Conn=1 為 118,301.609 RPS／3.382284 ms；Conn=4 為 119,988.351 RPS／3.224105 ms | 增加 3 條 transport 只有 +1.43% RPS，且小於單輪波動；目前負載下不是 ClientConn 數量不足 |
| 是否為 stream admission 上限 | default=0 的 Conn=1/2/4 `NewStream` line 867 均為 0；人工設為 100 時依序為 287/161/0，RPS 依序比 default 低 23.16%/11.06%/0.27% | positive control 能辨識 quota wait，而 baseline 沒有相同 signature；排除 baseline `MaxConcurrentStreams` 瓶頸 |
| CPU 成本在哪裡 | profile-only 中 client／Game 各約使用 2.40–2.62 cores；Game workflow handler cumulative 只有 0.83–1.15%，dispatcher 約 0.87–1.23% | 單一 Go process 未吃滿各自 `GOMAXPROCS=4`；Echo handler／dispatcher 不是主要 CPU 消耗者 |
| app 熱點在哪裡 | CPU pprof 的 client／Game 主要 stack 為 gRPC HTTP/2 `loopyWriter`、`bufWriter.Flush`、protobuf 與 `rawsyscalln`；trace syscall delay 的 writer/Flush 占 client 61.11–77.06%、Game 75.79–85.10% cumulative | request 的主要 app 執行成本位於 gRPC transport 到 kernel write 的交界；cumulative 百分比包含下游，不互相相加 |
| 是否有 scheduler 壓力 | client scheduler delay 約 90% 歸因於 `operateHeaders` 喚醒點；host idle 平均 8.12%/8.58%，client/Game CSW delta 各約 35–38 萬 | 大量 RPC goroutine 已 runnable 但等待 CPU；結合 host 低 idle，確認同機 client、Game、kernel 與背景工作存在 shared-host scheduler contention |
| 是否為 GC 或 goroutine leak | client trace window GC pause 累計只有 0.103/0.139 秒；client goroutines 分別維持 415→415、430→430；error delta 與 failed_workers 都是 0 | 排除 GC pause、goroutine 持續成長與錯誤重試作為主要容量限制 |
| 是否為網路 loss/backpressure | loopback RTT 約 1.22–1.59 ms；Conn=1 的 retransmission counter 固定不再增加；socket queue 只有單點非零 | 沒有 sustained packet loss 或 socket backpressure 證據；單點 queue 不用來命名根因 |

其中 `operateHeaders` 是 scheduler delay 的 attribution point，表示相關 goroutine 被喚醒後等待
排程；不能解讀成 header processing 使用約 90% CPU。on-CPU 熱點以 CPU pprof 為準。

## 8. 結論與決策

### 8.1 本次測試可確定的結論

1. 在 macOS 同機、Concurrency=400、兩端 `GOMAXPROCS=4`、32-byte direct unary Echo 的
   測試拓撲中，容量平台約為 118K–120K RPS。
2. 限制這個平台的瓶頸區域是 gRPC HTTP/2 transport、kernel socket syscall 與 shared-host
   scheduler 的跨層成本。host 平均只剩約 8% idle，是本次最直接的 saturation evidence；
   它代表此測試拓撲接近 host CPU capacity，不代表 Game process 單獨吃滿四個 P。
3. 現有證據排除 Echo handler／dispatcher、Redis、GC、goroutine leak、ClientConn 數量不足與
   baseline stream quota 是主要瓶頸。
4. 沒有發現 framework wrapper／dispatcher 的顯著 CPU 熱點或 baseline quota wait；profile 中的
   transport／syscall 成本符合本次大量小型 unary RPC 的正常執行路徑，沒有證據指向 framework
   的 gRPC 用法形成另一個額外瓶頸。

### 8.2 結論邊界與決策

- 本報告不能把跨層成本再單獨歸因為 grpc-go defect、macOS defect 或實體硬體極限，也不能將
  約 120K RPS 當成 Game 的 production capacity；client 與 Game 共用同一 host。
- 將 client 與 Game 分離只是在需要取得 Game 獨立容量、或量化 client/Game/kernel 各自占比時
  才需要的下一個實驗，不影響上述本次同機瓶頸結論。
- 不設定 production `MaxConcurrentStreams=100`，也不因本輪結果增加 connection pool 或調整
  flow-control。若目標是提升此類小型 unary RPC 的上限，應先在隔離環境重新建立 baseline，再決定
  是否值得針對 transport 次數、payload batching 或部署 CPU 做優化。

## 9. 條件式 `MaxConcurrentStreams=100` positive control

### 9.1 必要的程式調整

依設計第 7 節只加入一個 optional `ServerConfig` field；`0` 不建立
`grpc.MaxConcurrentStreams` option，保持既有 default。設定大於 0 時，與既有 unary
interceptor 一起傳給 `grpc.NewServer`。contract test 使用 blocking handler 驗證
同一 transport 的 handler admission 不超過設定值。

實際修改：

- `pkg/gatelink/server.go`：新增 field 與條件式 server option。
- `pkg/gatelink/gatelink_contract_test.go`：新增 `MaxConcurrentStreams=1` integration
  contract test。

驗證命令與結果：

```text
go test ./pkg/gatelink
go test -race ./pkg/gatelink
go test ./products/gameproduct ./examples/metrics/... ./pkg/serversend ./pkg/framework
```

上述測試均通過。

### 9.2 Controlled run

每個 Conn 值都使用新啟動的 Game 與 grpcload binary，固定 Concurrency=400、
GOMAXPROCS=4、32-byte Echo、warm-up=1、duration=30s、request timeout=10s、
macOS loopback plaintext gRPC。兩組的唯一設定差異是 Game 是否帶有：

```sh
CORE_CASINO_METRICS_GAME__GATE_TO_GAME__MAX_CONCURRENT_STREAMS=100
```

metrics 在 warm-up 後約 5 秒保存 `before`，約 25 秒保存 `near-end`；completion log
的 RPS 是整個 30 秒 admission window 的正式結果。每輪 error delta=0、failed_workers=0，
load process exit status=0。

| Conn | default=0 RPS | max=100 RPS | RPS 變化 | default 近端平均 latency | max=100 近端平均 latency | max=100 `NewStream` line 867 | default line 867 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 115,619.277 | 88,842.254 | -23.16% | 3.462655 ms | 4.562608 ms | 287 | 0 |
| 2 | 122,788.591 | 109,207.631 | -11.06% | 3.271318 ms | 3.680731 ms | 161 | 0 |
| 4 | 124,185.912 | 123,850.581 | -0.27% | 3.177412 ms | 3.232599 ms | 0 | 0 |

`http2_client.go:867` 是 grpc-go v1.72.0 `http2Client.NewStream` 在 stream quota
不可用時進入的共用 select；dump 不會保留 closure 名稱 `checkForStreamQuota`，所以以
source line 與完整 stack 判讀，不能只 grep `streamQuota`。在本次 controlled snapshot
中，default=0 三個 Conn 都是 0，`max=100` 只在 Conn=1/2 出現等待，Conn=4 沒有；這與
每條 transport 各 100 streams、總共 1/2/4 transports 的已知限制一致。

positive-control metrics window（before → near-end）：

| Conn | success delta | error delta | histogram mean | in_flight | client CPU delta | go_goroutines |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2,197,645 | 0 | 4.562608 ms | 400 → 400 | 54.8737 s | 415 → 415 |
| 2 | 2,724,128 | 0 | 3.680731 ms | 400 → 399 | 64.1254 s | 420 → 420 |
| 4 | 3,098,019 | 0 | 3.232599 ms | 398 → 400 | 68.0663 s | 430 → 430 |

`in_flight` 仍是 offered closed-loop concurrency，不是 saturation ratio。近端 histogram
是約 20 秒窗口，不能與完整 30 秒 RPS 混算；RPS 以 completion log 為準。
Game 端同一窗口的 `gaming_core_game_gate_commands_total{command="4043309073",result="success"}`
delta 分別為 2,197,654／2,724,255／3,098,836（Conn=1/2/4）；原始 Game before／near-end
exposition 也一併保存，作為 server-side 收到量的交叉核對。

### 9.3 判讀

- Conn=1 在人工上限下明顯降速且 latency 上升；Conn=2 部分恢復；Conn=4 幾乎恢復
  default throughput。這是 per-transport stream admission limit 的正向控制結果。
- 這個結果只證明 `MaxConcurrentStreams=100` 能製造可觀測的等待，不表示原始 baseline
  的瓶頸就是 stream quota；原始 baseline capacity 與 default-control 均沒有該等待
  signature。
- 因此不把 `100` 寫入 YAML 預設、不調整 production 值，也不因 positive control 變慢而
  宣稱 grpc-go defect 或 OS root cause。

## 10. 限制與選用的下一步

- 容量與 trace 都在同一台 macOS loopback 執行；client、Game、OrbStack 與背景程式共享
  host CPU，不能作 app 與 OS 的乾淨歸因。
- 第一批 trace 產物
  /tmp/gaming-core-casino-grpc-trace-20260911-112859/ 在收集期間 host idle 約
  0–15%，只保留作稽核，未用於本報告結論。
- 第二批 trace 的 endpoint、trace、goroutine、top、nettop、socket 與 metrics 皆成功保存；
  測試期間 host idle 約 8%，可判定本次同機拓撲存在 CPU／scheduler saturation，但不能用來
  宣稱 Game 的獨立容量或某個 OS／grpc-go defect。
- positive-control 的 `near-end` metrics 是測量期間 snapshot；load client 在 completion
  log 後立即關閉自己的 metrics listener，因此第一次 exploratory run 的空 `after` 檔案
  不納入本報告。正式數值採本節的 `before`／`near-end` 檔案與 completion log。
- 若要取得 Game 獨立容量或進一步拆分 client、Game、kernel 的成本，可將 grpcload 與 Game
  分到不同 host 或隔離 VM，固定 Concurrency=400、GOMAXPROCS=4、payload=32、duration=30s，
  各 Conn 組合重複三輪，再比較兩端 CPU、transport scheduler 與 socket evidence。這是更細的
  attribution 實驗，不是本次同機結論的前置條件。
- 在隔離實驗前，不調整 production connection pool、flow-control 或
  MaxConcurrentStreams。

## 11. Source 與原始資料

設計與執行規則：

- GATELINK_GRPC_TRACE_OS_DIAGNOSIS_PLAN.md
- GATELINK_GRPC_ROOT_CAUSE_DIAGNOSIS_DESIGN.md
- GATELINK_GRPC_ROOT_CAUSE_PROFILE_REPORT.md

測試程式：

- pkg/gatelink/server.go
- pkg/gatelink/gatelink_contract_test.go
- examples/metrics/grpcload/main.go
- examples/metrics/grpcload/metrics.go
- examples/metrics/game/main.go
- examples/metrics/internal/profilehttp/server.go

本報告數據來源：

- binary build directory: `/tmp/gaming-core-casino-grpc-positive.DhYbUw/`
- CPU profile-only raw profiles and logs: `/tmp/gaming-core-casino-profile.FWw6zW/400-fresh-*`
- /tmp/gaming-core-casino-grpc-median.kYlWXW/capacity-summary.tsv
- /tmp/gaming-core-casino-grpc-median.kYlWXW/capacity-conn1-run1/grpcload.log
- /tmp/gaming-core-casino-grpc-median.kYlWXW/capacity-conn1-run2/grpcload.log
- /tmp/gaming-core-casino-grpc-median.kYlWXW/capacity-conn1-run3/grpcload.log
- /tmp/gaming-core-casino-grpc-median.kYlWXW/capacity-conn4-run1/grpcload.log
- /tmp/gaming-core-casino-grpc-median.kYlWXW/capacity-conn4-run2/grpcload.log
- /tmp/gaming-core-casino-grpc-median.kYlWXW/capacity-conn4-run3/grpcload.log
- /tmp/gaming-core-casino-grpc-trace-20260911-113757/conn-1/
- /tmp/gaming-core-casino-grpc-trace-20260911-113757/conn-4/
- /tmp/gaming-core-casino-grpc-trace-20260911-113757/host-preflight.txt
- /tmp/gaming-core-casino-grpc-positive.DhYbUw/baseline-control/summary.tsv
- /tmp/gaming-core-casino-grpc-positive.DhYbUw/baseline-control/conn-1/
- /tmp/gaming-core-casino-grpc-positive.DhYbUw/baseline-control/conn-2/
- /tmp/gaming-core-casino-grpc-positive.DhYbUw/baseline-control/conn-4/
- /tmp/gaming-core-casino-grpc-positive.DhYbUw/supplemental/summary.tsv
- /tmp/gaming-core-casino-grpc-positive.DhYbUw/supplemental/conn-1/
- /tmp/gaming-core-casino-grpc-positive.DhYbUw/supplemental/conn-2/
- /tmp/gaming-core-casino-grpc-positive.DhYbUw/supplemental/conn-4/
