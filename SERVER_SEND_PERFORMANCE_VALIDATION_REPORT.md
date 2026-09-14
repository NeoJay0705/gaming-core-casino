# Server-send Broadcast／Player 效能驗證報告

## 1. 執行摘要

本報告更新自 async server-send batching 修正後重新執行的完整 campaign。實驗使用相同的
`examples/metrics` workflow，重新編譯並在每個 attempt 重啟 Game、Gate、load；不是沿用舊 process
或舊 binary。完整 campaign artifact root 為
[`artifacts/server-send/20260914T030251Z`](artifacts/server-send/20260914T030251Z)，另有針對
Broadcast 16ms 的 Game profile-only artifact
[`artifacts/server-send-profile/20260914T032122Z`](artifacts/server-send-profile/20260914T032122Z)。

結論先列如下：

- correctness gate 的 Broadcast／Player 兩個 10-connection run 都 `delivery_complete`。
- 四個 1000-connection matrix case 均取得 3/3 `evidence_valid` attempts。
- Broadcast 33ms、Player 33ms、Player 16ms 各 3/3 `delivery_complete`。
- Broadcast 16ms 有 2/3 `delivery_complete`，第 3 輪為 5 個 missed ticks；該輪沒有 sender、Gate 或
  client error，缺額正好為 5,000（5 ticks × 1,000 targets）。原始 `run-status.json` 的
  `delivery_failed=true` 是分類器把 planned 缺額重複算入 delivery；依實際 emitted ticks 的 terminal
  counters，該輪應判定為純 `cadence_failed`。
- async 修正後，Player 33ms 與 Player 16ms 的 producer cadence 在三輪都完整；`SendToPlayers`
  caller admission 的三輪 mean 中位數約 0.091–0.096 ms（16ms 與 33ms），不再把 Redis／gRPC 等待留在
  producer critical path。
- Player background worker 仍可觀測到約 7.5–8.2 ms 的 dependency work；16ms 下 queue wait p99
  為 0.5 ms、drain 後 queue／in-flight 均為 0。這是應持續監控的 capacity 指標，不代表目前已飽和。
- 本次沒有足夠證據把根因定論為 gRPC implementation、Redis、macOS kernel 或硬體極限。Broadcast
  16ms 單次 missed tick 是目前的候選邊界，不具三輪穩定性。

與上一份舊 binary 報告相比，這次 Player 兩個 cadence case 由連續 missed 改為三輪完整；這是不同
campaign，僅能作結果觀察，不能視為嚴格 A/B 因果證明。此次 binary 內含的必要修正是：Player
admission 在 clone 前完成 count／bytes preflight 並只計算一次 accounting，以及 Player／Broadcast
`Stop(ctx)` 在 deadline 到期後不再無限等待不合作的 delegate。

## 2. 實驗條件與執行方式

正式入口為：

```sh
examples/metrics/scripts/run-server-send-validation.sh \
  --artifact-root artifacts/server-send/20260914T030251Z --full-campaign
```

固定條件：

| 項目 | 值 |
|---|---|
| connections | 1,000（每個 correctness case 為 10） |
| cases | Broadcast／Player × 33ms／16ms |
| measured | 30s；每 case 3 個 evidence-valid attempts |
| warm-up／drain | 5s／10s |
| payload | 32 bytes application payload；實際 encoded PushMessage 74/76 bytes |
| process | Game、Gate、load 均 `GOMAXPROCS=4` |
| setup | 所有 WebSocket、Login、EnterRoom 與 reader ready 後才開始 workload |
| Redis | `127.0.0.1:6379`、Redis 8.4.2、standalone、外部 OrbStack dependency |
| host | macOS 14.5、Darwin arm64、8 logical CPUs、16 GiB |
| runtime | Go 1.27.1、grpc-go v1.72.0、go-redis v9.18.0 |
| scrape | Game／Gate／load 每秒；Redis INFO 每秒；摘要只取 measured window |

Game、Gate、load 與 OrbStack 內的 Redis 共用同一台 macOS host；因此 host 數據只能描述 shared-host
負載，不能單獨代表任一服務的 production capacity。

campaign 只 build 一次，所有 attempt 使用同一份 binary snapshot：

```text
game  0c4f0d038036179470c88755350b9ff95c3650dcd0c93eb0ad5307392338a74a
gate  7832f4abbc47429c03164ab6e6eace3525b7d6367f16a06b343fee306b3e0add
load  1674a86e67a504fb362b9771668316b0aeb9cebf6d854f9674f6f18f6c591d76
```

來源為 [`binary-checksums.txt`](artifacts/server-send/20260914T030251Z/binary-checksums.txt) 與
各 attempt 的 `source-state.txt`。campaign 中所有 14 個 attempts（2 correctness + 12 matrix）
均以：

```sh
examples/metrics/scripts/run-server-send-validation.sh --validate-only <attempt-dir>
```

重新驗證成功。測試期間未執行 Git staging 操作。

## 3. Correctness gate

| workload | connections | interval | planned | attempted | client received | missing／duplicate／gap／invalid／reader failure | Gate delivery | result |
|---|---:|---:|---:|---:|---:|---|---:|---|
| Broadcast | 10 | 33ms | 151 | 151 | 1,510 | 0／0／0／0／0 | 1,510 | `delivery_complete` |
| Player | 10 | 33ms | 151 | 151 | 1,510 | 0／0／0／0／0 | 1,510 | `delivery_complete` |

原始證據：[`correctness-gate.json`](artifacts/server-send/20260914T030251Z/correctness-gate.json)、
[`correctness/broadcast-33/run-1`](artifacts/server-send/20260914T030251Z/correctness/broadcast-33/run-1)、
[`correctness/player-33/run-1`](artifacts/server-send/20260914T030251Z/correctness/player-33/run-1)。

## 4. Matrix 每輪結果

`planned`／`attempted`／`missed` 是 configured 30 秒 measured window；`ticks/s` 與 `client msg/s` 使用各輪
`measured_running_start/end` 的實際 elapsed（約 29.965–29.989 秒），`client missing` 是每個 reader
至少缺少的訊息數。所有 attempts 的 sender partial/error、Gate delivery error、WebSocket write error、
client duplicate／gap／invalid／reader failure 都為 0。Gate write 比 delivery 多 1 是 setup/control
connection write，不是 push delivery。

| case／attempt | workload status | planned／attempted／missed | ticks/s | client msg/s | client missing | Gate delivery／WS write |
|---|---|---:|---:|---:|---:|---:|
| [Broadcast 33 r1](artifacts/server-send/20260914T030251Z/matrix/broadcast-33/run-1) | delivery_complete | 909／909／0 | 30.334 | 30,334.4 | 0 | 909,000／909,001 |
| [Broadcast 33 r2](artifacts/server-send/20260914T030251Z/matrix/broadcast-33/run-2) | delivery_complete | 909／909／0 | 30.334 | 30,334.2 | 0 | 909,000／909,001 |
| [Broadcast 33 r3](artifacts/server-send/20260914T030251Z/matrix/broadcast-33/run-3) | delivery_complete | 909／909／0 | 30.336 | 30,335.8 | 0 | 909,000／909,001 |
| [Player 33 r1](artifacts/server-send/20260914T030251Z/matrix/player-33/run-1) | delivery_complete | 909／909／0 | 30.335 | 30,334.5 | 0 | 909,000／909,001 |
| [Player 33 r2](artifacts/server-send/20260914T030251Z/matrix/player-33/run-2) | delivery_complete | 909／909／0 | 30.335 | 30,335.0 | 0 | 909,000／909,001 |
| [Player 33 r3](artifacts/server-send/20260914T030251Z/matrix/player-33/run-3) | delivery_complete | 909／909／0 | 30.334 | 30,334.2 | 0 | 909,000／909,001 |
| [Broadcast 16 r1](artifacts/server-send/20260914T030251Z/matrix/broadcast-16/run-1) | delivery_complete | 1,875／1,875／0 | 62.529 | 62,529.4 | 0 | 1,875,000／1,875,001 |
| [Broadcast 16 r2](artifacts/server-send/20260914T030251Z/matrix/broadcast-16/run-2) | delivery_complete | 1,875／1,875／0 | 62.523 | 62,522.9 | 0 | 1,875,000／1,875,001 |
| [Broadcast 16 r3](artifacts/server-send/20260914T030251Z/matrix/broadcast-16/run-3) | cadence_failed | 1,875／1,870／5 | 62.365 | 62,364.7 | 5,000 | 1,870,000／1,870,001 |
| [Player 16 r1](artifacts/server-send/20260914T030251Z/matrix/player-16/run-1) | delivery_complete | 1,875／1,875／0 | 62.530 | 62,530.1 | 0 | 1,875,000／1,875,001 |
| [Player 16 r2](artifacts/server-send/20260914T030251Z/matrix/player-16/run-2) | delivery_complete | 1,875／1,875／0 | 62.531 | 62,531.3 | 0 | 1,875,000／1,875,001 |
| [Player 16 r3](artifacts/server-send/20260914T030251Z/matrix/player-16/run-3) | delivery_complete | 1,875／1,875／0 | 62.530 | 62,530.5 | 0 | 1,875,000／1,875,001 |

Broadcast 16ms 的第 3 輪是 evidence-valid 但 workload `cadence_failed`，不是 environment-invalid；
實際 emitted 的 1,870 ticks 全部完成 Gate／client delivery，因此不能把 planned 與 attempted 的差額
改寫成 transport error，也不能從 campaign 移除。

## 5. Latency 與 async pipeline metrics

所有 latency 是 baseline 到 final Prometheus histogram counter／sum delta；quantile 是 bucket upper
bound，不是插值精確百分位。欄位格式為「三輪 mean 的 median（各輪 min–max）」；單位皆為 ms。

`Game admission` 只涵蓋 public sender validation、clone 與 queue admission；`queue wait` 是 admission
到 worker 開始處理；`worker` 包含背景 sender delegate；Gate 是 delivery handler enqueue；client 是
Game push timestamp 到 load reader 收到 frame。

| case | Game admission avg／p95／p99 | queue wait avg／p95／p99 | worker avg／p95／p99 | Gate avg／p95／p99 | client avg／p95／p99 |
|---|---|---|---|---|---|
| Broadcast 33ms | 0.006975 (0.006699–0.007464)／0.025／0.025 (0.025–0.050) | 0.006271 (0.006007–0.006449)／0.025／0.025 | 0.635123 (0.633926–0.657997)／1／2.5 (2.5–5) | 1.087590 (1.050954–1.108764)／2.5／5 | 3.024637 (2.990180–3.065388)／5 (5–10)／10 |
| Player 33ms | 0.095457 (0.091520–0.096755)／0.25／0.5 | 0.050967 (0.044559–0.082109)／0.25 (0.1–0.25)／0.25 (0.25–0.5) | 8.128643 (8.128586–8.208141)／25／25 | 1.064064 (1.049578–1.124845)／2.5 (2.5–5)／5 | 7.052647 (7.023181–7.163247)／10／25 |
| Broadcast 16ms | 0.005943 (0.005846–0.006353)／0.025／0.025 (0.025–0.050) | 0.008862 (0.006772–0.039164)／0.025／0.025 | 0.612639 (0.604718–0.647366)／1／5 (2.5–5) | 1.040218 (1.013101–1.057303)／2.5／5 | 3.006441 (2.986072–3.047176)／5／10 |
| Player 16ms | 0.090892 (0.088401–0.093833)／0.25／0.5 (0.25–0.5) | 0.073301 (0.066004–0.122440)／0.25／0.5 | 7.473607 (7.469023–7.567594)／10／25 | 1.046161 (1.022790–1.050688)／2.5／5 | 6.561685 (6.524702–6.621130)／10／25 |

### 5.1 Player batching 與 dependency

Player 每個 producer tick 仍只呼叫一次 `SendToPlayers`，輸入 1,000 個 login names。由於同一 run
使用同一 detached trace，worker 只合併相鄰且同 trace 的 job；它不改變 command 或跨 trace 合併。

| case | worker batch observations（各輪） | messages total／batch avg | encoded bytes total／batch avg | Redis presence avg／p95／p99 | Redis endpoint avg／p95／p99 | gRPC avg／p95／p99 |
|---|---:|---:|---:|---|---|---|
| Player 33ms | 909／909／909 | 909,000／1,000 | 106,126,010／116,750 | 3.304／5／10 | 0.258／0.5／1 | 4.114／10／10 |
| Player 16ms | 1,871／1,875／1,874 | 1,875,000／約1,000–1,002 | 219,041,750／116,822–117,072 | 2.856／5／10 | 0.244／0.5／1 | 3.938／10／10 |

Player 16ms 的 batch observation 少於 1,875 並非遺失：三輪 `messages total` 都是 1,875,000，
且 client／Gate terminal counters 完全對齊；這是 immediate same-trace coalescing 的結果。

Broadcast 每個 outer command 都是一次 Redis publish；三輪 Redis publish avg／p95／p99 為：
Broadcast 33ms `0.627／1／2.5 ms`，Broadcast 16ms `0.606／1／5 ms`（各數字為三輪中位數）。

## 6. Queue、delivery 與 saturation

queue 是每秒 scrape 的 sampled maximum，不是連續時間的絕對最大值；所有 attempt drain 後 Game
queue、Gate write queue、in-flight 與 readers contract 都回到要求的 terminal state。

| case | Game queue messages max（median／range） | Game queue bytes max（median／range） | Gate WebSocket write queue max（median／range） | write in-flight max（median／range） |
|---|---:|---:|---:|---:|
| Broadcast 33ms | 0／0–0 | 0／0–0 | 0／0–0 | 0／0–0 |
| Player 33ms | 1,000／0–1,000 | 115,890／0–116,890 | 253／0–373 | 0／0–6 |
| Broadcast 16ms | 0／0–0 | 0／0–0 | 276／248–457 | 1／0–1 |
| Player 16ms | 0／0–1,000 | 0／0–116,890 | 386／77–516 | 1／0–2 |

| case | Game／Gate／load process raw CPU max median（range） | host idle minimum median（range） | Redis one-core CPU average median（range） | Redis commands/s average median（range） |
|---|---:|---:|---:|---:|
| Broadcast 33ms | 0.6%／23.2%／22.4% (0.5–0.6／22.8–23.7／21.7–22.5) | 32.13% (30.80–38.28) | 0.97% (0.95–1.04) | 279.2 (276.6–282.3) |
| Player 33ms | 11.1%／26.2%／22.2% (9.8–12.0／26.1–27.5／22.1–22.6) | 28.84% (22.44–29.32) | 7.31% (7.12–7.40) | 29,904.3 (29,003.2–29,966.5) |
| Broadcast 16ms | 0.9%／44.4%／43.9% (0.8–0.9／43.2–44.9／42.7–44.2) | 23.97% (23.97–26.20) | 1.34% (1.33–1.35) | 313.7 (304.3–314.6) |
| Player 16ms | 20.1%／49.1%／42.7% (20.1–20.5／48.4–49.8／42.6–45.2) | 23.63% (23.24–25.66) | 12.31% (11.80–12.78) | 61,481.0 (59,440.4–61,521.3) |

上述 process CPU 是 macOS `top` raw `%CPU`，100% 約等於一個 logical CPU，不是整台 host 使用率。
`top` 原始 sample 只有秒級 timestamp；摘要只使用 measured window 內的 samples，邊界最多有一個
collector interval 的時間解析度限制。
由 measured window 邊界前後最近的 `process_cpu_seconds_total` scrape delta 除以 `GOMAXPROCS=4`，各
case 的 Game／Gate／load normalized CPU average（各輪 median）為：Broadcast 33ms `0.11%／5.27%／5.05%`、
Player 33ms `2.30%／5.75%／4.92%`、Broadcast 16ms `0.18%／10.14%／9.93%`、Player 16ms
`4.59%／11.22%／9.76%`。Redis commands/s 使用 `total_commands_processed` counter delta；上表
未使用 `instantaneous_ops_per_sec` 取代 active-window rate。Redis measured-window interval max median
（各輪 min–max）依序為 Broadcast 33ms `1.56% (1.56–1.81%)`、Player 33ms `9.16% (8.69–9.98%)`、
Broadcast 16ms `2.26% (2.12–2.34%)`、Player 16ms `14.39% (14.32–14.90%)`；commands/s interval max
依序為 `343.7 (339.6–382.0)`、`31,556.8 (31,479.8–31,606.2)`、`414.5 (380.0–427.1)`、
`63,709.2 (63,374.5–63,715.4)`。

四個 case 的 Redis active samples 都有效；`connected_clients` 最大 25、`blocked_clients=0`、
`rejected_connections=0`。Game Redis pool final snapshot 為 `total_connections=11`、
`idle_connections=11`、`pending_requests=0`、`wait_total=0`、`timeouts_total=0`。

## 7. Go runtime metrics

下表是 drain 後 final snapshot；1000 readers 尚存是 workload contract，不應直接解讀為 leak。scheduler
p99 是 baseline-to-final measured delta 的 bucket upper bound。

| case | Game goroutines／heap MB／sched p99 | Gate goroutines／heap MB／sched p99 | load goroutines／heap MB／sched p99 |
|---|---|---|---|
| Broadcast 33ms | 17／3.22 (3.09–3.42)／0.918ms | 2,021／66.52 (53.07–76.60)／10.486ms | 1,010／13.34 (11.96–15.75)／0.918ms |
| Player 33ms | 22 (22–23)／4.15 (3.74–4.24)／0.918ms (0.082–0.918) | 2,024／80.43 (71.04–80.91)／10.486ms | 1,010／17.18 (16.58–18.84)／10.486ms |
| Broadcast 16ms | 17／2.58 (2.55–2.64)／0.082ms (0.082–0.918) | 2,021／53.08 (44.72–57.22)／10.486ms | 1,010／15.83 (12.03–16.88)／0.918ms |
| Player 16ms | 22 (22–23)／5.10 (4.26–5.42)／0.918ms (0.082–0.918) | 2,024 (2,024–2,025)／47.69 (46.74–63.78)／10.486ms | 1,010／20.30 (12.43–20.48)／10.486ms |

Gate 的 scheduler p99 10.486ms bucket 與 WebSocket queue transient pressure 值得線上持續觀察，
但因 queue drain 為 0、delivery p99 仍為 5ms bucket，這批數據尚未構成 terminal saturation。

## 8. Profile-only（不納入 matrix median）

為針對 Broadcast 16ms 第 3 輪的候選 producer boundary，另執行：

```sh
examples/metrics/scripts/run-server-send-validation.sh \
  --artifact-root artifacts/server-send-profile/20260914T032122Z \
  --workload broadcast --interval 16ms --connections 1000 --attempts 1 \
  --duration 30s --warmup 5s --drain 10s --profile-target game --trace
```

profile-only attempt 為 `evidence_valid` 但有 observer effect：1,875 planned、1,873 attempted、2
missed，client／Gate delivery 為 1,873,000，沒有 transport error。它只用來診斷，不加入上方三輪
capacity 統計。由於正式 Broadcast 16ms baseline 只有 1/3 attempts 出現 cadence failure，尚未達到
設計要求的同一失守邊界至少 2/3；因此這次 profile 只作 exploratory evidence，不可作 root-cause 結論。

原始檔案：

- [CPU profile](artifacts/server-send-profile/20260914T032122Z/profile/broadcast-16/run-1/pprof/game-cpu-20s.pb.gz)
- [heap profile](artifacts/server-send-profile/20260914T032122Z/profile/broadcast-16/run-1/pprof/game-heap.pb.gz)
- [goroutine snapshot](artifacts/server-send-profile/20260914T032122Z/profile/broadcast-16/run-1/pprof/game-goroutine.txt)
- [Go trace](artifacts/server-send-profile/20260914T032122Z/profile/broadcast-16/run-1/pprof/game-trace-5s.out)
- [profile status/window](artifacts/server-send-profile/20260914T032122Z/profile/broadcast-16/run-1/pprof/status.tsv)

`go tool pprof -top` CPU profile 為 20.03s window、660ms samples（3.29%）；主要 flat samples 是
`runtime.kevent` 46.97%、`runtime.pthread_cond_wait` 27.27%、`syscall.rawsyscalln` 7.58%。
Application cumulative stack 中 `AsyncBroadcastSender` → `RedisBroadcastSender` → `redis.Store.Publish`
約 80ms；cumulative 值有重疊，不能相加成 CPU 使用率。

`go tool pprof -top` heap in-use 約 4.28MB；其中 profile／regex／allocator／Redis pool initialization
本身佔主要項目，不能把該 snapshot 當成 steady-state allocation 結論。

由 raw trace 以 `go tool trace -pprof=net`／`-pprof=sched` 檢視，net delay 主要是 HTTP `Accept`
約 4,176.81ms（95.57%）與 `Read` 約 193.60ms；scheduler delay 約 3.67ms，主要為
`runtime.chansend1`。這些是 goroutine blocking／delay samples，不是 CPU time，也沒有唯一指向
某個 gRPC syscall。profile collector 的 HTTP endpoint 與 trace 本身會影響該 run，故只作方向性證據。

## 9. Bottleneck 判讀

### 已由數據支持

1. **同步 producer boundary 已被移除。** Player admission p99 0.5ms 以內，且 Player 16ms 三輪
   都完整；Redis routing／gRPC completion 出現在 worker metrics，而不是 caller latency。
2. **Player worker 的主要可觀測成本是 dependency path。** Player 16ms worker avg 約 7.47ms；其中
   Redis presence 約 2.86ms、endpoint 約 0.24ms、gRPC 約 3.94ms。這些值可用來設定線上告警與
   capacity review，但仍未造成 queue 長時間累積。
3. **Broadcast 16ms 的單次失守在 cadence，非 delivery error。** 該輪 5 missed ticks 對應 5,000
   client missing；Gate delivery、WebSocket write 與 client protocol counters 對齊，queue drain
   成功。
4. **目前沒有明顯 Redis pool 或硬體 CPU 飽和。** Redis active-window CPU average 最高 case 中位數約
   12.31%、interval max 約 14.39%，pool wait／timeout 為 0；host idle minimum 的最低跨輪約 23.24%，
   不能稱 host CPU 已滿載。
5. **Gate 有 transient load。** Player 16ms Gate process CPU max 中位數約 49.1%，write queue
   sampled max 中位數 386（最高 516），但 terminal queue 為 0、delivery p99 為 5ms bucket；目前
   只能稱候選壓力，不能稱永久 saturation。

### 尚不能定論

- Game worker `grpc` dependency histogram 仍涵蓋 channel／stream 等待與 Gate response；沒有拆出
  gRPC transport 內部每階段，因此不能單靠它判定 grpc-go implementation 是根因。
- profile 的 `kevent`、condition wait、Accept／Read 主要反映 event loop 等待與 profile endpoint；
  它支持「等待／scheduler／network 方向」，不支持「OS 是唯一根因」。
- 單一 macOS host、單一 Gate／Game process、Redis standalone 與固定 1,000 targets 不能外推
  production capacity，也不能判定 `MaxConcurrentStreams` 或 channel 數量應立即修改。

## 10. 線上可採取的觀測與後續

優先持續觀察下列 bounded metrics：

- `gaming_core_game_server_send_queue_messages/bytes{operation}`：長時間上升代表 producer rate
  大於 worker service rate；先檢查 worker 與 dependency duration，再調整 rate／capacity。
- `gaming_core_game_server_send_queue_wait_duration_seconds`：p95／p99 接近 cadence interval 時，
  表示 backlog 已影響 latency。
- `gaming_core_game_server_send_worker_duration_seconds{operation,result}` 與
  `...dependency_duration_seconds{dependency,result}`：區分 Redis presence／endpoint、Redis publish
  與 gRPC 候選成本。
- Gate server-send delivery、WebSocket write queue／in-flight、client received／missing：確認問題
  發生在 Gate delivery 還是 Game producer。
- `go_sched_latencies_seconds`、process CPU、goroutines／heap、Redis pool wait／timeout 與 host
  CPU／memory／socket collectors：只在 application metrics 指向相同窗口時再升級為 OS/profile
  診斷。

下一個唯一必要動作是只重跑 Broadcast 16ms 三個 `evidence_valid` attempts，使用相同 binary／config
並盡量排除不相關 host background load；若相同 producer cadence failure 在三輪中至少出現 2/3，才針對
Game scheduler／timer path 執行 profile-only。若 Player worker queue 開始持續累積，才另行評估 per-Gate
worker 或 transport 調整。現有證據不足以直接加入 retry、Redis pipeline、增加 gRPC channel 或宣稱硬體極限。

## 11. Evidence index

每個 attempt 皆保存 `run-status.json`、`metadata.txt`、`phase-events.jsonl`、`commands.txt`、
`metrics/`、`os/`、Redis before／after 與每秒 `os/redis.tsv`。完整 manifest：
[`evidence-manifest.tsv`](artifacts/server-send/20260914T030251Z/evidence-manifest.tsv)。

四組完整 artifacts：

- [Broadcast 33ms](artifacts/server-send/20260914T030251Z/matrix/broadcast-33)
- [Player 33ms](artifacts/server-send/20260914T030251Z/matrix/player-33)
- [Broadcast 16ms](artifacts/server-send/20260914T030251Z/matrix/broadcast-16)
- [Player 16ms](artifacts/server-send/20260914T030251Z/matrix/player-16)

數據來源對照：

- workload ticks、missed、client correctness、Gate terminal：各 attempt `run-status.json`。
- admission／queue wait／worker／dependency／batch latency：Game `metrics/baseline-game.prom` 與
  `final-game.prom` 的 histogram count／sum／bucket delta。
- Gate delivery／WebSocket write／queue：Gate baseline/final snapshots 與 `metrics/*.tsv`。
- client delivery／received：load baseline/final snapshots。
- Go runtime：各 role final `/metrics` 的 goroutines、heap、scheduler histogram 與 process CPU。
- host/process／socket：`os/process-top.txt`、`host-top.txt`、`vm-stat.txt`、`nettop.txt`、
  `netstat-*`、`lsof-*`。
- Redis：`os/redis.tsv`、`redis-before.txt`、`redis-before-measured.txt`、`redis-after.txt`。
- pprof／trace：profile-only artifact 的 `pprof/`、`status.tsv`、`window.tsv`。

raw artifacts 應保留於 local／CI artifact storage，不提交 Git source history；本次只更新本報告，
沒有把 artifacts 加入 staging。
