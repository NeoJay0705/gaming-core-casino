# Server-send Broadcast／Player 效能驗證報告

## 1. Executive conclusion

本次正式 campaign 已完成，使用同一份 Game／Gate／load binary snapshot、Redis standalone、1000 條
WebSocket connections、`GOMAXPROCS=4`，並依設計完成 correctness gate、四個 matrix case 各三次
`evidence_valid` attempt，以及一次 Game profile-only（含 Go trace）。

- correctness gate：Broadcast 與 Player 均為 `delivery_complete`，各 10 connections、33ms、5 秒，
  `151 × 10 = 1,510` 筆 client messages 全部收到。
- Broadcast 33ms：3/3 `delivery_complete`，中位數約 30.3k messages/s，沒有 missed tick 或 client
  protocol loss。
- Player 33ms：3/3 `evidence_valid`，但全部 `cadence_failed`；每次少 1 個 tick，client 缺額正好
  `1 × 1,000`。
- Broadcast 16ms：3/3 `evidence_valid`，但 1 次 measured 少 4 ticks，另 2 次 warm-up 有 missed tick，
  因此 0/3 `delivery_complete`。其餘 measured delivery counters 仍與 client 對齊。
- Player 16ms：3/3 `cadence_failed`；measured missed ticks 為 19、30、34，中位數 30，client 缺額
  正好是 `missed × 1,000`。

目前可以確認的第一個失守 boundary 是 Game 的 synchronous Player server-send producer cadence：
`SendToPlayers` 的尾端 latency 在 16ms workload 下跨過下一個 scheduled tick。Player 16ms 的 sender
latency p95 中位數為 10ms、p99 為 25ms bucket，與 measured missed ticks 同時出現。

目前不能把根因定論為「gRPC 本身」或「OS 已飽和」：

- Gate delivery p99 維持 5ms bucket，client duplicate／gap／invalid／reader failure 全部為 0；
  drain 後 queue 與 in-flight gauges 都回到 0。
- Redis active-window CPU 約 0.95%～12.78%（Redis one-core equivalent）、pool wait/pending/timeout
  全部為 0，沒有 connection pool exhaustion 證據。
- macOS host idle 最低約 19.89%，Game／Gate／load sampled process CPU 最高約 18.7%／52.1%／44.7%，
  不是硬體 CPU 已達滿載的證據。
- Game CPU profile／trace 顯示 network、scheduler、Redis pipeline 與 gRPC call stack 的等待，
  但單次 profile 無法分離 Redis routing、gRPC channel／stream、Gate 或 shared-host scheduling 的
  個別貢獻。

因此本次可下的結論是：1000 connections 下，Broadcast 33ms 是目前唯一三輪完整穩定的 workload；
Player 33ms 與兩個 16ms workload 已觀測到 producer cadence 壓力，但尚未建立穩定 capacity 上限，
也不能宣稱已排除 application gRPC implementation。

## 2. Test scope and method

正式入口為 `examples/metrics/scripts/run-server-send-validation.sh --full-campaign`。本次 campaign
artifact root 為 [artifacts/server-send/20260913T123552Z](artifacts/server-send/20260913T123552Z)。

固定條件：

| 項目 | 值 |
|---|---|
| correctness | Broadcast／Player、10 connections、33ms、5s、固定 run-1 |
| matrix | Broadcast／Player × 33ms／16ms、1000 connections |
| measured duration | 30s |
| warm-up／drain | 5s／10s |
| application payload | 32 bytes；encoded payload 由 metadata 保存 |
| process | Game、Gate、load 均 `GOMAXPROCS=4` |
| scrape | Game／Gate／load `/metrics` 每秒；Redis INFO 每秒 |
| host | macOS 14.5、Darwin arm64、Mac14,3、8 logical CPUs、16 GiB |
| runtime | Go 1.27.1、grpc-go v1.72.0、go-redis v9.18.0 |
| Redis | 8.4.2、standalone、OrbStack、`127.0.0.1:6379` |

每個 formal campaign 只 build 一次；三個 binaries 的 SHA-256 為：

```text
game  daaafcf40a7ed2fbc3c9ff8573870dbc7b27cf084dc808d4dc3b4f9058625651
gate  e76508f97f2890a5749daecd8a3a2acd2a7fe5aa7c973a5697c6b30ea4367358
load  0809dfc1239ec0e83ad2368b96fc74eb604c4cfa59bbb31df9066a99419a9b6d
```

來源：[binary-checksums.txt](artifacts/server-send/20260913T123552Z/binary-checksums.txt)、
[source-state.txt](artifacts/server-send/20260913T123552Z/source-state.txt)。Redis 由既有 OrbStack
dependency 管理；orchestration 只執行 PING／INFO，不停止或重啟 Redis。

### 2.1 啟動問題與修正

兩次 preflight 問題沒有計入正式結果：

1. `20260913T123221Z` 在第一次 correctness 啟動時觸發 `set -u` 的 `timeout: unbound variable`；
   `wait_for_marker` 同一個 `local` 宣告先使用尚未初始化的 `timeout`。已拆開初始化。
2. `20260913T123305Z` 啟動後，Redis `INFO` 的標準 CRLF 使數值欄位含 `\r`，collector 將正常輸出誤判
   為 `missing_field`。已在 parser 取得 INFO 後移除 CR，並加入 fake CRLF regression fixture。

這兩次 artifacts 僅供故障稽核：[timeout failure](artifacts/server-send/20260913T123221Z)、
[Redis CRLF failure](artifacts/server-send/20260913T123305Z)。正式數據只使用上述
`20260913T123552Z` campaign。

## 3. Correctness gate

| workload | connections | interval | planned ticks | attempted | client received | duplicate/gap/invalid/readers | Gate delivery | result |
|---|---:|---:|---:|---:|---:|---|---:|---|
| Broadcast | 10 | 33ms | 151 | 151 | 1,510 | 0/0/0/0 | 1,510 | delivery_complete |
| Player | 10 | 33ms | 151 | 151 | 1,510 | 0/0/0/0 | 1,510 | delivery_complete |

兩個 correctness run 使用相同 campaign checksum；這是 matrix 開始的必要 gate。原始證據：

- [Broadcast correctness](artifacts/server-send/20260913T123552Z/correctness/broadcast-33/run-1)
- [Player correctness](artifacts/server-send/20260913T123552Z/correctness/player-33/run-1)
- [correctness-gate.json](artifacts/server-send/20260913T123552Z/correctness-gate.json)

## 4. Matrix per-run results

`planned/attempted/missed` 是 measured 30 秒窗口；`client missing` 是每個 reader 至少缺少的訊息數。
本次每條 connection 都是 1,000 個 room members／player targets，因此 missing 應等於
`missed × 1,000`。partial、error、duplicate、gap、invalid、reader failure 在所有 rows 均為 0；
Gate server-send write success 比 delivery 多 1，是 setup/control connection write，不是 push delivery。

| case / attempt | workload status | warm-up missed | planned / attempted / missed | actual ticks/s | client msg/s | client missing | Gate delivery / WS write |
|---|---|---:|---:|---:|---:|---:|---:|
| [Broadcast 33 r1](artifacts/server-send/20260913T123552Z/matrix/broadcast-33/run-1) | delivery_complete | 0 | 909 / 909 / 0 | 30.300 | 30,300.0 | 0 | 909,000 / 909,001 |
| [Broadcast 33 r2](artifacts/server-send/20260913T123552Z/matrix/broadcast-33/run-2) | delivery_complete | 0 | 909 / 909 / 0 | 30.300 | 30,300.0 | 0 | 909,000 / 909,001 |
| [Broadcast 33 r3](artifacts/server-send/20260913T123552Z/matrix/broadcast-33/run-3) | delivery_complete | 0 | 909 / 909 / 0 | 30.300 | 30,300.0 | 0 | 909,000 / 909,001 |
| [Player 33 r1](artifacts/server-send/20260913T123552Z/matrix/player-33/run-1) | cadence_failed | 0 | 909 / 908 / 1 | 30.267 | 30,266.7 | 1,000 | 908,000 / 908,001 |
| [Player 33 r2](artifacts/server-send/20260913T123552Z/matrix/player-33/run-2) | cadence_failed | 0 | 909 / 908 / 1 | 30.267 | 30,266.7 | 1,000 | 908,000 / 908,001 |
| [Player 33 r3](artifacts/server-send/20260913T123552Z/matrix/player-33/run-3) | cadence_failed | 0 | 909 / 908 / 1 | 30.267 | 30,266.7 | 1,000 | 908,000 / 908,001 |
| [Broadcast 16 r1](artifacts/server-send/20260913T123552Z/matrix/broadcast-16/run-1) | cadence_failed | 0 | 1,875 / 1,871 / 4 | 62.367 | 62,366.7 | 4,000 | 1,871,000 / 1,871,001 |
| [Broadcast 16 r2](artifacts/server-send/20260913T123552Z/matrix/broadcast-16/run-2) | cadence_failed | 3 | 1,875 / 1,875 / 0 | 62.500 | 62,500.0 | 0 | 1,875,000 / 1,875,001 |
| [Broadcast 16 r3](artifacts/server-send/20260913T123552Z/matrix/broadcast-16/run-3) | cadence_failed | 1 | 1,875 / 1,875 / 0 | 62.500 | 62,500.0 | 0 | 1,875,000 / 1,875,001 |
| [Player 16 r1](artifacts/server-send/20260913T123552Z/matrix/player-16/run-1) | cadence_failed | 1 | 1,875 / 1,856 / 19 | 61.867 | 61,866.7 | 19,000 | 1,856,000 / 1,856,001 |
| [Player 16 r2](artifacts/server-send/20260913T123552Z/matrix/player-16/run-2) | cadence_failed | 4 | 1,875 / 1,845 / 30 | 61.500 | 61,500.0 | 30,000 | 1,845,000 / 1,845,001 |
| [Player 16 r3](artifacts/server-send/20260913T123552Z/matrix/player-16/run-3) | cadence_failed | 7 | 1,875 / 1,841 / 34 | 61.367 | 61,366.7 | 34,000 | 1,841,000 / 1,841,001 |

所有 rows 的 `run-status.json` 都是 `evidence_valid`，且每個 attempt 都通過事後唯讀驗證：

```text
examples/metrics/scripts/run-server-send-validation.sh --validate-only <attempt-dir>
```

14/14 directories（2 correctness + 12 matrix）回傳成功；profile-only 也回傳成功。

Error rate：12 個 matrix attempts 的 sender `partial=0`、sender `error=0`，Gate delivery 與 WebSocket
write 的 `result="error"` delta 為 0，client `duplicate=0`、`sequence_gap=0`、`invalid=0`、
`reader_failures=0`。因此 application／delivery error rate 為 0；`cadence_failed` 是 scheduler missed
tick 分類，不是 error counter，也不應被改寫成 server error。

## 5. Three-run median and latency

下表 latency 來自 baseline 到 final Prometheus histogram counter delta。quantile 是 histogram bucket
upper bound，不是插值後的精確百分位；單位為 ms。`sender` 是 Game synchronous server-send call，
`Gate delivery` 是 Gate 對 room／player 的 enqueue delivery，`client` 是 Game push timestamp 到
load reader 收到 frame 的端到端時間。

| case | status | ticks/s median (range) | client msg/s median (range) | missed median (range) | sender avg / p50 / p95 / p99 | Gate avg / p50 / p95 / p99 | client avg / p50 / p95 / p99 |
|---|---|---:|---:|---:|---|---|---|
| Broadcast 33ms | 3/3 complete | 30.300 (30.300–30.300) | 30,300 (30,300–30,300) | 0 (0–0) | 0.616 / 1 / 2.5 / 5 | 1.143 / 2.5 / 2.5 / 5 | 3.064 / 5 / 10 / 10 |
| Player 33ms | 0/3 complete | 30.267 (30.267–30.267) | 30,266.7 (30,266.7–30,266.7) | 1 (1–1) | 8.077 / 10 / 25 / 25 | 1.107 / 1 / 2.5 / 5 | 6.858 / 10 / 10 / 25 |
| Broadcast 16ms | 0/3 complete | 62.500 (62.367–62.500) | 62,500 (62,366.7–62,500) | 0 (0–4) | 0.590 / 0.5 / 1 / 5 | 1.042 / 1 / 2.5 / 5 | 2.949 / 5 / 10 / 10 |
| Player 16ms | 0/3 complete | 61.500 (61.367–61.867) | 61,500 (61,366.7–61,866.7) | 30 (19–34) | 7.708 / 10 / 10 / 25 | 1.065 / 1 / 2.5 / 5 | 6.569 / 10 / 10 / 25 |

核心比較：Player sender average 約 7.7–8.1ms，遠高於 Broadcast 約 0.59–0.62ms；Player 的 Redis
batch lookup 與 endpoint grouping 會被納入 synchronous sender duration。雖然平均仍低於 16ms，
histogram p99 已到 25ms bucket，且 missed tick 與 sender critical path 同時出現，表示 tail latency
而非平均 latency 是目前 producer cadence 的壓力點。

## 6. Runtime and saturation data

process CPU 是 macOS `top` measured-window 的最高 sampled `%CPU`；不是 host CPU time，也未除以
`GOMAXPROCS`。host idle 是同一窗口最低 sampled idle。queue 是每秒 Prometheus scrape 的最高觀測值，
不是每一瞬間的絕對最大值。

| case | Game / Gate / load CPU max median (range) | host idle min median (range) | Gate queue max median (range) | write in-flight max median (range) | Redis CPU median (range) | Redis commands/s median (range) |
|---|---:|---:|---:|---:|---:|---:|
| Broadcast 33ms | 0.5% / 23.8% / 22.3% (0.5–0.6 / 23.7–24.6 / 22.3–22.8) | 30.00% (19.77–46.70) | 0 (0–223) | 0 (0–1) | 0.95% (0.92–0.98) | 278 (274–279) |
| Player 33ms | 9.3% / 26.7% / 22.3% (8.9–9.3 / 26.0–26.8 / 21.8–22.7) | 31.13% (27.87–43.82) | 269 (0–373) | 3 (0–9) | 7.28% (7.24–7.47) | 30,554 (30,527–30,613) |
| Broadcast 16ms | 0.9% / 46.3% / 44.0% (0.9–1.1 / 45.6–46.7 / 43.8–44.1) | 31.78% (29.93–31.80) | 178 (0–372) | 1 (0–2) | 1.27% (1.20–1.29) | 310 (306–312) |
| Player 16ms | 18.5% / 50.5% / 44.0% (18.2–18.7 / 49.9–52.1 / 42.6–44.7) | 22.15% (19.89–28.17) | 187 (7–383) | 1 (1–3) | 12.78% (12.67–13.13) | 61,920 (61,669–62,131) |

共同資源指標：四個 case 的 Redis active samples 均為 28 個；`connected_clients` 最大 25、
`blocked_clients=0`、`rejected_connections=0`。Game Redis pool 的 final snapshot 顯示
`total_connections=11`、`idle_connections=11`、`pending_requests=0`、`wait_total=0`、
`timeouts_total=0`。因此沒有 Redis pool wait 或連線耗盡證據。

| case | final Game / Gate / load goroutines | final heap MB（Game / Gate / load） | `go_sched_latencies_seconds` p99 upper bucket（Game / Gate / load） |
|---|---:|---:|---:|
| Broadcast 33ms | 16 / 2,021 / 1,010 | 2.51 / 78.42 / 15.63 | 0.918ms / 10.486ms / 0.918ms |
| Player 33ms | 20 / 2,024 / 1,010 | 5.33 / 82.31 / 14.19 | 0.082ms / 10.486ms / 10.486ms |
| Broadcast 16ms | 15 / 2,021 / 1,010 | 2.53 / 52.84 / 13.45 | 0.082ms / 10.486ms / 0.918ms |
| Player 16ms | 21 / 2,024 / 1,010 | 3.94 / 52.30 / 18.51 | 0.082ms / 10.486ms / 10.486ms |

final goroutine／heap 是 drain 後仍保留 1000 readers 的 snapshot，不能直接解讀為 connection teardown
後的 leak。所有 attempt drain 後 terminal gauges、write queue 與 readers contract 均通過 validator。

## 7. Bottleneck diagnosis

### 7.1 已由數據支持的判斷

1. **第一失守在 Game producer cadence。** Player 33ms 三次各 missed 1；Player 16ms 三次 missed
   19/30/34。missing 數均為 `missed × 1,000`，且 Gate delivery、WebSocket write、client receive
   完全相等，表示未在 Gate/client 端額外丟失。
2. **Player routing path 成本顯著高於 Broadcast。** Player sender average 約 8ms、Redis commands/s
   約 30.6k（16ms 約 61.9k）；Broadcast sender 約 0.6ms、Redis commands/s 約 0.3k。這符合 Player
   的 Redis ownership／endpoint lookup、grouping、chunking 與 gRPC forwarding 都在同步 sender call
   內的設計。
3. **Gate/WebSocket 有 transient pressure，但目前不是 terminal saturation。** Queue sampled max
   最高 383、write in-flight 最高 9；然而 drain 後都為 0，Gate delivery p99 仍在 5ms bucket，沒有
   queue overflow 或永久累積證據。
4. **目前沒有 host 或 Redis hard saturation 證據。** host 尚有 19.89% 最低 idle；Redis CPU、pool
   wait、blocked/rejected connections 都未接近設計上的飽和判斷。這只能排除目前觀測到的明顯飽和，
   不能排除 shared-host scheduling 的邊界影響。

### 7.2 尚不能定論的部分

- `Game server-send duration` 將 Redis routing 與 gRPC completion 一起計算；現有 metrics 沒有把
  endpoint lookup、每批 gRPC RPC、channel queue／stream wait 分開，因此不能單憑該 histogram 判定
  gRPC implementation 還是 Redis routing 是主要成本。
- Game profile 的 gRPC `RecvMsg`、Redis `Pipelined` 與 `BatchPlayerSender` cumulative stack 有重疊；
  cumulative time 不能相加，也不能視為 CPU 使用率。
- 本次是單一 macOS host、單一 Redis standalone、單一 Gate／Game process；不能外推 production
  capacity，也不能排除 OrbStack／macOS network scheduling 或背景負載。

## 8. Game profile-only and Go trace

為針對第一失守邊界，另跑 [profile-only Player 16ms / Game](artifacts/server-send-profile/20260913T125941Z/profile/player-16/run-1)，
條件為 1000 connections、30s measured、`GOMAXPROCS=4`、`--trace`。此 run 不計入 matrix median：

- `1,875` planned、`1,853` attempted、`22` missed；client received `1,853,000`，Gate delivery
  `1,853,000`，WebSocket server-send write `1,853,001`。
- CPU profile：20.11s window，5.12s samples（25.46%）。主要 flat samples 為
  `runtime.kevent` 36.52%、`runtime.pthread_cond_wait` 22.27%、`syscall.rawsyscalln` 16.99%。
  application cumulative stack：`pushRunner.sendPlayers`／`BatchPlayerSender.SendToPlayers` 約 11.13%，
  Redis `Pipelined` 約 10.35%；cumulative 值有重疊，不可相加。
- Go trace net delay：9.89s，其中 `internal/poll.(*FD).Read` 5.72s、`Accept` 4.17s；sync delay
  23.89s，其中 `runtime.selectgo` 13.78s、`runtime.chanrecv1` 8.92s。這是 goroutine blocking
  duration，不是 CPU time。
- Go trace syscall delay：51.17ms，其中 `syscall.syscall` 49.94ms；scheduler delay 95.50ms，
  `runtime.systemstack_switch` 43.62ms。這支持等待／network／scheduler 候選方向，但沒有唯一指向
  某個 gRPC syscall。
- heap snapshot in-use 約 6.66MB；其中 gRPC buffer pool 約 35.56%。profile collector 自身的
  `StartCPUProfile`／trace allocations 也在 snapshot 中，不能作 steady-state allocation 結論。

原始 pprof／trace：

- [CPU profile](artifacts/server-send-profile/20260913T125941Z/profile/player-16/run-1/pprof/game-cpu-20s.pb.gz)
- [heap profile](artifacts/server-send-profile/20260913T125941Z/profile/player-16/run-1/pprof/game-heap.pb.gz)
- [goroutine snapshot](artifacts/server-send-profile/20260913T125941Z/profile/player-16/run-1/pprof/game-goroutine.txt)
- [Go trace](artifacts/server-send-profile/20260913T125941Z/profile/player-16/run-1/pprof/game-trace-5s.out)
- [profile status/window](artifacts/server-send-profile/20260913T125941Z/profile/player-16/run-1/pprof/status.tsv)

## 9. Evidence index and reproducibility

每個 attempt directory 都包含 `run-status.json`、`metadata.txt`、`phase-events.jsonl`、
`commands.txt`、`metrics/`、`os/`、Redis before／after 與每秒 `os/redis.tsv`。完整 campaign manifest
為 [evidence-manifest.tsv](artifacts/server-send/20260913T123552Z/evidence-manifest.tsv)。

四個 case 的完整 artifacts：

- [Broadcast 33ms](artifacts/server-send/20260913T123552Z/matrix/broadcast-33)
- [Player 33ms](artifacts/server-send/20260913T123552Z/matrix/player-33)
- [Broadcast 16ms](artifacts/server-send/20260913T123552Z/matrix/broadcast-16)
- [Player 16ms](artifacts/server-send/20260913T123552Z/matrix/player-16)

核心資料來源欄位：

- RPS／missed／client correctness：各 attempt `run-status.json`。
- sender／Gate delivery／client latency：`metrics/baseline-*.prom` 與 `metrics/final-*.prom` 的
  histogram count／sum／bucket delta。
- queue／in-flight／readers：`metrics/*.tsv` 對應 snapshots。
- Go runtime：各 role final `/metrics` 的 goroutines、heap、GC、`go_sched_latencies_seconds`、
  `process_cpu_seconds_total`。
- process／host load：`os/process-top.txt`、`os/host-top.txt`、`os/vm-stat.txt`、`os/nettop.txt`、
  `os/netstat-before.txt`／`after`、`os/lsof-*`。
- Redis：`os/redis.tsv`、`redis-before.txt`、`redis-before-measured.txt`、`redis-after.txt`。
- pprof／trace：profile run `pprof/` 與 `status.tsv`／`window.tsv`。

本報告數字只引用持久化 artifacts；raw artifacts 應保留在 local／CI artifact storage，不應提交到
Git source history。測試期間未執行任何 Git staging 操作。

## 10. Next diagnostic boundary

若要把目前的候選縮小到 application gRPC、Redis routing 或 OS scheduling，下一輪只需針對 Player
16ms 做同條件對照：保留相同 1000 connections 與 collector，增加可區分「Redis lookup／grouping」
與「gRPC forwarding／response wait」的 stage metrics，或以預先解析 endpoint 的 controlled run
隔離 Redis lookup。現有數據已足以指出應先 profile Game Player sender，但不足以支持直接修改 gRPC
max streams、channel 數量或宣稱 OS 是唯一根因。
