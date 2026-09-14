# Client → Gate → Game Echo 效能驗證報告

## 1. 執行摘要

本次依 CLIENT_GATE_GAME_ECHO_PERFORMANCE_VALIDATION_DESIGN.md 重新建立 fresh process 與 binary，三個 Go process 均使用 GOMAXPROCS=2，並以 Redis 保持可用的同機 loopback topology 驗證完整 Client → Gate WebSocket → Gate→Game gRPC → Game handler → unary reply → Gate WebSocket write → Client 鏈路。100 connections 的三次有效嘗試中位數為 53,654.1 RPS、p95/p99 為 5/5 ms；200 connections 為 52,631.0 RPS、p95/p99 為 10/10 ms，RPS 下降約 1.91%，因此依停止條件在 200 停止。所有容量與 A/B/C attempts 均 correctness-valid，error、timeout、counter mismatch 與 drain 後 in-flight/queue 殘留均為零；目前最有證據的候選是 WebSocket/gRPC network I/O wait，尚未證明是 grpc-go implementation、kernel 或硬體根因。CPU/trace profile 已提供 writer、syscall 與 http2Client.operateHeaders/socket read 證據；WriteBuffer B/C 的收益分別為 +0.75% 與 -0.36%，未超過波動，因此不執行 D 或改變預設 buffer。

## 2. 版本、環境與 topology

### 2.1 Artifact 與執行時間

容量結果只使用下列新 campaign 的 3/3 valid attempts；correctness run 不納入中位數：

| Campaign | UTC 起迄 | 用途 |
|---|---|---|
| [retest-20260914T122506Z](artifacts/echo-performance/retest-20260914T122506Z) | 12:25:19–12:29:03 | correctness + 100/200 staircase |
| [retest-20260914T122506Z-profile](artifacts/echo-performance/retest-20260914T122506Z-profile) | 12:29:24–12:30:39 | 200 connections CPU/goroutine/trace profile-only |
| [retest-20260914T122506Z-buffer-a](artifacts/echo-performance/retest-20260914T122506Z-buffer-a) | 12:32:51–12:34:55 | A：Gate client/Game server default |
| [retest-20260914T122506Z-buffer-b](artifacts/echo-performance/retest-20260914T122506Z-buffer-b) | 12:35:09–12:37:13 | B：Gate client 64 KiB、Game server default |
| [retest-20260914T122506Z-buffer-c](artifacts/echo-performance/retest-20260914T122506Z-buffer-c) | 12:37:25–12:39:30 | C：Gate client default、Game server 64 KiB |

版本與環境來源為 [manifest.txt](artifacts/echo-performance/retest-20260914T122506Z/manifest.txt)、[binaries.sha256](artifacts/echo-performance/retest-20260914T122506Z/binaries.sha256) 與各 campaign manifest：

| 項目 | 值 |
|---|---|
| Git HEAD | e73acdce6831bcde2a22b09b801d90189d909273 |
| Git state | dirty（測試未執行任何 staging/commit 操作） |
| Go / grpc-go | go1.27.1 darwin/arm64 / v1.72.0 |
| OS | Darwin 23.5.0 arm64 |
| host logical CPU | 8 |
| process GOMAXPROCS | load、Gate、Game 均 2 |
| Gate connections per host | 1 |
| MaxConcurrentStreams | 0（不傳 option，使用 grpc-go default） |
| WriteBuffer baseline | Gate client/Game server 均 0（不傳 option，使用 grpc-go default） |

本次各 campaign 的 game、gate、load binary SHA-256 相同：

~~~
game  0fe7432800d8336ccc4c04341d3bc758940630d81128b4d5c3be4b49f78d8b64
gate  67af0a0a9bb90e13118550539d9b18857af2d50529acd4a9c9ad922ddaee1541
load  0f1a4a6721b32aad5f2d6390ff792457ddbaaf9cee073a56ec41038b8e4ef957
~~~

### 2.2 固定測試條件

| 項目 | 值 |
|---|---|
| route | game |
| capacity levels | 100 → 200 → 400 → 800 → 1600 → 3200，依停止條件提前終止 |
| measured window | 每次 30s admission；RPS 使用 marker 的實際 admission duration |
| payload | application payload 32 bytes |
| warm-up | 每條 WebSocket connection 1 筆 Echo，不計入 measured counters |
| setup | concurrency 32、setup timeout 2m |
| request timeout | 10s |
| scrape | Game、Gate、load 每 1s |
| 正式嘗試 | 每個 capacity level 3 次 fresh-process attempts |
| Redis | 127.0.0.1:6379，測試期間保持運作 |

### 2.3 請求邊界

~~~
load Client
  → Gate WebSocket read / dispatcher
  → Gate gRPC client
  → Game gRPC server / dispatcher / Echo handler
  → Forward unary response 原路回 Gate
  → Gate WebSocket write queue / writer
  → load Client read
~~~

Login、EnterRoom、session ownership 與 endpoint setup 使用 Redis；Echo measured hot path 沒有 per-request Redis GET。這是 macOS 單機 loopback 實驗，load、Gate、Game、Redis/OrbStack 與 kernel 共用 host，結果只代表本 topology，不外推為 production capacity。

### 2.4 實驗前後的可編譯與 contract 驗證

以下命令均成功，且未改變 Git staging：

~~~
bash -n examples/metrics/scripts/run-echo-performance-validation.sh \
  examples/metrics/scripts/run-echo-performance-validation-test.sh
bash examples/metrics/scripts/run-echo-performance-validation-test.sh
go test ./pkg/gatelink ./pkg/grpcserver ./products/gateproduct ./products/gameproduct ./examples/metrics/load
go test -race ./pkg/gatelink ./pkg/grpcserver ./examples/metrics/load
git diff --check
~~~

## 3. Attempt validity

每個 attempt 都檢查 readiness、四個 orchestration marker 的相同 run_id、warm-up 後 zero baseline、error/cancelled delta、三個 GOMAXPROCS、端到端 counter equality、histogram count、terminal gauges、host sample 與 process exit。所有下列 attempts 都是 valid=true；沒有 invalid reason。connections=1 是 correctness gate，不進入容量中位數。

| level | attempt | RPS | p95 | status |
|---:|---:|---:|---:|---|
| 1（correctness） | [01](artifacts/echo-performance/retest-20260914T122506Z/connections-0001/attempt-01/run-status.json) | 10,712.6 | 0.25 ms | valid；不納入 capacity |
| 100 | [01](artifacts/echo-performance/retest-20260914T122506Z/connections-0100/attempt-01/run-status.json) | 54,669.5667 | 5 ms | valid |
| 100 | [02](artifacts/echo-performance/retest-20260914T122506Z/connections-0100/attempt-02/run-status.json) | 53,654.0667 | 5 ms | valid |
| 100 | [03](artifacts/echo-performance/retest-20260914T122506Z/connections-0100/attempt-03/run-status.json) | 51,950.2 | 5 ms | valid |
| 200 | [01](artifacts/echo-performance/retest-20260914T122506Z/connections-0200/attempt-01/run-status.json) | 52,631.0 | 10 ms | valid |
| 200 | [02](artifacts/echo-performance/retest-20260914T122506Z/connections-0200/attempt-02/run-status.json) | 52,627.6 | 10 ms | valid |
| 200 | [03](artifacts/echo-performance/retest-20260914T122506Z/connections-0200/attempt-03/run-status.json) | 54,897.6667 | 10 ms | valid |

容量 campaign 的機器判定為 [campaign-status.json](artifacts/echo-performance/retest-20260914T122506Z/campaign-status.json) 的 valid=true；完整 attempt index 與原始 metrics/host/logs 均保留在各 attempt directory。

## 4. 容量結果

### 4.1 RPS、latency 與停止條件

| connections | 三次 RPS（原值） | median | min–max | 相對前 level | client p95 / p99 |
|---:|---|---:|---:|---:|---:|
| 100 | 54,669.5667 / 53,654.0667 / 51,950.2 | 53,654.0667 | 51,950.2–54,669.5667 | — | 5 / 5 ms |
| 200 | 52,631.0 / 52,627.6 / 54,897.6667 | 52,631.0 | 52,627.6–54,897.6667 | −1.9068% | 10 / 10 ms |

RPS 計算為 successful_echo_requests / admission_duration_seconds，不使用名義 duration 或 process 啟動以來的 cumulative counter。200 相較 100 的 RPS 中位數沒有成長且 p95/p99 加倍，符合 RPS 成長 <=5% 且 latency 增加 >=10% 的停止條件，因此不進入 400。

### 4.2 Client 與 server stage latency

以下數字由每個 attempt 的 baseline→final histogram delta 計算；quantile 是 bucket upper bound，不是插值百分位。mean 為 sum delta/count delta；stage 欄位列出三次 attempts 的 p95 中位數。

| level | E2E mean / p50 / p95 / p99 | Gate command p95 | Gate→Game gRPC p95 | Game handler p95 | Game request-player p95 | Gate delivery p95 | WebSocket write p95 |
|---:|---|---:|---:|---:|---:|---:|---:|
| 100 | 1.860 / 2.5 / 5 / 5 ms | 5 ms | 5 ms | 0.005 ms | 0.001 ms | 0.025 ms | 0.025 ms |
| 200 | 3.796 / 5 / 10 / 10 ms | 5 ms | 5 ms | 0.005 ms | 0.001 ms | 0.025 ms | 0.025 ms |

200 level 的 Gate command 與 Gate→Game gRPC mean 約 2.762/2.760 ms，Game handler mean 約 0.001600 ms；因此目前不能把 Game handler 本身視為主要延遲來源。原始 stage histogram 位於各 attempt 的 metrics/{service}-baseline.prom 與 metrics/{service}-final.prom。

### 4.3 Correctness、錯誤與 terminal state

每個 level 的 measured success counter delta 如下；同一列的七個 counter 必須完全相等：

~~~
load Echo success
= Gate websocket forward success
= Gate gRPC code=OK
= Game Echo command success
= Game request_player success
= Gate connection enqueue queued
= Gate server-send WebSocket write success
~~~

| level | 三次 load success（亦等於上述六個 server counters） | error / cancelled | connection_failures | duration _count mismatch |
|---:|---|---:|---:|---:|
| 100 | 1,640,087 / 1,609,622 / 1,558,506 | 0 / 0 | 0 | 0 |
| 200 | 1,578,930 / 1,578,828 / 1,646,930 | 0 / 0 | 0 | 0 |

每次 final snapshot 的 Gate WebSocket/Game gRPC/Game command/load in-flight、Gate write queue 與 write in-flight 都為 0；三個 /metrics 的 go_sched_gomaxprocs_threads 都為 2。Echo 沒有增加 async broadcast/player queue 或 Redis per-request dependency counter。

### 4.4 Runtime、saturation 與 host/OS

下表為三次 attempts 的 median；括號是三次 attempts 的 min–max。process CPU 是 measured window 內 process_cpu_seconds_total delta/30s，並同時列 raw macOS top %CPU 的 max。raw %CPU 的 100% 約等於一個 logical CPU；normalized 是相對本 process GOMAXPROCS=2 的比例。

| level | raw process max CPU（load / Gate / Game） | normalized average（load / Gate / Game） | RSS MB（load / Gate / Game） | goroutines（load / Gate / Game） | final GC max s（load / Gate / Game） | scheduler p99（load / Gate / Game） |
|---:|---|---|---|---|---|---|
| 100 | 54.1% (47.6–54.3) / 103.4% (103.3–104.8) / 90.8% (90.0–91.9) | 0.678 / 1.357 / 1.170 cores（33.9% / 67.8% / 58.5% of 2P） | 22.203 / 39.156 / 28.266 | 11 / 38 / 17 | 0.000783 / 0.000828 / 0.000695 | 0.918 / 10.486 / 10.486 ms |
| 200 | 58.2% (53.7–58.7) / 105.9% (104.2–106.2) / 92.3% (89.8–93.5) | 0.722 / 1.406 / 1.191 cores（36.1% / 70.3% / 59.6% of 2P） | 25.234 / 50.016 / 27.797 | 11 / 21 / 18 | 0.000991 / 0.001166 / 0.000801 | 0.918 / 10.486 / 10.486 ms |

Sampled maximum in-flight/queue：100 level 為 load 100、Gate command/gRPC 100、Gate write 2、Gate queue 1、Game command/request-player 1；200 level 為 load 200、Gate command/gRPC 200、Gate write 1、Gate queue 2、Game command/request-player 1。這些 in-flight 值在 closed-loop 模型中接近 connection count 是預期行為，不能單獨當成 saturation ratio；drain 後均回到 0。

host 原始資料來自各 attempt 的 [processes.tsv](artifacts/echo-performance/retest-20260914T122506Z/connections-0200/attempt-01/host/processes.tsv)、[top.txt](artifacts/echo-performance/retest-20260914T122506Z/connections-0200/attempt-01/host/top.txt)、nettop.csv 與 socket-queues.tsv：

| level | host idle min（median；range） | process context switches delta（load / Gate / Game） | SYSBSD delta（load / Gate / Game） | SYSMACH delta（load / Gate / Game） |
|---:|---:|---|---|---|
| 100 | 10.96%（4.4–13.52；每 attempt sample median 的 median 26.5%） | 1,540,915 / 1,449,820 / 1,352,129 | 5,819,643 / 5,130,041 / 1,101,472 | 57 / 60 / 56 |
| 200 | 4.71%（1.2–8.82；每 attempt sample median 的 median 21.3%） | 1,538,054 / 1,353,776 / 1,230,115 | 5,855,565 / 4,997,144 / 877,774 | 63 / 64 / 58 |

socket sampler 會保留符合實驗 ports 的所有 TCP state；100 level 每 attempt 約 8,692–8,814 rows，最大 Recv-Q/Send-Q 為 2,878–5,606 / 5,833–13,624 bytes；200 level 約 14,592–14,629 rows，最大 Recv-Q/Send-Q 為 8,575–12,942 / 18,352–18,650 bytes。這些是每秒快照，且包含 ESTABLISHED、LISTEN、TIME_WAIT，沒有連續 queue 堆積證據；RTT/retransmission 沒有被本輪 sampler 解析，完整原始輸出仍保留於各 attempt host/。

## 5. 瓶頸證據鏈

| 問題 | 關鍵數據 | 原始證據 | 可支持的推論 | 不能推論的事 |
|---|---|---|---|---|
| 容量是否出現平台 | 100→200 RPS 53,654.1→52,631.0（−1.91%），p95/p99 5→10 ms | [summary.tsv](artifacts/echo-performance/retest-20260914T122506Z/summary.tsv)、各 run-status.json | 本 topology 的 200 connections 是容量 knee 候選 | 不能宣稱實體硬體或 OS 已到極限 |
| Game handler 是否主要成本 | 200 handler mean 0.001600 ms、p95 0.005 ms；request-player p95 0.001 ms | connections-0200/attempt-*/metrics/game-{baseline,final}.prom | handler/本機 request-player 不是主要延遲區段 | 不能只靠此排除 Game gRPC transport |
| Gate gRPC 是否包含主要等待 | 200 Gate command/Gate→Game gRPC mean 約 2.762/2.760 ms，E2E mean 3.796 ms | connections-0200/attempt-*/metrics/{gate,load}-{baseline,final}.prom | 問題縮小到 Gate gRPC call / response transport 與其前後等待 | 不能直接定論 grpc-go 有 defect |
| writer/syscall 是否為候選 | Gate WebSocket write cumulative 30.36%；Game gRPC loopyWriter.run cumulative 19.19%、bufWriter.Flush 17.73%；load WebSocket write 45.93% | [CPU profiles](artifacts/echo-performance/retest-20260914T122506Z-profile/profiles/connection-0200/cpu/attempt-01/profiles) | network writer/flush/syscall 是可驗證候選 | 不能推導調大 WriteBuffer 一定有效 |
| 是否存在 network wait | Gate trace net internal/poll.(*FD).Read 98.61% delay；load 99.67%；Gate scheduler http2Client.operateHeaders 70.53% delay | [trace profiles](artifacts/echo-performance/retest-20260914T122506Z-profile/profiles/connection-0200/trace/attempt-01/profiles) | 證據指向 socket read / gRPC response-header wait；CPU + trace 已指向同一 I/O/transport 邊界 | trace 的 goroutine waiting time 不是 CPU time，不能單獨歸因 kernel |
| Redis 是否是 Echo hot-path 瓶頸 | Game pool final idle=11,total=11,pending=0,timeouts=0；Gate idle=100,total=100,pending=0,timeouts=0（少量 wait 來自 setup/ownership） | connections-0200/attempt-*/metrics/{game,gate}-final.prom | 沒有 Redis pool saturation 證據，且 Echo 不做 per-request Redis | 不能由 setup Redis 指標推導 production Redis capacity |
| socket queue 是否已 backpressure | 快照有非零 Send-Q/Recv-Q，但非持續、含多種 TCP state | 各 attempt host/socket-queues.tsv、host/nettop.csv | 可列為 kernel/receiver backpressure 候選 | 不能把單點快照當成 sustained backpressure |

因此本報告的證據層級是「縮小至 network I/O / transport 邊界，根因證據不足」，不是「已證明 grpc-go 或 OS 有問題」。

## 6. Profile-only 與 WriteBuffer A/B

### 6.1 Profile-only

在 200 connections、baseline buffer、同一組 binary 下，CPU profile 與 trace 使用不同 fresh process run，避免 profiler observer effect 互相污染。CPU run 同時抓 Game/Gate/load 20s CPU profile 與 goroutine；trace run 同時抓三個 process 5s Go trace。

| profile | 結果 | 原始證據 |
|---|---|---|
| CPU 20s | 3/3 profile status ok；RPS 44,211.0333，只作 observer-effect 參考 | [cpu/status.tsv](artifacts/echo-performance/retest-20260914T122506Z-profile/profiles/connection-0200/cpu/attempt-01/profiles/status.tsv)、各 *-cpu-20s.pb.gz/*-goroutine.txt |
| Trace 5s | 3/3 profile status ok；RPS 53,451.7667，不納入 capacity median | [trace/status.tsv](artifacts/echo-performance/retest-20260914T122506Z-profile/profiles/connection-0200/trace/attempt-01/profiles/status.tsv)、各 *-trace-5s.out |

go tool pprof -top -cum 的主要 stack：

| process | 主要 profile 證據 |
|---|---|
| Gate | syscall.rawsyscalln flat 93.96%；WebSocket read cumulative 56.26%、WebSocket write 30.36%；gRPC loopyWriter.run 約 5.50% |
| Game | syscall.rawsyscalln flat 27.94%；gRPC loopyWriter.run cumulative 19.19%；bufWriter.Flush 17.73%；Echo handler metrics 仍為微秒級 |
| load | syscall.rawsyscalln flat 80.15%；WebSocket write cumulative 45.93%、read 34.29% |

Trace 的 scheduler delay 顯示 Game processUnaryRPC cumulative 約 13.35%，Gate http2Client.operateHeaders 約 70.53%；trace net 顯示 Gate FD.Read 98.61%、load FD.Read 99.67%。這些 waiting time 表示 response/read path 的等待候選，不是單獨的 CPU 飽和證明。CPU 與 trace 已提供兩種獨立證據指向 I/O/transport 邊界，因此沒有再把 local Echo 擴展成另一個 capacity matrix；它不會改變本輪容量結論。

### 6.2 WriteBuffer A/B/C

Profile 同時出現 writer/flush/syscall stack，host 也有 context-switch/syscall 壓力，且 Echo correctness 全部通過，符合設計定義的條件式 A/B/C 觸發條件。A/B/C 只改一側的建立期 config，三個 binary checksum 均與 baseline 相同。

#### RPS 與 latency

| 條件 | Gate client | Game server | 三次 RPS | RPS median | MAD / median | E2E mean median | E2E p50 / p95 / p99 |
|---|---:|---:|---|---:|---:|---:|---:|
| A（[summary.tsv](artifacts/echo-performance/retest-20260914T122506Z-buffer-a/summary.tsv)） | default | default | 54,823.9333 / 53,710.4333 / 52,746.9333 | 53,710.4333 | 1.7939% | 3.7201 ms | 5 / 10 / 10 ms |
| B（[summary.tsv](artifacts/echo-performance/retest-20260914T122506Z-buffer-b/summary.tsv)） | 64 KiB | default | 54,704.3 / 53,056.4667 / 54,110.8667 | 54,110.8667 | 593.4333 / 1.0967% | 3.6924 ms | 5 / 10 / 10 ms |
| C（[summary.tsv](artifacts/echo-performance/retest-20260914T122506Z-buffer-c/summary.tsv)） | default | 64 KiB | 54,846.8667 / 53,325.9667 / 53,514.4667 | 53,514.4667 | 188.5 / 0.3522% | 3.7337 ms | 5 / 10 / 10 ms |

B 相對 A 為 +0.7455%，C 相對 A 為 −0.3649%；兩者都未達至少 5%，且 B 小於 A 的 MAD/median 1.7939%。兩者 p95/p99 都沒有改善，也沒有相同 RPS 下可重現的 CPU/syscall 下降，因此 B/C 均判定為無效益。依設計立即停止，不執行 D 或 128 KiB。

#### CPU/syscall 對照

以下是每條件三次 measured host samples 的 median（括號為 min–max）；CPU 為 raw top process 平均，SYSBSD 為 process sample 首尾 delta，順序均為 load / Gate / Game。

| 條件 | raw CPU average %（load / Gate / Game） | context switches delta（load / Gate / Game） | SYSBSD delta（load / Gate / Game） |
|---|---|---|---|
| A | 52.55 (51.33–53.94) / 99.75 (98.48–100.97) / 85.99 (84.35–87.78) | 1,577,239 (1,571,550–1,578,512) / 1,319,335 (1,305,190–1,337,496) / 1,239,941 (1,230,869–1,254,205) | 5,888,353 (5,849,108–5,904,000) / 5,009,327 (4,949,655–5,015,836) / 859,284 (846,101–869,171) |
| B | 53.84 (51.31–54.36) / 100.46 (97.74–101.24) / 86.57 (84.54–88.18) | 1,598,623 (1,595,019–1,618,463) / 1,331,598 (1,308,671–1,347,687) / 1,231,369 (1,201,942–1,234,484) | 5,925,015 (5,852,850–5,940,117) / 5,020,182 (4,963,422–5,026,675) / 845,998 (829,690–885,087) |
| C | 52.88 (52.29–53.76) / 99.92 (99.90–100.80) / 85.98 (85.73–87.58) | 1,587,483 (1,561,239–1,612,089) / 1,307,859 (1,301,233–1,342,577) / 1,222,136 (1,194,725–1,241,952) | 5,900,226 (5,780,128–5,938,685) / 4,932,883 (4,900,151–5,065,370) / 878,639 (841,640–879,865) |

上表 A/B/C 的所有 attempts 都 valid=true，詳見各 campaign 的 connections-0200/attempt-*/run-status.json。WriteBufferSize 是 userspace batching capacity，不是實際每次 net.Conn.Write 的 bytes；本輪沒有增加 per-write runtime metric，也沒有以單一 socket 快照推導 bytes。

## 7. 結論、限制與唯一下一步

### 結論

1. 在 8 logical CPU、三個 process 各 GOMAXPROCS=2、 一條 Gate→Game gRPC connection 的同機 loopback topology，200 WebSocket connections 是本次測試的容量 knee 候選；100→200 沒有 RPS 成長且 p95/p99 加倍。
2. 端到端 correctness 穩定：每個 measured success 都在 Gate、Game、delivery 與 WebSocket write counters 對齊，沒有 error、timeout、unexpected close 或 drain 殘留。
3. Game handler/request-player 保持微秒級；Gate command 與 gRPC response wait 隨壓力上升，CPU profile/trace 指向 WebSocket read/write、gRPC writer/response-header 與 syscall network I/O。這足以把問題縮小至 I/O/transport 邊界，但不足以定論 grpc-go、macOS kernel 或硬體根因。
4. Gate client 或 Game server 單側提高至 64 KiB 都沒有超過 run-to-run variation 的改善；目前不應改變 WriteBuffer default。

### 限制

- 這不是 production capacity：三個服務、Redis/OrbStack 與 host kernel 共用單一 macOS loopback。
- MaxConcurrentStreams=0、connections per host=1、WebSocket buffer/queue 未作 A/B；本報告不把它們的影響混入 WriteBuffer 結論。
- pprof/trace 只用來解釋成本，profile run 的 RPS 有 observer effect，不得併入容量 median。
- host/socket 是秒級快照；沒有連續 socket backpressure 或每次 write bytes 的證據。
- Git state 為 dirty；binary checksum、manifest 與 raw artifacts 已保存以便重現本次結果。

### 唯一下一步

在獨立 host，或具有 network namespace 隔離的 VM（沒有 Redis/OrbStack 與其他工作共享 CPU），以同一 binary 與 200 connections 重跑一次 baseline + profile/host trace，驗證目前的 network I/O/transport 候選是否仍成立；在此驗證前不同時調整 buffer、connection 數、OS 或 protocol。
