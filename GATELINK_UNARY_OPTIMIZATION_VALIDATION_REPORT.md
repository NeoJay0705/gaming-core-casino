# GateLink Unary Optimization Validation Report

## 1. 執行摘要

本報告使用最後一份完整 campaign 的 raw evidence：
[`artifacts/unary-optimization/20260914T180128Z-rerun/`](artifacts/unary-optimization/20260914T180128Z-rerun/)。
先前的 `20260914T174658Z` 與 `20260914T175507Z-rerun` 僅用於發現並修正摘要欄位與 client scheduler collector 問題，不納入本報告結論。

- correctness gate（`stream_workers=0`、concurrency=1、10 秒）通過。
- 正式矩陣 `stream_workers=0/4/8` 各 3 次，共 9 次，全部 valid；error、cancelled、payload/counter mismatch、partial 與 terminal in-flight 都是零。
- `stream_workers=4` 的 Game CPU/request median 相對 `0` 降低 **2.159%**；`8` 降低 **3.952%**。兩者都未達設計要求的 5%，因此本次 workload 不能把 reusable stream workers 稱為有效 candidate，也不應把它設成 framework default。
- RPS median 為 `101,131.367`（0）、`104,247.967`（4）、`103,023.867`（8）；候選沒有超過 2% 下降，但 RPS 增益不足以抵銷未達 CPU/request 門檻的結果。
- metadata bounded lookup benchmark 有明確改善（logging：`264.2 → 18.27 ns/op`、`480 → 16 B/op`；GateLink：`393.2 → 72.98 ns/op`、`576 → 112 B/op`），且 contract 不變，應保留 metadata 修正。
- profile 顯示主要成本在 gRPC HTTP/2 transport、runtime scheduler/netpoll/syscall，以及 logging/trace；Game 業務 handler 與 `request_player` reply-slot 區段只有微秒級。shared macOS host idle 約 `0.48%–2.57%`，可判斷測試接近此主機的 CPU 飽和，但不能據此定論實體硬體或作業系統是唯一根因。
- alloc profile 沒有足夠證據支持 custom buffer pool；目前沿用 grpc-go default pool。

依設計第 10 節與第 11 節的收斂規則，最終建議是：保留 metadata bounded lookup；`stream_workers` 未通過 production candidate 門檻，不設為 framework default，也不提供建議 production 值。因 worker=8 的改善穩定高於本次波動，且 validation runner 需要此建立期 seam 才能重現 0/4/8 A/B，因此保留 optional experimental config，預設維持 0；不新增 custom buffer pool。

## 2. 測試範圍與方法

實際執行的正式 campaign：

```text
bash examples/metrics/scripts/run-unary-optimization-validation.sh \
  --artifact-dir artifacts/unary-optimization/20260914T180128Z-rerun
```

路徑是 direct `grpcload → Game GateRequestService.Forward`。Game handler 透過既有 dispatcher 處理 Echo，`DirectRequestPlayerSender` 將 reply 寫入原 unary reply slot；這不是額外的 Gate 網路 server-send RPC。每一格都以 fresh Game 與 grpcload process 執行，上一格停止且 port 釋放後才進下一格。

固定條件（來源：[`manifest.txt`](artifacts/unary-optimization/20260914T180128Z-rerun/manifest.txt)、各 attempt 的 `metadata.json`）：

| 項目 | 值 |
|---|---:|
| `GOMAXPROCS`（Game/grpcload） | 4 / 4 |
| total concurrency | 400 closed-loop workers |
| ClientConn | 1 |
| application payload | 32 bytes |
| measured admission | 30 秒 |
| warm-up | 每 ClientConn 1 筆，不計入 measured metrics |
| request timeout | 10 秒 |
| worker variants | 0 / 4 / 8 |
| `MaxConcurrentStreams` | grpc-go default（metadata 為 0） |
| write buffer | grpc-go default（metadata 為 0） |
| metrics scrape | 每約 1 秒 |

正式 round-robin 順序為 `0→4→8`、`4→8→0`、`8→0→4`。correctness gate 不納入正式 median。每次都保存 baseline/final Prometheus、periodic Prometheus、orchestration marker、logs、host samples 與 binary checksums。

另執行兩次獨立 profile-only campaign（不把其 RPS 混入正式 median）：

- [`profile-worker-0`](artifacts/unary-optimization/20260914T180128Z-profile-worker-0/)
- [`profile-worker-8`](artifacts/unary-optimization/20260914T180128Z-profile-worker-8/)

每次在 measured window 同時取得 Game 與 grpcload 20 秒 CPU profile、alloc profile、goroutine dump 與 host samples。metadata benchmark 使用設計指定的 `-count 5 -benchmem`，完整 stdout 在 [`metadata-benchmark.txt`](artifacts/unary-optimization/20260914T180128Z-rerun/metadata-benchmark.txt)。

## 3. 版本、環境與稽核

來源：[`manifest.txt`](artifacts/unary-optimization/20260914T180128Z-rerun/manifest.txt)、[`binaries.sha256`](artifacts/unary-optimization/20260914T180128Z-rerun/binaries.sha256)。

| 項目 | 值 |
|---|---|
| Go | `go1.27.1 darwin/arm64` |
| grpc-go | `v1.72.0` |
| OS | macOS Darwin 23.5.0，Apple M2，arm64 |
| logical CPU | 8 |
| Git HEAD | `fd4f7ab1a07dccf3c1f966045ae7a1fd5a49321b` |
| Game SHA-256 | `c1a16a31936f00cc1d271811f9fdce38f1fa29fa664b6755fbb27564c881d72d` |
| grpcload SHA-256 | `a50731478615f20a42ab49508dab2a6e4d65e2ba7075d3f70d96f3f02dc4543d` |
| Redis | preflight `PING/PONG` 通過；OrbStack 未停止 |

campaign 執行前後均只讀取 `git status --short`；該次 campaign 完成當下 `git diff --cached --name-status` 為空，測試腳本未執行 `git add/reset/checkout/restore/commit` 或 `pkill`。raw artifacts 位於 `.gitignore` 的 `artifacts/unary-optimization/`，沒有加入 staging。

## 4. Attempt validity

所有 attempt 都符合設計第 8 節：markers 的 run ID 一致、measured 成功數大於零、success counter/histogram 與 marker 相等、error/cancelled/partial 為零、Game/grpcload in-flight 最終為零、兩端 `go_sched_gomaxprocs_threads=4`，且 periodic metrics、host samples 與 logs 存在。

| 類別 | worker | attempt | successful requests | 結果 | `run-status.json` |
|---|---:|---:|---:|---|---|
| correctness | 0 | 1 | 183,747 | valid | [`correctness/attempt-01/run-status.json`](artifacts/unary-optimization/20260914T180128Z-rerun/correctness/attempt-01/run-status.json) |
| formal | 0 | 1 | 3,033,941 | valid | [`workers-000/attempt-01/run-status.json`](artifacts/unary-optimization/20260914T180128Z-rerun/workers-000/attempt-01/run-status.json) |
| formal | 0 | 2 | 3,082,721 | valid | [`workers-000/attempt-02/run-status.json`](artifacts/unary-optimization/20260914T180128Z-rerun/workers-000/attempt-02/run-status.json) |
| formal | 0 | 3 | 2,978,159 | valid | [`workers-000/attempt-03/run-status.json`](artifacts/unary-optimization/20260914T180128Z-rerun/workers-000/attempt-03/run-status.json) |
| formal | 4 | 1 | 3,127,439 | valid | [`workers-004/attempt-01/run-status.json`](artifacts/unary-optimization/20260914T180128Z-rerun/workers-004/attempt-01/run-status.json) |
| formal | 4 | 2 | 3,131,463 | valid | [`workers-004/attempt-02/run-status.json`](artifacts/unary-optimization/20260914T180128Z-rerun/workers-004/attempt-02/run-status.json) |
| formal | 4 | 3 | 3,044,796 | valid | [`workers-004/attempt-03/run-status.json`](artifacts/unary-optimization/20260914T180128Z-rerun/workers-004/attempt-03/run-status.json) |
| formal | 8 | 1 | 3,090,716 | valid | [`workers-008/attempt-01/run-status.json`](artifacts/unary-optimization/20260914T180128Z-rerun/workers-008/attempt-01/run-status.json) |
| formal | 8 | 2 | 3,149,111 | valid | [`workers-008/attempt-02/run-status.json`](artifacts/unary-optimization/20260914T180128Z-rerun/workers-008/attempt-02/run-status.json) |
| formal | 8 | 3 | 3,062,211 | valid | [`workers-008/attempt-03/run-status.json`](artifacts/unary-optimization/20260914T180128Z-rerun/workers-008/attempt-03/run-status.json) |

machine summary：[`summary.tsv`](artifacts/unary-optimization/20260914T180128Z-rerun/summary.tsv)；campaign 結果：[`campaign-status.json`](artifacts/unary-optimization/20260914T180128Z-rerun/campaign-status.json)。

## 5. 正式矩陣：RPS 與 latency

下表是 `summary.tsv` 的三次原值；latency 的 p50/p95/p99 是 histogram bucket upper bound，不是插值出的精確 percentile。

| worker | attempt | RPS | mean (s) | p50 upper (s) | p95 upper (s) | p99 upper (s) |
|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 101,131.367 | 0.003948984 | 0.005 | 0.010 | 0.010 |
| 0 | 2 | 102,757.367 | 0.003887521 | 0.005 | 0.010 | 0.010 |
| 0 | 3 | 99,271.967 | 0.004023309 | 0.005 | 0.010 | 0.010 |
| 4 | 1 | 104,247.967 | 0.003831664 | 0.005 | 0.010 | 0.010 |
| 4 | 2 | 104,382.100 | 0.003826573 | 0.005 | 0.010 | 0.010 |
| 4 | 3 | 101,493.200 | 0.003935238 | 0.005 | 0.010 | 0.010 |
| 8 | 1 | 103,023.867 | 0.003876817 | 0.005 | 0.010 | 0.010 |
| 8 | 2 | 104,970.367 | 0.003804742 | 0.005 | 0.010 | 0.010 |
| 8 | 3 | 102,073.700 | 0.003912770 | 0.005 | 0.010 | 0.010 |

| worker | RPS median (min–max; MAD) | mean latency median (min–max; MAD) |
|---:|---:|---:|
| 0 | 101,131.367 (99,271.967–102,757.367; 1,626.000) | 3.948984 ms (3.887521–4.023309; 0.061463 ms) |
| 4 | 104,247.967 (101,493.200–104,382.100; 134.133) | 3.831664 ms (3.826573–3.935238; 0.103574 ms) |
| 8 | 103,023.867 (102,073.700–104,970.367; 950.167) | 3.876817 ms (3.804742–3.912770; 0.035953 ms) |

相對 worker 0：worker 4 RPS `+3.082%`、worker 8 `+1.871%`；兩者 p95/p99 bucket 沒有惡化。

## 6. 資源、scheduler 與 host 負載

### 6.1 CPU、allocation、GC

CPU/request、B/request、allocs/request 與 GC 欄位依設計公式，都是 baseline→final delta 除以 measured 成功數；`client` 是 grpcload，`game` 是 Game。`client_gc_seconds`/`game_gc_seconds` 在此 summary 代表 GC seconds/request；完整 raw counter 仍在各 attempt 的 Prometheus snapshots。

| worker | client CPU/request median (min–max; MAD) | Game CPU/request median (min–max; MAD) | client B/request median (min–max; MAD) | Game B/request median (min–max; MAD) |
|---:|---:|---:|---:|---:|
| 0 | 2.61100e-5 (2.60755e-5–2.61174e-5; 3.45e-8) | 2.46777e-5 (2.45589e-5–2.47252e-5; 1.188e-7) | 9,101.17 (9,101.15–9,101.23; 0.02) | 7,060.12 (7,060.07–7,060.15; 0.03) |
| 4 | 2.58389e-5 (2.58008e-5–2.59990e-5; 1.601e-7) | 2.41449e-5 (2.39370e-5–2.43501e-5; 2.052e-7) | 9,101.07 (9,101.06–9,101.26; 0.01) | 7,060.00 (7,059.97–7,060.00; 0) |
| 8 | 2.58780e-5 (2.57294e-5–2.59275e-5; 1.486e-7) | 2.37024e-5 (2.36102e-5–2.37773e-5; 7.49e-8) | 9,101.16 (9,101.11–9,101.31; 0.05) | 7,060.10 (7,059.99–7,060.11; 0.01) |

| worker | client allocs/request median (min–max; MAD) | Game allocs/request median (min–max; MAD) | client GC/request median (min–max; MAD) | Game GC/request median (min–max; MAD) |
|---:|---:|---:|---:|---:|
| 0 | 144.045 (144.044–144.046; 0.001) | 117.610 (117.610–117.611; 0) | 2.45176e-7 (2.40720e-7–2.53622e-7; 4.456e-9) | 3.33270e-7 (3.25097e-7–3.41605e-7; 8.173e-9) |
| 4 | 144.044 (144.043–144.047; 0.001) | 117.606 (117.602–117.608; 0.002) | 2.40452e-7 (2.40417e-7–2.47644e-7; 3.5e-11) | 3.39512e-7 (3.38146e-7–3.41532e-7; 1.366e-9) |
| 8 | 144.045 (144.045–144.048; 0) | 117.603 (117.602–117.604; 0.001) | 2.48033e-7 (2.35508e-7–2.48462e-7; 1.2525e-8) | 3.54833e-7 (3.49295e-7–3.56749e-7; 1.916e-9) |

Game CPU/request median 由 `2.46777e-5` 降至 `2.41449e-5`（4）與 `2.37024e-5`（8），但只達 2.159% 與 3.952%，低於 5% 門檻。Game B/request 與 allocs/request 幾乎不變；worker 8 的 Game GC/request 比 worker 0 高約 6.47%，雖然絕對值很小，不能視為改善證據。

### 6.2 Scheduler、goroutines、host

Scheduler p99 是 Go runtime histogram 的 bucket upper bound；profile-only 之外的 scheduler metric 來自正式 attempt 的 baseline/final delta。

| worker | client sched p99 median (min–max; MAD) | Game sched p99 median (min–max; MAD) | client max goroutines median (min–max; MAD) | Game max goroutines median (min–max; MAD) | host idle min median (min–max; MAD) |
|---:|---:|---:|---:|---:|---:|
| 0 | 0.01048576 s (same; 0) | 0.000917504 s (same; 0) | 415 (same; 0) | 89 (58–102; 13) | 1.69% (0.66–2.55; 0.86) |
| 4 | 0.01048576 s (same; 0) | 0.000917504 s (same; 0) | 415 (same; 0) | 99 (77–110; 11) | 0.99% (0.66–2.24; 0.33) |
| 8 | 0.01048576 s (same; 0) | 0.000917504 s (0.000917504–0.01048576; 0) | 415 (same; 0) | 109 (56–148; 39) | 1.18% (0.48–2.57; 0.70) |

host idle 很低，部分 `top.txt` snapshot 的 user+sys 超過 90%；這是 shared host 上接近 CPU 飽和的訊號。因 Game 與 grpcload 共用同一台 host，且沒有隔離 CPU、network stack 或其他背景負載，這只能支持「本測試在此環境受 CPU/runtime/transport 壓力」的推論，不能排除應用程式 gRPC/logging 成本，也不能單獨證明硬體極限。原始 host 證據位於各 attempt 的 [`host/top.txt`](artifacts/unary-optimization/20260914T180128Z-rerun/workers-008/attempt-03/host/top.txt) 與 [`host/processes.tsv`](artifacts/unary-optimization/20260914T180128Z-rerun/workers-008/attempt-03/host/processes.tsv)。

### 6.3 Game 內部階段 metrics

這些是相同正式 snapshots 中的 Game metrics；`request_player` 在此 direct Echo 是 reply-slot acceptance，不是另一個 network hop。

| worker | Gate command mean median | Gate command p95/p99 upper | `request_player` mean median | `request_player` p95/p99 upper |
|---:|---:|---:|---:|---:|
| 0 | 2.0018 µs (1.9891–2.0295) | 5 / 10 µs | 0.5861 µs (0.5702–0.5914) | 1 / 2.5 µs |
| 4 | 2.0173 µs (2.0127–2.0255) | 5 / 10 µs | 0.5678 µs (0.5580–0.5688) | 1 / 2.5 µs |
| 8 | 2.0212 µs (1.9738–2.0830) | 5 / 25 µs | 0.5598 µs (0.5528–0.5840) | 1 / 2.5 µs |

相較約 3.8–4.0 ms 的 end-to-end round trip，這兩個 Game 本機區段只有微秒級；因此目前數據把主要延遲定位在 gRPC transport/runtime，而非 Echo handler 本身，但不能把 histogram bucket 當成每個底層 syscall 的精確分解。

## 7. CPU profile：絕對 sample 與主要 stack

profile raw 與 `go tool pprof -top -cum` 輸出：

- worker 0 Game：[`game-cpu-20s.pb.gz`](artifacts/unary-optimization/20260914T180128Z-profile-worker-0/profiles/workers-000/attempt-01/profiles/game-cpu-20s.pb.gz)、[`game-cpu-top-cum.txt`](artifacts/unary-optimization/20260914T180128Z-profile-worker-0/profiles/workers-000/attempt-01/profiles/game-cpu-top-cum.txt)
- worker 0 client：[`grpcload-cpu-20s.pb.gz`](artifacts/unary-optimization/20260914T180128Z-profile-worker-0/profiles/workers-000/attempt-01/profiles/grpcload-cpu-20s.pb.gz)、[`grpcload-cpu-top-cum.txt`](artifacts/unary-optimization/20260914T180128Z-profile-worker-0/profiles/workers-000/attempt-01/profiles/grpcload-cpu-top-cum.txt)
- worker 8 Game：[`game-cpu-20s.pb.gz`](artifacts/unary-optimization/20260914T180128Z-profile-worker-8/profiles/workers-008/attempt-01/profiles/game-cpu-20s.pb.gz)、[`game-cpu-top-cum.txt`](artifacts/unary-optimization/20260914T180128Z-profile-worker-8/profiles/workers-008/attempt-01/profiles/game-cpu-top-cum.txt)
- worker 8 client：[`grpcload-cpu-20s.pb.gz`](artifacts/unary-optimization/20260914T180128Z-profile-worker-8/profiles/workers-008/attempt-01/profiles/grpcload-cpu-20s.pb.gz)、[`grpcload-cpu-top-cum.txt`](artifacts/unary-optimization/20260914T180128Z-profile-worker-8/profiles/workers-008/attempt-01/profiles/grpcload-cpu-top-cum.txt)

| profile | wall duration | total CPU samples |
|---|---:|---:|
| Game, worker 0 | 20.13 s | 55.06 s |
| grpcload, worker 0 | 20.13 s | 58.47 s |
| Game, worker 8 | 20.12 s | 59.11 s |
| grpcload, worker 8 | 20.14 s | 62.32 s |

sample 秒數高於 wall duration 是因為 `GOMAXPROCS=4` 的多核 CPU samples；cumulative frame 會重疊，以下秒數不可相加。

| process / worker | 主要 cumulative stacks（絕對 sample seconds） |
|---|---|
| Game / 0 | `runtime.schedule` 20.01；`runtime.kevent` 13.40；`runtime.systemstack` 11.85；gRPC `serveStreams` 8.27；`bufWriter.Flush`/`FD.Write` 7.08；`processUnaryRPC` 7.95；`runtime.morestack` 3.03。 |
| Game / 8 | `runtime.schedule` 24.10；`runtime.kevent` 14.06；gRPC `loopyWriter.run` 10.18；`FD.Write` 8.86；`processUnaryRPC` 6.62；`runtime.morestack` 2.72。 |
| client / 0 | `ClientConn.Invoke` 17.21；`syscall.rawsyscalln` 17.01；`runtime.schedule` 16.88；`io.ReadFull` 13.60；logging `fillRandom` 10.52；`runtime.morestack` 2.39。 |
| client / 8 | `syscall.rawsyscalln` 17.97；`runtime.schedule` 19.44；`UnaryClientInterceptor`/`ClientConn.Invoke` 17.39；logging `fillRandom` 10.59；`runtime.morestack` 2.64。 |

worker 8 的 Game `morestack` sample 比 worker 0 小（2.72 vs 3.03 秒），但 Game `runtime.schedule` 反而較大（24.10 vs 20.01 秒），client 方向也不一致；沒有形成設計要求的「mechanism stack 同方向下降」證據。profile 支持的是 transport/runtime 與 logging/trace 是主要成本，不能推論 `NumStreamWorkers` 可普遍改善 blocking handler。

## 8. Allocation profile 與 buffer pool 判斷

raw 與 pprof top：

- Game 0：[`alloc-space-top`](artifacts/unary-optimization/20260914T180128Z-profile-worker-0/profiles/workers-000/attempt-01/profiles/game-alloc-space-top.txt)、[`alloc-objects-top`](artifacts/unary-optimization/20260914T180128Z-profile-worker-0/profiles/workers-000/attempt-01/profiles/game-alloc-objects-top.txt)
- Game 8：[`alloc-space-top`](artifacts/unary-optimization/20260914T180128Z-profile-worker-8/profiles/workers-008/attempt-01/profiles/game-alloc-space-top.txt)、[`alloc-objects-top`](artifacts/unary-optimization/20260914T180128Z-profile-worker-8/profiles/workers-008/attempt-01/profiles/game-alloc-objects-top.txt)
- client 0/8：相同目錄下的 `grpcload-alloc-space-top.txt` 與 `grpcload-alloc-objects-top.txt`。

| process / worker | alloc_space total | alloc_objects total |
|---|---:|---:|
| Game / 0 | 14.15 GB | 242,771,345 |
| Game / 8 | 14.97 GB | 247,227,646 |
| client / 0 | 19.146 GB | 304,281,102 |
| client / 8 | 19.34 GB | 313,091,562 |

主要 allocation stack（flat bytes）：

- Game 0：`http2Server.operateHeaders` 2.96 GB（20.89%）、`readMetaFrame.func1` 2.64 GB（18.67%）、剩餘相容性所需 `metadata.FromIncomingContext` 0.79 GB（5.57%）。
- Game 8：相同三類為 3.11 GB（20.79%）、2.78 GB（18.58%）、0.857 GB（5.73%）。
- client 0：`http2Client.createHeaderFields` 1.876 GB（9.80%）、`newStream` 1.328 GB（6.94%）、`cancelCtx.propagateCancel` 1.082 GB（5.65%）、`metadata.MD.Copy` 0.929 GB（4.85%）。
- client 8：上述類別約 1.95 GB、1.30 GB、1.12 GB、0.95 GB，方向沒有形成 custom pool 的直接證據。

profile 中仍可看到 grpc-go transport 的 metadata copy；這是 transport/header 與保留的 `Forward` presence/compatibility copy，並不表示 bounded lookup 修正失效。bounded helper 的完整行為由 benchmark/contract 驗證，不能以一次 allocation profile 量化所有省下的 copy。

目前 payload 只有 32 bytes，且 profile 的主要來源是 HTTP/2 stream/header、context 與 metadata，不是明確的 receive/marshal buffer size class。因此依設計第 10.4 節，證據不足以另開 custom buffer pool 設計，應沿用 grpc-go v1.72.0 default pool。

## 9. Metadata benchmark

完整原始輸出：[`metadata-benchmark.txt`](artifacts/unary-optimization/20260914T180128Z-rerun/metadata-benchmark.txt)。下表列出五次原值與 median。

| package / case | 五次 `ns/op` | median `ns/op` | `B/op` | `allocs/op` |
|---|---|---:|---:|---:|
| logging / legacy | 265.6, 274.0, 262.3, 264.2, 262.6 | 264.2 | 480 | 7 |
| logging / bounded | 18.14, 18.69, 18.27, 18.29, 18.04 | 18.27 | 16 | 1 |
| gatelink / legacy | 392.2, 393.2, 391.9, 403.4, 393.9 | 393.2 | 576 | 10 |
| gatelink / bounded | 73.30, 72.98, 72.88, 76.49, 72.55 | 72.98 | 112 | 4 |

bounded 相對 legacy：logging `ns/op -93.09%`、`B/op -96.67%`、allocs `-85.71%`；GateLink `ns/op -81.45%`、`B/op -80.56%`、allocs `-60.00%`。所有 contract tests（missing/malformed/duplicate/trim/error behavior）均通過。這支持保留 bounded extraction，但不代表 HPACK 或 transport metadata 成本已消失。

## 10. 證據鏈、支持與不能推論的事

| 證據 | 支持的推論 | 不能推論的事 |
|---|---|---|
| [`summary.tsv`](artifacts/unary-optimization/20260914T180128Z-rerun/summary.tsv) 九次 valid、RPS/latency/resource delta | 本 workload 的 worker A/B 可重現且 correctness 成立 | 不代表 production payload、blocking handler 或多連線 topology 同樣結果 |
| Game/Client `process_cpu_seconds_total`、`B/request`、scheduler metrics | worker 4/8 未達 5% Game CPU/request 門檻；host/runtime 壓力明顯 | 不可把 process-wide allocation 全歸因於單一 RPC，也不能單獨證明硬體極限 |
| CPU profile 的 `runtime.schedule`、`kevent`、HTTP/2 write/Invoke、logging stacks | 主要成本落在 runtime/transport/syscall 與 logging/trace | 不能把 cumulative seconds 相加，或宣稱某一 stack 是唯一根因 |
| Game stage histogram 約微秒、E2E 約毫秒 | Echo handler/reply-slot 不是目前主要延遲段 | 不足以拆出每個 kernel syscall 或 network buffer wait 的精確時間 |
| metadata benchmark 與 contract tests | bounded metadata copy 是行為等價且可量測的局部優化 | 不代表 HPACK、outgoing metadata 或所有 future metadata workload 都改善 |
| alloc profile 主要為 HTTP/2/header/context/metadata | 目前沒有 custom buffer pool 的必要證據 | 不代表 default pool 對未來 production-size payload 永遠最佳 |

## 11. 最終決策與限制

1. **Metadata bounded lookup：保留。** 這是已定位、行為等價且 benchmark 明顯改善的局部修正；保留 `Forward` compatibility 所需的一次完整 metadata copy。
2. **`stream_workers`：不設為 production default，但保留 optional experimental seam。** worker 4/8 都未達 5% CPU/request 門檻，profile mechanism 也未同方向下降，因此不提供建議 production 值；worker=8 的改善仍穩定高於 baseline relative MAD，validation runner 需要此設定才能重現相同 A/B，故保留 config，預設維持 0。若後續 workload 也無法證明超過波動的效益，再依設計移除 config/example。
3. **custom buffer pool：不做。** grpc-go default pool 已存在，且本次 alloc profile 沒有 buffer pool 不適合的證據。
4. **錯誤與容量：** 本次 400 concurrency、單 ClientConn、loopback synthetic Echo 沒有 error 或 correctness failure；host idle 很低，說明此環境已接近 CPU 壓力區，但不是跨環境容量承諾。
5. **觀測限制：** profile-only 使用 pprof observer，與正式 RPS 分離；macOS host 與兩個 process 共用資源；`p95/p99` 是 bucket upper bound；raw Prometheus 與 profile 才是可稽核的數據基礎。

## 12. Repository regression

實驗後執行並通過：

```text
go test ./...
go test -race ./pkg/logging ./pkg/gatelink ./pkg/grpcserver ./examples/metrics/grpcload
go vet ./...
bash -n examples/metrics/scripts/run-unary-optimization-validation.sh
bash -n examples/metrics/scripts/run-unary-optimization-validation-test.sh
bash examples/metrics/scripts/run-unary-optimization-validation-test.sh
git diff --check
```

該次測試完成當下，`git diff --cached --name-status` 為空；測試腳本未改變 staging。此敘述是 campaign 稽核紀錄，不代表閱讀報告當下的 repository 狀態。
