# Server Send Broadcast／Player 效能驗證設計

## 1. 目的與結論

本次在既有 `examples/metrics` 上加入可重複執行的 server send 壓測，驗證兩條已存在的正式 framework
能力：

1. Game 使用 `serversend.BroadcastSender`，經 Redis Pub/Sub primary（失敗時才 fallback gRPC）送到 Gate，
   Gate 經 `RemoteCommandChannel` dispatcher 解出 room routing envelope，對房內連線送出 opaque client
   protobuf payload。
2. Game 使用 `serversend.PlayerSender.SendToPlayers`，以 1000 人的單一 logical batch 查詢 Redis
   ownership／Gate endpoint、按 endpoint 分組及按 1 MiB encoded bytes 切批，再使用既有 generic gRPC
   `Forward` 送到 Gate；Gate 經同一個 `RemoteCommandChannel` dispatcher 解出 login name，client payload
   保持 opaque，最後送到各自 WebSocket。

第一輪固定 1000 個已登入且進入同一房間的 WebSocket connections，分開執行下列四個案例：

| 案例 | 每次 sender call | interval | 理論 sender calls/s | 理論 client messages/s |
|---|---:|---:|---:|---:|
| Broadcast 33ms | 1 room command | 33ms | 約 30.30 | 約 30,303 |
| Broadcast 16ms | 1 room command | 16ms | 62.5 | 62,500 |
| Player 33ms | 1 batch × 1000 players | 33ms | 約 30.30 | 約 30,303 |
| Player 16ms | 1 batch × 1000 players | 16ms | 62.5 | 62,500 |

理論值只用來驗證 admission 與 delivery 完整性；報告一律使用實際 emitted sender calls 與實際收到的
client messages，不用理論值取代量測。

現有 Gate、Redis pool、Go runtime/process 與 pprof 能力保留。現有 Game metrics 只觀測
`request_player` count，無法知道 Broadcast／Player sender call 的 latency、error 與 in-flight；現有 load
client 也只能做 synchronous Echo round trip，無法持續讀取 asynchronous push。因此必須補齊這兩個
觀測缺口，否則即使測出 client 掉訊息，也無法判斷問題發生在 producer、Gate queue/write 或 client。

本設計不建立新的通用壓測 framework，不改變正式 server-send contract，也不為了診斷預先加入 Redis
command stage、gRPC internal stream 或 OS-specific application metrics。先用穩定公開邊界縮小問題，再對被
指向的 process 做獨立 pprof run。

## 2. 需求理解與合理假設

- 測試目標是 server push throughput、latency、error 與 saturation，不在正式測量期間混入 Echo request。
- Redis 正常的 primary path 是第一輪 baseline；Redis failure 與 gRPC fallback 已有 contract tests，本輪不把
  fault injection 混入 capacity 結果。
- 所有 1000 人先完成 WebSocket、Login、EnterRoom，且進入固定 `load-room`；全部 ready 後才允許 Game
  producer 開始。
- payload 固定為 32 bytes 的 example user data。外層 `PushMessage` 仍包含 run ID、sequence 與 send
  timestamp，因此報告必須同時記錄 application payload bytes 與實際 protobuf encoded bytes。
- Game、Gate 與 load 預設以 `GOMAXPROCS=4` 執行；Redis 不套 Go process 限制。若改值，該結果只能在相同
  值的 runs 間比較。
- 每個案例先做 5 秒 warm-up，再做 30 秒 measured run，drain 最多 10 秒；每個 attempt 都重新啟動
  Game、Gate 與 load，每個 case 取得三個可比較 attempts。
- 第一輪可在同一台 macOS host 執行，但報告必須標示 shared-host limitation，並分別收集 Game、Gate、load
  與 Redis process 資源。若要宣稱單一服務或 host capacity，需在後續把 load generator 與 services 分離。
- Game 與 client 使用不同 process 的 wall clock 計算 push end-to-end latency。同 host 時共用 OS clock；跨
  host 重跑前必須確認 clock synchronization。無法確認時，client latency 只能作趨勢參考，Gate 與 sender
  duration 仍可獨立判讀。

上述假設足以決定實作方向，沒有必須先由使用者選擇、且會改變 framework 架構的歧義。

## 3. 測量邊界與資料流

### 3.1 Broadcast

```text
example fixed-rate runner
  -> BroadcastSender.Broadcast
  -> Redis PUBLISH
  -> Gate Redis subscriber
  -> RemoteCommandChannel dispatcher
  -> BroadcastRoom(room_id)
  -> 1000 per-connection write queues
  -> WebSocket writers
  -> 1000 load readers
```

Game sender duration只涵蓋 `Broadcast` call；Redis primary 成功代表 publish 被接受，不代表 Gate 或 client
已收到。Gate server-send delivery duration 涵蓋 Gate 收到 remote command 後至 WebSocket write terminal。
Client push duration從 Game 建立該 tick payload 的 timestamp 到 client 完成 frame read。三個 Histogram 各自
判讀，不相減 quantile 推導不存在的 stage latency。

### 3.2 Player Send

```text
example fixed-rate runner
  -> PlayerSender.SendToPlayers(1000 messages)
  -> Redis pipeline: 1000 ownership lookups
  -> Redis pipeline: distinct Gate endpoint lookups
  -> group by exact endpoint
  -> split by existing 1 MiB encoded limit
  -> existing generic gRPC Forward
  -> Gate RemoteCommandChannel dispatcher
  -> login-name filter + per-connection write queues
  -> WebSocket writers
  -> 1000 load readers
```

Game sender duration涵蓋 Redis routing、group/chunk preparation 與 gRPC completion；Gate 回覆仍只代表 local
delivery handler 完成 enqueue，不等待 WebSocket write。WebSocket terminal 結果由既有 Gate metrics 提供。

Player baseline 每個 tick 必須只呼叫一次 `SendToPlayers`，輸入為 1000 個不同 login names。不得在 example
先自行查 Redis、分 endpoint 或拆成 1000 次 sender call，否則會繞過本次要驗證的正式 batch contract。

## 4. Example protocol 與控制方式

新增 `examples/metrics/internal/protocol/push.proto`；它只屬於 example，不形成 framework production API：

```protobuf
enum PushMode {
  PUSH_MODE_UNSPECIFIED = 0;
  PUSH_MODE_BROADCAST = 1;
  PUSH_MODE_PLAYER = 2;
}

message StartPushRequest {
  string run_id = 1;
  PushMode mode = 2;
  string room_id = 3;
  repeated string login_names = 4;
  uint64 interval_millis = 5;
  uint64 duration_millis = 6;
  uint32 payload_bytes = 7;
}

message StartPushResponse {
  string run_id = 1;
  uint64 planned_ticks = 2;
}

message PushMessage {
  string run_id = 1;
  uint64 sequence = 2;
  int64 sent_unix_nano = 3;
  bytes payload = 4;
}
```

新增固定 command IDs，並由現有 protocol contract test 鎖定唯一性：

- `StartPushRequestCommandID = 0xF1000041`：WebSocket → Gate → Game。
- `StartPushResponseCommandID = 0xF1000042`：使用既有 `RequestPlayerSender` 原 unary reply 回 controller。
- `PushMessageCommandID = 0xF1000043`：Broadcast／Player 最後交給 client 的共同 payload。

Orchestration 另使用 example-only filesystem markers `final-ready.json` 與 `final-scrape.json`：前者由
load 在 measured drain／summary 完成後建立，後者由 script 完成 final metrics snapshot 後建立；兩者都帶
measured `run_id`，不形成 production API。

Load 完成全部 1000 人 setup 並啟動每條連線的唯一 reader 後，由第一條 connection 傳一筆
`StartPushRequest`。Request 帶實際 login list，避免 Game 假設 load 的命名規則，也不增加 Redis 協調 key
或額外 HTTP control server。Game 驗證 request、接受 runner 工作並以 `StartPushResponse` 回覆；
`planned_ticks` 是 `floor(duration / interval)`，讓 load 能判定完整接收與 bounded drain。Reader 能按
command ID 處理 response 與 push 的可能交錯，不能假設 response 一定先寫到 socket。

限制：

- `run_id` 非空且有固定長度上限，只寫入 payload／log，不作 Prometheus label。
- mode 只能為 Broadcast 或 Player。
- interval 本輪只接受 16ms 或 33ms；duration 必須為正且設合理上限。
- Broadcast 必須有 room ID；Player 必須恰好收到與 prepared connections 相同、非空且不重複的 login list。
- payload bytes 第一輪固定 32，仍檢查 encoded message 與既有 1 MiB contract。
- 同一 Game process 同時只允許一個 active workload；第二筆 request 回傳明確 error，不取代現有 run。

目前 example 將單次 workload duration 上限固定為 1 小時；這是 bounded validation 的保護，不是正式
server-send API 的全域限制。

這些 validation 是保護 example 的 bounded 行為，不建立可任意編排流量的 DSL。

## 5. Example runner 與 lifecycle

在 `examples/metrics/internal/workflow` 增加 package-private `pushRunner`：

- 由 `GameModule` 透過 DI 取得 `serversend.BroadcastSender`、`serversend.PlayerSender`、
  `serversend.RequestPlayerSender` 與 `prometheus.Registerer`。
- 以 `framework.PhaseService` hook 管理 accepting state 與 cancellation；App stop 必須取消 active run 並等待
  goroutine 結束，不得在 service shutdown 後繼續 sender call。
- handler 只負責 decode／validate、提交一個 bounded job、設定 unary response；真正的固定頻率 loop 不阻塞
  Gate → Game request handler。
- Handler 傳入 runner 的 context 使用 `logging.Detach` 保留 W3C trace、移除 request cancellation；runner 再
  受自己的 duration 與 service lifecycle cancel 控制。不得直接保存即將隨 unary 返回而取消的 request
  context，也不得使用無法被 App stop 取消的 `context.Background()` 執行 sender。
- 每個 run 使用一個 goroutine，sender calls 不重疊。若前一次 call 超過下一個 schedule，跳過已錯過的
  ticks、記錄 `missed`，不可補發 burst，也不可建立無上限 goroutine 或 queue。
- 每輪計畫 `floor(duration / interval)` 個 ticks，第一個 tick 於 runner 接受工作後立即執行，後續依固定
  schedule 執行；這項語意由 fake clock contract test 鎖定。
- sequence 從 1 單調遞增，只在實際開始 sender call 時產生；同一 tick 的 1000 個 Player messages 共用
  sequence 與 `sent_unix_nano`。
- sender error 記錄後繼續下一個未錯過的 tick，使 error rate 可觀測；context cancellation 立即結束。
- run 完成輸出一筆 structured summary log：run ID、mode、interval、duration、targets、attempted／success／
  partial／error／missed calls、起訖 timestamp。不得把 login list 或 payload 寫入 log。

Warm-up 與 measured run 使用兩筆相同設定但不同 run ID 的 control request。Warm-up completion 的
`planned／attempted／success／partial／error／missed` 先由 orchestration 寫入 example-only
`warmup-result.json`；load 完成 quiet drain 後寫入 `warmup-drained.json`，script 驗證 lifecycle／queue
與 terminal gauges，保存 Prometheus baseline，再開始 measured run。`missed`、partial 或 error 是 workload
evidence，不是阻止 measured 的 pass gate；只有 control、process、reader、collector 或 drain validation
失敗才停止該 attempt。不在 production metrics 增加 `phase` label。

## 6. 必要 metrics 修改

### 6.1 Game product metrics

沿用現有 bounded metric `gaming_core_game_server_send_requests_total{operation,result}`，將裝飾器套用到三種
正式 sender：

- `operation="request_player"`；
- `operation="player"`；
- `operation="broadcast"`。

`result` 固定為：

- `success`：`err == nil`；
- `partial`：有非零 `Receipt.AcceptedAt` 且 `err != nil`；
- `error`：沒有 acceptance 且 `err != nil`。

再增加兩個必要 metrics：

| Metric | Type / labels | 用途 |
|---|---|---|
| `gaming_core_game_server_send_duration_seconds` | Histogram `{operation,result}` | 定位 producer API call 是否變慢；涵蓋各 sender 公開 contract 的完整同步範圍 |
| `gaming_core_game_server_send_in_flight` | GaugeVec `{operation}` | 判斷 sender call 是否長時間占用／堆疊；operation 是固定集合 |

Request-player 也使用相同 duration／in-flight decorator，避免一個 metric family 對同一 operation 只有部分
語意。不得加入 login name、room ID、run ID、endpoint、error text 或 command ID labels；也不新增 batch size
Histogram。Player baseline 的 batch size 已固定並記錄於 run metadata，Gate 的 per-target counter可核對實際
logical delivery；現在新增 batch size series不會改變處置決策。

### 6.2 Example runner metrics

Example Game module使用注入的 App-local registerer，只補一個正式 sender metrics 無法表達的 schedule
metric：

| Metric | Type / labels | 用途 |
|---|---|---|
| `gaming_core_example_push_missed_ticks_total` | Counter `{mode}` | sender call超過schedule時跳過的ticks；確認runner是否維持cadence |

成功／partial／error call數及duration直接使用正式Game sender metrics，不在example重複記錄；active run由
control response、起訖log與lifecycle state驗證，不增加Gauge。不另建runner duration Histogram。

### 6.3 Load client metrics

沿用 load-private registry 與 `/metrics` listener，新增：

| Metric | Type / labels | 用途 |
|---|---|---|
| `gaming_core_example_load_push_messages_total` | Counter `{mode,result}` | `result=received|duplicate|sequence_gap|invalid`；觀測 client terminal outcome |
| `gaming_core_example_load_push_delivery_duration_seconds` | Histogram `{mode}` | 有效 PushMessage frame（含 duplicate／sequence gap）從 Game 建立 tick 至 client 完成 read 的 end-to-end latency；invalid frame 沒有可用 timestamp，不納入 Histogram |
| `gaming_core_example_load_push_readers` | Gauge | 正在讀取的 WebSocket readers；測量前應為 1000，結束後歸零 |

每條 connection 只能有一個 goroutine 呼叫 `ReadMessage`。reader 驗證 16-byte header、command ID、protobuf、
run ID 與 sequence；登入名稱或 connection index不可成為 label。測量結束 log 輸出 aggregate received、
duplicate、sequence-gap、invalid、connection failures 與每條 connection 最終 sequence 的 aggregate missing
結果；不列出 1000 筆 connection 明細。

### 6.4 已有且直接使用的 metrics

不修改下列既有 metrics：

- Gate connections／closes；
- Gate `server_send_requests_total{target="player",result}`；Broadcast room目前沒有獨立queued counter，直接
  使用下列delivery terminal Histogram count；
- Gate `server_send_delivery_duration_seconds{target,result}`；
- WebSocket writes、write duration、writes in-flight；
- write queue messages、capacity、full total；
- Redis pool total／idle connections、pending requests、wait count／duration、timeouts；
- Go runtime/process collectors，包括 CPU、memory、goroutines、GC、GOMAXPROCS 與 scheduler latency。

現有 metrics 已能回答 Gate delivery 與 process saturation，再新增同義 metrics 會超出需求。

## 7. Load client 行為

擴充現有 `examples/metrics/load`，不新增第二套 WebSocket client：

- `-workload echo|broadcast|player`，default `echo`，保留現有使用方式。
- push modes 固定使用 `-connections 1000`、`-payload-bytes 32`；`-push-interval` 接受 `33ms` 或 `16ms`。
- 增加 `-push-warmup-duration`（default 5s）與 `-drain-timeout`（default 10s）；`-duration` 是 measured
  producer admission window。
- 正式 baseline 額外傳入 example-only `-orchestration-dir`。Load 完成 setup 後以 atomic rename 寫入
  `clients-ready.json`，並在 setup timeout 內等待 script 建立 `start` marker；只有 marker 出現後才能提交
  warm-up control。Load 收到每個 control response 時，寫入帶 timestamp／run ID 的
  `warmup-started.json`／`measured-started.json`；script 解析 Game structured completion log 後寫入
  `warmup-result.json` 與 `phase-events.jsonl`。`-orchestration-dir` 未設定時保留現有手動行為，但該次
  執行不得當成正式 baseline。
- setup 仍以既有 bounded concurrency 完成 WebSocket、Login、EnterRoom；push reader 只在 setup direct reads
  全部完成後啟動，避免 concurrent WebSocket reads。
- warm-up control run 完成後，script 先寫入 `warmup-result.json`；load 驗證 run ID 與 counters，等待每條
  reader 至少一個 configured interval 的 quiet drain，再以 atomic rename 寫入 `warmup-drained.json`。
  script 驗證 reader／protocol counters、process 存活、Gate queue 與 terminal gauges，保存 baseline 後以
  atomic rename 寫入 `baseline-ready.json`；load 必須在 bounded timeout 內看到此 marker 才能提交 measured
  control。warm-up `missed`、partial、error 或 theoretical client 缺額不阻止 measured；若發生 connection、
  decode、duplicate、sequence-gap、invalid、reader failure、process exit 或 drain timeout，才不開始 measured。
- measured duration 到期後不再產生 server calls；load 等待 producer時間結束並 bounded drain。Orchestration
  模式由 load 在 summary 後寫入 `final-ready.json` 並暫停，script 保存同時點的 final metrics 後寫入
  `final-scrape.json`；load 確認 marker 後才關閉 reader／metrics listener，確保 final scrape 先於關閉。
- 若 control response timeout、reader 少於 1000、任一 connection 關閉、runner 回傳 invalid request，該 run
  依第 10 節分類，但仍保留 metrics、logs 與 OS artifacts，不使用未定義的籠統 `failed` 狀態。

Echo mode 的 flag、closed-loop 行為與 metrics 不變。Push 不與 Echo 同一 run 混合，避免 reader demultiplexing
之外還引入不必要的 request workload。

## 8. 測試矩陣與執行順序

每次先關閉舊 process；campaign 開始時只編譯一次 Game、Gate、load，保存到 campaign root 的 immutable
binary snapshot，再由每個 attempt 複製並驗證相同 checksum 後啟動。確認 Redis dependency 可用並啟動
Game、Gate，確認 `/ready` 後才啟動 load。不得以修改前的 binary 做修改後測試。

Redis 由 OrbStack／container 或團隊指定的 dependency runtime 管理；orchestration 不停止或重啟 Redis，只以
`PING`／`INFO` 做 dependency preflight，並在 active window 每秒保存 `os/redis.tsv`；`redis-before.txt`／
`redis-after.txt` 仍保留完整 raw snapshot。這避免破壞承載其他服務的共享 Redis，同時仍把 Redis 可用性列為
本次 attempt 的必要前提。

固定值：

| 參數 | 值 |
|---|---|
| connections / room members / player targets | 1000 |
| application payload | 32 bytes |
| setup concurrency | 32 |
| setup timeout | 2m |
| request/control timeout | 10s |
| warm-up | 5s |
| measured duration | 30s |
| drain timeout | 10s |
| Game/Gate/load GOMAXPROCS | 4 |
| Prometheus scrape interval | 1s |
| measured repetitions | 3 per case |

四個 cases 應交錯順序執行，避免 host temperature 或 background load 隨時間單向偏移。例如 run 1 使用
Broadcast33、Player33、Broadcast16、Player16；run 2 反向；run 3 再使用原順序。每個案例以三次合法 run
的中位數為主要結果，同時保留 min／max，不只報最好的一次；只有三次均進入 measured 才計算 30 秒
measured median。此處「合法」明確指第 10 節的 `evidence_valid`，不等同所有三輪都
`delivery_complete`。

Correctness gate：先用 10 connections、33ms、5s 各跑一次 Broadcast 與 Player。只有 command routing、
counter equality、client decode 及 shutdown 全部通過，才執行 1000 人矩陣。

### 8.1 唯一執行入口與 phase state machine

為避免人工依文字步驟執行時漏掉 collector、沿用舊 binary，或把 workload failure 誤判為環境無效，正式
baseline 必須由 repository 內單一 `examples/metrics/scripts/run-server-send-validation.sh` orchestration
script 啟動。README 中的個別命令只能用於開發除錯，不得據此產生可比較的 baseline／capacity 結論。
該 script 不實作新的壓測框架，只固定本設計既有 binary、collector 與驗收順序。

正式 campaign 使用 script 的 `--full-campaign` 入口；它只 build 一次 campaign binary snapshot，先執行
Broadcast／Player 各一個 10-connection、33ms correctness run，兩者均為 `evidence_valid` 且
`delivery_complete` 後，才依交錯順序執行四個 1000-connection matrix cases。每個 matrix case 需要三個
`evidence_valid` attempts，最多補兩個 `environment_invalid` attempts。單案例 flags 僅供 smoke、debug
與 profile-only，不代表 formal correctness／matrix 結果。
Correctness 每個 workload 僅執行固定的 `run-1`；若該 attempt 無效即停止 campaign，不套用 matrix retry。

Attempt artifacts 依 phase 分隔，避免 correctness 與 matrix 的相同 case name 覆寫：

```text
<campaign>/correctness/{broadcast-33,player-33}/run-1/
<campaign>/matrix/{broadcast-33,player-33,broadcast-16,player-16}/run-<n>/
<campaign>/profile/<case>/run-1/
```

每個 attempt metadata 都保存 `campaign_root` 與 `campaign_phase`；validator 以這兩個欄位尋找並核對
campaign 的 `binary-checksums.txt`／`source-state.txt`，不以固定相對層級猜測 root。Correctness 通過後
原子寫入 `<campaign>/correctness-gate.json`；matrix attempt 必須驗證此 marker 存在。

每次 attempt 必須依序執行並將每次轉移寫入 `phase-events.jsonl`，不得以固定 `sleep` 推測 phase：

```text
build -> dependency_ready -> services_ready -> load_started
      -> clients_ready -> collectors_ready -> warmup_running -> warmup_draining
warmup_draining -> measured_baseline -> measured_running -> measured_draining
                                      -> final_snapshot -> classified
warmup_draining --lifecycle/evidence fail--> final_snapshot -> classified
```

- `build`：campaign 開始時保存 commit、working tree 狀態、build timestamp 及三個 binary checksum；每個
  attempt 的 `build` phase 只複製並重新驗證該 snapshot，不從可能變動的 working tree 重建。
- `collectors_ready`：application metrics 與第 9.4 節的必要 collector 均完成 preflight；未通過不得開始
  warm-up；script 只有此時才能建立 `start` marker。
- `clients_ready`：1000 條 connection 均完成 Login／EnterRoom，push reader 恰為 1000。
- `warmup_running`／`measured_running` 的開始時間由 load 在收到對應 `StartPushResponse` 時輸出，不由外部
  程序估算；Game completion summary 提供各 phase 結束時間及 planned／attempted／missed。
- Game completion 後，script 以 atomic rename 寫入 `warmup-result.json`；load 驗證後完成 quiet drain 並寫入
  `warmup-drained.json`。script 驗證 process／reader／protocol、Gate queue 與 terminal gauges，保存 warm-up
  後 baseline、Redis snapshot 與 process snapshot，依序寫入 `warmup_draining`、`measured_baseline` phase
  events，最後以 atomic rename 建立 `baseline-ready.json`（含 warm-up `run_id` 與 timestamp）。Load 必須
  驗證 marker 的 `run_id` 後才可送出 measured control；因此 phase event 不會落後 workload。Warm-up missed、
  partial、error 或 theoretical delivery shortage 不阻止 measured；只有上述 lifecycle／evidence failure
  才停止 attempt。
- measured summary 後，load 以 `final-ready.json` 暫停；script 完成 final metrics snapshot 後以同一
  `run_id` 寫入 `final-scrape.json`，load 才關閉 reader／metrics listener。若 barrier timeout，attempt 不得
  宣稱 evidence valid。
- 所有 collector 從 warm-up 前持續至 final snapshot 後；Redis collector 每秒保存 server counters 與 CPU，
  不得在 load 結束後才補抓應屬於 active window 的資料。

`phase-events.jsonl` 每筆至少含 `timestamp`、`case`、`attempt`、`run_id`、`phase` 與 `event=start|end`。
script 最後驗證 artifacts 並產生 machine-readable `run-status.json`，至少包含 collector preflight 結果、
warm-up 與 measured 是否執行、`baseline-ready` 是否已建立、第 10 節分類、第一個失守的 phase／boundary、
planned／attempted／success／missed、warm-up 的 cadence／sender／delivery failure 獨立欄位、client received、
terminal in-flight／queue 是否歸零及每項 evidence 相對路徑。
若無此檔或引用的檔案不存在，該 attempt 只能視為 `environment_invalid`。這兩個檔案是執行協議，不增加
application API 或 production metrics。相同 script 提供 `--validate-only <artifact-dir>` 重做只讀驗證，
不另建第二套判定邏輯。

## 9. 必須收集的資料與證據來源

### 9.1 Run metadata

每輪保存：

- Git commit SHA、`git status --short`、tracked／untracked source checksum 與 binary build timestamp；同一
  campaign root 保存 `bin/{game,gate,load}` 及 `binary-checksums.txt`，每個 attempt 另保存相同的 checksum；
- case、run number、run ID、起訖時間、warm-up／measured／drain window；
- connections、room members、player targets、interval、duration、payload與 encoded payload bytes；
- Game／Gate／load PID、完整 command、config path、GOMAXPROCS；
- Go version、GOOS/GOARCH、grpc-go 與 go-redis module version；
- host model、logical CPU、memory、macOS version；
- Redis version、deployment mode與 address；
- metrics、pprof endpoints。

不得保存 token、credential 或實際業務 payload。Example login names可由 run ID 重建，不逐筆寫入報告。

### 9.2 Prometheus

Game、Gate、load 以 1 秒頻率在同一 Prometheus 時間軸 scrape。每輪保存 warm-up 後／measured 前 baseline，
以及 drain 後 final snapshot；同時保留 measured window 的 time series，不能只拿最後一次 scrape，因 Gauge
peak、queue accumulation 與短暫 CPU saturation 會消失。

至少保存以下原始 series 或 query API JSON：

- 第 6 節所有新增 metrics；
- Gate server-send、delivery、WebSocket write／queue／connection metrics；
- Redis pool metrics；
- `process_cpu_seconds_total`、process RSS／virtual memory；
- goroutines、heap、GC pause／cycles、GOMAXPROCS、`go_sched_latencies_seconds`。

主要推導：

```promql
# producer sender calls/s
rate(gaming_core_game_server_send_requests_total{
  operation="broadcast",result="success"
}[10s])

# sender p95；Player case 將 operation 改為 player
histogram_quantile(0.95, sum by (le) (
  rate(gaming_core_game_server_send_duration_seconds_bucket{
    operation="broadcast",result="success"
  }[30s])
))

# client receive messages/s
rate(gaming_core_example_load_push_messages_total{result="received"}[10s])

# client end-to-end p95
histogram_quantile(0.95, sum by (le) (
  rate(gaming_core_example_load_push_delivery_duration_seconds_bucket[30s])
))

# Gate delivery p95；case 分別使用 room 或 player
histogram_quantile(0.95, sum by (le) (
  rate(gaming_core_gate_server_send_delivery_duration_seconds_bucket{
    target="room",result="success"
  }[30s])
))

# aggregate write queue utilization；同時取 measured window max
gaming_core_gate_websocket_write_queue_messages
/
gaming_core_gate_websocket_write_queue_capacity

# Redis pool contention
rate(gaming_core_redis_pool_wait_total[30s])
rate(gaming_core_redis_pool_wait_seconds_total[30s])
gaming_core_redis_pool_pending_requests

# 各 Go process 使用 GOMAXPROCS 的近似比例
rate(process_cpu_seconds_total[30s])
/
go_sched_gomaxprocs_threads
```

報告的整輪平均 RPS 使用 counter delta 除以實際 measured elapsed；p50／p95／p99 使用該輪 Histogram bucket
delta。不得平均 Histogram `_sum` 後稱為 p95，也不得把不同 stage 的 p95 相減。

### 9.3 Redis server

每輪仍保存 measured 前後完整的 `INFO stats`、`INFO cpu`、`INFO memory`、`INFO clients` 與
`INFO commandstats` raw snapshots；另外由同一 orchestration lifecycle 每秒執行 `INFO stats cpu clients`，
寫入 `os/redis.tsv`：

```text
timestamp  used_cpu_sys  used_cpu_user  total_commands_processed
instantaneous_ops_per_sec  connected_clients  blocked_clients  rejected_connections  status
```

collector 必須在 warm-up 前啟動、final snapshot 後停止；validation 要求 measured start 前至少一筆、measured
window 內至少兩筆、measured end 後至少一筆，timestamp 嚴格遞增且所有 row `status=ok`。command failure 或
缺欄位標記為 evidence unavailable，不偽造 0。

報告以相鄰 row 計算 active-window 指標：

```text
Redis CPU one-core % = delta(used_cpu_sys + used_cpu_user) / delta(wall_time_seconds) * 100
Redis commands/s = delta(total_commands_processed) / delta(wall_time_seconds)
```

至少列 measured window 的 CPU average／max interval、commands/s average／max、blocked clients 與
rejected connections delta。`cmdstat_publish` 與 aggregate `cmdstat_get` call/usec delta 可由完整 raw
snapshot 輔助判讀，但無法區分 ownership GET 與 endpoint GET，不從該資料虛構個別成本。這些資料用來區分
client pool 沒等待但 Redis server 自身已飽和的情況；不因此修改 framework Redis metrics。

### 9.4 OS 與 process load

baseline run 從 measured 前開始以 1 秒頻率保存：

- host CPU user／system／idle、load average；
- memory used/free、memory pressure、page fault、swap；
- Game、Gate、load 各 PID 的 CPU、RSS、thread count；Redis 以 9.3 的 INFO collector 觀測，不虛構 container
  PID；
- host及相關 process context switches（工具可提供時）；
- network bytes/packets、TCP retransmit/error、socket數；
- open-file limit與測試期間實際 fd 數。

目前 macOS 可使用 `top`、`vm_stat`、`nettop` 與測量前後的 `netstat -s`／`lsof -p`；Linux 正式環境使用
node exporter／cAdvisor 及 `pidstat`、`sar` 或等價工具。報告必須記錄實際 command 與原始輸出檔，不能只
留下人工摘要。OS collector 在 active phase 中途失敗時不強制殺掉 application，仍執行 bounded shutdown
並保存資料，但該 attempt 分類為 `environment_invalid`，不得進入比較或 host-level saturation 結論。

第一輪 macOS collector 不得只保存 `vm_stat` 與 `netstat`；orchestration script 至少執行並保存下列等價
資料。命令中的 sample count 由 warm-up、measured 及 drain 總長度計算，process PID 在 services ready 後
代入：

```text
LC_ALL=C top -l <samples> -s 1 -n 0
LC_ALL=C top -l <samples> -s 1 \
  -pid <game-pid> -pid <gate-pid> -pid <load-pid> \
  -stats pid,command,cpu,mem,threads,csw,state
LC_ALL=C vm_stat -c <samples> 1
LC_ALL=C nettop -L <samples> -s 1 -n -P \
  -p <game-pid> -p <gate-pid> -p <load-pid>
LC_ALL=C netstat -s -p tcp             # measured 前與 final 各一次，報告使用 delta
LC_ALL=C lsof -nP -a -p <pid>         # 各 process 於 measured 前與 peak 附近各一次
```

本 script 不使用 `nettop -J bytes_in,bytes_out`；部分 macOS 版本在 idle／短連線或過濾條件下會只輸出
header，省略 process rows。保留 `nettop` 的完整 CSV 欄位仍包含 bytes／packets 及 PID，並在
`commands.txt` 記錄實際命令。collector preflight 尚未開始 workload 時，允許 nettop 暫時只有 header，
但必須確認 collector process 存活；active window／final validation 必須取得 Game、Gate、load 三個 PID
rows，否則標記 `network_process_unavailable`，不可宣稱已收集。若 OS 版本不支援某參數，可換等價工具，但
必須產生相同欄位並在 `commands.txt` 記錄替代命令。Redis 位於 OrbStack／container 時不虛構 host PID：Redis `INFO` 仍
依第 9.3 節必收，metadata 另記錄 dependency runtime；host `top` 用來涵蓋該 runtime 對整機的影響。

collector preflight 必須在 warm-up 前驗證：

- Game、Gate 與 load metrics endpoint 連續兩次 scrape 成功，且必要 metric family 都存在；
- preflight 只檢查不需 traffic 即存在的穩定 family：Game `gaming_core_game_gate_commands_in_flight`、Gate
  `gaming_core_gate_websocket_connections`、load `gaming_core_example_load_push_readers`，以及所有 process
  的 `go_sched_latencies_seconds` 與 `process_cpu_seconds_total`；不為測試預先建立未使用的 sender label
  children；
- `top` 至少能解析 host CPU user／system／idle 及 load average，CPU 三項合計約 100%；
- process 資料包含正確 PID 及 CPU／RSS／thread count；
- `vm_stat` 有遞增 timestamp，或由 collector 補上 monotonic timestamp；
- `nettop` 若無法取得有效 row，明確標記 `network_process_unavailable`，不得標成成功。

Warm-up 後 baseline scrape 再檢查當次 operation 的 Game sender、Gate delivery 與 server-send WebSocket write
families；因此仍能發現 registration／wiring 錯誤，而不改變 production metrics 的零值輸出語意。

每筆 application metrics 與 OS sample 必須帶 RFC3339Nano 或 Unix nano timestamp。1 秒 collector 在 active
phase 的相鄰 sample gap 不得超過 2.5 秒，且至少覆蓋該 phase 首尾；不符合者將 attempt 分類為
`environment_invalid`，但保留 artifacts。Application metrics 可由 Prometheus 或 bounded scrape loop
取得；若使用 scrape loop，每個 snapshot 須同時保存 timestamp、endpoint 及 HTTP status，不能只靠無時間
資訊的 `.prom` 檔名。

報告 CPU 時必須分開：host CPU 使用 `100 - idle`；process CPU 保留工具原值並說明 macOS 的 100% 約為
一個 logical CPU，不得把 Gate 70% 寫成整台 host 使用 70%。另外列出
`rate(process_cpu_seconds_total) / GOMAXPROCS` 作為 Go process 相對其設定上限的比例，不能用不同語意數字
互相替代。

### 9.5 pprof

pprof 只在 baseline 三輪確認可重現後，針對 metrics 指向的 process 另跑 profile-only case：

- CPU profile 20 秒；
- measured window 中段的 heap與 goroutine snapshot；
- 若 CPU不高但 scheduler／network wait 明顯，再抓 5 秒 Go trace。

一次只 profile 一個主要 process；profile run 標明 observer effect，不納入三輪 RPS／latency中位數。若
Broadcast sender快但 Gate queue／write變慢，先 profile Gate；若 Player sender duration及Redis wait升高，
先 profile Game，並交叉檢查 Redis server；若 server正常而 client receive落後，先檢查 load process。

「baseline 三輪確認可重現」是指同一 case 已有三個第 10 節的 `evidence_valid` attempts，且相同的第一個
失守邊界至少出現兩次；不要求 overload case 先取得三個 `delivery_complete` 結果。若要求必須先零 missed
才 profile，反而會讓真正的 overload case 永遠無法診斷。

Profile-only run 的收集時間由 `phase-events.jsonl` 中的 `measured_running start` 觸發，不以啟動 load 後
sleep 固定秒數猜測。30 秒 measured window 固定排程如下：

```text
T+04s .. T+24s  target process CPU profile（20s）
T+14s           target process heap 與 goroutine snapshot
T+24s .. T+29s  Go trace（只有 metrics 指向 scheduler/network wait 時）
```

排程比 measured window 邊界提前約 1 秒，以吸收 pprof HTTP handler 與 timestamp
overhead；profile 的實際 start／end 仍必須由 validation 確認完全落在 `T+00s..T+30s`，不得以排程預估取代
實際時間。

CPU profile、snapshot 或 trace 任何一項落在 measured window 外，報告必須標示 `out_of_window` 且不得用於
active load root-cause 判斷。每個檔案須非空，CPU／heap 須能由保存的同版 binary 以
`go tool pprof -top` 解析，trace 須能由 `go tool trace` 開啟；保存實際 HTTP command、開始／結束時間、
target PID 與 binary checksum。若需要比較 Game、Gate 與 load，分別重跑 profile-only case，不在同一輪
同時 profile 多個 process。

Profile-only 仍走相同的 warm-up result／quiet drain／baseline barrier 與 measured lifecycle；不另設略過
warm-up 的 workaround。Profile run 仍標明 observer effect，不納入三輪 RPS／latency median 或 capacity pass。

## 10. Correctness 與有效 run 判定

本設計將「證據是否有效」與「workload 是否達標」分開，避免把 missed tick、delivery loss 及 collector
失敗都寫成同一種 `failed`：

1. `environment_invalid`：binary／config 不符、startup/readiness 失敗、舊 process 未關閉、必要 metrics
   缺失、active window collector 覆蓋不完整，或 `run-status.json` 無法核對。此類 attempt 不進入中位數，
   修正環境後補跑；原 artifacts 保留。
2. `cadence_failed`：證據完整，但 `M > 0`，同時 `R = S × C` 且 Gate terminal counts 相等。表示 producer
   沒有產生全部 planned ticks，不能稱為 delivery loss。
3. `sender_failed`：`P > 0` 或 `E > 0`，獨立於 client delivery 結果。
4. `delivery_failed`：對實際成功 sender calls 仍有 `R != S × C`、duplicate／gap／invalid、connection
   close、queue full 或 terminal count 不等。
5. `delivery_complete`：證據完整且下列全部 correctness invariants 成立。

後四種都是 `evidence_valid` workload 結果，可用於呈現 attempt variation；只有 `delivery_complete` 代表該
條件達標。一次 attempt 可能同時有 `cadence_failed` 與 `delivery_failed`，`run-status.json` 須保存全部原因，
報告以最早失守的 phase／boundary 作主要分類。

令：

- `S` = 受評估 phase 中 Game product sender `success` calls；
- `P` = 受評估 phase 中 Game product sender `partial` calls；
- `E` = 受評估 phase 中 Game product sender `error` calls；
- `M` = 受評估 phase 的 example runner `missed_ticks`；
- `C = 1000`；
- `K = 1`，即受評估 phase control request 的 `StartPushResponse` WebSocket write；
- `R` = 受評估 phase 的 client `received` messages；
- `D` = duplicate，`G` = sequence gap，`I` = invalid。

在單 Gate、Redis healthy baseline 中，`delivery_complete` 必須滿足：

```text
P = 0
E = 0
M = 0
D = 0
G = 0
I = 0
R = S × C
Broadcast: Gate room delivery terminal success count delta = S × C
Player: Gate player queued delta = S × C
Gate server-send WebSocket write success delta = S × C + K
所有 terminal in-flight gauges = 0
所有 write queue messages = 0 after drain
```

任一 33ms 或 16ms case 出現 `M` 時，報告必須同時列 planned、attempted 與
`planned - attempted`，並核對 client 缺額是否正好為 `M × C`。若正好且其餘 delivery invariants 成立，
使用 `cadence_failed`，不得寫成「Gate 或 WebSocket 掉訊息」。Warm-up 的 `M` 不再阻止 measured；若
measured 因 lifecycle／evidence failure 未開始，才不得填入 30 秒 RPS 或 latency。

每個 case 取得三個 `evidence_valid` attempts；workload failure 不為了挑到 pass 而刪除。`environment_invalid`
才補跑，但總執行次數固定為 `requested_valid_attempts + 2`；超過上限即停止 campaign、回傳非零狀態並保留
所有 artifacts，不在同一次 command 中無限重試。對 actual ticks/s、client msg/s 與 latency 可列三輪
variation／median，但 capacity 結論必須另列 `delivery_complete` 比例；三輪未全部達標時，不得宣稱該
offered load 可穩定承載。壓力 case 即使失敗仍是容量證據，但不能拿「只計成功」的 RPS 宣稱完整吞吐。

## 11. 報告格式

實際測試完成後新增 `SERVER_SEND_PERFORMANCE_VALIDATION_REPORT.md`，內容固定如下。

### 11.1 Executive conclusion

先回答：

- 33ms及16ms下，Broadcast／Player能否完整送達 1000 人；
- capacity knee是否已出現；
- 第一個出現壓力的可觀測邊界；
- 結論是已證實 root cause、候選 root cause，或證據仍不足；
- 下一個唯一必要動作。

### 11.2 Environment and method

列出第 9.1 節 metadata、測試矩陣、重啟／warm-up／drain方式、三輪中位數規則，以及 shared-host／clock
限制。連結實際 config、commit與 artifact index。

### 11.3 Per-run raw results

每輪至少一列：

| case | attempt | evidence status | workload status | planned/attempted | actual ticks/s | missed/error | client msg/s | loss/duplicate | sender p50/p95/p99 | Gate delivery p50/p95/p99 | client p50/p95/p99 | queue max | host idle min | Game/Gate/load/Redis CPU max | Redis wait |
|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|

不得只放 median 而隱藏 run-to-run variation。Warm-up lifecycle／evidence 失敗而未執行 measured 的 attempt
仍列一行，所有未執行的 measured 欄位填 `not_run`，不得以 warm-up 數據代替；只有 workload missed／partial／
error 仍進入 measured。CPU 欄必須標出是 process raw %CPU、GOMAXPROCS-normalized
或 host 比例。

### 11.4 Median comparison

分別比較：

- 同 operation 的 33ms → 16ms：offered load約 2 倍時，delivery RPS是否同比增加、latency／queue／error如何
  改變；
- 同 interval 的 Broadcast ↔ Player：區分 Redis Publish path與 Redis batch lookup + gRPC path成本；
- client delivery與 producer sender：避免只看 sender acceptance就宣稱 1000 clients已收到。

Median 只在三個 `evidence_valid` attempts 間計算並附 workload status；它描述實際觀測值，不自動代表
pass。若三輪有任一 `cadence_failed`／`sender_failed`／`delivery_failed`，結論寫明「此 offered load 未
穩定達標」；不得因 actual-throughput median 看似良好而省略失敗比例。若任一 attempt 在 warm-up 結束，
不把 5 秒 warm-up 與 30 秒 measured 數據混算；另報 warm-up status／actual tick rate，measured median
標為 `not_available`。

### 11.5 Bottleneck evidence

每個結論必須用「現象 → metrics/OS/profile evidence → 能排除什麼 → 不能排除什麼」格式。例如：

```text
現象：Player 16ms actual tick rate低於目標且 missed增加。
證據：Game player sender p99超過16ms；Game sender in-flight維持1；Redis pool wait與Redis CPU同時上升；
      Gate queue ratio仍低、Gate CPU未接近上限。
判斷：瓶頸位於Game PlayerSender至Redis routing邊界，Gate/WebSocket不是第一個飽和點。
限制：未看Game CPU profile前，不能區分protobuf preparation、go-redis或kernel syscall。
```

只有 metrics、OS與pprof stack方向一致時才寫「root cause」。只有 broad latency差異時寫「候選瓶頸」。

Root-cause 結論另須同時通過下列 evidence gate；缺任一項時報告只能寫候選，並把缺項列為下一個唯一動作：

- 同一 case 有三個 `evidence_valid` attempts，且相同第一失守邊界至少出現兩次；
- application metrics 與 host／process collectors 覆蓋相同 active window，沒有第 9.4 節缺口；
- target process profile／trace 完全位於 measured window，stack 與 metrics 指向同一邊界；
- 若 load 與 services 共用 host，只能判斷 shared-host system 的瓶頸；要宣稱 Gate／Game capacity 或排除
  load interference，必須以分離 load host 的 control run 重現。

第一失守邊界依資料判讀，不依 process CPU 最大值猜測：`M > 0` 先比較 sender duration 與 interval，再看
Game scheduler latency／trace；Gate queue/full 或 delivery latency 先上升才指向 Gate enqueue/write；Gate
terminal success 已完整但 client 落後才指向 network／load。Process `%CPU` 未接近一個或多個 core 上限時，
不得只因它是所有 process 中最高就宣稱 CPU saturation。

### 11.6 Evidence index

列出每個表格數字所對應的原始檔：

```text
artifacts/server-send/<timestamp>/<phase>/<case>/run-<n>/
  metadata.txt
  phase-events.jsonl
  run-status.json
  commands.txt
  control/                # clients-ready/start/warmup-started/warmup-result/warmup-drained/baseline-ready/measured-started/final-ready/final-scrape markers
  game.log
  gate.log
  load.log
  metrics/                 # *.prom snapshots、role.tsv timestamp/status/path index、baseline/final snapshots
  redis-before.txt
  redis-after.txt
  process-top.txt
  redis-before-measured.txt
  redis-ping.txt
  redis-server-info.txt
  os/                      # host/process top、vm_stat、nettop、netstat 與 lsof snapshots
    redis.tsv              # 每秒 Redis INFO CPU／commands／clients samples
    lsof-collectors-ready-<pid>.txt
    lsof-measured-baseline-<pid>.txt
    lsof-final-<pid>.txt
  pprof/                   # 只有 profile-only run
```

Campaign root 另保存 `bin/{game,gate,load}`、`binary-checksums.txt`、`source-state.txt`、`build-commands.txt`
與 `evidence-manifest.tsv`；manifest 列出相對路徑、size 與 SHA-256。Raw artifacts 應留在 local／CI artifact
儲存，不提交到 Git source history。

報告不可引用未保存的終端畫面或只有 `/tmp` 路徑且已不存在的證據。

## 12. 錯誤與失敗策略

- Control request validation失敗：handler回傳 error；不啟動或取代 workload。
- Runner sender error：記錄 bounded result與 structured log，繼續後續 schedule；context error則結束。
- Client decode／sequence 錯誤：記錄 bounded metric 並標記 `delivery_failed`；不 panic。
- 單一 reader 在 clients ready 後斷線：取消該輪 control context、標記 `delivery_failed`，bounded drain 後
  保留 artifacts；clients ready 前斷線則為 `environment_invalid`。
- Metrics registration 失敗：Game/load startup 失敗並標記 `environment_invalid`，避免靜默執行不可觀測的
  benchmark。
- Initial Game/Gate bind 或 Redis dependency 失敗：沿用 framework startup failure 及 rollback，attempt 為
  `environment_invalid`。
- pprof bind 失敗：沿用既有 explicit config failure；不影響服務 readiness，但該 profile-only attempt 為
  `environment_invalid`。
- Collector preflight 失敗：不啟動 warm-up；active window 資料缺漏：完成 bounded shutdown 並標記
  `environment_invalid`，不得以人工摘要補成有效 run。
- Warm-up missed tick：完成 result marker、quiet drain、baseline 與 artifacts 後仍啟動 measured，並在
  `run-status.json` 保留 `warmup_missed`／`cadence_failed`；不得只以 load 端 `drain timeout` 描述原因。

## 13. 測試策略

### 13.1 Unit／contract tests

- protocol command IDs、field numbers、enum與full names固定且不重複；
- runner validation：mode、duration、interval、target count、duplicate login、payload limit及single-active；
- fake clock/ticker 驗證planned ticks、第一個tick立即執行、固定頻率、不重疊、missed tick不burst補發、
  sequence與timestamp；
- Broadcast每個tick恰好呼叫一次；Player每個tick恰好以一批1000 messages呼叫一次；
- sender decorators的success／partial／error count、duration與in-flight歸零；
- lifecycle stop取消並等待active runner；
- load每條connection只有一個reader，可處理control response/push交錯；
- orchestration barrier 必須在 `start` marker 前維持 1000 readers 且不提交 warm-up；warm-up result 含 missed
  仍可完成 quiet drain、warmup-drained、baseline barrier 並提交 measured；stale run ID、invalid counters、
  reader failure 與 queue drain timeout 必須停止 measured；timeout／atomic marker／phase timestamps 均可
  deterministic 測試；
- client received／duplicate／sequence-gap／invalid與duration Histogram count；
- drain成功、timeout與connection failure都能退出且不洩漏goroutine；
- Redis INFO parser 正常輸出、缺欄位與 command failure；active-window sample 的 timestamp／coverage validation；
- campaign binary checksum mismatch 與 `requested_valid_attempts + 2` retry 上限會使 validation 失敗；
- metric preflight 使用穩定 families，warm-up baseline 才檢查 operation families；
- Echo既有contract不變。

Package-private clock/ticker seam只為 deterministic scheduler test，不做公開 generic scheduler API。

### 13.2 Integration／manual validation

1. `go test` 與 `go test -race` 覆蓋 modified packages及 `examples/metrics/...`。
2. 10 connections correctness gate分別跑 Broadcast／Player。
3. 執行 orchestration collector preflight，刻意令一個必要 collector 失敗，確認 warm-up 不會開始且結果為
   `environment_invalid`。
4. 1000 connections 四案例，各取得三個 `evidence_valid` attempts；workload 失敗保留，只有環境無效才
   補跑。
5. 依三輪第一個失守邊界選一個瓶頸候選，另跑一次 profile-only case，驗證所有 profile timestamp 位於
   measured window。
6. 執行 orchestration script 的 `--validate-only` 核對 phase、sample coverage、counter equality、分類與
   evidence links，再依第 11 節產生報告。

## 14. 具體檔案修改

必要修改：

- `examples/metrics/internal/protocol/push.proto`、generated `push.pb.go`
  - 新增 example control與push messages。
- `examples/metrics/internal/protocol/command.go`
  - 新增三個固定example command IDs。
- `examples/metrics/internal/protocol/protocol_contract_test.go`
  - 鎖定 IDs及protobuf descriptors。
- `examples/metrics/internal/workflow/game.go`
  - 註冊 StartPush handler，沿用既有 BroadcastRoom helper。
- `examples/metrics/internal/workflow/push.go`、`push_test.go`
  - 實作 bounded fixed-rate runner、DI/lifecycle、player batch建立及example metrics。
- `products/gameproduct/metrics.go`、相關 contract tests
  - 補 sender duration/in-flight與 Broadcast／Player measured decorators；不預先建立未使用的 sender label
    children。
- `products/gateproduct/metrics.go`
  - 保留既有 bounded server-send metrics，不預先建立未使用的 target/result children；不新增 label 或改變
    delivery path。
- `products/gameproduct/server_send.go`
  - 只把既有 Player／Broadcast sender包進 measured decorator，不改 sender選擇與transport。
- `examples/metrics/load/main.go`、`push.go`、`main_test.go`
  - 增加push mode、單reader、control、warm-up、measured與drain流程，以及baseline專用orchestration barrier
    與 phase／final-scrape markers（`clients-ready`、`start`、`warmup-started`、`warmup-result`、
    `warmup-drained`、`baseline-ready`、`measured-started`、`final-ready`、`final-scrape`）；warm-up workload
    failure 仍先完成 bounded drain 與 baseline，不另設略過 warm-up 的 workaround。
- `examples/metrics/load/metrics.go`、相關 metrics tests
  - 增加三個load-private push metrics，並以與 App registry 相同的 runtime matcher 暴露
    `go_sched_latencies_seconds`，使 collector preflight 能觀測 load process scheduler wait。
- `examples/metrics/README.md`
  - 增加四案例執行方式、Prometheus queries、資料收集與pprof順序。
- `examples/metrics/scripts/run-server-send-validation.sh`
  - 提供唯一 baseline 入口，管理重建／重啟、phase events、metrics 與 macOS collectors、warm-up baseline
    barrier、每秒 Redis INFO samples、campaign binary snapshot、三輪 attempts、bounded retry、profile 時間窗及
    bounded shutdown；內含只讀 `--validate-only` 模式，且不得修改 Git staging。
- `examples/metrics/scripts/run-server-send-validation-test.sh`
  - source 同一入口的 private validation helpers，使用 `mktemp` fixture 驗證 marker counters、checksum、baseline
    operation／terminal gauges、retry 上限與 invalid 摘要；不啟動服務或 Redis。
- `examples/metrics/flow_contract_test.go`
  - 以少量connections驗證Broadcast與Player完整鏈路及counter equality。

實測後新增，不在功能實作階段預填結果：

- `SERVER_SEND_PERFORMANCE_VALIDATION_REPORT.md`；
- `artifacts/server-send/...` 的可保存原始資料。若 repository policy不追蹤大型profile/raw artifacts，報告仍
  必須連到團隊指定的持久化位置並保存checksums。

明確不修改：

- `pkg/serversend` message、sender、Redis Pub/Sub、batch routing、gRPC transport與1 MiB contract；
- Gate generic gRPC service、Redis subscriber、RemoteCommandChannel dispatcher、room/player handlers；
- WebSocket 16-byte header與write queue語意；
- Redis fallback條件；
- health、readiness、metrics或pprof listener架構；
- session ownership renewal scheduler。

## 15. 關鍵決策與取捨

### 15.1 Player以單一1000人batch送出

這是 `PlayerSender` 的正式使用方式，能測到batch Redis lookup、endpoint grouping與encoded-byte chunking。
拆成1000次呼叫會測到不同workload並放大RPC/Redis round trip，不能回答現有batch實作的容量。

### 15.2 Fixed-rate、non-overlapping且skip missed ticks

requested 33ms／16ms代表固定admission cadence。Unbounded goroutine會把壓力轉為heap/goroutine堆積；burst
catch-up會改變瞬時流量。單一in-flight call加missed counter能直接回答「此sender是否維持目標頻率」。多
producer concurrency是後續獨立容量問題，本輪不預先加入。

### 15.3 使用example control command

它讓Game只在1000 clients ready後啟動，並直接示範external product module如何注入正式senders。相較於
固定startup sleep、額外HTTP control server或Redis coordination key，它增加的介面最少且結果可重現。

### 15.4 Product metrics只加公開sender邊界

Sender duration/in-flight是線上能立即處置的穩定邊界。Redis pipeline stage、protobuf marshal、grpc-go
writer與syscall成本沒有穩定business label，先由pprof/trace判斷；未有證據前加入會使metrics與實作細節
耦合。

## 16. 已知限制與後續擴充

- Redis Pub/Sub是at-most-once；本輪會量到loss但不提供replay或reliability改造。
- 單Gate只驗證每個operation的基本成本，不代表multi-Gate fanout scaling；需另定endpoint矩陣後才能測。
- 單一producer不代表多Game instances同時發送；本輪先建立可比較baseline。
- 1000 players及32-byte payload只是一個固定workload point，不外推任意payload／batch size。
- shared-host結果無法單獨宣稱production硬體capacity。
- pprof是sampled diagnosis；若需要長期線上分析，再評估continuous profiling，不在本次加入。

## 17. Self review：必要性與需求符合度

| 需求 | 設計對應 | 結論 |
|---|---|---|
| 1000人連線 | setup barrier、1000 readers與correctness equality | 符合 |
| Broadcast 33ms／16ms | fixed-rate BroadcastSender案例 | 符合 |
| Player Send 1000人 | 每tick單一1000人SendToPlayers batch | 符合 |
| 從metrics看R/E/D/S | Game sender、Gate delivery/write/queue、client receive、Redis/runtime | 符合 |
| metrics不足再補 | 只補producer與client現有盲點，沿用其餘既有series | 符合 |
| pprof找主要成本 | baseline後針對被指向process另跑profile-only | 符合 |
| 看OS負載 | 同window的host/per-process/network原始資料 | 符合 |
| 使用example並貼近framework實際開發 | example module注入正式senders與registerer、dispatcher command control | 符合 |
| 收集資料與報告方法 | 第9至11節定義來源、計算、表格與evidence index | 符合 |
| 避免執行者對 failed／三輪／profile 時機有不同解讀 | 單一入口、phase events、兩層 run 分類及 artifact validation | 符合 |

Review後確認沒有刪減需求，也沒有加入非必要的transport或framework抽象：

- 新protocol只解決ready後啟動與client latency/correctness，屬於可重現測試必要資料；
- runner只有一個bounded job與固定兩種mode，不是通用job scheduler；
- 沿用一個Game sender counter並增加duration／in-flight，補上正式sender觀測盲點，labels皆bounded；
- load reader與metrics是驗證asynchronous push實際送達的最低需求；
- orchestration barrier、phase markers與判定script只存在於example，不進入framework production API；
- artifact validation與執行共用同一script，沒有第二套狀態判定邏輯；
- OS與pprof保持外部診斷工具，不污染application API；
- 不修改已通過contract review的server-send／Gate資料路徑。

因此本設計剛好涵蓋本次效能驗證、線上可觀測性缺口與可稽核報告，沒有 Redis Stream、fallback fault
benchmark、multi-Gate、通用scenario DSL、dashboard/alert、continuous profiling或transport內部metrics等
超出需求內容。
