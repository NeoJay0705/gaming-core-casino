# GateLink player affinity、DNS refresh 與多連線設計

## 1. 結論

本次只修改 Gate → Game 的 `pkg/gatelink` unary client，使登入後的 player request：

1. 以 `login_name` 做 deterministic hash，盡量送往相同 Game instance，以利用該 instance 的 local cache；
2. 對 `dns:///` target 定時重新解析，在 Game scale up／down 後更新可選 endpoint；
3. 對每個 resolved Game endpoint 建立可設定數量的 gRPC `ClientConn`，避免單一 HTTP/2 connection 的 stream
   capacity 成為不必要的上限；
4. 保留既有 Game `grpc.server.max_concurrent_streams`，並在 example config 清楚列出其責任、預設值與相關
   client 參數。

架構採用 `gatelink.Client` 內部管理「定時 DNS resolver → immutable endpoint snapshot → per-endpoint
connection pool」。不新增 RPC、不修改 protobuf payload、不讓 Game handler 感知 routing，也不實作 request retry、
動態 pool autoscaling 或通用 service discovery framework。

---

## 2. 需求理解與合理假設

### 2.1 需求

- hash key 是已驗證的 `login_name`。
- affinity 的目標是同一個 Game process／endpoint，不是同一條 HTTP/2 connection。
- 「盡量相同」表示 preferred Game 不可用時，可使用其他 ready Game；可用性優先於 strict stickiness。
- DNS 必須在成功解析後仍固定週期 refresh，不能只等待 transport failure 觸發 re-resolution。
- Game scale up／down 不需重啟 Gate；新 request 使用最新 endpoint snapshot。
- 每個 Game endpoint 的 gRPC connections 數量可設定。
- 每條 server transport 的 `max_concurrent_streams` 可設定；`0` 保留 grpc-go default。
- 對這條鏈路有直接影響且可安全調整的參數要出現在 example config，並提供預設值說明。

### 2.2 已確認的現況

- `pkg/gatelink.Client` 現在建立一個長生命週期 `grpc.ClientConn`，service config 固定使用 `round_robin`。
- `round_robin` 可在多個 resolved address 間分流，但不提供以 `login_name` 為 key 的 affinity，也不會為同一
  address 建立可設定數量的 transports。
- 專案使用 grpc-go `v1.72.0`。其 DNS resolver 初次解析成功後會等待 `ResolveNow`；
  `MinResolutionInterval` 只限制 re-resolution 的最短間隔，不代表固定週期 refresh。
- Gate 在 Forward 未註冊的 WebSocket command 前，已透過 `SessionRegistry.State(connectionID)` 驗證 player
  已登入且已進房，因此此處可取得可信任的 `login_name`。
- Game 的 `grpc.server.max_concurrent_streams` 已存在，不需要新增另一個 server-side 實作。

### 2.3 本設計採用的假設

- production `grpc.clients.game.target` 使用 `dns:///host:port`，且 host 是能直接列出各 Game instance IP 的
  headless DNS。若 DNS 只回傳 ClusterIP／VIP，hash 只能黏在該 VIP，無法保證抵達同一 Game process。
- 一個 resolved `IP:port` 視為一個 Game endpoint。DNS 回傳順序不構成 endpoint identity。
- login、enter-room 等 Gate-local command 不會送到 Game；所有 Gate → Game Forward 都應已有
  `login_name`。
- request 可能不是 idempotent。transport failure 後不得自動改送另一個 Game，以免業務 command 重複執行。
- DNS record TTL 與中間 cache 仍可能使實際更新晚於 `dns_refresh_interval`；client 只能控制查詢頻率，不能
  繞過 resolver／CoreDNS cache。

---

## 3. 範圍與非目標

### 3.1 必要修改

- 在 `pkg/gatelink` 增加 request-local affinity key、periodic DNS resolution、rendezvous hash picker 及
  per-endpoint connection pool。
- 在 Gate WebSocket Forward 前，把 `SessionRegistry` 中的 `login_name` 放入 request context。
- 擴充 `grpc.clients.game` config：`dns_refresh_interval`、`connections_per_host`。
- 沿用並說明 `grpc.server.max_concurrent_streams`。
- 更新 Gate／Game example config 與必要 contract tests。
- 更新既有 direct gRPC load example，只在 request context 注入穩定的 synthetic affinity key，讓它遵守新的
  GateLink client contract；不改 load workload、metrics 或 CLI。
- DNS refresh failure、recovery 與 topology change 使用既有 structured logger 記錄 bounded 資訊。

### 3.2 明確不做

- 不修改 `gatelink.proto`、command ID、opaque protobuf payload、dispatcher 或 unary reply contract。
- 不把 `login_name` 寫入 gRPC metadata、protobuf、metric label 或新增的 routing／refresh log；它只存在 Gate
  process 的 context，並只作本機 picker input。本次不改動其他既有業務 log。
- 不修改 Game → Gate server send、Redis Pub/Sub broadcast 或 Gate-to-Gate fan-out。
- 不新增 client-side `max_streams`。HTTP/2 `MAX_CONCURRENT_STREAMS` 由 Game server 宣告，grpc-go client 會遵守。
- 不新增 client semaphore、queue、retry、hedging、circuit breaker、health-check RPC、keepalive 或 backoff config。
- 不做依流量自動增減 connections；`connections_per_host` 是固定 topology 參數。
- 不新增通用 resolver plugin system、xDS、service mesh、consistent-hash config 選項或額外第三方 dependency。
- 不新增 Prometheus metrics。既有 RPC RPS／latency／error／in-flight metrics 繼續觀察結果；DNS topology 與
  refresh failure 先由低頻 structured log 提供維運資訊。

---

## 4. 架構與模組邊界

### 4.1 資料流

```text
WebSocket packet
  → Gate dispatcher：未註冊 command
  → SessionRegistry.State(connectionID)
      ├─ 驗證已登入、已進房
      └─ 取得 login_name
  → gatelink.WithAffinityKey(ctx, login_name)
  → gatelink.Client.Forward
      ├─ 讀取 immutable endpoint snapshot
      ├─ rendezvous hash(login_name, endpoint identity)
      ├─ 從 preferred ready endpoint 的 connection pool 選一條 ready connection
      └─ GateRequestServiceClient.Forward
  → Game gRPC server
  → 既有 dispatcher／handler／unary reply
```

另一條獨立控制流負責 topology：

```text
Client.Start
  → 立即 DNS lookup
  → 建立／重用 endpoint pools
  → publish immutable snapshot
  → 每 dns_refresh_interval 重複 lookup 與 reconcile
  → Client.Stop 取消 lookup loop 並關閉所有 ClientConn
```

### 4.2 `pkg/gatelink` 責任

`pkg/gatelink` 擁有 Gate → Game transport，因此也擁有：

- target parsing 與 DNS endpoint discovery；
- player affinity selection；
- 每個 endpoint 的 connection pool；
- 所有底層 `grpc.ClientConn` 的建立與關閉；
- request timeout、既有 logging／request-context interceptors；
- refresh lifecycle 與 topology structured log。

Game handler、Gate dispatcher 與 product module都不接觸 endpoint、hash 或 connection index。

### 4.3 為何不實作 grpc-go custom balancer

grpc-go custom balancer 適合一般 address picking，但本需求同時要求 request context affinity 與每個 address
多個獨立 `ClientConn`。直接在 `gatelink.Client` 內管理小型 endpoint snapshot，可避免 global balancer registration、
service-config JSON extension 與 SubConn state machine 外洩，並讓 DNS、hash 與連線數能以 package-local fake
做 deterministic tests。

這不是通用 connection pool：它只服務 `GateRequestService.Forward`，維持既有 `gatelink.Client` public
boundary。

---

## 5. 核心資料模型與主要介面

### 5.1 Public config

擴充 `pkg/gatelink.ClientConfig`：

```go
type ClientConfig struct {
	Target              string        `config:"target" yaml:"target"`
	Timeout             time.Duration `config:"timeout" yaml:"timeout"`
	DNSRefreshInterval  time.Duration `config:"dns_refresh_interval" yaml:"dns_refresh_interval"`
	ConnectionsPerHost  int           `config:"connections_per_host" yaml:"connections_per_host"`
}
```

實際欄位對齊既有 gofmt；名稱中的 `Host` 沿用 operator 常用語意，實際單位是 resolved `IP:port` endpoint。

| Config | 必要性 | Default | 驗證／語意 |
|---|---|---:|---|
| `target` | 必填 | 無 | production dynamic routing 使用 `dns:///host:port`；raw `host:port` 視為單一 static target |
| `timeout` | 非必要 | `10s` | 不可為負；限制一筆 Forward |
| `dns_refresh_interval` | 非必要 | `10s` | 不可為負；只對 `dns:///` 生效，`0` 套用 default |
| `connections_per_host` | 非必要 | `1` | 不可為負；`0` 套用 default；每個 resolved endpoint 的固定 connection 數 |
| `grpc.server.max_concurrent_streams` | 非必要 | `0` | Game server 現有設定；`0` 使用 grpc-go default |

static target 不啟動 DNS loop；若對 static target 明確設定非零 `dns_refresh_interval`，config validation 應回傳
錯誤，避免使用者誤以為它會主動發現多個 Game。

不設定任意上限值；實作者不得自行加入未經需求驗證的 `64` 或其他 magic cap。文件應提醒 connection 數會乘上
endpoint 數，operator 必須配合 file descriptor、Game connection 數與壓測結果調整。

### 5.2 Affinity context

在 `pkg/gatelink/metadata.go` 增加：

```go
func WithAffinityKey(ctx context.Context, key string) context.Context
func AffinityKeyFromContext(ctx context.Context) (string, bool)
```

- context key 使用 package-private type。
- `WithAffinityKey` 對 nil context 使用 `context.Background()`；空字串不應建立有效 affinity。
- `Client.Forward` 要求非空 key，否則在發出 RPC 前回傳明確錯誤。login name 是既有 opaque identity，禁止
  `TrimSpace`、大小寫轉換或其他 normalization；hash 必須使用完整原值。
- 這個值不由 `withOutgoingRequestMetadata` serialize，因此 Game 看不到 login name。
- API 使用 transport-neutral 的 `AffinityKey` 名稱；目前唯一合法來源仍是已驗證的 `login_name`，不開放 config
  選擇其他欄位。

### 5.3 Internal endpoint snapshot

建議新增 package-private types，名稱可依實作微調：

```go
type endpointSnapshot struct {
	endpoints []*endpointPool // 依 canonical address 排序
}

type endpointPool struct {
	address string
	conns   []*clientConnection
	next    atomic.Uint64
}

type clientConnection struct {
	conn   *grpc.ClientConn
	client GateRequestServiceClient
}
```

- snapshot 建立後不可修改；`Forward` 以 atomic pointer 讀取，hot path 不取得 topology mutex。
- canonical address 使用 `net.JoinHostPort`，DNS 結果須去重並排序，確保各 Gate 不受 DNS answer 順序影響。
- 未變更的 address 必須重用既有 pool；新增 address 建立恰好 `connections_per_host` 個 connections；移除 address
  先從新 snapshot 排除，再關閉其 connections。
- 建立 ClientConn 時沿用目前的 transport credentials，以及 logging、W3C trace、Gate request metadata
  interceptors；不得因 pool 化遺漏既有 cross-cutting behavior。
- 每個新 ClientConn 呼叫 `Connect()` 啟動 lazy transport establishment，避免新增 endpoint 永遠因 Idle 而不被
  ready-aware picker 採用。

### 5.4 Internal resolver seam

production 使用 `net.Resolver`，test 使用 package-private seam；不增加 public resolver abstraction：

```go
type hostResolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}
```

target parser 只需支援本專案實際需要的兩種形式：

- `dns:///host:port`：立即查詢並啟動 periodic refresh；
- raw `host:port`：視為 static endpoint，不做 periodic refresh。

不實作 SRV、TXT service config、自訂 DNS authority 或其他 URI scheme。DNS lookup 使用 bounded child context，
deadline 不長於 `dns_refresh_interval`；不再增加獨立 DNS timeout knob，避免本次擴張為完整 DNS client。

---

## 6. Routing 與 connection selection

### 6.1 Endpoint affinity

使用 rendezvous hashing（highest-random-weight）：對每個 endpoint 計算穩定的
`score(affinityKey, canonicalAddress)`，選最高分者。

選擇理由：

- 同一 topology、相同 login name 在所有 Gate process 得到相同 Game endpoint；
- 新增 endpoint 時，只有被新 endpoint 取得最高分的 keys 會移動；
- 移除 endpoint 時，只有原本選到該 endpoint 的 keys 必須移動；
- 不需要 virtual node count 或可調 hash policy。

hash 實作使用標準函式庫的固定演算法與明確 byte framing，禁止使用 process-random seed。不得把 hash value、
login name 或 endpoint 放入 metric label。

目前 endpoint 數量下，per-request `O(number of endpoints)` 可換取簡單且容易驗證的實作。只有 production
profile 證明 picker 成本成為瓶頸時，才另案改為 ring／Maglev；本次不預先加入。

### 6.2 Ready-aware failover

picker 在一次 scan 中保留：

- 所有 endpoint 中最高分的 preferred endpoint；
- 至少有一條 `connectivity.Ready` connection 的 endpoint 中，分數最高者。

若有 ready endpoint，選分數最高的 ready endpoint；若全部尚未 Ready，選原本 preferred endpoint，讓該次
grpc-go call 依現有 fail-fast／deadline 行為結束。這使暫時失效的 preferred Game 不會阻塞其他健康 Game，
同時不需要額外 health-check RPC。

若 call 已交給某條 connection 後失敗，直接回傳原錯誤，不嘗試第二個 endpoint。這是避免 non-idempotent
command 重複執行的必要限制。

### 6.3 Per-endpoint connection selection

選定 Game endpoint 後，在該 pool 的 Ready connections 間以 atomic round-robin 選擇；若當下沒有 Ready
connection，選一條 connection 交由 grpc-go 回報 connectivity error。

connection 選擇不再使用 login name，因為 local cache 屬於 Game process，同一 process 下使用哪條 HTTP/2
connection 不影響 cache affinity。round-robin 可以讓高併發 players 分散使用已設定的 transports，並配合 Game
server 的 `max_concurrent_streams`。

---

## 7. DNS refresh 與 lifecycle

### 7.1 Start

- `NewClient` 只驗證 config、解析 target 並建立內部狀態，不啟動 goroutine。
- `Start` 對 static target 建立一個 endpoint pool；對 DNS target 啟動一個 lifecycle-owned goroutine，先立即
  lookup，再等待 `dns_refresh_interval`。
- DNS 初次尚未成功時，`Forward` 不得 panic 或任意選空 endpoint；應回傳 gRPC `Unavailable` 類型錯誤。
- 為維持既有 framework 的 lazy upstream semantics，DNS 初次失敗不使 Gate process 啟動失敗；resolver 持續
  依 interval 重試。config／target parse error仍由 `NewClient` 同步回傳。
- 重複 `Start` 與 Start-after-Stop 明確回傳 lifecycle error。
- `Forward` before Start 回傳明確 lifecycle error；既有直接使用 client 的 contract tests 必須先啟動 client。

### 7.2 Refresh reconcile

每次成功 lookup：

1. canonicalize、deduplicate、sort addresses；
2. 空結果視為 lookup failure；
3. 若集合未變，不重建 snapshot、不重建 connections，也不輸出週期性 success log；
4. 重用未變 endpoint pools；
5. 先完整建立新增 pools；任一建立失敗時關閉本輪新建資源並保留 last-known-good snapshot；
6. atomic publish 新 snapshot；
7. 關閉已移除 endpoint pools。

refresh failure 或空結果保留 last-known-good snapshot，避免短暫 DNS 問題把所有健康 routes 清空。第一次失敗
輸出 warning；連續相同失敗不應每次洗版，可只在狀態轉換或採 bounded rate；恢復時輸出 recovery，endpoint
集合改變時輸出 added／removed／total counts。

log 不包含 login name、payload 或 credential。endpoint address list 也不是判斷 topology 是否更新的必要欄位，
預設只記 counts。

### 7.3 Stop 與 concurrent refresh

- `Stop` 取消 resolver context、等待 refresh goroutine 結束，再關閉目前與待清理的所有 ClientConn。
- Stop 為 idempotent；nil receiver 為 no-op。
- `Forward`、refresh 與 Stop 必須通過 race test。refresh publish 後被移除的 ClientConn 可能使極少數已取得舊
  snapshot 的 in-flight RPC 回傳 transport error；不得為掩蓋此情況加入自動 retry。
- caller 的 request timeout 與 cancellation 維持既有行為。

---

## 8. Product wiring 與 config 修改

### 8.1 Gate wiring

修改 `products/gateproduct/grpc.go`：

- `gateGRPCClientConfig` 增加 `dns_refresh_interval`、`connections_per_host`；
- `gateGameGRPCConfig` 原樣映射到 `gatelink.ClientConfig`，並保留 strict config binding；
- `newGateGameGRPCClient` 注入既有 `logging.Factory`，建立 `grpc.client.game` component logger；
- `pkg/gatelink` 提供與既有 server 風格一致的 `NewClientWithLogger` 給 product wiring；原 `NewClient` 保留給
  low-level tests，不新增 global logger；
- logger 只供低頻 resolver lifecycle 使用，RPC error仍由現有 Gate call site 記錄。

修改 `products/gateproduct/websocket.go`：

```go
state, exists := s.registry.State(session.ID())
// 維持既有 login／room validation。
ctx = gatelink.WithAffinityKey(ctx, string(state.LoginName))
reply, err := s.gameClient.Forward(ctx, request)
```

必須在既有 state validation 之後、`Forward` 之前設定，不重查 Redis，也不解析 client protobuf payload。

### 8.2 Example config

更新：

- `configs/examples/gateproduct.yaml`
- `examples/metrics/configs/gate.yaml`
- `configs/examples/gameproduct.yaml`
- `examples/metrics/configs/game.yaml`

Gate 範例完整呈現：

```yaml
grpc:
  clients:
    game:
      # 必填；production affinity 需要可解析各 instance 的 headless DNS。
      target: "dns:///gameproduct-headless:9090"
      # 非必要；預設 10s。
      # timeout: "10s"
      # 非必要；只適用 dns:/// target；預設 10s。
      # dns_refresh_interval: "10s"
      # 非必要；每個 resolved endpoint 的 HTTP/2 connections；預設 1。
      # connections_per_host: 1
```

metrics local example 可保留 raw `127.0.0.1:19090` static target；它仍列出 `connections_per_host`，但不列或
啟用 `dns_refresh_interval`，避免暗示 local static target 會做 discovery。

Game 範例保留既有：

```yaml
grpc:
  server:
    # 非必要；每條 HTTP/2 connection 的 concurrent streams；
    # 0 使用 grpc-go default，預設 0。
    # max_concurrent_streams: 0
```

不把 `max_concurrent_streams` 複製到 Gate client config，也不把 `connections_per_host` 放到 Game server config。

---

## 9. 錯誤處理

| 情境 | 行為 |
|---|---|
| target 空白、格式錯誤、unsupported scheme | `NewClient` 同步失敗 |
| timeout／refresh interval／connections 數為負 | config validation 失敗 |
| static target 設定 DNS refresh | config validation 失敗 |
| Forward 缺少 affinity key | RPC 發出前失敗；Gate 沿用既有 Forward error close policy |
| DNS 初次或後續查詢失敗 | warning、保留 last-known-good、下一 interval 重試 |
| DNS 回傳空集合 | 視同 refresh failure，不清空 snapshot |
| 新 endpoint pool 建立失敗 | 關閉本輪 partial resources，保留舊 snapshot |
| 沒有 endpoint／沒有可用 transport | 回傳 `Unavailable`／grpc-go connectivity error |
| 已送出的 RPC transport failure | 原錯誤回傳，不自動 retry 或換 Game |
| caller deadline／cancel | 沿用既有 context 與 gRPC status 行為 |
| Stop during refresh | 取消 lookup、等待 goroutine、關閉 connections |

錯誤與 log 不包含 login name 或 payload。config error 必須包含完整 config key，讓 operator 能直接修正。

---

## 10. 測試策略

### 10.1 `pkg/gatelink` unit／contract tests

必要覆蓋：

- config defaults 與負值／static refresh validation；
- affinity context 的 nil、空值與正常取值；
- 相同 key、相同 topology 永遠選到相同 endpoint；
- DNS answer 順序改變不改變結果；
- 新增 endpoint 時，只有改選新增 endpoint 的 keys 會變；
- 移除 endpoint 時，原本不在該 endpoint 的 keys 不變；
- preferred endpoint non-Ready 時選擇下一個 ready endpoint；
- 同 endpoint 的 Ready connections 會 round-robin，且不跨 Game endpoint破壞 affinity；
- fake resolver 的立即 lookup、periodic refresh、failure 保留 last-known-good、recovery 與 add/remove reconcile；
- unchanged DNS result 不重建 ClientConn；
- `connections_per_host=N` 對每個 endpoint 恰好建立 N 個 ClientConn；
- 每個 pooled connection 都保留 logging、trace 與 Gate request metadata interceptors；
- missing affinity key 不發 RPC；
- RPC failure不重送其他 endpoint；
- concurrent `Forward`／refresh／Stop 通過 `go test -race`。

時間相關測試應使用可控制的 refresh trigger／clock seam，避免依賴長時間 `time.Sleep`。seam 保持
package-private，不擴張 public API。

### 10.2 Gate product tests

- strict binding 接受並正確映射兩個新 client config；
- invalid config 在 app composition 時失敗並指出完整 key；
- WebSocket 已登入、已進房的 forwarded request 使用該 session 的 login name 作 affinity key；
- 未登入／未進房仍在 Forward 前關閉連線，既有狀態機不改；
- 不從 payload 解出 login name，也不查 Redis。

### 10.3 驗證命令

實作完成後至少執行：

```text
go test -count=1 ./...
go test -race -count=1 ./pkg/gatelink ./products/gateproduct
go vet ./...
git diff --check
```

實際 scale／affinity 測試留到 user review 編譯版本後再執行，沿用「先看實作，再實測」的既有工作方式。

---

## 11. 關鍵決策與取捨

### 11.1 Rendezvous hash 而非 modulo／round-robin

modulo hash 在 endpoint 數量改變時會搬動大量 players；round-robin 無法利用 player-local cache。rendezvous
hash 以最少必要狀態提供穩定 affinity，且不需引入 ring tuning knobs。

### 11.2 Best-effort affinity 而非 strict affinity

只在 preferred endpoint Ready 時才堅持使用它；否則選下一個 ready endpoint。這符合「盡量」且避免單一
Game 故障拖垮其全部 players。Game 恢復後，新 request會依 hash 回到 preferred endpoint；cache cold miss 是
可接受取捨。

### 11.3 多連線只解決 transport capacity，不改 server stream ownership

`connections_per_host` 增加每個 Game 的 HTTP/2 transports；`max_concurrent_streams` 仍由 Game server對每條
transport 宣告。兩者可以搭配壓測，但不應合併成同一 config，也不保證增加連線一定提高 RPS。

### 11.4 不做失敗後 retry

connectivity state 只能在送出前協助選擇；送出後的 error 無法證明 Game 未執行 command。沒有 idempotency key
前，自動 retry 會引入比短暫失敗更嚴重的重複業務操作。

### 11.5 Last-known-good DNS snapshot

短暫 DNS failure 不應讓健康長連線立即失效。保留舊 snapshot 能提高可用性，但若所有舊 Game 都已移除，呼叫
會持續失敗直到 DNS 恢復；此限制由 structured warning 與既有 RPC error metrics 呈現。

---

## 12. 已知限制與後續擴充方向

- affinity 只保證 Gate 根據 resolved endpoint 選擇；若 endpoint 後方仍有 L4/L7 load balancer，最終 Game
  instance 可能不同。
- DNS cache／TTL 可能延後 scale 事件可見時間。
- readiness-based failover 依 grpc-go connectivity state，存在狀態剛變更的競態；RPC error仍是最終事實。
- scale down 關閉 removed endpoint connections 時，少量已取得舊 snapshot 的 RPC 可能失敗。
- rendezvous picker 是 `O(endpoints)`；只有實測確認成為 hot path 才需要替換。
- 本次不保證跨 Game local cache 一致性。若業務要求 strict ownership、cache migration 或 exactly-once，需要另案
  設計，不能由 gRPC routing 解決。
- 未來若 endpoint 數、multi-zone、權重或 outlier detection 成為需求，再評估 xDS／service mesh；本次不預留
  抽象層。

---

## 13. 具體檔案修改清單

| 檔案 | 必要修改 |
|---|---|
| `pkg/gatelink/client.go` | 擴充 config；管理 lifecycle、snapshot、pooled Forward 與 Stop |
| `pkg/gatelink/metadata.go` | 增加本機 affinity context helper，不傳入 metadata |
| `pkg/gatelink/resolver.go`（新增） | target parsing、periodic DNS lookup、canonicalization 與 reconcile trigger |
| `pkg/gatelink/routing.go`（新增） | immutable endpoint snapshot、rendezvous endpoint picker、per-endpoint connection selection |
| `pkg/gatelink/*_contract_test.go` | config、routing、refresh、pool、error與 concurrency contracts |
| `products/gateproduct/grpc.go` | strict bind 新 config，注入 gatelink resolver logger |
| `products/gateproduct/websocket.go` | 從既有 session state 將 login name 放入 affinity context |
| `products/gateproduct/*_contract_test.go` | config mapping 與 WebSocket affinity wiring |
| `examples/metrics/grpcload/main.go` | 以既有 synthetic connection identity 作 request-local affinity key |
| `configs/examples/gateproduct.yaml` | 列出 dynamic target、refresh、per-host connections 與 defaults |
| `examples/metrics/configs/gate.yaml` | 列出 static local target 可用的 connections 設定 |
| Game 兩份 example YAML | 釐清現有 `max_concurrent_streams` 是 per-connection server setting |

不修改其他 product、proto、generated code、dispatcher、serversend、Redis、observability metrics 或 load workload／CLI；
grpcload 僅補上必要的 request-local affinity key。

---

## 14. Self-review：必要性與需求符合性

| 設計項目 | 對應需求 | 是否必要 |
|---|---|---|
| affinity context | 把已驗證 login name交給 picker且不解析 payload | 必要 |
| rendezvous endpoint picker | 同 player盡量到同 Game，scale 時減少 remap | 必要 |
| ready-aware fallback | 「盡量」而非故障時 strict stickiness | 必要 |
| periodic DNS resolver | scale up/down 後主動更新 | 必要 |
| immutable snapshot／reconcile | 高併發 Forward 與 refresh 可安全並行 | 必要 |
| per-endpoint ClientConn pool | 可調每個 Game 的 HTTP/2 connections | 必要 |
| server max streams 說明 | 釐清既有設定與 client connection 數的責任 | 必要；不新增實作 |
| config defaults／strict validation | 可維運且避免看似生效的錯誤設定 | 必要 |
| lifecycle、error handling、race tests | production-grade 動態 topology 的最低品質 | 必要 |
| topology structured log | 線上判斷 refresh 是否失敗或套用 scale 事件 | 必要且 bounded |

已移除或明確排除 custom balancer framework、request retry、動態 pool scaling、client max-stream semaphore、
health-check RPC、xDS、額外 metrics、hash policy knobs與其他 gRPC tuning config。它們都不是目前需求的必要條件。

Review 結論：此設計完整覆蓋 login-name hash routing、定時 DNS refresh、per-host connections 與 server
max-streams config，修改集中於 Gate → Game `gatelink` 邊界；沒有改變業務 API、payload 或其他傳輸鏈路，內容皆
為實作與驗證此需求所必需，沒有超出需求。
