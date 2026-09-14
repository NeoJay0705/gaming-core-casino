package gateproduct

import (
	"errors"
	"strconv"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type outboundSource string

const (
	outboundSourceHandler    outboundSource = "handler"
	outboundSourceServerSend outboundSource = "server_send"
)

type serverSendTarget string

const (
	serverSendTargetConnection serverSendTarget = "connection"
	serverSendTargetPlayer     serverSendTarget = "player"
	serverSendTargetRoom       serverSendTarget = "room"
)

// requestDurationBuckets 覆蓋一般 command、gRPC 與 client round-trip 的
// 微秒至秒級延遲；不使用 DefBuckets，避免 5ms 以下的 quantile 失真。
var requestDurationBuckets = []float64{
	0.0001, 0.00025, 0.0005,
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
	0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// fineDurationBuckets 給 handler、delivery 與 WebSocket write 使用，保留
// 微秒級路徑的可觀測解析度，同時涵蓋慢 client 的秒級結果。
var fineDurationBuckets = []float64{
	0.000001, 0.0000025, 0.000005, 0.00001,
	0.000025, 0.00005, 0.0001, 0.00025, 0.0005,
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
	0.1, 0.25, 0.5, 1, 2.5, 5,
}

func gateCommandRouteAndLabel(commandDispatcher *dispatcher.Dispatcher, commandID uint32) (route, command string) {
	if commandDispatcher != nil && commandDispatcher.IsRegistered(WebSocketChannel, dispatcher.CommandID(commandID)) {
		return "local", strconv.FormatUint(uint64(commandID), 10)
	}
	return "game", "forward"
}

// boundedGRPCCode 只接受 gRPC 定義的標準 code；未知數值聚合為 Unknown，
// 避免下游回傳的任意 status code 擴張 Prometheus label cardinality。
func boundedGRPCCode(err error) string {
	code := status.Code(err)
	if code < codes.OK || code > codes.Unauthenticated {
		return codes.Unknown.String()
	}
	return code.String()
}

// gateMetrics 僅包含 ACTIONABLE_METRICS_DESIGN.md 定義的 bounded、可執行
// Gate 觀測值。所有更新都是同步 Prometheus 操作，不改變 transport 行為。
type gateMetrics struct {
	websocketConnections      prometheus.Gauge
	websocketConnectionsTotal prometheus.Counter
	websocketConnectionCloses *prometheus.CounterVec
	websocketCommands         *prometheus.CounterVec
	websocketCommandDuration  *prometheus.HistogramVec
	websocketCommandsInFlight prometheus.Gauge

	gameGRPCRequests        *prometheus.CounterVec
	gameGRPCDuration        *prometheus.HistogramVec
	gameGRPCInFlight        prometheus.Gauge
	websocketWrites         *prometheus.CounterVec
	websocketWriteDuration  *prometheus.HistogramVec
	websocketWritesInFlight prometheus.Gauge
	writeQueueMessages      prometheus.Gauge
	writeQueueCapacity      prometheus.Gauge
	writeQueueFull          *prometheus.CounterVec

	serverSendRequests         *prometheus.CounterVec
	serverSendDeliveryDuration *prometheus.HistogramVec

	asyncQueueMessages      *prometheus.GaugeVec
	asyncQueueBytes         *prometheus.GaugeVec
	asyncQueueCapacityMsgs  *prometheus.GaugeVec
	asyncQueueCapacityBytes *prometheus.GaugeVec
	asyncQueueRejected      *prometheus.CounterVec
	asyncQueueWait          *prometheus.HistogramVec
	asyncWorkerDuration     *prometheus.HistogramVec
	asyncDependencyDuration *prometheus.HistogramVec
	asyncFallback           *prometheus.CounterVec
	asyncDiscarded          *prometheus.CounterVec

	sessionOwnershipActiveLeases  prometheus.Gauge
	sessionOwnershipRenewals      *prometheus.CounterVec
	sessionOwnershipBatchDuration prometheus.Histogram
	sessionOwnershipBatchSize     prometheus.Histogram
	sessionOwnershipSchedulerLag  prometheus.Histogram
	sessionOwnershipOverdueLeases prometheus.Gauge
}

func newGateMetrics(registerer prometheus.Registerer) (*gateMetrics, error) {
	if registerer == nil {
		return nil, errors.New("gate metrics: registerer is nil")
	}
	m := &gateMetrics{
		websocketConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gaming_core_gate_websocket_connections",
			Help: "Current number of active Gate WebSocket connections.",
		}),
		websocketConnectionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gaming_core_gate_websocket_connections_total",
			Help: "Total number of Gate WebSocket connections accepted.",
		}),
		websocketConnectionCloses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gaming_core_gate_websocket_connection_closes_total",
			Help: "Total number of Gate WebSocket connection closures by terminal reason.",
		}, []string{"reason"}),
		websocketCommands: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gaming_core_gate_websocket_commands_total",
			Help: "Total number of Gate WebSocket application commands by route, command, and result.",
		}, []string{"route", "command", "result"}),
		websocketCommandDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gaming_core_gate_websocket_command_duration_seconds",
			Help:    "Gate WebSocket command processing duration in seconds.",
			Buckets: requestDurationBuckets,
		}, []string{"route", "command", "result"}),
		websocketCommandsInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gaming_core_gate_websocket_commands_in_flight",
			Help: "Current number of Gate WebSocket commands being dispatched or forwarded.",
		}),
		gameGRPCRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gaming_core_gate_game_grpc_requests_total",
			Help: "Total number of Gate-to-Game gRPC requests by bounded status code.",
		}, []string{"code"}),
		gameGRPCDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gaming_core_gate_game_grpc_duration_seconds",
			Help:    "Gate-to-Game gRPC request duration in seconds.",
			Buckets: requestDurationBuckets,
		}, []string{"code"}),
		gameGRPCInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gaming_core_gate_game_grpc_in_flight",
			Help: "Current number of Gate-to-Game gRPC requests in flight.",
		}),
		websocketWrites: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gaming_core_gate_websocket_writes_total",
			Help: "Total number of Gate WebSocket application binary writes by source and result.",
		}, []string{"source", "result"}),
		websocketWriteDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gaming_core_gate_websocket_write_duration_seconds",
			Help:    "Gate WebSocket application binary write duration in seconds.",
			Buckets: fineDurationBuckets,
		}, []string{"source", "result"}),
		websocketWritesInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gaming_core_gate_websocket_writes_in_flight",
			Help: "Current number of Gate WebSocket application binary writes in flight.",
		}),
		writeQueueMessages: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gaming_core_gate_websocket_write_queue_messages",
			Help: "Current number of queued Gate WebSocket application messages across active connections.",
		}),
		writeQueueCapacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gaming_core_gate_websocket_write_queue_capacity",
			Help: "Total capacity of Gate WebSocket application write queues across active connections.",
		}),
		writeQueueFull: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gaming_core_gate_websocket_write_queue_full_total",
			Help: "Total number of Gate WebSocket application enqueue failures caused by a full queue.",
		}, []string{"source"}),
		serverSendRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gaming_core_gate_server_send_requests_total",
			Help: "Total number of Gate server-send requests by target and enqueue result.",
		}, []string{"target", "result"}),
		serverSendDeliveryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gaming_core_gate_server_send_delivery_duration_seconds",
			Help:    "Gate server-send receive-to-write terminal duration in seconds.",
			Buckets: fineDurationBuckets,
		}, []string{"target", "result"}),
		asyncQueueMessages: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gaming_core_gate_server_send_outbound_queue_messages",
			Help: "Current queued Gate outbound Broadcast commands.",
		}, []string{"operation"}),
		asyncQueueBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gaming_core_gate_server_send_outbound_queue_bytes",
			Help: "Current deterministic encoded bytes queued for Gate outbound Broadcast.",
		}, []string{"operation"}),
		asyncQueueCapacityMsgs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gaming_core_gate_server_send_outbound_queue_capacity_messages",
			Help: "Configured Gate outbound Broadcast queue message capacity.",
		}, []string{"operation"}),
		asyncQueueCapacityBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gaming_core_gate_server_send_outbound_queue_capacity_bytes",
			Help: "Configured Gate outbound Broadcast queue byte capacity.",
		}, []string{"operation"}),
		asyncQueueRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gaming_core_gate_server_send_outbound_queue_rejected_total",
			Help: "Total rejected Gate outbound Broadcast queue admissions.",
		}, []string{"operation", "reason"}),
		asyncQueueWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gaming_core_gate_server_send_outbound_queue_wait_duration_seconds",
			Help:    "Time from Gate outbound Broadcast admission until worker processing starts.",
			Buckets: fineDurationBuckets,
		}, []string{"operation"}),
		asyncWorkerDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gaming_core_gate_server_send_outbound_worker_duration_seconds",
			Help:    "Background Gate outbound Broadcast worker duration.",
			Buckets: fineDurationBuckets,
		}, []string{"operation", "result"}),
		asyncDependencyDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gaming_core_gate_server_send_outbound_dependency_duration_seconds",
			Help:    "Gate outbound Broadcast dependency duration by bounded dependency and result.",
			Buckets: fineDurationBuckets,
		}, []string{"operation", "dependency", "result"}),
		asyncFallback: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gaming_core_gate_server_send_outbound_fallback_total",
			Help: "Total Gate outbound Broadcast fallback activations.",
		}, []string{"operation", "reason"}),
		asyncDiscarded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gaming_core_gate_server_send_outbound_discarded_total",
			Help: "Total Gate outbound Broadcast commands discarded during shutdown.",
		}, []string{"operation", "reason"}),
		sessionOwnershipActiveLeases: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gaming_core_gate_session_ownership_active_leases",
			Help: "Current number of active Gate session ownership leases.",
		}),
		sessionOwnershipRenewals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gaming_core_gate_session_ownership_renewals_total",
			Help: "Total Gate session ownership renewal outcomes by bounded result.",
		}, []string{"result"}),
		sessionOwnershipBatchDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "gaming_core_gate_session_ownership_renewal_batch_duration_seconds",
			Help:    "Gate session ownership renewal Redis pipeline batch duration in seconds.",
			Buckets: requestDurationBuckets,
		}),
		sessionOwnershipBatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "gaming_core_gate_session_ownership_renewal_batch_size",
			Help:    "Number of leases included in each Gate session ownership renewal batch.",
			Buckets: []float64{1, 4, 16, 32, 64, 128, 256},
		}),
		sessionOwnershipSchedulerLag: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "gaming_core_gate_session_ownership_scheduler_lag_seconds",
			Help:    "Delay between a Gate session ownership lease due time and its renewal scheduling.",
			Buckets: fineDurationBuckets,
		}),
		sessionOwnershipOverdueLeases: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gaming_core_gate_session_ownership_overdue_leases",
			Help: "Current number of Gate session ownership leases awaiting renewal after their due time.",
		}),
	}
	collectors := []prometheus.Collector{
		m.websocketConnections,
		m.websocketConnectionsTotal,
		m.websocketConnectionCloses,
		m.websocketCommands,
		m.websocketCommandDuration,
		m.websocketCommandsInFlight,
		m.gameGRPCRequests,
		m.gameGRPCDuration,
		m.gameGRPCInFlight,
		m.websocketWrites,
		m.websocketWriteDuration,
		m.websocketWritesInFlight,
		m.writeQueueMessages,
		m.writeQueueCapacity,
		m.writeQueueFull,
		m.serverSendRequests,
		m.serverSendDeliveryDuration,
		m.asyncQueueMessages,
		m.asyncQueueBytes,
		m.asyncQueueCapacityMsgs,
		m.asyncQueueCapacityBytes,
		m.asyncQueueRejected,
		m.asyncQueueWait,
		m.asyncWorkerDuration,
		m.asyncDependencyDuration,
		m.asyncFallback,
		m.asyncDiscarded,
		m.sessionOwnershipActiveLeases,
		m.sessionOwnershipRenewals,
		m.sessionOwnershipBatchDuration,
		m.sessionOwnershipBatchSize,
		m.sessionOwnershipSchedulerLag,
		m.sessionOwnershipOverdueLeases,
	}
	for _, collector := range collectors {
		if err := registerer.Register(collector); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (m *gateMetrics) observeCommand(route, command, result string, elapsed time.Duration) {
	if m == nil {
		return
	}
	m.websocketCommands.WithLabelValues(route, command, result).Inc()
	m.websocketCommandDuration.WithLabelValues(route, command, result).Observe(elapsed.Seconds())
}

func (m *gateMetrics) observeGameGRPC(code string, elapsed time.Duration) {
	if m == nil {
		return
	}
	m.gameGRPCRequests.WithLabelValues(code).Inc()
	m.gameGRPCDuration.WithLabelValues(code).Observe(elapsed.Seconds())
}

func (m *gateMetrics) observeServerSendRequest(target, result string) {
	if m != nil {
		m.serverSendRequests.WithLabelValues(target, result).Inc()
	}
}

func (m *gateMetrics) observeServerSendDelivery(target, result string, elapsed time.Duration) {
	if m != nil {
		m.serverSendDeliveryDuration.WithLabelValues(target, result).Observe(elapsed.Seconds())
	}
}

func (m *gateMetrics) SetSessionOwnershipActiveLeases(value float64) {
	if m != nil {
		m.sessionOwnershipActiveLeases.Set(value)
	}
}

func (m *gateMetrics) ObserveSessionOwnershipRenewal(result string) {
	if m != nil {
		m.sessionOwnershipRenewals.WithLabelValues(result).Inc()
	}
}

func (m *gateMetrics) ObserveSessionOwnershipBatch(elapsed time.Duration, size int) {
	if m != nil {
		m.sessionOwnershipBatchDuration.Observe(elapsed.Seconds())
		m.sessionOwnershipBatchSize.Observe(float64(size))
	}
}

func (m *gateMetrics) ObserveSessionOwnershipSchedulerLag(elapsed time.Duration) {
	if m != nil {
		m.sessionOwnershipSchedulerLag.Observe(elapsed.Seconds())
	}
}

func (m *gateMetrics) SetSessionOwnershipOverdueLeases(value float64) {
	if m != nil {
		m.sessionOwnershipOverdueLeases.Set(value)
	}
}

func gateAsyncOperation(operation string) (string, bool) {
	if operation != "broadcast" {
		return "", false
	}
	return operation, true
}

func gateAsyncResult(result string) (string, bool) {
	switch result {
	case "success", "partial", "error":
		return result, true
	default:
		return "", false
	}
}

func gateAsyncDependency(dependency string) (string, bool) {
	switch dependency {
	case "redis_publish", "grpc":
		return dependency, true
	default:
		return "", false
	}
}

func (m *gateMetrics) SetQueueCapacity(operation string, messages, bytes int) {
	operation, ok := gateAsyncOperation(operation)
	if m == nil || !ok {
		return
	}
	m.asyncQueueCapacityMsgs.WithLabelValues(operation).Set(float64(messages))
	m.asyncQueueCapacityBytes.WithLabelValues(operation).Set(float64(bytes))
}

func (m *gateMetrics) ObserveQueue(operation string, messages, bytes int) {
	operation, ok := gateAsyncOperation(operation)
	if m == nil || !ok {
		return
	}
	m.asyncQueueMessages.WithLabelValues(operation).Set(float64(messages))
	m.asyncQueueBytes.WithLabelValues(operation).Set(float64(bytes))
}

func (m *gateMetrics) ObserveQueueRejected(operation, reason string) {
	operation, ok := gateAsyncOperation(operation)
	if m == nil || !ok {
		return
	}
	switch reason {
	case "full", "too_large", "not_running":
	default:
		return
	}
	m.asyncQueueRejected.WithLabelValues(operation, reason).Inc()
}

func (m *gateMetrics) ObserveQueueWait(operation string, elapsed time.Duration) {
	operation, ok := gateAsyncOperation(operation)
	if m == nil || !ok {
		return
	}
	m.asyncQueueWait.WithLabelValues(operation).Observe(elapsed.Seconds())
}

func (m *gateMetrics) ObserveWorker(operation, result string, elapsed time.Duration) {
	operation, operationOK := gateAsyncOperation(operation)
	result, resultOK := gateAsyncResult(result)
	if m == nil || !operationOK || !resultOK {
		return
	}
	m.asyncWorkerDuration.WithLabelValues(operation, result).Observe(elapsed.Seconds())
}

func (m *gateMetrics) ObservePlayerBatch(int, int) {}

func (m *gateMetrics) ObserveDependency(operation, dependency, result string, elapsed time.Duration) {
	operation, operationOK := gateAsyncOperation(operation)
	dependency, dependencyOK := gateAsyncDependency(dependency)
	result, resultOK := gateAsyncResult(result)
	if m == nil || !operationOK || !dependencyOK || !resultOK {
		return
	}
	m.asyncDependencyDuration.WithLabelValues(operation, dependency, result).Observe(elapsed.Seconds())
}

func (m *gateMetrics) ObserveFallback(operation, reason string) {
	operation, ok := gateAsyncOperation(operation)
	if m == nil || !ok {
		return
	}
	if reason != "redis_error" && reason != "no_subscriber" {
		return
	}
	m.asyncFallback.WithLabelValues(operation, reason).Inc()
}

func (m *gateMetrics) ObserveDiscarded(operation, reason string, messages, _ int) {
	operation, ok := gateAsyncOperation(operation)
	if m == nil || !ok || reason != "shutdown_timeout" || messages <= 0 {
		return
	}
	m.asyncDiscarded.WithLabelValues(operation, reason).Add(float64(messages))
}

var _ serversend.AsyncMetricsObserver = (*gateMetrics)(nil)
