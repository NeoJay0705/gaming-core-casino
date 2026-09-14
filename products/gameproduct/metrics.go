package gameproduct

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/prometheus/client_golang/prometheus"
)

// handlerDurationBuckets 保留 Game handler 微秒級 latency 的解析度，並
// 涵蓋慢 request 的秒級結果；不使用 DefBuckets 的 5ms 起點。
var handlerDurationBuckets = []float64{
	0.000001, 0.0000025, 0.000005, 0.00001,
	0.000025, 0.00005, 0.0001, 0.00025, 0.0005,
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
	0.1, 0.25, 0.5, 1, 2.5, 5,
}

type gameMetrics struct {
	gateCommands         *prometheus.CounterVec
	gateCommandDuration  *prometheus.HistogramVec
	gateCommandsInFlight prometheus.Gauge

	serverSendRequests *prometheus.CounterVec
	serverSendDuration *prometheus.HistogramVec
	serverSendInFlight *prometheus.GaugeVec

	asyncQueueMessages       *prometheus.GaugeVec
	asyncQueueBytes          *prometheus.GaugeVec
	asyncQueueCapacityMsgs   *prometheus.GaugeVec
	asyncQueueCapacityBytes  *prometheus.GaugeVec
	asyncQueueRejected       *prometheus.CounterVec
	asyncQueueWait           *prometheus.HistogramVec
	asyncWorkerDuration      *prometheus.HistogramVec
	asyncPlayerBatchMessages prometheus.Histogram
	asyncPlayerBatchBytes    prometheus.Histogram
	asyncDependencyDuration  *prometheus.HistogramVec
	asyncFallback            *prometheus.CounterVec
	asyncDiscarded           *prometheus.CounterVec
}

const (
	serverSendOperationRequestPlayer = "request_player"
	serverSendOperationPlayer        = "player"
	serverSendOperationBroadcast     = "broadcast"
	serverSendResultSuccess          = "success"
	serverSendResultPartial          = "partial"
	serverSendResultError            = "error"
)

func gameCommandLabel(commandDispatcher *dispatcher.Dispatcher, commandID uint32) string {
	if commandDispatcher != nil && commandDispatcher.IsRegistered(GateRequestChannel, dispatcher.CommandID(commandID)) {
		return strconv.FormatUint(uint64(commandID), 10)
	}
	return "unknown"
}

func newGameMetrics(registerer prometheus.Registerer) (*gameMetrics, error) {
	if registerer == nil {
		return nil, errors.New("game metrics: registerer is nil")
	}
	m := &gameMetrics{
		gateCommands: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gaming_core_game_gate_commands_total",
			Help: "Total number of Game commands received from Gate by command and result.",
		}, []string{"command", "result"}),
		gateCommandDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gaming_core_game_gate_command_duration_seconds",
			Help:    "Game command handler duration in seconds.",
			Buckets: handlerDurationBuckets,
		}, []string{"command", "result"}),
		gateCommandsInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gaming_core_game_gate_commands_in_flight",
			Help: "Current number of Game Gate commands being handled.",
		}),
		serverSendRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gaming_core_game_server_send_requests_total",
			Help: "Total number of Game server-send requests by operation and result.",
		}, []string{"operation", "result"}),
		serverSendDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gaming_core_game_server_send_duration_seconds",
			Help:    "Game server-send API duration in seconds by operation and result.",
			Buckets: handlerDurationBuckets,
		}, []string{"operation", "result"}),
		serverSendInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gaming_core_game_server_send_in_flight",
			Help: "Current number of Game server-send API calls in flight by operation.",
		}, []string{"operation"}),
		asyncQueueMessages: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gaming_core_game_server_send_queue_messages",
			Help: "Current queued Game server-send messages or Broadcast commands.",
		}, []string{"operation"}),
		asyncQueueBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gaming_core_game_server_send_queue_bytes",
			Help: "Current deterministic encoded bytes queued for Game server-send.",
		}, []string{"operation"}),
		asyncQueueCapacityMsgs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gaming_core_game_server_send_queue_capacity_messages",
			Help: "Configured Game server-send queue message capacity.",
		}, []string{"operation"}),
		asyncQueueCapacityBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gaming_core_game_server_send_queue_capacity_bytes",
			Help: "Configured Game server-send queue byte capacity.",
		}, []string{"operation"}),
		asyncQueueRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gaming_core_game_server_send_queue_rejected_total",
			Help: "Total rejected Game server-send queue admissions.",
		}, []string{"operation", "reason"}),
		asyncQueueWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gaming_core_game_server_send_queue_wait_duration_seconds",
			Help:    "Time from Game server-send admission until worker processing starts.",
			Buckets: handlerDurationBuckets,
		}, []string{"operation"}),
		asyncWorkerDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gaming_core_game_server_send_worker_duration_seconds",
			Help:    "Background Game server-send worker duration.",
			Buckets: handlerDurationBuckets,
		}, []string{"operation", "result"}),
		asyncPlayerBatchMessages: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "gaming_core_game_server_send_player_batch_messages",
			Help:    "Number of Player messages in each Game worker batch.",
			Buckets: []float64{1, 4, 16, 32, 64, 128, 256, 512, 1000, 5000, 10000},
		}),
		asyncPlayerBatchBytes: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "gaming_core_game_server_send_player_batch_bytes",
			Help:    "Deterministic bytes in each Game Player worker batch.",
			Buckets: []float64{1024, 16 * 1024, 64 * 1024, 256 * 1024, 1 << 20, 4 << 20, 8 << 20},
		}),
		asyncDependencyDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gaming_core_game_server_send_dependency_duration_seconds",
			Help:    "Game server-send dependency duration by bounded dependency and result.",
			Buckets: handlerDurationBuckets,
		}, []string{"operation", "dependency", "result"}),
		asyncFallback: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gaming_core_game_server_send_fallback_total",
			Help: "Total Game server-send fallback activations.",
		}, []string{"operation", "reason"}),
		asyncDiscarded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gaming_core_game_server_send_discarded_total",
			Help: "Total Game server-send messages discarded during shutdown.",
		}, []string{"operation", "reason"}),
	}
	for _, collector := range []prometheus.Collector{
		m.gateCommands,
		m.gateCommandDuration,
		m.gateCommandsInFlight,
		m.serverSendRequests,
		m.serverSendDuration,
		m.serverSendInFlight,
		m.asyncQueueMessages,
		m.asyncQueueBytes,
		m.asyncQueueCapacityMsgs,
		m.asyncQueueCapacityBytes,
		m.asyncQueueRejected,
		m.asyncQueueWait,
		m.asyncWorkerDuration,
		m.asyncPlayerBatchMessages,
		m.asyncPlayerBatchBytes,
		m.asyncDependencyDuration,
		m.asyncFallback,
		m.asyncDiscarded,
	} {
		if err := registerer.Register(collector); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (m *gameMetrics) observeGateCommand(command, result string, elapsed time.Duration) {
	if m == nil {
		return
	}
	m.gateCommands.WithLabelValues(command, result).Inc()
	m.gateCommandDuration.WithLabelValues(command, result).Observe(elapsed.Seconds())
}

func (m *gameMetrics) startServerSend(operation string) {
	if m != nil {
		m.serverSendInFlight.WithLabelValues(operation).Inc()
	}
}

type measuredRequestPlayerSender struct {
	delegate serversend.RequestPlayerSender
	metrics  *gameMetrics
}

func (s *measuredRequestPlayerSender) SendToRequestPlayer(ctx context.Context, message serversend.RequestPlayerMessage) (serversend.Receipt, error) {
	if s == nil || s.delegate == nil {
		return serversend.Receipt{}, errors.New("game server send: request player sender is not configured")
	}
	const operation = serverSendOperationRequestPlayer
	startedAt := time.Now()
	if s.metrics != nil {
		s.metrics.startServerSend(operation)
	}
	receipt, err := s.delegate.SendToRequestPlayer(ctx, message)
	if s.metrics != nil {
		s.metrics.finishServerSend(operation, receipt, err, time.Since(startedAt))
	}
	return receipt, err
}

func (m *gameMetrics) finishServerSend(operation string, receipt serversend.Receipt, err error, elapsed time.Duration) {
	if m == nil {
		return
	}
	m.serverSendInFlight.WithLabelValues(operation).Dec()
	result := serverSendResultSuccess
	if err != nil {
		result = serverSendResultError
		if !receipt.AcceptedAt.IsZero() {
			result = serverSendResultPartial
		}
	}
	m.serverSendRequests.WithLabelValues(operation, result).Inc()
	m.serverSendDuration.WithLabelValues(operation, result).Observe(elapsed.Seconds())
}

type measuredPlayerSender struct {
	delegate serversend.PlayerSender
	metrics  *gameMetrics
}

func (s *measuredPlayerSender) SendToPlayers(ctx context.Context, messages []serversend.PlayerMessage) (serversend.Receipt, error) {
	if s == nil || s.delegate == nil {
		return serversend.Receipt{}, errors.New("game server send: player sender is not configured")
	}
	startedAt := time.Now()
	if s.metrics != nil {
		s.metrics.startServerSend(serverSendOperationPlayer)
	}
	receipt, err := s.delegate.SendToPlayers(ctx, messages)
	if s.metrics != nil {
		s.metrics.finishServerSend(serverSendOperationPlayer, receipt, err, time.Since(startedAt))
	}
	return receipt, err
}

type measuredBroadcastSender struct {
	delegate serversend.BroadcastSender
	metrics  *gameMetrics
}

func (s *measuredBroadcastSender) Broadcast(ctx context.Context, message serversend.Message) (serversend.Receipt, error) {
	if s == nil || s.delegate == nil {
		return serversend.Receipt{}, errors.New("game server send: broadcast sender is not configured")
	}
	startedAt := time.Now()
	if s.metrics != nil {
		s.metrics.startServerSend(serverSendOperationBroadcast)
	}
	receipt, err := s.delegate.Broadcast(ctx, message)
	if s.metrics != nil {
		s.metrics.finishServerSend(serverSendOperationBroadcast, receipt, err, time.Since(startedAt))
	}
	return receipt, err
}

func gameAsyncOperation(operation string) (string, bool) {
	switch operation {
	case serverSendOperationPlayer, serverSendOperationBroadcast:
		return operation, true
	default:
		return "", false
	}
}

func gameAsyncResult(result string) (string, bool) {
	switch result {
	case serverSendResultSuccess, serverSendResultPartial, serverSendResultError:
		return result, true
	default:
		return "", false
	}
}

func gameAsyncDependency(dependency string) (string, bool) {
	switch dependency {
	case "redis_presence", "redis_endpoint", "redis_publish", "grpc":
		return dependency, true
	default:
		return "", false
	}
}

func (m *gameMetrics) SetQueueCapacity(operation string, messages, bytes int) {
	operation, ok := gameAsyncOperation(operation)
	if m == nil || !ok {
		return
	}
	m.asyncQueueCapacityMsgs.WithLabelValues(operation).Set(float64(messages))
	m.asyncQueueCapacityBytes.WithLabelValues(operation).Set(float64(bytes))
}

func (m *gameMetrics) ObserveQueue(operation string, messages, bytes int) {
	operation, ok := gameAsyncOperation(operation)
	if m == nil || !ok {
		return
	}
	m.asyncQueueMessages.WithLabelValues(operation).Set(float64(messages))
	m.asyncQueueBytes.WithLabelValues(operation).Set(float64(bytes))
}

func (m *gameMetrics) ObserveQueueRejected(operation, reason string) {
	operation, ok := gameAsyncOperation(operation)
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

func (m *gameMetrics) ObserveQueueWait(operation string, elapsed time.Duration) {
	operation, ok := gameAsyncOperation(operation)
	if m == nil || !ok {
		return
	}
	m.asyncQueueWait.WithLabelValues(operation).Observe(elapsed.Seconds())
}

func (m *gameMetrics) ObserveWorker(operation, result string, elapsed time.Duration) {
	operation, operationOK := gameAsyncOperation(operation)
	result, resultOK := gameAsyncResult(result)
	if m == nil || !operationOK || !resultOK {
		return
	}
	m.asyncWorkerDuration.WithLabelValues(operation, result).Observe(elapsed.Seconds())
}

func (m *gameMetrics) ObservePlayerBatch(messages, bytes int) {
	if m == nil {
		return
	}
	m.asyncPlayerBatchMessages.Observe(float64(messages))
	m.asyncPlayerBatchBytes.Observe(float64(bytes))
}

func (m *gameMetrics) ObserveDependency(operation, dependency, result string, elapsed time.Duration) {
	operation, operationOK := gameAsyncOperation(operation)
	dependency, dependencyOK := gameAsyncDependency(dependency)
	result, resultOK := gameAsyncResult(result)
	if m == nil || !operationOK || !dependencyOK || !resultOK {
		return
	}
	m.asyncDependencyDuration.WithLabelValues(operation, dependency, result).Observe(elapsed.Seconds())
}

func (m *gameMetrics) ObserveFallback(operation, reason string) {
	operation, ok := gameAsyncOperation(operation)
	if m == nil || !ok {
		return
	}
	if reason != "redis_error" && reason != "no_subscriber" {
		return
	}
	m.asyncFallback.WithLabelValues(operation, reason).Inc()
}

func (m *gameMetrics) ObserveDiscarded(operation, reason string, messages, _ int) {
	operation, ok := gameAsyncOperation(operation)
	if m == nil || !ok || reason != "shutdown_timeout" || messages <= 0 {
		return
	}
	m.asyncDiscarded.WithLabelValues(operation, reason).Add(float64(messages))
}

var _ serversend.AsyncMetricsObserver = (*gameMetrics)(nil)
