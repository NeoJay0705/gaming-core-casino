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
}

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
	}
	for _, collector := range []prometheus.Collector{
		m.gateCommands,
		m.gateCommandDuration,
		m.gateCommandsInFlight,
		m.serverSendRequests,
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

type measuredRequestPlayerSender struct {
	delegate serversend.RequestPlayerSender
	metrics  *gameMetrics
}

func (s *measuredRequestPlayerSender) SendToRequestPlayer(ctx context.Context, message serversend.RequestPlayerMessage) (serversend.Receipt, error) {
	if s == nil || s.delegate == nil {
		return serversend.Receipt{}, errors.New("game server send: request player sender is not configured")
	}
	const operation = "request_player"
	receipt, err := s.delegate.SendToRequestPlayer(ctx, message)
	result := "success"
	if err != nil {
		result = "error"
	}
	if s.metrics != nil {
		s.metrics.serverSendRequests.WithLabelValues(operation, result).Inc()
	}
	return receipt, err
}
