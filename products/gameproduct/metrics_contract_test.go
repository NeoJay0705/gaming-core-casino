package gameproduct

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGameMetricsContractUsesBoundedLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := newGameMetrics(registry)
	if err != nil {
		t.Fatalf("newGameMetrics() error = %v", err)
	}
	metrics.observeGateCommand("unknown", "error", time.Millisecond)
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	seen := make(map[string]bool, len(families))
	for _, family := range families {
		seen[family.GetName()] = true
	}
	for _, name := range []string{
		"gaming_core_game_gate_commands_total",
		"gaming_core_game_gate_command_duration_seconds",
	} {
		if !seen[name] {
			t.Fatalf("metric family %q was not gathered", name)
		}
	}
	if _, err := newGameMetrics(registry); err == nil {
		t.Fatal("duplicate metric registration error = nil")
	}
}

func TestGameCommandLabelContractUsesRegistrationAsTrustBoundary(t *testing.T) {
	commandDispatcher := dispatcher.New()
	if got := gameCommandLabel(commandDispatcher, 123456); got != "unknown" {
		t.Fatalf("unregistered label = %q, want unknown", got)
	}
	if err := commandDispatcher.Register(GateRequestChannel, 7, func(context.Context, []byte) error { return nil }); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if got := gameCommandLabel(commandDispatcher, 7); got != "7" {
		t.Fatalf("registered label = %q, want 7", got)
	}
}

func TestGameGateMetricsContractDistinguishesRegisteredAndUnknown(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := newGameMetrics(registry)
	if err != nil {
		t.Fatalf("newGameMetrics() error = %v", err)
	}
	commandDispatcher := dispatcher.New()
	if err := commandDispatcher.Register(GateRequestChannel, 7, func(context.Context, []byte) error { return nil }); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	server, err := newGameGateGRPCService(gameGRPCServerInputs{
		Dispatcher: commandDispatcher,
		Metrics:    metrics,
	})
	if err != nil {
		t.Fatalf("newGameGateGRPCService() error = %v", err)
	}
	if _, err := server.Forward(context.Background(), &gatelink.GateRequest{CommandId: 7}); err != nil {
		t.Fatalf("registered Forward() error = %v", err)
	}
	if _, err := server.Forward(context.Background(), &gatelink.GateRequest{CommandId: 8}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("unknown Forward() error = %v, want Unimplemented", err)
	}
	if got := counterValue(t, registry, "gaming_core_game_gate_commands_total", map[string]string{"command": "7", "result": "success"}); got != 1 {
		t.Fatalf("registered command count = %v, want 1", got)
	}
	if got := counterValue(t, registry, "gaming_core_game_gate_commands_total", map[string]string{"command": "unknown", "result": "error"}); got != 1 {
		t.Fatalf("unknown command count = %v, want 1", got)
	}
	assertNoGameCommandLabel(t, registry, "8")
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	if len(families) == 0 {
		t.Fatal("Gather() returned no Game metrics")
	}
}

func assertNoGameCommandLabel(t *testing.T, registry *prometheus.Registry, command string) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather game command metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "gaming_core_game_gate_commands_total" {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "command" && label.GetValue() == command {
					t.Fatalf("unregistered raw command label %q was exported", command)
				}
			}
		}
	}
}

func TestMeasuredRequestPlayerSenderContractRecordsSuccessAndError(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := newGameMetrics(registry)
	if err != nil {
		t.Fatalf("newGameMetrics() error = %v", err)
	}
	sentinel := errors.New("request-player failed")
	delegate := &recordingRequestPlayerSender{err: nil}
	sender := &measuredRequestPlayerSender{delegate: delegate, metrics: metrics}
	if _, err := sender.SendToRequestPlayer(context.Background(), serversend.RequestPlayerMessage{}); err != nil {
		t.Fatalf("successful SendToRequestPlayer() error = %v", err)
	}
	delegate.err = sentinel
	if _, err := sender.SendToRequestPlayer(context.Background(), serversend.RequestPlayerMessage{}); !errors.Is(err, sentinel) {
		t.Fatalf("failed SendToRequestPlayer() error = %v, want %v", err, sentinel)
	}
	if got := counterValue(t, registry, "gaming_core_game_server_send_requests_total", map[string]string{"operation": "request_player", "result": "success"}); got != 1 {
		t.Fatalf("server-send success count = %v, want 1", got)
	}
	if got := counterValue(t, registry, "gaming_core_game_server_send_requests_total", map[string]string{"operation": "request_player", "result": "error"}); got != 1 {
		t.Fatalf("server-send error count = %v, want 1", got)
	}
}

func TestMeasuredPlayerAndBroadcastSendersClassifyPartialAndTrackLatency(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := newGameMetrics(registry)
	if err != nil {
		t.Fatalf("newGameMetrics() error = %v", err)
	}
	partialErr := errors.New("partial delivery")
	player := &measuredPlayerSender{
		delegate: &recordingPlayerSender{results: []senderResult{
			{receipt: serversend.Receipt{AcceptedAt: time.Now()}},
			{err: partialErr, receipt: serversend.Receipt{AcceptedAt: time.Now()}},
			{err: errors.New("player failed")},
		}},
		metrics: metrics,
	}
	for i := 0; i < 3; i++ {
		_, _ = player.SendToPlayers(context.Background(), []serversend.PlayerMessage{{LoginName: "alice", Message: serversend.Message{CommandID: 1}}})
	}
	broadcast := &measuredBroadcastSender{
		delegate: &recordingBroadcastSender{results: []senderResult{
			{receipt: serversend.Receipt{AcceptedAt: time.Now()}},
			{err: partialErr, receipt: serversend.Receipt{AcceptedAt: time.Now()}},
			{err: errors.New("broadcast failed")},
		}},
		metrics: metrics,
	}
	for i := 0; i < 3; i++ {
		_, _ = broadcast.Broadcast(context.Background(), serversend.Message{CommandID: 1})
	}
	for _, operation := range []string{serverSendOperationPlayer, serverSendOperationBroadcast} {
		for _, result := range []string{serverSendResultSuccess, serverSendResultPartial, serverSendResultError} {
			labels := map[string]string{"operation": operation, "result": result}
			if got := counterValue(t, registry, "gaming_core_game_server_send_requests_total", labels); got != 1 {
				t.Fatalf("%s %s count = %v, want 1", operation, result, got)
			}
			if got := counterValue(t, registry, "gaming_core_game_server_send_duration_seconds", labels); got != 1 {
				t.Fatalf("%s %s duration count = %v, want 1", operation, result, got)
			}
		}
		if got := counterValue(t, registry, "gaming_core_game_server_send_in_flight", map[string]string{"operation": operation}); got != 0 {
			t.Fatalf("%s in-flight = %v, want 0", operation, got)
		}
	}
}

type senderResult struct {
	receipt serversend.Receipt
	err     error
}

type recordingPlayerSender struct {
	results []senderResult
	index   int
}

func (s *recordingPlayerSender) SendToPlayers(context.Context, []serversend.PlayerMessage) (serversend.Receipt, error) {
	result := s.results[s.index]
	s.index++
	return result.receipt, result.err
}

type recordingBroadcastSender struct {
	results []senderResult
	index   int
}

func (s *recordingBroadcastSender) Broadcast(context.Context, serversend.Message) (serversend.Receipt, error) {
	result := s.results[s.index]
	s.index++
	return result.receipt, result.err
}

type recordingRequestPlayerSender struct{ err error }

func (s *recordingRequestPlayerSender) SendToRequestPlayer(context.Context, serversend.RequestPlayerMessage) (serversend.Receipt, error) {
	if s.err != nil {
		return serversend.Receipt{}, s.err
	}
	return serversend.Receipt{AcceptedAt: time.Now()}, nil
}

func counterValue(t *testing.T, registry *prometheus.Registry, name string, wantLabels map[string]string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather %s: %v", name, err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			labels := make(map[string]string, len(metric.Label))
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if len(labels) != len(wantLabels) {
				continue
			}
			matched := true
			for key, want := range wantLabels {
				if labels[key] != want {
					matched = false
					break
				}
			}
			if !matched {
				continue
			}
			if metric.Counter != nil {
				return metric.GetCounter().GetValue()
			}
			if metric.Histogram != nil {
				return float64(metric.GetHistogram().GetSampleCount())
			}
			if metric.Gauge != nil {
				return metric.GetGauge().GetValue()
			}
		}
	}
	t.Fatalf("metric %q with labels %#v was not gathered", name, wantLabels)
	return 0
}
