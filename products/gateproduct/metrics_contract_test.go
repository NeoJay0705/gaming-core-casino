package gateproduct

import (
	"context"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGateCommandLabelContractUsesRegistrationAsTrustBoundary(t *testing.T) {
	commandDispatcher := dispatcher.New()
	if route, command := gateCommandRouteAndLabel(commandDispatcher, 123456); route != "game" || command != "forward" {
		t.Fatalf("unregistered label = (%q, %q), want (game, forward)", route, command)
	}
	if err := commandDispatcher.Register(WebSocketChannel, 7, func(context.Context, []byte) error { return nil }); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if route, command := gateCommandRouteAndLabel(commandDispatcher, 7); route != "local" || command != "7" {
		t.Fatalf("registered label = (%q, %q), want (local, 7)", route, command)
	}
}

func TestBoundedGRPCCodeContractAggregatesNonStandardCodes(t *testing.T) {
	if got := boundedGRPCCode(nil); got != codes.OK.String() {
		t.Fatalf("boundedGRPCCode(nil) = %q, want %q", got, codes.OK.String())
	}
	if got := boundedGRPCCode(status.Error(codes.Unavailable, "temporary")); got != codes.Unavailable.String() {
		t.Fatalf("boundedGRPCCode(standard) = %q, want %q", got, codes.Unavailable.String())
	}
	if got := boundedGRPCCode(status.Error(codes.Code(99), "custom")); got != codes.Unknown.String() {
		t.Fatalf("boundedGRPCCode(custom) = %q, want %q", got, codes.Unknown.String())
	}
}

func TestGateMetricsContractUsesBoundedLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := newGateMetrics(registry)
	if err != nil {
		t.Fatalf("newGateMetrics() error = %v", err)
	}
	metrics.observeCommand("game", "forward", "success", time.Millisecond)
	metrics.observeGameGRPC("OK", time.Millisecond)
	metrics.observeServerSendRequest("connection", "queued")
	metrics.observeServerSendDelivery("connection", "success", time.Millisecond)
	metrics.SetSessionOwnershipActiveLeases(1)
	metrics.ObserveSessionOwnershipRenewal("success")
	metrics.ObserveSessionOwnershipBatch(time.Millisecond, 1)
	metrics.ObserveSessionOwnershipSchedulerLag(time.Millisecond)
	metrics.SetSessionOwnershipOverdueLeases(1)
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	seen := make(map[string]bool, len(families))
	for _, family := range families {
		seen[family.GetName()] = true
	}
	for _, name := range []string{
		"gaming_core_gate_websocket_commands_total",
		"gaming_core_gate_game_grpc_requests_total",
		"gaming_core_gate_server_send_requests_total",
		"gaming_core_gate_server_send_delivery_duration_seconds",
		"gaming_core_gate_session_ownership_active_leases",
		"gaming_core_gate_session_ownership_renewals_total",
		"gaming_core_gate_session_ownership_renewal_batch_duration_seconds",
		"gaming_core_gate_session_ownership_renewal_batch_size",
		"gaming_core_gate_session_ownership_scheduler_lag_seconds",
		"gaming_core_gate_session_ownership_overdue_leases",
	} {
		if !seen[name] {
			t.Fatalf("metric family %q was not gathered", name)
		}
	}
	if _, err := newGateMetrics(registry); err == nil {
		t.Fatal("duplicate metric registration error = nil")
	}
}
