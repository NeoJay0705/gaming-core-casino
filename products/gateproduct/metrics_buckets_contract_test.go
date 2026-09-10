package gateproduct

import (
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestGateDurationBucketsContract(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := newGateMetrics(registry)
	if err != nil {
		t.Fatalf("newGateMetrics() error = %v", err)
	}
	metrics.observeCommand("game", "forward", "success", time.Millisecond)
	metrics.observeGameGRPC("OK", time.Millisecond)
	metrics.websocketWriteDuration.WithLabelValues("handler", "success").Observe(time.Microsecond.Seconds())
	metrics.observeServerSendDelivery("connection", "success", time.Microsecond)

	wantRequest := []float64{
		0.0001, 0.00025, 0.0005,
		0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
		0.1, 0.25, 0.5, 1, 2.5, 5, 10,
	}
	wantFine := []float64{
		0.000001, 0.0000025, 0.000005, 0.00001,
		0.000025, 0.00005, 0.0001, 0.00025, 0.0005,
		0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
		0.1, 0.25, 0.5, 1, 2.5, 5,
	}
	assertGateHistogramBuckets(t, registry, "gaming_core_gate_websocket_command_duration_seconds", map[string]string{
		"route": "game", "command": "forward", "result": "success",
	}, wantRequest)
	assertGateHistogramBuckets(t, registry, "gaming_core_gate_game_grpc_duration_seconds", map[string]string{
		"code": "OK",
	}, wantRequest)
	assertGateHistogramBuckets(t, registry, "gaming_core_gate_websocket_write_duration_seconds", map[string]string{
		"source": "handler", "result": "success",
	}, wantFine)
	assertGateHistogramBuckets(t, registry, "gaming_core_gate_server_send_delivery_duration_seconds", map[string]string{
		"target": "connection", "result": "success",
	}, wantFine)
}

func assertGateHistogramBuckets(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string, want []float64) {
	t.Helper()
	metric := gatheredMetric(t, registry, name, labels)
	got := make([]float64, 0, len(metric.GetHistogram().GetBucket()))
	for _, bucket := range metric.GetHistogram().GetBucket() {
		got = append(got, bucket.GetUpperBound())
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("histogram %s buckets = %v, want %v", name, got, want)
	}
}
