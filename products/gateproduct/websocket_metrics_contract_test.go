package gateproduct

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/logging"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestWebSocketSendBinaryCarriesDetachedTraceContext(t *testing.T) {
	connection := newWebSocketConnection(nil, 1, time.Second)
	parent, cancel := context.WithCancel(context.Background())
	traced, err := logging.ContinueOrNew(parent, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if err != nil {
		t.Fatal(err)
	}
	wantTrace, wantSpan, _ := logging.IDsFromContext(traced)
	if err := connection.SendBinary(traced, []byte("response")); err != nil {
		t.Fatal(err)
	}
	cancel()
	message := <-connection.writeCh
	if message.ctx == nil || message.ctx.Err() != nil {
		t.Fatalf("queued context = %v, want detached context", message.ctx)
	}
	gotTrace, gotSpan, ok := logging.IDsFromContext(message.ctx)
	if !ok || gotTrace != wantTrace || gotSpan != wantSpan {
		t.Fatalf("queued trace = %q/%q/%t, want %q/%q", gotTrace, gotSpan, ok, wantTrace, wantSpan)
	}
}

func TestWebSocketQueueMetricsContract(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := newGateMetrics(registry)
	if err != nil {
		t.Fatalf("newGateMetrics() error = %v", err)
	}
	connection := newWebSocketConnection(nil, 1, time.Second, metrics)
	connection.markAccepted()
	if err := connection.SendBinary(context.Background(), []byte("first")); err != nil {
		t.Fatalf("first SendBinary() error = %v", err)
	}
	if err := connection.sendOutbound(outboundMessage{
		data:       []byte("second"),
		source:     outboundSourceServerSend,
		receivedAt: time.Now(),
		target:     serverSendTargetConnection,
	}); !errors.Is(err, errWebSocketWriteQueueFull) {
		t.Fatalf("second sendOutbound() error = %v, want queue-full", err)
	}
	_ = connection.closeWithReason(closeReasonForwardError)
	if got := connection.currentCloseReason(); got != closeReasonWriteQueueFull {
		t.Fatalf("close reason = %q, want %q", got, closeReasonWriteQueueFull)
	}
	connection.finish()
	if got := gaugeValue(t, metrics.writeQueueMessages); got != 0 {
		t.Fatalf("queue messages = %v, want 0 after finish", got)
	}
	if got := gaugeValue(t, metrics.writeQueueCapacity); got != 0 {
		t.Fatalf("queue capacity = %v, want 0 after finish", got)
	}
	if got := gaugeValue(t, metrics.websocketConnections); got != 0 {
		t.Fatalf("websocket connections = %v, want 0 after finish", got)
	}
	if got := gatheredCounterValue(t, registry, "gaming_core_gate_websocket_write_queue_full_total", map[string]string{"source": "server_send"}); got != 1 {
		t.Fatalf("queue full counter = %v, want 1", got)
	}
	if got := gatheredCounterValue(t, registry, "gaming_core_gate_websocket_connection_closes_total", map[string]string{"reason": closeReasonWriteQueueFull}); got != 1 {
		t.Fatalf("queue-full close counter = %v, want 1", got)
	}
}

func TestWebSocketServerSendDiscardMetricsContract(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := newGateMetrics(registry)
	if err != nil {
		t.Fatalf("newGateMetrics() error = %v", err)
	}
	connection := newWebSocketConnection(nil, 1, time.Second, metrics)
	connection.markAccepted()
	if err := connection.sendOutbound(outboundMessage{
		data:       []byte("discarded"),
		source:     outboundSourceServerSend,
		receivedAt: time.Now(),
		target:     serverSendTargetConnection,
	}); err != nil {
		t.Fatalf("sendOutbound() error = %v", err)
	}
	_ = connection.closeWithReason(closeReasonServerClosed)
	connection.finish()

	if got := histogramSampleCount(t, registry, "gaming_core_gate_server_send_delivery_duration_seconds", map[string]string{
		"target": "connection",
		"result": "dropped",
	}); got != 1 {
		t.Fatalf("dropped delivery observations = %d, want 1", got)
	}
	if got := gaugeValue(t, metrics.writeQueueMessages); got != 0 {
		t.Fatalf("queue messages = %v, want 0", got)
	}
}

func TestWebSocketWriteErrorMetricsContract(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := newGateMetrics(registry)
	if err != nil {
		t.Fatalf("newGateMetrics() error = %v", err)
	}
	connection := newWebSocketConnection(nil, 1, time.Second, metrics)
	connection.markAccepted()
	if err := connection.sendOutbound(outboundMessage{
		data:       []byte("write-error"),
		source:     outboundSourceServerSend,
		receivedAt: time.Now(),
		target:     serverSendTargetConnection,
	}); err != nil {
		t.Fatalf("sendOutbound() error = %v", err)
	}
	done := make(chan struct{})
	go func() {
		connection.writeLoop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writeLoop did not finish after nil-connection write error")
	}
	connection.finish()

	if got := gatheredCounterValue(t, registry, "gaming_core_gate_websocket_writes_total", map[string]string{
		"source": "server_send",
		"result": "error",
	}); got != 1 {
		t.Fatalf("write error counter = %v, want 1", got)
	}
	if got := histogramSampleCount(t, registry, "gaming_core_gate_server_send_delivery_duration_seconds", map[string]string{
		"target": "connection",
		"result": "error",
	}); got != 1 {
		t.Fatalf("write error delivery observations = %d, want 1", got)
	}
	if got := gaugeValue(t, metrics.websocketWritesInFlight); got != 0 {
		t.Fatalf("writes in-flight = %v, want 0", got)
	}
	if got := connection.currentCloseReason(); got != closeReasonWriteError {
		t.Fatalf("close reason = %q, want %q", got, closeReasonWriteError)
	}
}

func TestWebSocketShutdownAndServerCloseReasonsRemainDistinct(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := newGateMetrics(registry)
	if err != nil {
		t.Fatalf("newGateMetrics() error = %v", err)
	}
	serverClosed := newWebSocketConnection(nil, 1, time.Second, metrics)
	serverClosed.markAccepted()
	_ = serverClosed.Close()
	serverClosed.finish()
	shutdown := newWebSocketConnection(nil, 1, time.Second, metrics)
	shutdown.markAccepted()
	_ = shutdown.closeWithReason(closeReasonShutdown)
	shutdown.finish()

	if got := gatheredCounterValue(t, registry, "gaming_core_gate_websocket_connection_closes_total", map[string]string{"reason": closeReasonServerClosed}); got != 1 {
		t.Fatalf("server-closed counter = %v, want 1", got)
	}
	if got := gatheredCounterValue(t, registry, "gaming_core_gate_websocket_connection_closes_total", map[string]string{"reason": closeReasonShutdown}); got != 1 {
		t.Fatalf("shutdown counter = %v, want 1", got)
	}
}

func gaugeValue(t *testing.T, gauge prometheus.Gauge) float64 {
	t.Helper()
	metric := new(dto.Metric)
	if err := gauge.Write(metric); err != nil {
		t.Fatalf("write gauge metric: %v", err)
	}
	return metric.GetGauge().GetValue()
}

func gatheredCounterValue(t *testing.T, registry *prometheus.Registry, name string, wantLabels map[string]string) float64 {
	t.Helper()
	metric := gatheredMetric(t, registry, name, wantLabels)
	return metric.GetCounter().GetValue()
}

func histogramSampleCount(t *testing.T, registry *prometheus.Registry, name string, wantLabels map[string]string) uint64 {
	t.Helper()
	metric := gatheredMetric(t, registry, name, wantLabels)
	return metric.GetHistogram().GetSampleCount()
}

func gatheredMetric(t *testing.T, registry *prometheus.Registry, name string, wantLabels map[string]string) *dto.Metric {
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
			if matched {
				return metric
			}
		}
	}
	t.Fatalf("metric %q with labels %#v was not gathered", name, wantLabels)
	return nil
}
