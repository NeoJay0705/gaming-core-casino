package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/protocol"
	"github.com/NeoJay0705/gaming-core-casino/products/gateproduct"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestLoadMetricsRecordsTerminalResultsAndReturnsInFlightToZero(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := newLoadMetrics(registry)
	if err != nil {
		t.Fatalf("newLoadMetrics() error = %v", err)
	}

	metrics.startEcho()
	metrics.finishEcho(context.Background(), nil, time.Millisecond)
	metrics.startEcho()
	metrics.finishEcho(context.Background(), errors.New("read failed"), 2*time.Millisecond)
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancel)
	metrics.startEcho()
	metrics.finishEcho(expired, context.DeadlineExceeded, 3*time.Millisecond)

	for _, result := range []string{loadResultSuccess, loadResultError, loadResultCancelled} {
		if got := counterSampleValue(t, registry, "gaming_core_example_load_echo_round_trips_total", result); got != 1 {
			t.Fatalf("round trips result=%q = %v, want 1", result, got)
		}
		if got := histogramSampleCount(t, registry, "gaming_core_example_load_echo_round_trip_duration_seconds", result); got != 1 {
			t.Fatalf("duration count result=%q = %d, want 1", result, got)
		}
	}
	if got := gaugeSampleValue(t, registry, "gaming_core_example_load_echo_round_trips_in_flight"); got != 0 {
		t.Fatalf("in-flight = %v, want 0", got)
	}
}

func TestEchoRoundTripRecordsClientMetrics(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	serverResult := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			serverResult <- err
			return
		}
		defer connection.Close()
		if _, _, err := connection.ReadMessage(); err != nil {
			serverResult <- err
			return
		}
		response := gateproduct.EncodeWebSocketPacket(gateproduct.WebSocketPacket{CommandID: protocol.EchoResponseCommandID})
		serverResult <- connection.WriteMessage(websocket.BinaryMessage, response)
	}))
	t.Cleanup(server.Close)

	connection, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial test server: %v", err)
	}
	defer connection.Close()
	registry := prometheus.NewRegistry()
	metrics, err := newLoadMetrics(registry)
	if err != nil {
		t.Fatalf("newLoadMetrics() error = %v", err)
	}
	if err := echoRoundTrip(context.Background(), connection, []byte("echo"), 1, metrics); err != nil {
		t.Fatalf("echoRoundTrip() error = %v", err)
	}
	if err := <-serverResult; err != nil {
		t.Fatalf("test server: %v", err)
	}
	if got := counterSampleValue(t, registry, "gaming_core_example_load_echo_round_trips_total", loadResultSuccess); got != 1 {
		t.Fatalf("successful round trips = %v, want 1", got)
	}
	if got := histogramSampleCount(t, registry, "gaming_core_example_load_echo_round_trip_duration_seconds", loadResultSuccess); got != 1 {
		t.Fatalf("successful duration count = %d, want 1", got)
	}
	if got := gaugeSampleValue(t, registry, "gaming_core_example_load_echo_round_trips_in_flight"); got != 0 {
		t.Fatalf("in-flight = %v, want 0", got)
	}
}

func TestLoadMetricsRejectsNilAndDuplicateRegisterer(t *testing.T) {
	if _, err := newLoadMetrics(nil); err == nil {
		t.Fatal("newLoadMetrics(nil) error = nil")
	}
	registry := prometheus.NewRegistry()
	if _, err := newLoadMetrics(registry); err != nil {
		t.Fatalf("first newLoadMetrics() error = %v", err)
	}
	if _, err := newLoadMetrics(registry); err == nil {
		t.Fatal("duplicate newLoadMetrics() error = nil")
	}
}

func TestLoadObserverExposesOnlyMetricsAndShutsDown(t *testing.T) {
	observer, err := newLoadObserver("127.0.0.1:0")
	if err != nil {
		t.Fatalf("newLoadObserver() error = %v", err)
	}
	var shutdownOnce sync.Once
	shutdown := func() {
		shutdownOnce.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := observer.Shutdown(ctx); err != nil {
				t.Fatalf("Shutdown() error = %v", err)
			}
		})
	}
	t.Cleanup(shutdown)

	observer.metrics.startEcho()
	observer.metrics.finishEcho(context.Background(), nil, time.Millisecond)
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://" + observer.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read /metrics: read=%v close=%v", readErr, closeErr)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want 200", response.StatusCode)
	}
	if !strings.Contains(string(body), `gaming_core_example_load_echo_round_trips_total{result="success"} 1`) {
		t.Fatalf("GET /metrics body does not contain successful Echo counter:\n%s", body)
	}

	response, err = client.Get("http://" + observer.Addr() + "/ready")
	if err != nil {
		t.Fatalf("GET /ready: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /ready status = %d, want 404", response.StatusCode)
	}

	shutdown()
}

func histogramSampleCount(t *testing.T, gatherer prometheus.Gatherer, name, result string) uint64 {
	t.Helper()
	return metricSample(t, gatherer, name, result).GetHistogram().GetSampleCount()
}

func counterSampleValue(t *testing.T, gatherer prometheus.Gatherer, name, result string) float64 {
	t.Helper()
	return metricSample(t, gatherer, name, result).GetCounter().GetValue()
}

func gaugeSampleValue(t *testing.T, gatherer prometheus.Gatherer, name string) float64 {
	t.Helper()
	return metricSample(t, gatherer, name, "").GetGauge().GetValue()
}

func metricSample(t *testing.T, gatherer prometheus.Gatherer, name, result string) *dto.Metric {
	t.Helper()
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if result == "" || metricLabel(metric, "result") == result {
				return metric
			}
		}
	}
	t.Fatalf("metric %q result=%q not found", name, result)
	return nil
}

func metricLabel(metric *dto.Metric, name string) string {
	for _, label := range metric.GetLabel() {
		if label.GetName() == name {
			return label.GetValue()
		}
	}
	return ""
}
