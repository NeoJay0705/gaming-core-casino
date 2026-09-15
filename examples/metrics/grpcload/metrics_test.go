package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestGRPCLoadMetricsRecordsTerminalResults(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := newGRPCLoadMetrics(registry)
	if err != nil {
		t.Fatalf("newGRPCLoadMetrics() error = %v", err)
	}

	metrics.startRoundTrip()
	metrics.finishRoundTrip(context.Background(), nil, time.Millisecond)
	metrics.startRoundTrip()
	metrics.finishRoundTrip(context.Background(), errors.New("rpc failed"), 2*time.Millisecond)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	metrics.startRoundTrip()
	metrics.finishRoundTrip(cancelled, context.Canceled, 3*time.Millisecond)

	for _, result := range []string{grpcLoadResultSuccess, grpcLoadResultError, grpcLoadResultCancelled} {
		metric := findMetric(t, registry, "gaming_core_example_grpc_load_round_trips_total", result)
		if got := metric.GetCounter().GetValue(); got != 1 {
			t.Fatalf("round trips result=%q = %v, want 1", result, got)
		}
		metric = findMetric(t, registry, "gaming_core_example_grpc_load_round_trip_duration_seconds", result)
		if got := metric.GetHistogram().GetSampleCount(); got != 1 {
			t.Fatalf("duration count result=%q = %d, want 1", result, got)
		}
	}
	metric := findMetric(t, registry, "gaming_core_example_grpc_load_round_trips_in_flight", "")
	if got := metric.GetGauge().GetValue(); got != 0 {
		t.Fatalf("in-flight = %v, want 0", got)
	}
}

func TestGRPCLoadMetricsRejectsNilAndDuplicateRegisterer(t *testing.T) {
	if _, err := newGRPCLoadMetrics(nil); err == nil {
		t.Fatal("newGRPCLoadMetrics(nil) error = nil")
	}
	registry := prometheus.NewRegistry()
	if _, err := newGRPCLoadMetrics(registry); err != nil {
		t.Fatalf("first newGRPCLoadMetrics() error = %v", err)
	}
	if _, err := newGRPCLoadMetrics(registry); err == nil {
		t.Fatal("duplicate newGRPCLoadMetrics() error = nil")
	}
}

func TestGRPCLoadMetricsUseBoundedBuckets(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := newGRPCLoadMetrics(registry)
	if err != nil {
		t.Fatalf("newGRPCLoadMetrics() error = %v", err)
	}
	metrics.startRoundTrip()
	metrics.finishRoundTrip(context.Background(), nil, time.Millisecond)
	metric := findMetric(t, registry, "gaming_core_example_grpc_load_round_trip_duration_seconds", grpcLoadResultSuccess)
	got := make([]float64, 0, len(metric.GetHistogram().GetBucket()))
	for _, bucket := range metric.GetHistogram().GetBucket() {
		got = append(got, bucket.GetUpperBound())
	}
	if !reflect.DeepEqual(got, grpcLoadRoundTripDurationBuckets) {
		t.Fatalf("duration buckets = %v, want %v", got, grpcLoadRoundTripDurationBuckets)
	}
}

func TestGRPCLoadObserverServesOnlyMetricsAndShutsDown(t *testing.T) {
	observer, err := newGRPCLoadObserver("127.0.0.1:0")
	if err != nil {
		t.Fatalf("newGRPCLoadObserver() error = %v", err)
	}
	observer.metrics.startRoundTrip()
	observer.metrics.finishRoundTrip(context.Background(), nil, time.Millisecond)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = observer.Shutdown(ctx)
	})

	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://" + observer.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("GET /metrics body: read=%v close=%v", readErr, closeErr)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want %d; body=%q", response.StatusCode, http.StatusOK, body)
	}
	if !strings.Contains(string(body), `gaming_core_example_grpc_load_round_trips_total{result="success"} 1`) {
		t.Fatalf("metrics body does not contain successful round trip:\n%s", body)
	}
	if !strings.Contains(string(body), "go_goroutines ") {
		t.Fatalf("metrics body does not contain Go runtime metrics:\n%s", body)
	}
	if !strings.Contains(string(body), "go_sched_latencies_seconds_bucket") {
		t.Fatalf("metrics body does not contain scheduler latency metrics:\n%s", body)
	}

	response, err = client.Get("http://" + observer.Addr() + "/ready")
	if err != nil {
		t.Fatalf("GET /ready: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /ready status = %d, want %d", response.StatusCode, http.StatusNotFound)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := observer.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if err := observer.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown() error = %v", err)
	}
}

func findMetric(t *testing.T, gatherer prometheus.Gatherer, name, result string) *dto.Metric {
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
