package main

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRoundTripDurationBucketsContract(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := newLoadMetrics(registry)
	if err != nil {
		t.Fatalf("newLoadMetrics() error = %v", err)
	}
	metrics.startEcho()
	metrics.finishEcho(context.Background(), nil, time.Millisecond)

	want := []float64{
		0.0001, 0.00025, 0.0005,
		0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
		0.1, 0.25, 0.5, 1, 2.5, 5, 10,
	}
	metric := metricSample(t, registry, "gaming_core_example_load_echo_round_trip_duration_seconds", loadResultSuccess)
	got := make([]float64, 0, len(metric.GetHistogram().GetBucket()))
	for _, bucket := range metric.GetHistogram().GetBucket() {
		got = append(got, bucket.GetUpperBound())
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round-trip metric buckets = %v, want %v", got, want)
	}
}
