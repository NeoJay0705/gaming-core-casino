package gameproduct

import (
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestHandlerDurationBucketsContract(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := newGameMetrics(registry)
	if err != nil {
		t.Fatalf("newGameMetrics() error = %v", err)
	}
	metrics.observeGateCommand("unknown", "error", time.Millisecond)

	want := []float64{
		0.000001, 0.0000025, 0.000005, 0.00001,
		0.000025, 0.00005, 0.0001, 0.00025, 0.0005,
		0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
		0.1, 0.25, 0.5, 1, 2.5, 5,
	}
	metric := gatheredGameMetric(t, registry, "gaming_core_game_gate_command_duration_seconds", map[string]string{
		"command": "unknown", "result": "error",
	})
	got := make([]float64, 0, len(metric.GetHistogram().GetBucket()))
	for _, bucket := range metric.GetHistogram().GetBucket() {
		got = append(got, bucket.GetUpperBound())
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("handler duration metric buckets = %v, want %v", got, want)
	}
}

func gatheredGameMetric(t *testing.T, registry *prometheus.Registry, name string, wantLabels map[string]string) *dto.Metric {
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
			if reflect.DeepEqual(labels, wantLabels) {
				return metric
			}
		}
	}
	t.Fatalf("metric %q with labels %#v was not gathered", name, wantLabels)
	return nil
}
