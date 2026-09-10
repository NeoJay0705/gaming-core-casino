package redis

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestRegisterPoolMetricsContract(t *testing.T) {
	registerer := prometheus.NewRegistry()
	client := &Client{}
	if err := RegisterPoolMetrics(registerer, client); err != nil {
		t.Fatalf("RegisterPoolMetrics() error = %v", err)
	}
	if _, err := registerer.Gather(); err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	if err := RegisterPoolMetrics(registerer, client); err == nil {
		t.Fatal("duplicate RegisterPoolMetrics() error = nil")
	}
}

func TestPoolMetricsContractReadsStartedRedisPool(t *testing.T) {
	server := miniredis.RunT(t)
	client, err := New(testSnapshot(t, "redis:\n  addr: "+server.Addr()+"\n  key_prefix: metrics-test\n  pool_size: 4\n  min_idle_conns: 1\n"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	registry := prometheus.NewRegistry()
	if err := RegisterPoolMetrics(registry, client); err != nil {
		t.Fatalf("RegisterPoolMetrics() error = %v", err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Stop(context.Background()) })
	expected := map[string]dto.MetricType{
		"gaming_core_redis_pool_total_connections":  dto.MetricType_GAUGE,
		"gaming_core_redis_pool_idle_connections":   dto.MetricType_GAUGE,
		"gaming_core_redis_pool_pending_requests":   dto.MetricType_GAUGE,
		"gaming_core_redis_pool_wait_total":         dto.MetricType_COUNTER,
		"gaming_core_redis_pool_wait_seconds_total": dto.MetricType_COUNTER,
		"gaming_core_redis_pool_timeouts_total":     dto.MetricType_COUNTER,
	}
	assertRedisPoolMetricsFamilies(t, registry, expected, true)
	total := redisPoolMetricValue(t, registry, "gaming_core_redis_pool_total_connections")
	idle := redisPoolMetricValue(t, registry, "gaming_core_redis_pool_idle_connections")
	if total < 0 || idle < 0 || total < idle {
		t.Fatalf("pool connection relationship is invalid: total=%v idle=%v", total, idle)
	}
	if err := client.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	assertRedisPoolMetricsFamilies(t, registry, expected, false)
}

func assertRedisPoolMetricsFamilies(t *testing.T, registry *prometheus.Registry, expected map[string]dto.MetricType, wantSamples bool) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	seen := make(map[string]bool, len(expected))
	for _, family := range families {
		metricType, ok := expected[family.GetName()]
		if !ok {
			continue
		}
		seen[family.GetName()] = true
		if family.GetType() != metricType {
			t.Errorf("metric %s type = %s, want %s", family.GetName(), family.GetType(), metricType)
		}
		if wantSamples && len(family.Metric) != 1 {
			t.Errorf("metric %s samples = %d, want 1", family.GetName(), len(family.Metric))
		}
		if !wantSamples && len(family.Metric) != 0 {
			t.Errorf("metric %s retained %d samples after Stop", family.GetName(), len(family.Metric))
		}
	}
	if wantSamples {
		for name := range expected {
			if !seen[name] {
				t.Errorf("metric %s was not gathered while pool was started", name)
			}
		}
	}
}

func redisPoolMetricValue(t *testing.T, registry *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather(%s) error = %v", name, err)
	}
	for _, family := range families {
		if family.GetName() != name || len(family.Metric) == 0 {
			continue
		}
		metric := family.Metric[0]
		switch family.GetType() {
		case dto.MetricType_GAUGE:
			return metric.GetGauge().GetValue()
		case dto.MetricType_COUNTER:
			return metric.GetCounter().GetValue()
		}
	}
	t.Fatalf("metric %s was not gathered", name)
	return 0
}
