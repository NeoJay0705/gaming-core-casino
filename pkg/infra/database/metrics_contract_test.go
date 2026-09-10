package database

import (
	"context"
	"database/sql"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"gorm.io/gorm"
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

func TestPoolMetricsContractReadsStartedSQLPool(t *testing.T) {
	sqlDB := sql.OpenDB(testConnector{})
	client := &Client{
		cfg:  Config{DSN: "test", MaxOpenConns: 4, MaxIdleConns: 2, ConnMaxLifetime: defaultConnMaxLifetime},
		open: func(Config) (*gorm.DB, *sql.DB, error) { return &gorm.DB{}, sqlDB, nil },
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
		"gaming_core_database_pool_max_open_connections": dto.MetricType_GAUGE,
		"gaming_core_database_pool_open_connections":     dto.MetricType_GAUGE,
		"gaming_core_database_pool_in_use_connections":   dto.MetricType_GAUGE,
		"gaming_core_database_pool_idle_connections":     dto.MetricType_GAUGE,
		"gaming_core_database_pool_wait_total":           dto.MetricType_COUNTER,
		"gaming_core_database_pool_wait_seconds_total":   dto.MetricType_COUNTER,
	}
	assertPoolMetricsFamilies(t, registry, expected, true)
	if got := poolMetricValue(t, registry, "gaming_core_database_pool_max_open_connections"); got != 4 {
		t.Fatalf("max open connections = %v, want 4", got)
	}
	open := poolMetricValue(t, registry, "gaming_core_database_pool_open_connections")
	inUse := poolMetricValue(t, registry, "gaming_core_database_pool_in_use_connections")
	idle := poolMetricValue(t, registry, "gaming_core_database_pool_idle_connections")
	if open != inUse+idle || inUse < 0 || idle < 0 {
		t.Fatalf("pool connection relationship is invalid: open=%v in_use=%v idle=%v", open, inUse, idle)
	}
	if err := client.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	assertPoolMetricsFamilies(t, registry, expected, false)
}

func assertPoolMetricsFamilies(t *testing.T, registry *prometheus.Registry, expected map[string]dto.MetricType, wantSamples bool) {
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

func poolMetricValue(t *testing.T, registry *prometheus.Registry, name string) float64 {
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
