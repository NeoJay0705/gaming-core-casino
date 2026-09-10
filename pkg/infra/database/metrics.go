package database

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// RegisterPoolMetrics 將 database/sql pool snapshot 暴露為固定名稱的
// Prometheus metrics。collector 不持有連線，未啟動或已停止時只回傳空
// snapshot，避免 metrics scrape 改變 lifecycle 或觸發 I/O。
func RegisterPoolMetrics(registerer prometheus.Registerer, client *Client) error {
	if registerer == nil {
		return errors.New("database metrics: registerer is nil")
	}
	if client == nil {
		return errors.New("database metrics: client is nil")
	}
	return registerer.Register(newPoolCollector(client))
}

type poolCollector struct {
	client *Client
	descs  []*prometheus.Desc
}

func newPoolCollector(client *Client) *poolCollector {
	return &poolCollector{
		client: client,
		descs: []*prometheus.Desc{
			prometheus.NewDesc("gaming_core_database_pool_max_open_connections", "Configured maximum number of open database connections.", nil, nil),
			prometheus.NewDesc("gaming_core_database_pool_open_connections", "Current number of open database connections.", nil, nil),
			prometheus.NewDesc("gaming_core_database_pool_in_use_connections", "Current number of database connections in use.", nil, nil),
			prometheus.NewDesc("gaming_core_database_pool_idle_connections", "Current number of idle database connections.", nil, nil),
			prometheus.NewDesc("gaming_core_database_pool_wait_total", "Total number of waits for a database connection.", nil, nil),
			prometheus.NewDesc("gaming_core_database_pool_wait_seconds_total", "Total time spent waiting for a database connection in seconds.", nil, nil),
		},
	}
}

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	if c == nil {
		return
	}
	for _, desc := range c.descs {
		ch <- desc
	}
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	if c == nil || c.client == nil {
		return
	}
	c.client.mu.RLock()
	sqlDB := c.client.sqlDB
	c.client.mu.RUnlock()
	if sqlDB == nil {
		return
	}
	stats := sqlDB.Stats()
	ch <- prometheus.MustNewConstMetric(c.descs[0], prometheus.GaugeValue, float64(stats.MaxOpenConnections))
	ch <- prometheus.MustNewConstMetric(c.descs[1], prometheus.GaugeValue, float64(stats.OpenConnections))
	ch <- prometheus.MustNewConstMetric(c.descs[2], prometheus.GaugeValue, float64(stats.InUse))
	ch <- prometheus.MustNewConstMetric(c.descs[3], prometheus.GaugeValue, float64(stats.Idle))
	ch <- prometheus.MustNewConstMetric(c.descs[4], prometheus.CounterValue, float64(stats.WaitCount))
	ch <- prometheus.MustNewConstMetric(c.descs[5], prometheus.CounterValue, stats.WaitDuration.Seconds())
}
