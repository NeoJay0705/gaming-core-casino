package redis

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// RegisterPoolMetrics 將 go-redis pool snapshot 暴露為固定名稱的
// Prometheus metrics。collector 只讀取 PoolStats，不改變 Redis lifecycle。
func RegisterPoolMetrics(registerer prometheus.Registerer, client *Client) error {
	if registerer == nil {
		return errors.New("redis metrics: registerer is nil")
	}
	if client == nil {
		return errors.New("redis metrics: client is nil")
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
			prometheus.NewDesc("gaming_core_redis_pool_total_connections", "Current total number of Redis pool connections.", nil, nil),
			prometheus.NewDesc("gaming_core_redis_pool_idle_connections", "Current number of idle Redis pool connections.", nil, nil),
			prometheus.NewDesc("gaming_core_redis_pool_pending_requests", "Current number of requests pending for Redis pool connections.", nil, nil),
			prometheus.NewDesc("gaming_core_redis_pool_wait_total", "Total number of waits for a Redis pool connection.", nil, nil),
			prometheus.NewDesc("gaming_core_redis_pool_wait_seconds_total", "Total time spent waiting for a Redis pool connection in seconds.", nil, nil),
			prometheus.NewDesc("gaming_core_redis_pool_timeouts_total", "Total number of Redis pool timeouts.", nil, nil),
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
	client := c.client.client
	c.client.mu.RUnlock()
	if client == nil {
		return
	}
	stats := client.PoolStats()
	ch <- prometheus.MustNewConstMetric(c.descs[0], prometheus.GaugeValue, float64(stats.TotalConns))
	ch <- prometheus.MustNewConstMetric(c.descs[1], prometheus.GaugeValue, float64(stats.IdleConns))
	ch <- prometheus.MustNewConstMetric(c.descs[2], prometheus.GaugeValue, float64(stats.PendingRequests))
	ch <- prometheus.MustNewConstMetric(c.descs[3], prometheus.CounterValue, float64(stats.WaitCount))
	ch <- prometheus.MustNewConstMetric(c.descs[4], prometheus.CounterValue, float64(stats.WaitDurationNs)/1e9)
	ch <- prometheus.MustNewConstMetric(c.descs[5], prometheus.CounterValue, float64(stats.Timeouts))
}
