package localmq

import "github.com/prometheus/client_golang/prometheus"

type clientMetrics struct {
	publishTotal        *prometheus.CounterVec
	publishLatency      *prometheus.HistogramVec
	queueDepth          *prometheus.GaugeVec
	groupCommitMessages *prometheus.HistogramVec
	groupCommitBytes    *prometheus.HistogramVec
	walBytes            *prometheus.GaugeVec
	filesystemFree      prometheus.Gauge
	consumerLag         *prometheus.GaugeVec
	oldestAge           *prometheus.GaugeVec
	rebalanceTotal      *prometheus.CounterVec
	checkpointFailures  *prometheus.CounterVec
	blockedLanes        *prometheus.GaugeVec
	gcBytes             *prometheus.CounterVec
	skippedMessages     *prometheus.CounterVec
	skippedBytes        *prometheus.CounterVec
}

func newClientMetrics() *clientMetrics {
	return &clientMetrics{
		publishTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "localmq_publish_total", Help: "Total localmq publish attempts.",
		}, []string{"topic", "result"}),
		publishLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "localmq_publish_latency_seconds", Help: "Localmq publish latency.",
		}, []string{"topic"}),
		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "localmq_publish_queue_depth", Help: "Current bounded localmq publish queue depth.",
		}, []string{"topic"}),
		groupCommitMessages: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "localmq_group_commit_messages", Help: "Messages per localmq group commit.",
		}, []string{"topic"}),
		groupCommitBytes: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "localmq_group_commit_bytes", Help: "Bytes per localmq group commit.",
		}, []string{"topic"}),
		walBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "localmq_wal_bytes", Help: "Approximate localmq WAL bytes by topic.",
		}, []string{"topic"}),
		filesystemFree: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "localmq_filesystem_free_bytes", Help: "Filesystem free bytes visible to localmq.",
		}),
		consumerLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "localmq_consumer_lag_messages", Help: "Approximate localmq consumer lag.",
		}, []string{"topic", "group"}),
		oldestAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "localmq_oldest_unconsumed_age_seconds", Help: "Age of the oldest unconsumed localmq record.",
		}, []string{"topic", "group"}),
		rebalanceTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "localmq_rebalance_total", Help: "Localmq consumer ownership reconciliations.",
		}, []string{"topic", "group"}),
		checkpointFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "localmq_checkpoint_publish_failures_total", Help: "Localmq checkpoint publication failures.",
		}, []string{"topic", "group"}),
		blockedLanes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "localmq_blocked_lanes", Help: "Number of localmq lanes currently blocked.",
		}, []string{"topic", "group"}),
		gcBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "localmq_gc_bytes_total", Help: "Bytes logically and physically reclaimed by localmq GC.",
		}, []string{"topic", "reason"}),
		skippedMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "localmq_best_effort_skipped_messages_total", Help: "Best-effort messages skipped after retention passed their checkpoint.",
		}, []string{"topic", "group"}),
		skippedBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "localmq_best_effort_skipped_bytes_total", Help: "Best-effort payload bytes skipped after retention passed their checkpoint.",
		}, []string{"topic", "group"}),
	}
}

func (m *clientMetrics) Describe(ch chan<- *prometheus.Desc) {
	for _, collector := range m.collectors() {
		collector.Describe(ch)
	}
}

func (m *clientMetrics) Collect(ch chan<- prometheus.Metric) {
	for _, collector := range m.collectors() {
		collector.Collect(ch)
	}
}

func (m *clientMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.publishTotal, m.publishLatency, m.queueDepth, m.groupCommitMessages,
		m.groupCommitBytes, m.walBytes, m.filesystemFree, m.consumerLag,
		m.oldestAge, m.rebalanceTotal, m.checkpointFailures, m.blockedLanes,
		m.gcBytes, m.skippedMessages, m.skippedBytes,
	}
}
