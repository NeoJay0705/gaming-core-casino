package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	loadResultSuccess   = "success"
	loadResultError     = "error"
	loadResultCancelled = "cancelled"

	pushResultReceived    = "received"
	pushResultDuplicate   = "duplicate"
	pushResultSequenceGap = "sequence_gap"
	pushResultInvalid     = "invalid"
)

// roundTripDurationBuckets 覆蓋壓測端觀察到的微秒至秒級 round-trip，
// 避免 DefBuckets 的 5ms 起點掩蓋低延遲分布。
var roundTripDurationBuckets = []float64{
	0.0001, 0.00025, 0.0005,
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
	0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// loadMetrics 使用壓測端自己的 registry，觀測 Echo round trip 與非同步 push
// delivery；不與 framework product registry 混用，也不把 connection 或
// request identity 放入 labels。
type loadMetrics struct {
	echoRoundTrips         *prometheus.CounterVec
	echoRoundTripDuration  *prometheus.HistogramVec
	echoRoundTripsInFlight prometheus.Gauge
	pushMessages           *prometheus.CounterVec
	pushDeliveryDuration   *prometheus.HistogramVec
	pushReaders            prometheus.Gauge
}

func newLoadMetrics(registerer prometheus.Registerer) (*loadMetrics, error) {
	if registerer == nil {
		return nil, errors.New("load metrics: registerer is nil")
	}
	metrics := &loadMetrics{
		echoRoundTrips: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gaming_core_example_load_echo_round_trips_total",
			Help: "Total number of load-client Echo round trips by terminal result.",
		}, []string{"result"}),
		echoRoundTripDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gaming_core_example_load_echo_round_trip_duration_seconds",
			Help:    "Load-client Echo end-to-end round-trip duration in seconds.",
			Buckets: roundTripDurationBuckets,
		}, []string{"result"}),
		echoRoundTripsInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gaming_core_example_load_echo_round_trips_in_flight",
			Help: "Current number of load-client Echo round trips awaiting a terminal result.",
		}),
		pushMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gaming_core_example_load_push_messages_total",
			Help: "Total number of asynchronous push messages observed by terminal result and mode.",
		}, []string{"mode", "result"}),
		pushDeliveryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gaming_core_example_load_push_delivery_duration_seconds",
			Help:    "End-to-end duration from Game push timestamp to load-client frame read in seconds.",
			Buckets: roundTripDurationBuckets,
		}, []string{"mode"}),
		pushReaders: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gaming_core_example_load_push_readers",
			Help: "Current number of load-client WebSocket push reader goroutines.",
		}),
	}
	for _, collector := range []prometheus.Collector{
		metrics.echoRoundTrips,
		metrics.echoRoundTripDuration,
		metrics.echoRoundTripsInFlight,
		metrics.pushMessages,
		metrics.pushDeliveryDuration,
		metrics.pushReaders,
	} {
		if err := registerer.Register(collector); err != nil {
			return nil, fmt.Errorf("register load metrics: %w", err)
		}
	}
	return metrics, nil
}

func (m *loadMetrics) startPushReader() {
	if m != nil {
		m.pushReaders.Inc()
	}
}

func (m *loadMetrics) finishPushReader() {
	if m != nil {
		m.pushReaders.Dec()
	}
}

func (m *loadMetrics) observePush(mode, result string, elapsed time.Duration) {
	if m == nil {
		return
	}
	m.pushMessages.WithLabelValues(mode, result).Inc()
	if result != pushResultInvalid && elapsed >= 0 {
		m.pushDeliveryDuration.WithLabelValues(mode).Observe(elapsed.Seconds())
	}
}

func (m *loadMetrics) startEcho() {
	if m != nil {
		m.echoRoundTripsInFlight.Inc()
	}
}

func (m *loadMetrics) finishEcho(ctx context.Context, err error, elapsed time.Duration) {
	if m == nil {
		return
	}
	result := loadResultSuccess
	if err != nil {
		result = loadResultError
		if isExpectedLoadTermination(ctx, err) {
			result = loadResultCancelled
		}
	}
	m.echoRoundTripsInFlight.Dec()
	m.echoRoundTrips.WithLabelValues(result).Inc()
	m.echoRoundTripDuration.WithLabelValues(result).Observe(elapsed.Seconds())
}

type loadObserver struct {
	metrics  *loadMetrics
	server   *http.Server
	listener net.Listener
	done     chan error
}

func newLoadObserver(listenAddr string) (*loadObserver, error) {
	if strings.TrimSpace(listenAddr) == "" {
		return nil, errors.New("load metrics: listen address is required")
	}
	registry := prometheus.NewRegistry()
	for _, collector := range []prometheus.Collector{
		collectors.NewGoCollector(
			collectors.WithGoCollectorRuntimeMetrics(
				collectors.GoRuntimeMetricsRule{
					Matcher: regexp.MustCompile(`^/sched/latencies:seconds$`),
				},
			),
		),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	} {
		if err := registry.Register(collector); err != nil {
			return nil, fmt.Errorf("register load runtime collector: %w", err)
		}
	}
	metrics, err := newLoadMetrics(registry)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen for load metrics: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	observer := &loadObserver{
		metrics:  metrics,
		server:   &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second},
		listener: listener,
		done:     make(chan error, 1),
	}
	go func() {
		err := observer.server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		observer.done <- err
	}()
	return observer, nil
}

func (o *loadObserver) Shutdown(ctx context.Context) error {
	if o == nil || o.server == nil {
		return nil
	}
	shutdownErr := o.server.Shutdown(ctx)
	if shutdownErr != nil {
		shutdownErr = errors.Join(shutdownErr, o.server.Close())
	}
	return errors.Join(shutdownErr, <-o.done)
}

func (o *loadObserver) Addr() string {
	if o == nil || o.listener == nil {
		return ""
	}
	return o.listener.Addr().String()
}
