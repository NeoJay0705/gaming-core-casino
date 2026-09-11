package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	grpcLoadResultSuccess   = "success"
	grpcLoadResultError     = "error"
	grpcLoadResultCancelled = "cancelled"
)

// grpcLoadRoundTripDurationBuckets 與既有 Echo request/E2E histogram 共用
// 相同的邊界，讓 direct gRPC 與 WebSocket game/local 結果可直接閱讀。
var grpcLoadRoundTripDurationBuckets = []float64{
	0.0001, 0.00025, 0.0005,
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
	0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

type grpcLoadMetrics struct {
	roundTrips         *prometheus.CounterVec
	roundTripDuration  *prometheus.HistogramVec
	roundTripsInFlight prometheus.Gauge
}

func newGRPCLoadMetrics(registerer prometheus.Registerer) (*grpcLoadMetrics, error) {
	if registerer == nil {
		return nil, errors.New("grpc load metrics: registerer is nil")
	}
	metrics := &grpcLoadMetrics{
		roundTrips: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gaming_core_example_grpc_load_round_trips_total",
			Help: "Total number of direct gRPC Echo round trips by terminal result.",
		}, []string{"result"}),
		roundTripDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gaming_core_example_grpc_load_round_trip_duration_seconds",
			Help:    "Direct gRPC Echo round-trip duration in seconds.",
			Buckets: grpcLoadRoundTripDurationBuckets,
		}, []string{"result"}),
		roundTripsInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gaming_core_example_grpc_load_round_trips_in_flight",
			Help: "Current number of direct gRPC Echo round trips awaiting a terminal result.",
		}),
	}
	for _, collector := range []prometheus.Collector{
		metrics.roundTrips,
		metrics.roundTripDuration,
		metrics.roundTripsInFlight,
	} {
		if err := registerer.Register(collector); err != nil {
			return nil, fmt.Errorf("register gRPC load metric: %w", err)
		}
	}
	return metrics, nil
}

func (m *grpcLoadMetrics) startRoundTrip() {
	if m != nil {
		m.roundTripsInFlight.Inc()
	}
}

func (m *grpcLoadMetrics) finishRoundTrip(ctx context.Context, err error, elapsed time.Duration) {
	if m == nil {
		return
	}
	result := grpcLoadResultSuccess
	if err != nil {
		result = grpcLoadResultError
		if isExpectedGRPCLoadTermination(ctx, err) {
			result = grpcLoadResultCancelled
		}
	}
	m.roundTripsInFlight.Dec()
	m.roundTrips.WithLabelValues(result).Inc()
	m.roundTripDuration.WithLabelValues(result).Observe(elapsed.Seconds())
}

func isExpectedGRPCLoadTermination(ctx context.Context, err error) bool {
	if ctx == nil || err == nil {
		return false
	}
	return errors.Is(ctx.Err(), context.Canceled)
}

type grpcLoadObserver struct {
	metrics  *grpcLoadMetrics
	server   *http.Server
	listener net.Listener
	done     chan error

	shutdownOnce sync.Once
	shutdownDone chan struct{}
	shutdownErr  error
}

func newGRPCLoadObserver(listenAddr string) (*grpcLoadObserver, error) {
	if strings.TrimSpace(listenAddr) == "" {
		return nil, errors.New("grpc load metrics: listen address is required")
	}
	registry := prometheus.NewRegistry()
	for _, collector := range []prometheus.Collector{
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	} {
		if err := registry.Register(collector); err != nil {
			return nil, fmt.Errorf("register gRPC load runtime collector: %w", err)
		}
	}
	metrics, err := newGRPCLoadMetrics(registry)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen for gRPC load metrics: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	observer := &grpcLoadObserver{
		metrics:      metrics,
		server:       &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second},
		listener:     listener,
		done:         make(chan error, 1),
		shutdownDone: make(chan struct{}),
	}
	go func() {
		serveErr := observer.server.Serve(listener)
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		observer.done <- serveErr
	}()
	return observer, nil
}

// Shutdown 優雅停止 private metrics listener；操作具 idempotent 特性且允許 nil receiver。
func (o *grpcLoadObserver) Shutdown(ctx context.Context) error {
	if o == nil || o.server == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	o.shutdownOnce.Do(func() {
		shutdownErr := o.server.Shutdown(ctx)
		if shutdownErr != nil {
			shutdownErr = errors.Join(shutdownErr, o.server.Close())
		}
		if o.listener != nil {
			if closeErr := o.listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
				shutdownErr = errors.Join(shutdownErr, closeErr)
			}
		}
		serveErr := <-o.done
		o.shutdownErr = errors.Join(shutdownErr, serveErr)
		close(o.shutdownDone)
	})
	<-o.shutdownDone
	return o.shutdownErr
}

func (o *grpcLoadObserver) Addr() string {
	if o == nil || o.listener == nil {
		return ""
	}
	return o.listener.Addr().String()
}
