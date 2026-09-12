package observability

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/logging"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	healthPath     = "/health"
	readinessPath  = "/ready"
	metricsPath    = "/metrics"
	healthBody     = "ok\n"
	readyBody      = "ready\n"
	notReadyBody   = "not ready\n"
	probeContent   = "text/plain; charset=utf-8"
	readHeaderTime = 5 * time.Second
	idleTimeout    = 60 * time.Second
)

// httpServer 是由 framework lifecycle 管理的共用 HTTP server。
type httpServer struct {
	cfg Config

	server *http.Server

	mu       sync.Mutex
	listener net.Listener
	started  bool
	stopped  bool
	ready    atomic.Bool
	logger   *logging.Logger
}

func newHTTPServer(cfg Config, owner *registryOwner) (*httpServer, error) {
	return newHTTPServerWithLogger(cfg, owner, nil)
}

func newHTTPServerWithLogger(cfg Config, owner *registryOwner, factory *logging.Factory) (*httpServer, error) {
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	if owner == nil || owner.registry == nil {
		return nil, fmt.Errorf("observability registry is nil")
	}

	result := &httpServer{cfg: cfg}
	if factory != nil {
		logger, err := factory.Component("observability.http")
		if err != nil {
			return nil, err
		}
		result.logger = logger
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", result.handleHealth)
	mux.HandleFunc("GET /ready", result.handleReadiness)
	mux.Handle("GET /metrics", promhttp.HandlerFor(owner.registry, promhttp.HandlerOpts{}))
	result.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTime,
		IdleTimeout:       idleTimeout,
	}
	return result, nil
}

func (s *httpServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeProbe(w, http.StatusOK, healthBody)
}

func (s *httpServer) handleReadiness(w http.ResponseWriter, _ *http.Request) {
	if s.ready.Load() {
		writeProbe(w, http.StatusOK, readyBody)
		return
	}
	writeProbe(w, http.StatusServiceUnavailable, notReadyBody)
}

func writeProbe(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", probeContent)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

// Start 先同步 bind listener，再啟動 HTTP serve goroutine。
func (s *httpServer) Start(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("observability HTTP server is nil")
	}
	if ctx == nil {
		return fmt.Errorf("observability HTTP server context is nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("observability HTTP server start cancelled: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return fmt.Errorf("observability HTTP server is stopped")
	}
	if s.started {
		return fmt.Errorf("observability HTTP server is already started")
	}
	listener, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("observability HTTP server listen on %s: %w", s.cfg.ListenAddr, err)
	}
	if err := ctx.Err(); err != nil {
		_ = listener.Close()
		return fmt.Errorf("observability HTTP server start cancelled: %w", err)
	}
	s.listener = listener
	s.started = true
	server := s.server
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			if s.logger != nil {
				s.logger.Error(context.Background(), "serve", "observability HTTP server stopped unexpectedly", err,
					slog.String("addr", listener.Addr().String()))
			}
		}
	}()
	return nil
}

// Stop graceful shutdown HTTP server；deadline 到期時強制關閉剩餘連線。
func (s *httpServer) Stop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	s.mu.Lock()
	if !s.started {
		s.stopped = true
		s.mu.Unlock()
		return nil
	}
	server := s.server
	listener := s.listener
	s.started = false
	s.stopped = true
	s.listener = nil
	s.mu.Unlock()

	shutdownErr := server.Shutdown(ctx)
	listenerErr := closeListener(listener)
	if shutdownErr == nil && listenerErr == nil {
		return nil
	}
	closeErr := server.Close()
	return errors.Join(shutdownErr, listenerErr, closeErr)
}

func closeListener(listener net.Listener) error {
	if listener == nil {
		return nil
	}
	if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}

func (s *httpServer) addr() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *httpServer) setReady(ready bool) {
	if s != nil {
		s.ready.Store(ready)
	}
}

var _ framework.ManagedResource = (*httpServer)(nil)
