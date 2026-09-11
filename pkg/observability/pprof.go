package observability

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/NeoJay0705/gaming-core-casino/internal/profilehttp"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
)

// pprofServer 將共用 loopback pprof helper 接入 framework lifecycle；它不會
// 把 profiling endpoint 加入既有 health/readiness/metrics listener。
type pprofServer struct {
	listenAddr string

	mu      sync.Mutex
	server  *profilehttp.Server
	started bool
	stopped bool
}

func newPprofServer(cfg Config) (*pprofServer, error) {
	listenAddr := strings.TrimSpace(cfg.PprofListenAddr)
	if listenAddr != "" {
		if err := profilehttp.ValidateLoopbackAddress(listenAddr); err != nil {
			return nil, fmt.Errorf("observability pprof: %w", err)
		}
	}
	return &pprofServer{listenAddr: listenAddr}, nil
}

// Start 啟動 optional pprof listener；空 address 時保持 disabled。
func (s *pprofServer) Start(ctx context.Context) error {
	if s == nil {
		return errors.New("observability pprof server is nil")
	}
	if ctx == nil {
		return errors.New("observability pprof server context is nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("observability pprof server start cancelled: %w", err)
	}

	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return errors.New("observability pprof server is stopped")
	}
	if s.started {
		s.mu.Unlock()
		return errors.New("observability pprof server is already started")
	}
	s.started = true
	listenAddr := s.listenAddr

	server, err := profilehttp.Start(listenAddr)
	if err != nil {
		s.started = false
		s.stopped = true
		s.mu.Unlock()
		return fmt.Errorf("observability pprof server start: %w", err)
	}
	if err := ctx.Err(); err != nil {
		shutdownErr := server.Shutdown(context.Background())
		s.started = false
		s.stopped = true
		s.mu.Unlock()
		return errors.Join(fmt.Errorf("observability pprof server start cancelled: %w", err), shutdownErr)
	}
	s.server = server
	s.mu.Unlock()
	return nil
}

// Stop 停止 optional pprof listener；操作具 idempotent 特性。
func (s *pprofServer) Stop(ctx context.Context) error {
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
	s.server = nil
	s.started = false
	s.stopped = true
	s.mu.Unlock()
	return server.Shutdown(ctx)
}

func (s *pprofServer) addr() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	server := s.server
	s.mu.Unlock()
	return serverAddr(server)
}

func serverAddr(server *profilehttp.Server) string {
	if server == nil {
		return ""
	}
	return server.Addr()
}

var _ framework.ManagedResource = (*pprofServer)(nil)
