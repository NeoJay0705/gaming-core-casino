// Package grpcserver owns one product-level gRPC listener.
//
// Business packages register generated services on this server; this package
// deliberately does not know any service-specific request or routing type.
package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/NeoJay0705/gaming-core-casino/pkg/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Config configures one product-level gRPC server.
type Config struct {
	ListenAddr string `config:"listen_addr" yaml:"listen_addr"`
	// MaxConcurrentStreams 為正值時限制每條 gRPC transport 的並行 stream；
	// 0 保留 grpc-go default。
	MaxConcurrentStreams uint32 `config:"max_concurrent_streams" yaml:"max_concurrent_streams"`
	// WriteBufferSizeBytes 為正值時設定每條 gRPC transport 的 write buffer；
	// 0 不傳入 option，保留 grpc-go default。
	WriteBufferSizeBytes int `config:"write_buffer_size_bytes" yaml:"write_buffer_size_bytes"`
	// StreamWorkers 為正值時啟用 grpc-go reusable stream workers；0 保留
	// grpc-go default。此 option 只降低 goroutine stack churn，不限制並行 RPC。
	StreamWorkers uint32 `config:"stream_workers" yaml:"stream_workers"`
}

// Server owns one listener and one grpc.Server. Services must be registered
// during application composition, before Start is called.
type Server struct {
	cfg    Config
	logger *logging.Logger

	mu         sync.Mutex
	server     *grpc.Server
	listener   net.Listener
	registered map[string]struct{}
	started    bool
	stopped    bool
}

// New validates config without binding a port.
func New(cfg Config) (*Server, error) {
	return newServer(cfg, nil)
}

// NewWithLogger 建立帶有 component-scoped structured logger 的 gRPC server。
// logger 為 nil 時仍可供 low-level contract tests 使用，但 production
// product 應由 logging.Factory 注入 logger。
func NewWithLogger(cfg Config, logger *logging.Logger) (*Server, error) {
	return newServer(cfg, logger)
}

func newServer(cfg Config, logger *logging.Logger) (*Server, error) {
	cfg.ListenAddr = strings.TrimSpace(cfg.ListenAddr)
	if cfg.ListenAddr == "" {
		return nil, errors.New("grpc server: listen address is required")
	}
	if _, _, err := net.SplitHostPort(cfg.ListenAddr); err != nil {
		return nil, fmt.Errorf("grpc server: invalid listen address %q: %w", cfg.ListenAddr, err)
	}
	if _, err := net.ResolveTCPAddr("tcp", cfg.ListenAddr); err != nil {
		return nil, fmt.Errorf("grpc server: invalid listen address %q: %w", cfg.ListenAddr, err)
	}
	if cfg.WriteBufferSizeBytes < 0 {
		return nil, errors.New("grpc server: write_buffer_size_bytes must not be negative")
	}
	serverOptions := make([]grpc.ServerOption, 0, 4)
	if cfg.MaxConcurrentStreams > 0 {
		serverOptions = append(serverOptions, grpc.MaxConcurrentStreams(cfg.MaxConcurrentStreams))
	}
	if cfg.WriteBufferSizeBytes > 0 {
		serverOptions = append(serverOptions, grpc.WriteBufferSize(cfg.WriteBufferSizeBytes))
	}
	if cfg.StreamWorkers > 0 {
		serverOptions = append(serverOptions, grpc.NumStreamWorkers(cfg.StreamWorkers))
	}
	serverOptions = append(serverOptions, grpc.ChainUnaryInterceptor(
		logging.UnaryServerInterceptor(),
		recoveryInterceptor(logger),
	))
	return &Server{
		cfg:        cfg,
		logger:     logger,
		server:     grpc.NewServer(serverOptions...),
		registered: make(map[string]struct{}),
	}, nil
}

// Register invokes one generated protobuf registration function before the
// server starts. serviceName is tracked to turn duplicate registrations into
// composition errors rather than grpc.Server fatal logging.
func (s *Server) Register(serviceName string, register func(grpc.ServiceRegistrar)) error {
	if s == nil {
		return errors.New("grpc server: server is nil")
	}
	serviceName = strings.TrimSpace(serviceName)
	if serviceName == "" {
		return errors.New("grpc server: service name is required")
	}
	if register == nil {
		return fmt.Errorf("grpc server: service %q registration function is nil", serviceName)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return errors.New("grpc server: server is stopped")
	}
	if s.started {
		return errors.New("grpc server: service registration is closed")
	}
	if _, exists := s.registered[serviceName]; exists {
		return fmt.Errorf("grpc server: service %q is already registered", serviceName)
	}
	s.registered[serviceName] = struct{}{}
	// Keep the state lock while invoking the generated registration callback so
	// a concurrent Start cannot observe a half-composed server.
	register(s.server)
	return nil
}

// Start binds the listener synchronously, then serves requests in a goroutine.
func (s *Server) Start(ctx context.Context) error {
	if s == nil {
		return errors.New("grpc server: server is nil")
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return errors.New("grpc server: server is stopped")
	}
	if s.started {
		return errors.New("grpc server: server is already started")
	}
	listener, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("grpc server: listen on %s: %w", s.cfg.ListenAddr, err)
	}
	s.listener = listener
	s.started = true
	server := s.server
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			if s.logger != nil {
				s.logger.Error(context.Background(), "serve", "gRPC server stopped unexpectedly", err,
					slog.String("addr", listener.Addr().String()))
			}
		}
	}()
	return nil
}

// Stop gracefully drains active RPCs until ctx expires, then force-stops the
// server. Stop is idempotent and makes Start-after-Stop fail.
func (s *Server) Stop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	server := s.server
	s.listener = nil
	started := s.started
	s.mu.Unlock()
	if !started {
		return nil
	}

	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		server.Stop()
		return ctx.Err()
	}
}

// Addr returns the actual bound address, including an ephemeral port selected
// through ListenAddr ending in :0.
func (s *Server) Addr() string {
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

func recoveryInterceptor(logger *logging.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				if logger != nil {
					logger.Error(ctx, info.FullMethod, "gRPC unary handler panicked", fmt.Errorf("%v", recovered),
						slog.String("method", info.FullMethod),
						slog.String("stack", string(debug.Stack())))
				}
				response = nil
				err = status.Error(codes.Internal, "gRPC unary handler failed")
			}
		}()
		return handler(ctx, request)
	}
}

var _ interface {
	Start(context.Context) error
	Stop(context.Context) error
} = (*Server)(nil)
