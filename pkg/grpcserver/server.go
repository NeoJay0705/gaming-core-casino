// Package grpcserver owns one product-level gRPC listener.
//
// Business packages register generated services on this server; this package
// deliberately does not know any service-specific request or routing type.
package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"

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
}

// Server owns one listener and one grpc.Server. Services must be registered
// during application composition, before Start is called.
type Server struct {
	cfg Config

	mu         sync.Mutex
	server     *grpc.Server
	listener   net.Listener
	registered map[string]struct{}
	started    bool
	stopped    bool
}

// New validates config without binding a port.
func New(cfg Config) (*Server, error) {
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
	serverOptions := make([]grpc.ServerOption, 0, 2)
	if cfg.MaxConcurrentStreams > 0 {
		serverOptions = append(serverOptions, grpc.MaxConcurrentStreams(cfg.MaxConcurrentStreams))
	}
	serverOptions = append(serverOptions, grpc.ChainUnaryInterceptor(recoveryInterceptor))
	return &Server{
		cfg:        cfg,
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
			log.Printf("[grpc server] serve stopped: addr=%s err=%v", listener.Addr(), err)
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

func recoveryInterceptor(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Printf("[grpc server] unary handler panicked: method=%s panic=%v", info.FullMethod, recovered)
			response = nil
			err = status.Error(codes.Internal, "gRPC unary handler failed")
		}
	}()
	return handler(ctx, request)
}

var _ interface {
	Start(context.Context) error
	Stop(context.Context) error
} = (*Server)(nil)
