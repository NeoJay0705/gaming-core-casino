package gatelink

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ServerConfig configures the Game-side Gate request listener.
type ServerConfig struct {
	ListenAddr string `config:"listen_addr" yaml:"listen_addr"`
	// MaxConcurrentStreams 限制每條 gRPC transport 的並行 stream；0 保留 grpc-go default。
	MaxConcurrentStreams uint32 `config:"max_concurrent_streams" yaml:"max_concurrent_streams"`
}

// RequestHandler is the Game product registration point for requests received
// from Gate. The payload is opaque transport data; handlers must accept ctx to
// access request metadata and cancellation.
type RequestHandler interface {
	HandleGateRequest(context.Context, Request) error
}

// RequestHandlerFunc adapts a function into RequestHandler.
type RequestHandlerFunc func(context.Context, Request) error

func (f RequestHandlerFunc) HandleGateRequest(ctx context.Context, request Request) error {
	return f(ctx, request)
}

// Server owns the Game-side direct Gate request listener.
type Server struct {
	UnimplementedGateRequestServiceServer

	cfg     ServerConfig
	handler RequestHandler

	mu       sync.Mutex
	server   *grpc.Server
	listener net.Listener
	stopping bool
}

// NewServer builds the Game-side listener. A missing handler is accepted only
// while no business dispatcher has been installed; requests then return
// Unimplemented instead of being silently dropped.
func NewServer(cfg ServerConfig, handler RequestHandler) (*Server, error) {
	cfg.ListenAddr = strings.TrimSpace(cfg.ListenAddr)
	server := &Server{cfg: cfg, handler: handler}
	if cfg.ListenAddr == "" {
		return nil, errors.New("gatelink: listen_addr is required")
	}
	if _, _, err := net.SplitHostPort(cfg.ListenAddr); err != nil {
		return nil, fmt.Errorf("gatelink: invalid listen_addr %q: %w", cfg.ListenAddr, err)
	}
	return server, nil
}

func isNilRequestHandler(handler RequestHandler) bool {
	if handler == nil {
		return true
	}
	value := reflect.ValueOf(handler)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// Forward delivers the opaque binary payload and the single request metadata
// carrier to the registered Game handler, then returns its optional reply.
func (s *Server) Forward(ctx context.Context, request *GateRequest) (*ForwardResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if request.GetCommandId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "command_id is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || isNilRequestHandler(s.handler) {
		return nil, status.Error(codes.Unimplemented, "gate request handler is not configured")
	}
	replySlot := newForwardReplySlot()
	defer replySlot.close()
	ctx = withForwardReplySlot(ctx, replySlot)
	handlerRequest := Request{
		CommandID: request.GetCommandId(),
		Payload:   append([]byte(nil), request.GetPayload()...),
	}
	handlerErr := s.handler.HandleGateRequest(ctx, handlerRequest)
	reply := replySlot.finish(handlerErr == nil)
	if handlerErr != nil {
		if status.Code(handlerErr) != codes.Unknown {
			return nil, handlerErr
		}
		if errors.Is(handlerErr, context.Canceled) {
			return nil, status.Error(codes.Canceled, handlerErr.Error())
		}
		if errors.Is(handlerErr, context.DeadlineExceeded) {
			return nil, status.Error(codes.DeadlineExceeded, handlerErr.Error())
		}
		return nil, status.Error(codes.Internal, "gate request handler failed")
	}
	response := &ForwardResponse{}
	if reply != nil {
		response.Reply = &ForwardReply{
			CommandId:         reply.CommandID,
			Payload:           reply.Payload,
			ExpectedLoginName: reply.ExpectedLoginName,
		}
	}
	return response, nil
}

// Start binds the listener synchronously, so readiness is never reached when
// the configured port is unavailable.
func (s *Server) Start(ctx context.Context) error {
	if s == nil {
		return errors.New("gatelink: nil server")
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		return errors.New("gatelink: server is stopped")
	}
	if s.server != nil {
		return errors.New("gatelink: server already started")
	}
	listener, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("gatelink: listen on %s: %w", s.cfg.ListenAddr, err)
	}
	serverOptions := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(incomingRequestContextInterceptor, recoveryInterceptor),
	}
	if s.cfg.MaxConcurrentStreams > 0 {
		serverOptions = append(serverOptions, grpc.MaxConcurrentStreams(s.cfg.MaxConcurrentStreams))
	}
	server := grpc.NewServer(serverOptions...)
	RegisterGateRequestServiceServer(server, s)
	s.listener, s.server = listener, server
	go func() { _ = server.Serve(listener) }()
	return nil
}

// Stop stops accepting requests and gracefully drains active RPCs until ctx
// expires, at which point it force-stops the server.
func (s *Server) Stop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	server := s.server
	s.stopping = true
	s.server, s.listener = nil, nil
	s.mu.Unlock()
	if server == nil {
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

func recoveryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (response any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Printf("[gatelink] unary handler panicked: method=%s panic=%v", info.FullMethod, recovered)
			response = nil
			err = status.Error(codes.Internal, "gate request handler failed")
		}
	}()
	return handler(ctx, req)
}

// Addr returns the actual bound address, including an ephemeral port selected
// through listen_addr ending in :0.
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

var _ GateRequestServiceServer = (*Server)(nil)
