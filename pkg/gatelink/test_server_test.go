package gatelink

import (
	"context"

	"github.com/NeoJay0705/gaming-core-casino/pkg/grpcserver"
	"google.golang.org/grpc"
)

// ServerConfig／NewServer 是 package contract tests 的小型組裝 helper。
// production API 已由 GateRequestService 與 pkg/grpcserver 分離；此 helper
// 只讓既有 service contract 測試以同一組件驗證 wire 行為，不會進入正式套件。
type ServerConfig = grpcserver.Config

type testServer struct {
	service *GateRequestService
	server  *grpcserver.Server
}

func NewServer(cfg ServerConfig, handler RequestHandler) (*testServer, error) {
	service, err := NewGateRequestService(handler)
	if err != nil {
		return nil, err
	}
	server, err := grpcserver.New(cfg)
	if err != nil {
		return nil, err
	}
	if err := server.Register(GateRequestService_ServiceDesc.ServiceName, func(registrar grpc.ServiceRegistrar) {
		RegisterGateRequestServiceServer(registrar, service)
	}); err != nil {
		return nil, err
	}
	return &testServer{service: service, server: server}, nil
}

func (s *testServer) Forward(ctx context.Context, request *GateRequest) (*ForwardResponse, error) {
	return s.service.Forward(ctx, request)
}

func (s *testServer) Start(ctx context.Context) error { return s.server.Start(ctx) }
func (s *testServer) Stop(ctx context.Context) error  { return s.server.Stop(ctx) }
func (s *testServer) Addr() string                    { return s.server.Addr() }
