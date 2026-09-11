package gateproduct

import (
	"context"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"github.com/NeoJay0705/gaming-core-casino/pkg/grpcserver"
	"google.golang.org/grpc"
)

// contractGameServer is a test-only composition helper. Production code uses
// the product grpcserver directly; the helper keeps websocket contract tests
// focused on the Gate-to-Game service boundary.
type contractGameServer struct {
	service *gatelink.GateRequestService
	server  *grpcserver.Server
}

func newContractGameServer(t *testing.T, handler gatelink.RequestHandler) *contractGameServer {
	t.Helper()
	service, err := gatelink.NewGateRequestService(handler)
	if err != nil {
		t.Fatalf("new GateRequest service: %v", err)
	}
	server, err := grpcserver.New(grpcserver.Config{ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("new product gRPC server: %v", err)
	}
	if err := server.Register(gatelink.GateRequestService_ServiceDesc.ServiceName, func(registrar grpc.ServiceRegistrar) {
		gatelink.RegisterGateRequestServiceServer(registrar, service)
	}); err != nil {
		t.Fatalf("register GateRequest service: %v", err)
	}
	return &contractGameServer{service: service, server: server}
}

func (s *contractGameServer) Start(ctx context.Context) error { return s.server.Start(ctx) }
func (s *contractGameServer) Stop(ctx context.Context) error  { return s.server.Stop(ctx) }
func (s *contractGameServer) Addr() string                    { return s.server.Addr() }
