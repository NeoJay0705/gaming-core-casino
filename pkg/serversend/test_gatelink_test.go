package serversend

import (
	"context"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"github.com/NeoJay0705/gaming-core-casino/pkg/grpcserver"
	"google.golang.org/grpc"
)

type testGateRequestServer struct {
	service *gatelink.GateRequestService
}

func newTestGateRequestServer(t *testing.T, handler gatelink.RequestHandler) *testGateRequestServer {
	t.Helper()
	service, err := gatelink.NewGateRequestService(handler)
	if err != nil {
		t.Fatal(err)
	}
	server, err := grpcserver.New(grpcserver.Config{ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Register(gatelink.GateRequestService_ServiceDesc.ServiceName, func(registrar grpc.ServiceRegistrar) {
		gatelink.RegisterGateRequestServiceServer(registrar, service)
	}); err != nil {
		t.Fatal(err)
	}
	return &testGateRequestServer{service: service}
}

func (s *testGateRequestServer) Forward(ctx context.Context, request *gatelink.GateRequest) (*gatelink.ForwardResponse, error) {
	return s.service.Forward(ctx, request)
}
