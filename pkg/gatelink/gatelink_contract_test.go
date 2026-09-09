package gatelink

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestContractForwardsOpaquePayloadAndRequestContext(t *testing.T) {
	handler := &recordingHandler{requests: make(chan recordedRequest, 1)}
	server, err := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0"}, handler)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })

	client, err := NewClient(ClientConfig{Target: server.Addr(), Timeout: time.Second})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Stop(context.Background()) })
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("start client: %v", err)
	}

	payload := []byte{0x00, 0xE1, 0xFF}
	request := Request{
		CommandID: 0xE10003,
		Payload:   payload,
	}
	ctx := WithGateRequestContext(context.Background(), GateRequestContext{TraceID: "trace-123", Source: RequestSource{GateID: "gate-a", ConnectionID: "connection-42"}})
	if err := client.Forward(ctx, request); err != nil {
		t.Fatalf("forward: %v", err)
	}
	payload[0] = 0x99
	select {
	case got := <-handler.requests:
		if got.request.CommandID != request.CommandID {
			t.Fatalf("command id = %d, want %d", got.request.CommandID, request.CommandID)
		}
		if string(got.request.Payload) != string([]byte{0x00, 0xE1, 0xFF}) {
			t.Fatalf("payload = %x, want original binary bytes", got.request.Payload)
		}
		if got.requestContext.Source != (RequestSource{GateID: "gate-a", ConnectionID: "connection-42"}) {
			t.Fatalf("source = %#v, want Gate connection source", got.requestContext.Source)
		}
		if got.requestContext.TraceID != "trace-123" {
			t.Fatalf("trace id = %q, want trace-123", got.requestContext.TraceID)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not receive request")
	}
}

func TestContractUsesRoundRobinAcrossResolvedBackends(t *testing.T) {
	backendA, addressA := startCountingServer(t)
	backendB, addressB := startCountingServer(t)
	resolverBuilder := manual.NewBuilderWithScheme("gatelinkcontract")
	resolverBuilder.InitialState(resolver.State{Addresses: []resolver.Address{{Addr: addressA}, {Addr: addressB}}})
	resolver.Register(resolverBuilder)

	client, err := NewClient(ClientConfig{Target: "gatelinkcontract:///game", Timeout: time.Second})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Stop(context.Background()) })
	for i := 1; i <= 20; i++ {
		ctx := WithGateRequestContext(context.Background(), GateRequestContext{Source: RequestSource{ConnectionID: "connection-test"}})
		if err := client.Forward(ctx, testRequest(uint32(i))); err != nil {
			t.Fatalf("forward(%d): %v", i, err)
		}
	}
	if backendA.calls.Load() == 0 || backendB.calls.Load() == 0 {
		t.Fatalf("round robin calls = A:%d B:%d, want both backends used", backendA.calls.Load(), backendB.calls.Load())
	}
}

func TestContractRequiresClientTarget(t *testing.T) {
	_, err := NewClient(ClientConfig{})
	if err == nil || !strings.Contains(err.Error(), "target is required") {
		t.Fatalf("new client error = %v, want missing target", err)
	}
}

func TestContractDefaultsForwardTimeout(t *testing.T) {
	client, err := NewClient(ClientConfig{Target: "dns:///gameproduct:9090"})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Stop(context.Background()) })
	if client.cfg.Timeout != DefaultTimeout {
		t.Fatalf("client timeout = %s, want %s", client.cfg.Timeout, DefaultTimeout)
	}
}

func TestContractRejectsMissingListenAddress(t *testing.T) {
	_, err := NewServer(ServerConfig{}, nil)
	if err == nil || !strings.Contains(err.Error(), "listen_addr is required") {
		t.Fatalf("new server error = %v, want missing listen_addr", err)
	}
}

func TestContractRejectsRequestsWithoutRegisteredHandler(t *testing.T) {
	server, err := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0"}, nil)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	_, err = server.Forward(context.Background(), &GateRequest{CommandId: 1})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("forward without handler status = %s, want %s", status.Code(err), codes.Unimplemented)
	}
}

func TestContractRejectsCommandIDZeroBeforeCallingHandler(t *testing.T) {
	var calls atomic.Int32
	server, err := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0"}, RequestHandlerFunc(func(context.Context, Request) error {
		calls.Add(1)
		return nil
	}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if _, err := server.Forward(context.Background(), &GateRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("direct Forward command 0 status = %s, want %s", status.Code(err), codes.InvalidArgument)
	}
	if calls.Load() != 0 {
		t.Fatalf("handler calls after direct command 0 = %d, want 0", calls.Load())
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })
	client, err := NewClient(ClientConfig{Target: server.Addr(), Timeout: time.Second})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Stop(context.Background()) })
	ctx := WithGateRequestContext(context.Background(), GateRequestContext{Source: RequestSource{ConnectionID: "connection-1"}})
	if err := client.Forward(ctx, Request{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Client.Forward command 0 status = %s, want %s", status.Code(err), codes.InvalidArgument)
	}
	if _, err := client.client.Forward(ctx, &GateRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("gRPC Forward command 0 status = %s, want %s", status.Code(err), codes.InvalidArgument)
	}
	if calls.Load() != 0 {
		t.Fatalf("handler calls after command 0 = %d, want 0", calls.Load())
	}
}

func TestContractRecoversHandlerPanicAndKeepsServerAvailable(t *testing.T) {
	var calls atomic.Int32
	server, err := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0"}, RequestHandlerFunc(func(context.Context, Request) error {
		if calls.Add(1) == 1 {
			panic("test handler panic")
		}
		return nil
	}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })
	client, err := NewClient(ClientConfig{Target: server.Addr(), Timeout: time.Second})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Stop(context.Background()) })
	ctx := WithGateRequestContext(context.Background(), GateRequestContext{Source: RequestSource{ConnectionID: "connection-1"}})
	if err := client.Forward(ctx, Request{CommandID: 1}); status.Code(err) != codes.Internal {
		t.Fatalf("panic response status = %s, want %s", status.Code(err), codes.Internal)
	}
	if err := client.Forward(ctx, Request{CommandID: 1}); err != nil {
		t.Fatalf("forward after handler panic: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("handler calls = %d, want 2", calls.Load())
	}
}

func TestContractRejectsRequestWithoutSourceConnectionID(t *testing.T) {
	handler := &recordingHandler{requests: make(chan recordedRequest, 1)}
	server, err := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0"}, handler)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })
	client, err := NewClient(ClientConfig{Target: server.Addr()})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Stop(context.Background()) })
	if err := client.Forward(context.Background(), Request{CommandID: 1}); err == nil || !strings.Contains(err.Error(), "connection_id is required") {
		t.Fatalf("forward without source error = %v, want missing connection_id", err)
	}
}

type recordedRequest struct {
	request        Request
	requestContext GateRequestContext
}

type recordingHandler struct{ requests chan recordedRequest }

func (h *recordingHandler) HandleGateRequest(ctx context.Context, request Request) error {
	requestContext, _ := GateRequestContextFrom(ctx)
	request.Payload = append([]byte(nil), request.Payload...)
	h.requests <- recordedRequest{request: request, requestContext: requestContext}
	return nil
}

func testRequest(commandID uint32) Request {
	return Request{CommandID: commandID, Payload: []byte{byte(commandID)}}
}

type countingServer struct {
	UnimplementedGateRequestServiceServer
	calls atomic.Int32
}

func (s *countingServer) Forward(context.Context, *GateRequest) (*emptypb.Empty, error) {
	s.calls.Add(1)
	return &emptypb.Empty{}, nil
}

func startCountingServer(t *testing.T) (*countingServer, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen backend: %v", err)
	}
	grpcServer := grpc.NewServer()
	backend := &countingServer{}
	RegisterGateRequestServiceServer(grpcServer, backend)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})
	return backend, listener.Addr().String()
}
