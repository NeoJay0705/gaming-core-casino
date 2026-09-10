package gatelink

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
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
	ctx := WithGateRequestContext(context.Background(), GateRequestContext{TraceID: "trace-123", Source: RequestSource{GateID: "gate-a", ConnectionID: "connection-42", ReplyEndpoint: "127.0.0.1:19091"}})
	if _, err := client.Forward(ctx, request); err != nil {
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
		if got.requestContext.Source != (RequestSource{GateID: "gate-a", ConnectionID: "connection-42", ReplyEndpoint: "127.0.0.1:19091"}) {
			t.Fatalf("source = %#v, want Gate connection source", got.requestContext.Source)
		}
		if got.requestContext.TraceID != "trace-123" {
			t.Fatalf("trace id = %q, want trace-123", got.requestContext.TraceID)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not receive request")
	}
}

func TestContractForwardReplyRoundTripCopiesPayload(t *testing.T) {
	replyPayload := []byte("reply")
	server, err := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0"}, RequestHandlerFunc(func(ctx context.Context, _ Request) error {
		if err := SetForwardReply(ctx, Reply{
			CommandID:         2,
			Payload:           replyPayload,
			ExpectedLoginName: "alice",
		}); err != nil {
			return err
		}
		replyPayload[0] = 'X'
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
	reply, err := client.Forward(ctx, Request{CommandID: 1})
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	if reply == nil || reply.CommandID != 2 || string(reply.Payload) != "reply" || reply.ExpectedLoginName != "alice" {
		t.Fatalf("reply = %#v, want copied command, payload, and expected login", reply)
	}
	reply.Payload[0] = 'Y'
	if string(replyPayload) != "Xeply" {
		t.Fatalf("caller reply payload = %q, want server-side copy mutation only", replyPayload)
	}
}

func TestContractForwardReplyAllowsNoReply(t *testing.T) {
	server, err := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0"}, RequestHandlerFunc(func(context.Context, Request) error {
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
	reply, err := client.Forward(ctx, Request{CommandID: 1})
	if err != nil || reply != nil {
		t.Fatalf("no-reply forward = reply:%#v error:%v, want nil/nil", reply, err)
	}
}

func TestContractForwardReplyAllowsOnlyOneAndClosesAfterHandler(t *testing.T) {
	var secondErr error
	var handlerContext context.Context
	server, err := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0"}, RequestHandlerFunc(func(ctx context.Context, _ Request) error {
		handlerContext = ctx
		if err := SetForwardReply(ctx, Reply{CommandID: 2, Payload: []byte("first")}); err != nil {
			return err
		}
		secondErr = SetForwardReply(ctx, Reply{CommandID: 3, Payload: []byte("second")})
		return nil
	}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	response, err := server.Forward(context.Background(), &GateRequest{CommandId: 1})
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	if !errors.Is(secondErr, ErrForwardReplyAlreadySet) {
		t.Fatalf("second reply error = %v, want ErrForwardReplyAlreadySet", secondErr)
	}
	if response.GetReply().GetCommandId() != 2 || string(response.GetReply().GetPayload()) != "first" {
		t.Fatalf("response reply = %#v, want first reply", response.GetReply())
	}
	if err := SetForwardReply(handlerContext, Reply{CommandID: 4}); !errors.Is(err, ErrForwardReplyUnavailable) {
		t.Fatalf("reply after handler error = %v, want ErrForwardReplyUnavailable", err)
	}
}

func TestContractForwardReplyConcurrentCallsAcceptOnlyOne(t *testing.T) {
	slot := newForwardReplySlot()
	ctx := withForwardReplySlot(context.Background(), slot)
	results := make(chan error, 2)
	for commandID := uint32(2); commandID <= 3; commandID++ {
		go func(commandID uint32) {
			results <- SetForwardReply(ctx, Reply{CommandID: commandID})
		}(commandID)
	}
	var successes int
	for i := 0; i < 2; i++ {
		if err := <-results; err == nil {
			successes++
		} else if !errors.Is(err, ErrForwardReplyAlreadySet) {
			t.Fatalf("concurrent reply error = %v, want duplicate error", err)
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent reply successes = %d, want 1", successes)
	}
	if reply := slot.finish(true); reply == nil {
		t.Fatal("concurrent reply slot finished without a reply")
	}
}

func TestContractForwardReplyIsDiscardedOnHandlerError(t *testing.T) {
	server, err := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0"}, RequestHandlerFunc(func(ctx context.Context, _ Request) error {
		if err := SetForwardReply(ctx, Reply{CommandID: 2, Payload: []byte("discard")}); err != nil {
			return err
		}
		return errors.New("handler failed")
	}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	response, err := server.Forward(context.Background(), &GateRequest{CommandId: 1})
	if status.Code(err) != codes.Internal || response != nil {
		t.Fatalf("failed forward = response:%#v error:%v, want nil/Internal", response, err)
	}
}

func TestContractRejectsMalformedForwardReplies(t *testing.T) {
	cases := []struct {
		name     string
		response *ForwardResponse
	}{
		{name: "nil response"},
		{name: "zero reply command", response: &ForwardResponse{Reply: &ForwardReply{}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := &Client{
				cfg:    ClientConfig{Timeout: time.Second},
				client: forwardClientStub{response: tc.response},
			}
			reply, err := client.Forward(context.Background(), Request{CommandID: 1})
			if reply != nil || status.Code(err) != codes.Internal {
				t.Fatalf("malformed forward = reply:%#v error:%v, want nil/Internal", reply, err)
			}
		})
	}
}

func TestContractClientCopiesForwardReplyPayload(t *testing.T) {
	payload := []byte("reply")
	client := &Client{
		client: forwardClientStub{response: &ForwardResponse{
			Reply: &ForwardReply{CommandId: 2, Payload: payload},
		}},
	}
	reply, err := client.Forward(context.Background(), Request{CommandID: 1})
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	reply.Payload[0] = 'X'
	if string(payload) != "reply" {
		t.Fatalf("stub response payload = %q, want unchanged after client mapping", payload)
	}
}

func TestContractRejectsDuplicateReplyEndpointMetadata(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		connectionIDMetadataKey, "connection-1",
		replyEndpointMetadataKey, "127.0.0.1:19091",
		replyEndpointMetadataKey, "127.0.0.1:19092",
	))
	if _, err := withIncomingRequestContext(ctx); err == nil || !strings.Contains(err.Error(), "duplicate "+replyEndpointMetadataKey) {
		t.Fatalf("duplicate reply endpoint error = %v, want duplicate metadata error", err)
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
		if _, err := client.Forward(ctx, testRequest(uint32(i))); err != nil {
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
	if _, err := client.Forward(ctx, Request{}); status.Code(err) != codes.InvalidArgument {
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
	server, err := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0"}, RequestHandlerFunc(func(ctx context.Context, _ Request) error {
		if calls.Add(1) == 1 {
			if err := SetForwardReply(ctx, Reply{CommandID: 2, Payload: []byte("discard")}); err != nil {
				return err
			}
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
	if reply, err := client.Forward(ctx, Request{CommandID: 1}); reply != nil || status.Code(err) != codes.Internal {
		t.Fatalf("panic response = reply:%#v status:%s, want nil/Internal", reply, status.Code(err))
	}
	if _, err := client.Forward(ctx, Request{CommandID: 1}); err != nil {
		t.Fatalf("forward after handler panic: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("handler calls = %d, want 2", calls.Load())
	}
}

func TestContractRejectsCanceledForwardReplyContext(t *testing.T) {
	slot := newForwardReplySlot()
	ctx, cancel := context.WithCancel(withForwardReplySlot(context.Background(), slot))
	cancel()
	if err := SetForwardReply(ctx, Reply{CommandID: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled reply error = %v, want context.Canceled", err)
	}
	if reply := slot.finish(true); reply != nil {
		t.Fatalf("canceled reply slot = %#v, want empty", reply)
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
	if _, err := client.Forward(context.Background(), Request{CommandID: 1}); err == nil || !strings.Contains(err.Error(), "connection_id is required") {
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

type forwardClientStub struct {
	response *ForwardResponse
}

func (s forwardClientStub) Forward(context.Context, *GateRequest, ...grpc.CallOption) (*ForwardResponse, error) {
	return s.response, nil
}

func (s *countingServer) Forward(context.Context, *GateRequest) (*ForwardResponse, error) {
	s.calls.Add(1)
	return &ForwardResponse{}, nil
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
