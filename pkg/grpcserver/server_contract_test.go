package grpcserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestNewValidatesListenAddress(t *testing.T) {
	for _, address := range []string{"", "127.0.0.1", "127.0.0.1:not-a-port"} {
		t.Run(address, func(t *testing.T) {
			if _, err := New(Config{ListenAddr: address}); err == nil {
				t.Fatalf("New(%q) error = nil", address)
			}
		})
	}
	server, err := New(Config{ListenAddr: " 127.0.0.1:0 ", MaxConcurrentStreams: 7})
	if err != nil {
		t.Fatalf("New(valid) error = %v", err)
	}
	if server.cfg.ListenAddr != "127.0.0.1:0" || server.cfg.MaxConcurrentStreams != 7 {
		t.Fatalf("normalized config = %#v", server.cfg)
	}
}

func TestRegisterRejectsInvalidAndLateRegistration(t *testing.T) {
	server := newContractServer(t, 0)
	for _, test := range []struct {
		name string
		call func() error
	}{
		{name: "blank name", call: func() error { return server.Register(" ", func(grpc.ServiceRegistrar) {}) }},
		{name: "nil callback", call: func() error { return server.Register("contract.Empty", nil) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); err == nil {
				t.Fatal("registration error = nil")
			}
		})
	}
	registerTestService(t, server, "contract.Ping")
	if err := server.Register("contract.Ping", func(grpc.ServiceRegistrar) {}); err == nil {
		t.Fatal("duplicate service registration unexpectedly succeeded")
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := server.Register("contract.Late", func(grpc.ServiceRegistrar) {}); err == nil {
		t.Fatal("registration after Start unexpectedly succeeded")
	}
	if err := server.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := server.Register("contract.AfterStop", func(grpc.ServiceRegistrar) {}); err == nil {
		t.Fatal("registration after Stop unexpectedly succeeded")
	}
	if err := server.Start(context.Background()); err == nil {
		t.Fatal("Start after Stop unexpectedly succeeded")
	}
}

func TestMultipleServicesShareOneListenerAndPanicIsRecovered(t *testing.T) {
	server := newContractServer(t, 0)
	registerTestService(t, server, "contract.PingA")
	registerTestService(t, server, "contract.PingB")
	registerTestService(t, server, "contract.Panic")
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })

	conn, err := grpc.NewClient(server.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	for _, method := range []string{"/contract.PingA/Ping", "/contract.PingB/Ping"} {
		response := new(wrapperspb.StringValue)
		if err := conn.Invoke(context.Background(), method, new(emptypb.Empty), response); err != nil {
			t.Fatalf("invoke %s: %v", method, err)
		}
		if response.GetValue() == "" {
			t.Fatalf("invoke %s returned empty response", method)
		}
	}
	panicResponse := new(wrapperspb.StringValue)
	err = conn.Invoke(context.Background(), "/contract.Panic/Ping", new(emptypb.Empty), panicResponse)
	if status.Code(err) != codes.Internal {
		t.Fatalf("panic status = %s (%v), want %s", status.Code(err), err, codes.Internal)
	}
}

func TestStartHonorsCanceledContextAndStopDeadline(t *testing.T) {
	server := newContractServer(t, 0)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := server.Start(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start(canceled) error = %v, want context.Canceled", err)
	}
	if server.Addr() != "" {
		t.Fatalf("address after canceled Start = %q", server.Addr())
	}

	blocking := newContractServer(t, 0)
	started := make(chan struct{})
	service, err := gatelink.NewGateRequestService(gatelink.RequestHandlerFunc(func(ctx context.Context, _ gatelink.Request) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}))
	if err != nil {
		t.Fatalf("new blocking GateRequest service: %v", err)
	}
	if err := blocking.Register(gatelink.GateRequestService_ServiceDesc.ServiceName, func(registrar grpc.ServiceRegistrar) {
		gatelink.RegisterGateRequestServiceServer(registrar, service)
	}); err != nil {
		t.Fatalf("register blocking GateRequest service: %v", err)
	}
	if err := blocking.Start(context.Background()); err != nil {
		t.Fatalf("start blocking server: %v", err)
	}
	client, err := gatelink.NewClient(gatelink.ClientConfig{Target: blocking.Addr(), Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("new blocking client: %v", err)
	}
	defer client.Stop(context.Background())
	callDone := make(chan error, 1)
	go func() {
		callDone <- func() error {
			_, err := client.Forward(gatelink.WithGateRequestContext(context.Background(), gatelink.GateRequestContext{Source: gatelink.RequestSource{ConnectionID: "blocking"}}), gatelink.Request{CommandID: 1})
			return err
		}()
	}()
	select {
	case <-started:
	case err := <-callDone:
		t.Fatalf("blocking RPC ended before handler started: %v", err)
	case <-time.After(time.Second):
		t.Fatal("blocking RPC did not start")
	}
	stopContext, stopCancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer stopCancel()
	if err := blocking.Stop(stopContext); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop(deadline) error = %v, want context deadline exceeded", err)
	}
	select {
	case <-callDone:
	case <-time.After(time.Second):
		t.Fatal("blocking RPC remained after forced stop")
	}
	if err := blocking.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}

type contractUnaryServer interface {
	Ping(context.Context, *emptypb.Empty) (*wrapperspb.StringValue, error)
}

type contractUnaryService struct {
	handle func(context.Context, *emptypb.Empty) (*wrapperspb.StringValue, error)
}

func (s contractUnaryService) Ping(ctx context.Context, request *emptypb.Empty) (*wrapperspb.StringValue, error) {
	if s.handle != nil {
		return s.handle(ctx, request)
	}
	return &wrapperspb.StringValue{Value: "ok"}, nil
}

var contractUnaryServiceDesc = grpc.ServiceDesc{
	ServiceName: "contract.Ping",
	HandlerType: (*contractUnaryServer)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Ping",
		Handler: func(srv any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			request := new(emptypb.Empty)
			if err := decode(request); err != nil {
				return nil, err
			}
			invoke := func(ctx context.Context, request any) (any, error) {
				return srv.(contractUnaryServer).Ping(ctx, request.(*emptypb.Empty))
			}
			if interceptor == nil {
				return invoke(ctx, request)
			}
			return interceptor(ctx, request, &grpc.UnaryServerInfo{FullMethod: "/contract.Ping/Ping"}, invoke)
		},
	}},
}

func newContractServer(t *testing.T, maxStreams uint32) *Server {
	t.Helper()
	server, err := New(Config{ListenAddr: "127.0.0.1:0", MaxConcurrentStreams: maxStreams})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return server
}

func registerTestService(t *testing.T, server *Server, serviceName string) {
	registerTestServiceWithHandler(t, server, serviceName, nil)
}

func registerTestServiceWithHandler(t *testing.T, server *Server, serviceName string, handler func(context.Context, *emptypb.Empty) (*wrapperspb.StringValue, error)) {
	t.Helper()
	service := contractUnaryService{}
	if handler != nil {
		service.handle = handler
	} else if serviceName == "contract.Panic" {
		service.handle = func(context.Context, *emptypb.Empty) (*wrapperspb.StringValue, error) { panic("contract panic") }
	}
	description := contractUnaryServiceDesc
	description.ServiceName = serviceName
	if err := server.Register(serviceName, func(registrar grpc.ServiceRegistrar) {
		registrar.RegisterService(&description, service)
	}); err != nil {
		t.Fatalf("register %s: %v", serviceName, err)
	}
}

var _ contractUnaryServer = contractUnaryService{}
