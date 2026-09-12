package logging

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestUnaryServerInterceptorContractExtractsTraceparent(t *testing.T) {
	interceptor := UnaryServerInterceptor()
	parent := contractTraceparent
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(traceParentHeader, parent, "gate-id", "gate-a"))
	called := false
	_, err := interceptor(ctx, "request", &grpc.UnaryServerInfo{FullMethod: "/demo.Service/Forward"}, func(ctx context.Context, request any) (any, error) {
		called = true
		if request != "request" {
			t.Fatalf("request = %v", request)
		}
		traceID, spanID, ok := IDsFromContext(ctx)
		if !ok || traceID != "4bf92f3577b34da6a3ce929d0e0e4736" || spanID == "00f067aa0ba902b7" {
			t.Fatalf("server trace IDs = %q/%q/%t", traceID, spanID, ok)
		}
		if got, _ := metadata.FromIncomingContext(ctx); got.Get("gate-id")[0] != "gate-a" {
			t.Fatal("server interceptor dropped transport metadata")
		}
		return "reply", nil
	})
	if err != nil || !called {
		t.Fatalf("server interceptor result = called:%t error:%v", called, err)
	}
}

func TestUnaryServerInterceptorContractDuplicateParentStartsNewTrace(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(traceParentHeader, contractTraceparent, traceParentHeader, contractTraceparent))
	_, err := UnaryServerInterceptor()(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/demo.Service/Forward"}, func(ctx context.Context, _ any) (any, error) {
		traceID, _, ok := IDsFromContext(ctx)
		if !ok || traceID == "4bf92f3577b34da6a3ce929d0e0e4736" {
			t.Fatalf("duplicate parent was continued: %q/%t", traceID, ok)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnaryClientInterceptorContractInjectsChildAndPreservesMetadata(t *testing.T) {
	parentCtx, err := ContinueOrNew(context.Background(), contractTraceparent)
	if err != nil {
		t.Fatal(err)
	}
	parentTraceID, parentSpanID, _ := IDsFromContext(parentCtx)
	ctx := metadata.NewOutgoingContext(parentCtx, metadata.Pairs("connection-id", "connection-1"))
	called := false
	err = UnaryClientInterceptor()(ctx, "/demo.Service/Forward", "request", "reply", nil, func(ctx context.Context, method string, request, reply any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		called = true
		if method != "/demo.Service/Forward" || request != "request" || reply != "reply" {
			t.Fatalf("invoker arguments = %q/%v/%v", method, request, reply)
		}
		traceID, spanID, ok := IDsFromContext(ctx)
		if !ok || traceID != parentTraceID || spanID == parentSpanID {
			t.Fatalf("client trace IDs = %q/%q; parent = %q/%q", traceID, spanID, parentTraceID, parentSpanID)
		}
		values, ok := metadata.FromOutgoingContext(ctx)
		if !ok || values.Get("connection-id")[0] != "connection-1" {
			t.Fatal("client interceptor dropped existing metadata")
		}
		parents := values.Get(traceParentHeader)
		if len(parents) != 1 || !strings.HasPrefix(parents[0], "00-"+parentTraceID+"-"+spanID+"-") {
			t.Fatalf("injected traceparent = %v", parents)
		}
		return nil
	}, nil)
	if err != nil || !called {
		t.Fatalf("client interceptor result = called:%t error:%v", called, err)
	}
}
