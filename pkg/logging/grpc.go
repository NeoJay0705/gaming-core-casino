package logging

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// UnaryServerInterceptor 在 product handler 前 extract traceparent。
// 缺少、格式錯誤或重複值都建立新的 root trace；metadata 品質不會讓原本
// 合法的 RPC 失敗。
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		traceparent := ""
		if values, ok := metadata.FromIncomingContext(ctx); ok {
			parents := values.Get(traceParentHeader)
			if len(parents) == 1 {
				traceparent = parents[0]
			}
		}
		traced, err := ContinueOrNew(ctx, traceparent)
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("logging: initialize trace for %s: %v", info.FullMethod, err))
		}
		return handler(traced, request)
	}
}

// UnaryClientInterceptor 建立 child span，並把 canonical traceparent 注入
// outgoing gRPC metadata。既有 transport metadata 會先 copy 後保留。
func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, request, reply any, connection *grpc.ClientConn, invoker grpc.UnaryInvoker, options ...grpc.CallOption) error {
		traced, err := Child(ctx)
		if err != nil {
			return fmt.Errorf("logging: initialize outbound trace for %s: %w", method, err)
		}
		traceparent, ok := TraceParentFromContext(traced)
		if !ok {
			return fmt.Errorf("logging: outbound trace for %s is unavailable", method)
		}
		outgoing, _ := metadata.FromOutgoingContext(traced)
		if outgoing == nil {
			outgoing = metadata.MD{}
		} else {
			outgoing = outgoing.Copy()
		}
		outgoing.Set(traceParentHeader, traceparent)
		traced = metadata.NewOutgoingContext(traced, outgoing)
		return invoker(traced, method, request, reply, connection, options...)
	}
}
