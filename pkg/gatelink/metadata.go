// Package gatelink defines the direct Gate-to-Game transport contract.
package gatelink

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type requestContextCarrierKey struct{}

// affinityKeyContextKey 標記 Gate client picker 使用的 request-local key。
// 它刻意不屬於 gRPC metadata contract，Game server 不需要知道 endpoint 的
// 選擇依據。
type affinityKeyContextKey struct{}

// GateRequestContext 包含跨 transport 的 request data。刻意使用單一 context
// value，新增 transport 欄位時不必修改所有 handler signature。
type GateRequestContext struct {
	Source RequestSource
}

// RequestContextCarrier 暴露 inbound Gate request context 中與 transport 無關的
// 部分。product 可以在自己的 carrier 附加 transport data，而不讓 gatelink
// 依賴該 transport。
type RequestContextCarrier interface {
	GateRequestContext() GateRequestContext
}

type gateRequestContextCarrier struct{ value GateRequestContext }

func (c gateRequestContextCarrier) GateRequestContext() GateRequestContext { return c.value }

// Request is the opaque Gate request delivered to a Game handler. CommandID
// stays outside Payload so a future dispatcher can route the packet
// without decoding product business data.
type Request struct {
	CommandID uint32
	Payload   []byte
}

// RequestSource identifies the Gate connection that originated Request.
// GateID is optional for ordinary Gate-to-Game requests; ConnectionID is the
// required identity of the originating WebSocket connection.
type RequestSource struct {
	GateID       string
	ConnectionID string
}

const (
	gateIDMetadataKey       = "x-gate-request-gate-id"
	connectionIDMetadataKey = "x-gate-request-connection-id"
)

// WithGateRequestContext returns a context carrying metadata for one Gate
// request. A nil context is treated as context.Background.
func WithGateRequestContext(ctx context.Context, requestContext GateRequestContext) context.Context {
	return WithRequestContextCarrier(ctx, gateRequestContextCarrier{value: requestContext})
}

// WithRequestContextCarrier stores one inbound Gate request carrier. Products
// use it to attach transport-specific data while preserving the metadata used
// by the Gate-to-Game client interceptor.
func WithRequestContextCarrier(ctx context.Context, carrier RequestContextCarrier) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if carrier == nil {
		return ctx
	}
	return context.WithValue(ctx, requestContextCarrierKey{}, carrier)
}

// RequestContextCarrierFrom returns the full carrier attached to ctx.
func RequestContextCarrierFrom(ctx context.Context) (RequestContextCarrier, bool) {
	if ctx == nil {
		return nil, false
	}
	carrier, ok := ctx.Value(requestContextCarrierKey{}).(RequestContextCarrier)
	return carrier, ok
}

// GateRequestContextFrom returns the transport-neutral metadata from the
// carrier attached to ctx.
func GateRequestContextFrom(ctx context.Context) (GateRequestContext, bool) {
	carrier, ok := RequestContextCarrierFrom(ctx)
	if !ok || carrier == nil {
		return GateRequestContext{}, false
	}
	return carrier.GateRequestContext(), true
}

// WithAffinityKey 將 local routing key 附加到 ctx。Gate 在轉送 request 前使用
// 已驗證的 login name；此值保持 opaque，不會被 normalization 或 serialize
// 到 gRPC。
func WithAffinityKey(ctx context.Context, key string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, affinityKeyContextKey{}, key)
}

// AffinityKeyFromContext 取出 ctx 中非空的 local routing key。空 key 視為缺少，
// 避免 caller 意外 fallback 到 connection ID 等不相干的 identity。
func AffinityKeyFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	key, ok := ctx.Value(affinityKeyContextKey{}).(string)
	return key, ok && key != ""
}

func outgoingRequestContextInterceptor(
	ctx context.Context,
	method string,
	req, reply any,
	connection *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	options ...grpc.CallOption,
) error {
	ctx, err := withOutgoingRequestMetadata(ctx)
	if err != nil {
		return err
	}
	return invoker(ctx, method, req, reply, connection, options...)
}

func withOutgoingRequestMetadata(ctx context.Context) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	requestContext, ok := GateRequestContextFrom(ctx)
	if !ok || strings.TrimSpace(requestContext.Source.ConnectionID) == "" {
		return nil, errors.New("gatelink: request source connection_id is required")
	}
	metadataValues, _ := metadata.FromOutgoingContext(ctx)
	metadataValues = metadataValues.Copy()
	metadataValues.Set(connectionIDMetadataKey, strings.TrimSpace(requestContext.Source.ConnectionID))
	if gateID := strings.TrimSpace(requestContext.Source.GateID); gateID != "" {
		metadataValues.Set(gateIDMetadataKey, gateID)
	}
	return metadata.NewOutgoingContext(ctx, metadataValues), nil
}

func withIncomingRequestContext(ctx context.Context) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	connectionID, err := singleMetadataValues(
		metadata.ValueFromIncomingContext(ctx, connectionIDMetadataKey),
		connectionIDMetadataKey,
		true,
	)
	if err != nil {
		return nil, err
	}
	gateID, err := singleMetadataValues(
		metadata.ValueFromIncomingContext(ctx, gateIDMetadataKey),
		gateIDMetadataKey,
		false,
	)
	if err != nil {
		return nil, err
	}
	return WithGateRequestContext(ctx, GateRequestContext{
		Source: RequestSource{GateID: gateID, ConnectionID: connectionID},
	}), nil
}

// singleMetadataValues 驗證 bounded lookup 取得的單一 metadata key。
// 仍保留原本的 duplicate、空值與錯誤文字 contract；呼叫端不需要建立
// 完整 metadata.MD copy。
func singleMetadataValues(entries []string, key string, required bool) (string, error) {
	if len(entries) == 0 {
		if required {
			return "", errors.New("gatelink: request source connection_id is required")
		}
		return "", nil
	}
	if len(entries) != 1 {
		return "", errors.New("gatelink: request metadata contains duplicate " + key)
	}
	value := strings.TrimSpace(entries[0])
	if required && value == "" {
		return "", errors.New("gatelink: request source connection_id is required")
	}
	return value, nil
}
