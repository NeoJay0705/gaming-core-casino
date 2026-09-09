// Package gatelink defines the direct Gate-to-Game transport contract.
package gatelink

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type requestContextCarrierKey struct{}

// GateRequestContext contains cross-cutting request data. It is intentionally a
// single context value so adding a transport field does not change every
// handler signature.
type GateRequestContext struct {
	TraceID string
	Source  RequestSource
}

// RequestContextCarrier exposes the transport-neutral portion of an inbound
// Gate request context. A product may carry additional transport data in its
// own carrier without making gatelink depend on that transport.
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
// GateID is optional until Gate instance identity is configured. ConnectionID
// is required so downstream code can preserve the player-facing route.
type RequestSource struct {
	GateID       string
	ConnectionID string
}

const (
	traceIDMetadataKey      = "x-gate-request-trace-id"
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

func incomingRequestContextInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	ctx, err := withIncomingRequestContext(ctx)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return handler(ctx, req)
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
	if traceID := strings.TrimSpace(requestContext.TraceID); traceID != "" {
		metadataValues.Set(traceIDMetadataKey, traceID)
	}
	if gateID := strings.TrimSpace(requestContext.Source.GateID); gateID != "" {
		metadataValues.Set(gateIDMetadataKey, gateID)
	}
	return metadata.NewOutgoingContext(ctx, metadataValues), nil
}

func withIncomingRequestContext(ctx context.Context) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	metadataValues, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.New("gatelink: request metadata is required")
	}
	connectionID, err := singleMetadataValue(metadataValues, connectionIDMetadataKey, true)
	if err != nil {
		return nil, err
	}
	traceID, err := singleMetadataValue(metadataValues, traceIDMetadataKey, false)
	if err != nil {
		return nil, err
	}
	gateID, err := singleMetadataValue(metadataValues, gateIDMetadataKey, false)
	if err != nil {
		return nil, err
	}
	return WithGateRequestContext(ctx, GateRequestContext{
		TraceID: traceID,
		Source:  RequestSource{GateID: gateID, ConnectionID: connectionID},
	}), nil
}

func singleMetadataValue(values metadata.MD, key string, required bool) (string, error) {
	entries := values.Get(key)
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
