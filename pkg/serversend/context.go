package serversend

import (
	"context"
	"strings"

	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
)

type requestContextKey struct{}

// RequestContext contains metadata propagated with one Game-to-Gate delivery.
// It deliberately stays transport-neutral and can grow without changing every
// sender or receiver signature.
type RequestContext struct{ TraceID string }

// WithRequestContext attaches one Server Send metadata value to ctx.
func WithRequestContext(ctx context.Context, value RequestContext) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requestContextKey{}, value)
}

// RequestContextFrom returns the metadata attached to ctx.
func RequestContextFrom(ctx context.Context) (RequestContext, bool) {
	if ctx == nil {
		return RequestContext{}, false
	}
	value, ok := ctx.Value(requestContextKey{}).(RequestContext)
	return value, ok
}

// traceIDFromContext reads the Server Send carrier first, then the broader
// Gate request carrier. This keeps trace propagation stable when a request has
// both transport-neutral and Server Send-specific context values.
func traceIDFromContext(ctx context.Context) string {
	if value, ok := RequestContextFrom(ctx); ok {
		if traceID := strings.TrimSpace(value.TraceID); traceID != "" {
			return traceID
		}
	}
	if value, ok := gatelink.GateRequestContextFrom(ctx); ok {
		return strings.TrimSpace(value.TraceID)
	}
	return ""
}
