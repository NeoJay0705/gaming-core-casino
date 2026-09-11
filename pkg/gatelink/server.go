package gatelink

import (
	"context"
	"errors"
	"reflect"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// RequestHandler is the Game product registration point for requests received
// from Gate. The payload is opaque transport data; handlers must accept ctx to
// access request metadata and cancellation.
type RequestHandler interface {
	HandleGateRequest(context.Context, Request) error
}

// RequestHandlerFunc adapts a function into RequestHandler.
type RequestHandlerFunc func(context.Context, Request) error

func (f RequestHandlerFunc) HandleGateRequest(ctx context.Context, request Request) error {
	return f(ctx, request)
}

// GateRequestService implements the generated GateRequestService contract.
// The product-level grpcserver owns its listener and lifecycle; this service
// only validates requests and invokes the registered product handler.
type GateRequestService struct {
	UnimplementedGateRequestServiceServer
	handler RequestHandler
}

// NewGateRequestService builds a Game-side Gate request service. A missing
// handler is accepted so an unconfigured request returns Unimplemented rather
// than being silently dropped.
func NewGateRequestService(handler RequestHandler) (*GateRequestService, error) {
	return &GateRequestService{handler: handler}, nil
}

func isNilRequestHandler(handler RequestHandler) bool {
	if handler == nil {
		return true
	}
	value := reflect.ValueOf(handler)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// Forward delivers the opaque binary payload and the single request metadata
// carrier to the registered Game handler, then returns its optional reply.
// gRPC supplies incoming metadata; direct contract tests may call Forward with
// an already decorated context and therefore do not need a network metadata
// object.
func (s *GateRequestService) Forward(ctx context.Context, request *GateRequest) (*ForwardResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if request.GetCommandId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "command_id is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := metadata.FromIncomingContext(ctx); ok {
		parsed, err := withIncomingRequestContext(ctx)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		ctx = parsed
	}
	if s == nil || isNilRequestHandler(s.handler) {
		return nil, status.Error(codes.Unimplemented, "gate request handler is not configured")
	}
	replySlot := newForwardReplySlot()
	defer replySlot.close()
	ctx = withForwardReplySlot(ctx, replySlot)
	handlerRequest := Request{
		CommandID: request.GetCommandId(),
		Payload:   append([]byte(nil), request.GetPayload()...),
	}
	handlerErr := s.handler.HandleGateRequest(ctx, handlerRequest)
	reply := replySlot.finish(handlerErr == nil)
	if handlerErr != nil {
		if status.Code(handlerErr) != codes.Unknown {
			return nil, handlerErr
		}
		if errors.Is(handlerErr, context.Canceled) {
			return nil, status.Error(codes.Canceled, handlerErr.Error())
		}
		if errors.Is(handlerErr, context.DeadlineExceeded) {
			return nil, status.Error(codes.DeadlineExceeded, handlerErr.Error())
		}
		return nil, status.Error(codes.Internal, "gate request handler failed")
	}
	response := &ForwardResponse{}
	if reply != nil {
		response.Reply = &ForwardReply{
			CommandId:         reply.CommandID,
			Payload:           reply.Payload,
			ExpectedLoginName: reply.ExpectedLoginName,
		}
	}
	return response, nil
}

var _ GateRequestServiceServer = (*GateRequestService)(nil)
