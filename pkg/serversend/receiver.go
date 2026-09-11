package serversend

import (
	"context"
	"errors"
	"reflect"
	"strings"

	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const traceIDMetadataKey = "x-server-send-trace-id"

// LocalReceiver 是 Gate-local 的 exact player delivery boundary。generic remote
// command 直接使用 Dispatcher，不在此轉成 player 或 room delivery request。
type LocalReceiver interface {
	SendToPlayer(context.Context, PlayerMessage) (DeliveryStatus, error)
}

// GateDeliveryService implements the generated GateDelivery contract. The
// product-level grpcserver owns its listener and lifecycle; this service only
// 負責轉接 exact player delivery 與 generic command ingress。
type GateDeliveryService struct {
	UnimplementedGateDeliveryServer
	receiver          LocalReceiver
	commandDispatcher *dispatcher.Dispatcher
}

// NewGateDeliveryService builds a Gate delivery service without binding a
// listener。兩個 dependency 都是必要的，因為 service 保留既有 SendToPlayer
// contract，並新增 generic Forward ingress。
func NewGateDeliveryService(receiver LocalReceiver, commandDispatcher *dispatcher.Dispatcher) (*GateDeliveryService, error) {
	if isNilLocalReceiver(receiver) {
		return nil, errors.New("gate delivery: local receiver is required")
	}
	if commandDispatcher == nil {
		return nil, errors.New("gate delivery: dispatcher is required")
	}
	return &GateDeliveryService{receiver: receiver, commandDispatcher: commandDispatcher}, nil
}

func isNilLocalReceiver(receiver LocalReceiver) bool {
	if receiver == nil {
		return true
	}
	value := reflect.ValueOf(receiver)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (s *GateDeliveryService) SendToPlayer(ctx context.Context, request *SendToPlayerRequest) (*DeliveryResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	ctx, err := withIncomingRequestContext(ctx)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	message := PlayerMessage{LoginName: LoginName(request.GetLoginName()), Message: Message{CommandID: request.GetCommandId(), Payload: append([]byte(nil), request.GetPayload()...)}}
	if err := message.validatePayload(DefaultMaxPayloadBytes); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if s == nil || isNilLocalReceiver(s.receiver) {
		return nil, status.Error(codes.Unavailable, "gate delivery receiver is not configured")
	}
	delivery, err := s.receiver.SendToPlayer(ctx, message)
	if err != nil {
		return nil, receiverError(err)
	}
	if delivery != DeliveryStatus_DELIVERY_STATUS_DELIVERED && delivery != DeliveryStatus_DELIVERY_STATUS_IGNORED {
		return nil, status.Error(codes.Internal, "gate delivery receiver returned an invalid status")
	}
	return deliveryResponse(delivery), nil
}

// Forward 接受與 Gate-to-Game Forward 相同的 opaque command shape。Gate product
// handler 僅由 dispatcher registration 選擇；此層永不解碼 business protobuf payload。
func (s *GateDeliveryService) Forward(ctx context.Context, request *gatelink.GateRequest) (*emptypb.Empty, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	ctx, err := withIncomingRequestContext(ctx)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if s == nil || s.commandDispatcher == nil {
		return nil, status.Error(codes.Unavailable, ErrDispatcherUnavailable.Error())
	}
	_, err = dispatchRemoteCommand(ctx, s.commandDispatcher, Message{
		CommandID: request.GetCommandId(),
		Payload:   append([]byte(nil), request.GetPayload()...),
	})
	if err != nil {
		return nil, remoteCommandError(err)
	}
	return &emptypb.Empty{}, nil
}

func deliveryResponse(delivery DeliveryStatus) *DeliveryResponse {
	count := uint32(0)
	if delivery == DeliveryStatus_DELIVERY_STATUS_DELIVERED {
		count = 1
	}
	return &DeliveryResponse{Status: delivery, DeliveredCount: count}
}

func receiverError(err error) error {
	if errors.Is(err, ErrTargetNotConnected) {
		return status.Error(codes.NotFound, err.Error())
	}
	return remoteCommandError(err)
}

func remoteCommandError(err error) error {
	if errors.Is(err, ErrCommandNotRegistered) {
		return status.Error(codes.Unimplemented, err.Error())
	}
	if errors.Is(err, ErrMessageInvalid) || errors.Is(err, ErrDestinationInvalid) || errors.Is(err, ErrPayloadTooLarge) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, ErrDispatcherUnavailable) {
		return status.Error(codes.Unavailable, err.Error())
	}
	if status.Code(err) != codes.Unknown {
		return err
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	return status.Error(codes.Internal, "gate delivery failed")
}

func outgoingRequestContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	traceID := traceIDFromContext(ctx)
	if strings.TrimSpace(traceID) == "" {
		return ctx
	}
	values, _ := metadata.FromOutgoingContext(ctx)
	values = values.Copy()
	values.Set(traceIDMetadataKey, strings.TrimSpace(traceID))
	return metadata.NewOutgoingContext(ctx, values)
}

func withIncomingRequestContext(ctx context.Context) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	values, _ := metadata.FromIncomingContext(ctx)
	traceIDs := values.Get(traceIDMetadataKey)
	if len(traceIDs) > 1 {
		return nil, status.Error(codes.InvalidArgument, "gate delivery request contains duplicate trace id")
	}
	if len(traceIDs) == 1 {
		ctx = WithRequestContext(ctx, RequestContext{TraceID: strings.TrimSpace(traceIDs[0])})
	}
	return ctx, nil
}

var _ GateDeliveryServer = (*GateDeliveryService)(nil)
