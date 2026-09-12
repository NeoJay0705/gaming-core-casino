package serversend

import (
	"context"
	"errors"

	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// GateDeliveryService implements the generated GateDelivery contract. The
// product-level grpcserver owns its listener and lifecycle; this service only
// 負責將 generic command ingress 轉交 dispatcher。
type GateDeliveryService struct {
	UnimplementedGateDeliveryServer
	commandDispatcher *dispatcher.Dispatcher
}

// NewGateDeliveryService builds a Gate delivery service without binding a
// listener。dispatcher 是唯一 dependency；各 product handler 在同一個
// RemoteCommandChannel namespace 註冊。
func NewGateDeliveryService(commandDispatcher *dispatcher.Dispatcher) (*GateDeliveryService, error) {
	if commandDispatcher == nil {
		return nil, errors.New("gate delivery: dispatcher is required")
	}
	return &GateDeliveryService{commandDispatcher: commandDispatcher}, nil
}

// Forward 接受與 Gate-to-Game Forward 相同的 opaque command shape。Gate product
// handler 僅由 dispatcher registration 選擇；此層永不解碼 business protobuf payload。
func (s *GateDeliveryService) Forward(ctx context.Context, request *gatelink.GateRequest) (*emptypb.Empty, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if s == nil || s.commandDispatcher == nil {
		return nil, status.Error(codes.Unavailable, ErrDispatcherUnavailable.Error())
	}
	_, err := dispatchRemoteCommand(ctx, s.commandDispatcher, Message{
		CommandID: request.GetCommandId(),
		Payload:   append([]byte(nil), request.GetPayload()...),
	})
	if err != nil {
		return nil, remoteCommandError(err)
	}
	return &emptypb.Empty{}, nil
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

var _ GateDeliveryServer = (*GateDeliveryService)(nil)
