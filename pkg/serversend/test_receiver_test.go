package serversend

import (
	"context"

	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/grpcserver"
	"google.golang.org/grpc"
)

// ReceiverConfig／NewReceiverServer 是 service contract tests 的組裝 helper。
// 正式 API 由 GateDeliveryService 與 product-level grpcserver 分離；此 helper
// 只在測試中建立同一個通用 listener。
type ReceiverConfig = grpcserver.Config

type testReceiverServer struct {
	service *GateDeliveryService
	server  *grpcserver.Server
}

func NewReceiverServer(cfg ReceiverConfig, receiver interface {
	HandleRemote(context.Context, uint32, []byte) error
}, commandIDs ...uint32) (*testReceiverServer, error) {
	commandDispatcher, err := newTestDeliveryDispatcher(receiver, commandIDs...)
	if err != nil {
		return nil, err
	}
	service, err := NewGateDeliveryService(commandDispatcher)
	if err != nil {
		return nil, err
	}
	server, err := grpcserver.New(cfg)
	if err != nil {
		return nil, err
	}
	if err := server.Register(GateDelivery_ServiceDesc.ServiceName, func(registrar grpc.ServiceRegistrar) {
		RegisterGateDeliveryServer(registrar, service)
	}); err != nil {
		return nil, err
	}
	return &testReceiverServer{service: service, server: server}, nil
}

func newTestDeliveryDispatcher(receiver interface {
	HandleRemote(context.Context, uint32, []byte) error
}, commandIDs ...uint32) (*dispatcher.Dispatcher, error) {
	commandDispatcher := dispatcher.New()
	for _, commandID := range commandIDs {
		commandID := commandID
		if err := commandDispatcher.Register(RemoteCommandChannel, dispatcher.CommandID(commandID), func(ctx context.Context, payload []byte) error {
			return receiver.HandleRemote(ctx, commandID, payload)
		}); err != nil {
			return nil, err
		}
	}
	return commandDispatcher, nil
}

func (s *testReceiverServer) Start(ctx context.Context) error { return s.server.Start(ctx) }
func (s *testReceiverServer) Stop(ctx context.Context) error  { return s.server.Stop(ctx) }
func (s *testReceiverServer) Addr() string                    { return s.server.Addr() }
