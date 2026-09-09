package serversend

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const traceIDMetadataKey = "x-server-send-trace-id"

// LocalReceiver is the Gate-local delivery boundary. It does not expose
// WebSocket framing or transport details to Game-side callers.
type LocalReceiver interface {
	SendToConnection(context.Context, ConnectionID, LoginName, Message) (DeliveryStatus, error)
	SendToPlayer(context.Context, PlayerMessage) (DeliveryStatus, error)
	BroadcastRoom(context.Context, BroadcastMessage) (delivered int, err error)
}

// ReceiverConfig configures the Gate-side Game-to-Gate gRPC listener.
type ReceiverConfig struct {
	ListenAddr      string `config:"listen_addr" yaml:"listen_addr"`
	MaxPayloadBytes int    `config:"max_payload_bytes" yaml:"max_payload_bytes"`
}

// ReceiverServer owns one Gate delivery listener.
type ReceiverServer struct {
	UnimplementedGateDeliveryServer

	cfg      ReceiverConfig
	receiver LocalReceiver

	mu       sync.Mutex
	server   *grpc.Server
	listener net.Listener
	stopping bool
}

// NewReceiverServer builds a Gate delivery listener without binding its port.
func NewReceiverServer(cfg ReceiverConfig, receiver LocalReceiver) (*ReceiverServer, error) {
	var err error
	if cfg.ListenAddr, err = validateListenAddr(cfg.ListenAddr); err != nil {
		return nil, err
	}
	if cfg.MaxPayloadBytes, err = normalizedPayloadLimit(cfg.MaxPayloadBytes); err != nil {
		return nil, err
	}
	if isNilLocalReceiver(receiver) {
		return nil, errors.New("server send: local receiver is required")
	}
	return &ReceiverServer{cfg: cfg, receiver: receiver}, nil
}

func validateListenAddr(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", errors.New("server send: listen address is required")
	}
	if _, _, err := net.SplitHostPort(value); err != nil {
		return "", fmt.Errorf("server send: invalid listen address %q: %w", value, err)
	}
	return value, nil
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

// Start binds the listener synchronously before serving requests.
func (s *ReceiverServer) Start(ctx context.Context) error {
	if s == nil {
		return errors.New("server send: receiver server is nil")
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		return errors.New("server send: receiver server is stopped")
	}
	if s.server != nil {
		return errors.New("server send: receiver server is already started")
	}
	listener, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("server send: listen on %s: %w", s.cfg.ListenAddr, err)
	}
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(incomingRequestContextInterceptor, recoveryInterceptor))
	RegisterGateDeliveryServer(server, s)
	s.listener, s.server = listener, server
	go func() { _ = server.Serve(listener) }()
	return nil
}

// Stop drains active RPCs until ctx expires, then force-stops the listener.
func (s *ReceiverServer) Stop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	server := s.server
	s.stopping = true
	s.server, s.listener = nil, nil
	s.mu.Unlock()
	if server == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		server.Stop()
		return ctx.Err()
	}
}

// Addr returns the bound address, including a port selected by :0.
func (s *ReceiverServer) Addr() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *ReceiverServer) SendToConnection(ctx context.Context, request *SendToConnectionRequest) (*DeliveryResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	message := RequestPlayerMessage{ExpectedLoginName: LoginName(request.GetExpectedLoginName()), Message: Message{CommandID: request.GetCommandId(), Payload: append([]byte(nil), request.GetPayload()...)}}
	if request.GetConnectionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "connection_id is required")
	}
	if err := message.validatePayload(s.cfg.MaxPayloadBytes); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	delivery, err := s.receiver.SendToConnection(ctx, ConnectionID(request.GetConnectionId()), message.ExpectedLoginName, message.Message)
	if err != nil {
		return nil, receiverError(err)
	}
	return deliveryResponse(delivery, boolToCount(delivery == DeliveryStatus_DELIVERY_STATUS_DELIVERED)), nil
}

func (s *ReceiverServer) SendToPlayer(ctx context.Context, request *SendToPlayerRequest) (*DeliveryResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	message := PlayerMessage{LoginName: LoginName(request.GetLoginName()), Message: Message{CommandID: request.GetCommandId(), Payload: append([]byte(nil), request.GetPayload()...)}}
	if err := message.validatePayload(s.cfg.MaxPayloadBytes); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	delivery, err := s.receiver.SendToPlayer(ctx, message)
	if err != nil {
		return nil, receiverError(err)
	}
	return deliveryResponse(delivery, boolToCount(delivery == DeliveryStatus_DELIVERY_STATUS_DELIVERED)), nil
}

func (s *ReceiverServer) BroadcastRoom(ctx context.Context, request *BroadcastRoomRequest) (*DeliveryResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	message := BroadcastMessage{RoomID: RoomID(request.GetRoomId()), Message: Message{CommandID: request.GetCommandId(), Payload: append([]byte(nil), request.GetPayload()...)}}
	if err := message.validatePayload(s.cfg.MaxPayloadBytes); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	delivered, err := s.receiver.BroadcastRoom(ctx, message)
	if err != nil {
		return nil, receiverError(err)
	}
	if delivered < 0 {
		return nil, status.Error(codes.Internal, "receiver returned invalid delivery count")
	}
	statusValue := DeliveryStatus_DELIVERY_STATUS_IGNORED
	if delivered > 0 {
		statusValue = DeliveryStatus_DELIVERY_STATUS_DELIVERED
	}
	return deliveryResponse(statusValue, delivered), nil
}

func deliveryResponse(delivery DeliveryStatus, delivered int) *DeliveryResponse {
	return &DeliveryResponse{Status: delivery, DeliveredCount: uint32(delivered)}
}

func boolToCount(value bool) int {
	if value {
		return 1
	}
	return 0
}

func receiverError(err error) error {
	if errors.Is(err, ErrTargetNotConnected) {
		return status.Error(codes.NotFound, err.Error())
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

func incomingRequestContextInterceptor(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	values, _ := metadata.FromIncomingContext(ctx)
	traceIDs := values.Get(traceIDMetadataKey)
	if len(traceIDs) > 1 {
		return nil, status.Error(codes.InvalidArgument, "server send request contains duplicate trace id")
	}
	if len(traceIDs) == 1 {
		ctx = WithRequestContext(ctx, RequestContext{TraceID: strings.TrimSpace(traceIDs[0])})
	}
	return handler(ctx, request)
}

func recoveryInterceptor(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Printf("[server send] unary handler panicked: method=%s panic=%v", info.FullMethod, recovered)
			response = nil
			err = status.Error(codes.Internal, "gate delivery failed")
		}
	}()
	return handler(ctx, request)
}

var _ GateDeliveryServer = (*ReceiverServer)(nil)
var _ interface {
	Start(context.Context) error
	Stop(context.Context) error
} = (*ReceiverServer)(nil)
