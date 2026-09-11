package serversend

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestGRPCTransportContractForwardsOpaqueCommandAndKeepsPlayerDelivery(t *testing.T) {
	receiver := &recordingReceiver{}
	server, err := NewReceiverServer(ReceiverConfig{ListenAddr: "127.0.0.1:0"}, receiver, 12)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	endpoint := GateEndpoint{GateID: "gate-a", Address: server.Addr()}
	ctx := WithRequestContext(context.Background(), RequestContext{TraceID: "trace-123"})

	statusValue, err := transport.SendToPlayer(ctx, endpoint, PlayerMessage{LoginName: "alice", Message: Message{CommandID: 11, Payload: []byte("player")}})
	if err != nil || statusValue != DeliveryStatus_DELIVERY_STATUS_DELIVERED {
		t.Fatalf("send to player = status:%s error:%v, want delivered/nil", statusValue, err)
	}
	payload := []byte("opaque-command")
	if err := transport.Forward(ctx, endpoint, Message{CommandID: 12, Payload: payload}); err != nil {
		t.Fatalf("forward command: %v", err)
	}
	payload[0] = 'X'

	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	if receiver.player.LoginName != "alice" {
		t.Fatalf("receiver player = %q, want alice", receiver.player.LoginName)
	}
	if receiver.remoteCommandID != 12 || string(receiver.remotePayload) != "opaque-command" {
		t.Fatalf("receiver remote command = id:%d payload:%q", receiver.remoteCommandID, receiver.remotePayload)
	}
	if receiver.traceID != "trace-123" {
		t.Fatalf("receiver trace id = %q, want trace-123", receiver.traceID)
	}
}

func TestGRPCTransportContractMapsPlayerIgnoredAndValidationFailures(t *testing.T) {
	receiver := &recordingReceiver{playerStatus: DeliveryStatus_DELIVERY_STATUS_IGNORED}
	server, err := NewReceiverServer(ReceiverConfig{ListenAddr: "127.0.0.1:0"}, receiver, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })
	transport, err := NewGRPCTransport(TransportConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	endpoint := GateEndpoint{GateID: "gate-a", Address: server.Addr()}
	if _, err := transport.SendToPlayer(context.Background(), endpoint, PlayerMessage{LoginName: "alice", Message: Message{CommandID: 1}}); !errors.Is(err, ErrTargetNotConnected) {
		t.Fatalf("ignored player error = %v, want ErrTargetNotConnected", err)
	}
	if _, err := transport.SendToPlayer(context.Background(), endpoint, PlayerMessage{LoginName: "alice"}); !errors.Is(err, ErrMessageInvalid) {
		t.Fatalf("invalid message error = %v, want ErrMessageInvalid", err)
	}
	if _, err := transport.SendToPlayer(context.Background(), GateEndpoint{GateID: "gate-a", Address: "bad"}, PlayerMessage{LoginName: "alice", Message: Message{CommandID: 1}}); !errors.Is(err, ErrDestinationInvalid) {
		t.Fatalf("invalid endpoint error = %v, want ErrDestinationInvalid", err)
	}
}

func TestGateDeliveryForwardRejectsPayloadAboveProtocolLimit(t *testing.T) {
	receiver := &recordingReceiver{}
	server, err := NewReceiverServer(ReceiverConfig{ListenAddr: "127.0.0.1:0"}, receiver, 1)
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.service.Forward(context.Background(), &gatelink.GateRequest{CommandId: 1, Payload: make([]byte, DefaultMaxPayloadBytes+1)})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), ErrPayloadTooLarge.Error()) {
		t.Fatalf("oversized Forward payload = response:%v error:%v, want InvalidArgument/ErrPayloadTooLarge", response, err)
	}
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	if receiver.remoteCommandID != 0 {
		t.Fatalf("oversized payload reached remote handler: %#v", receiver.remotePayload)
	}
}

func TestGRPCTransportContractRejectsPayloadAboveProtocolLimit(t *testing.T) {
	transport, err := NewGRPCTransport(TransportConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.Forward(context.Background(), GateEndpoint{GateID: "gate-a", Address: "127.0.0.1:1"}, Message{CommandID: 1, Payload: make([]byte, DefaultMaxPayloadBytes+1)}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversized transport payload = %v, want ErrPayloadTooLarge", err)
	}
}

func TestGenericIngressesShareOneDispatcher(t *testing.T) {
	dispatcher := dispatcher.New()
	var calls int
	if err := dispatcher.Register(RemoteCommandChannel, 77, func(_ context.Context, payload []byte) error {
		calls++
		if string(payload) != "same-payload" {
			t.Fatalf("handler payload = %q, want same-payload", payload)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	receiver := &recordingReceiver{}
	service, err := NewGateDeliveryService(receiver, dispatcher)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Forward(context.Background(), &gatelink.GateRequest{CommandId: 77, Payload: []byte("same-payload")}); err != nil {
		t.Fatalf("gRPC Forward: %v", err)
	}
	encoded, err := proto.Marshal(&gatelink.GateRequest{CommandId: 77, Payload: []byte("same-payload")})
	if err != nil {
		t.Fatal(err)
	}
	subscriber := &RedisBroadcastSubscriber{commandDispatcher: dispatcher}
	if err := subscriber.handle(context.Background(), encoded); err != nil {
		t.Fatalf("Redis subscriber: %v", err)
	}
	if calls != 2 {
		t.Fatalf("shared dispatcher handler calls = %d, want 2", calls)
	}
}

func TestGateDeliveryForwardMapsRegistrationAndHandlerErrors(t *testing.T) {
	commandDispatcher := dispatcher.New()
	if err := commandDispatcher.Register(RemoteCommandChannel, 78, func(context.Context, []byte) error {
		return errors.New("handler failed")
	}); err != nil {
		t.Fatal(err)
	}
	receiver := &recordingReceiver{}
	service, err := NewGateDeliveryService(receiver, commandDispatcher)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Forward(context.Background(), &gatelink.GateRequest{CommandId: 79}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("unregistered Forward status = %s, want %s", status.Code(err), codes.Unimplemented)
	}
	if _, err := service.Forward(context.Background(), &gatelink.GateRequest{CommandId: 78}); status.Code(err) != codes.Internal {
		t.Fatalf("handler Forward status = %s, want %s", status.Code(err), codes.Internal)
	}
}

type recordingReceiver struct {
	mu sync.Mutex

	player          PlayerMessage
	remoteCommandID uint32
	remotePayload   []byte
	traceID         string
	playerStatus    DeliveryStatus
}

func (r *recordingReceiver) SendToPlayer(ctx context.Context, message PlayerMessage) (DeliveryStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.player = message
	r.traceID, _ = receivedTraceIDFromContext(ctx)
	if r.playerStatus != DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED {
		return r.playerStatus, nil
	}
	return DeliveryStatus_DELIVERY_STATUS_DELIVERED, nil
}

func (r *recordingReceiver) HandleRemote(ctx context.Context, commandID uint32, payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.remoteCommandID = commandID
	r.remotePayload = append([]byte(nil), payload...)
	r.traceID, _ = receivedTraceIDFromContext(ctx)
	return nil
}

func receivedTraceIDFromContext(ctx context.Context) (string, bool) {
	value, ok := RequestContextFrom(ctx)
	return value.TraceID, ok
}
