package serversend

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPCTransportContractDeliversTargetsAndPropagatesTrace(t *testing.T) {
	receiver := &recordingReceiver{}
	server, err := NewReceiverServer(ReceiverConfig{ListenAddr: "127.0.0.1:0"}, receiver)
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

	statusValue, err := transport.SendToConnection(ctx, endpoint, RequestPlayerMessage{ExpectedLoginName: "alice", Message: Message{CommandID: 11, Payload: []byte("direct")}}, "connection-1")
	if err != nil || statusValue != DeliveryStatus_DELIVERY_STATUS_DELIVERED {
		t.Fatalf("send to connection = status:%s error:%v, want delivered/nil", statusValue, err)
	}
	statusValue, err = transport.SendToPlayer(ctx, endpoint, PlayerMessage{LoginName: "alice", Message: Message{CommandID: 12, Payload: []byte("player")}})
	if err != nil || statusValue != DeliveryStatus_DELIVERY_STATUS_DELIVERED {
		t.Fatalf("send to player = status:%s error:%v, want delivered/nil", statusValue, err)
	}
	delivered, err := transport.BroadcastRoom(ctx, endpoint, BroadcastMessage{RoomID: "room-a", Message: Message{CommandID: 13, Payload: []byte("room")}})
	if err != nil || delivered != 2 {
		t.Fatalf("broadcast room = delivered:%d error:%v, want 2/nil", delivered, err)
	}

	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	if receiver.connectionID != "connection-1" || receiver.expectedLoginName != "alice" || receiver.player.LoginName != "alice" || receiver.broadcast.RoomID != "room-a" {
		t.Fatalf("receiver targets = connection:%q expected:%q player:%q room:%q", receiver.connectionID, receiver.expectedLoginName, receiver.player.LoginName, receiver.broadcast.RoomID)
	}
	if receiver.traceID != "trace-123" {
		t.Fatalf("receiver trace id = %q, want trace-123", receiver.traceID)
	}
}

func TestGRPCTransportContractMapsIgnoredAndValidationFailures(t *testing.T) {
	receiver := &recordingReceiver{playerStatus: DeliveryStatus_DELIVERY_STATUS_IGNORED}
	server, err := NewReceiverServer(ReceiverConfig{ListenAddr: "127.0.0.1:0"}, receiver)
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

func TestReceiverContractRejectsPayloadAboveConfiguredLimit(t *testing.T) {
	receiver := &recordingReceiver{}
	server, err := NewReceiverServer(ReceiverConfig{ListenAddr: "127.0.0.1:0", MaxPayloadBytes: 3}, receiver)
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.SendToPlayer(context.Background(), &SendToPlayerRequest{LoginName: "alice", CommandId: 1, Payload: []byte("1234")})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), ErrPayloadTooLarge.Error()) {
		t.Fatalf("oversized receiver payload = response:%v error:%v, want InvalidArgument/ErrPayloadTooLarge", response, err)
	}
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	if receiver.player.CommandID != 0 {
		t.Fatalf("oversized payload reached local receiver: %#v", receiver.player)
	}
}

func TestGRPCTransportContractRejectsPayloadAboveConfiguredLimit(t *testing.T) {
	transport, err := NewGRPCTransport(TransportConfig{MaxPayloadBytes: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transport.SendToPlayer(context.Background(), GateEndpoint{GateID: "gate-a", Address: "127.0.0.1:1"}, PlayerMessage{LoginName: "alice", Message: Message{CommandID: 1, Payload: []byte("1234")}}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversized transport payload = %v, want ErrPayloadTooLarge", err)
	}
}

type recordingReceiver struct {
	mu sync.Mutex

	connectionID      ConnectionID
	expectedLoginName LoginName
	player            PlayerMessage
	broadcast         BroadcastMessage
	traceID           string
	playerStatus      DeliveryStatus
}

func (r *recordingReceiver) SendToConnection(ctx context.Context, connectionID ConnectionID, expectedLoginName LoginName, message Message) (DeliveryStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.connectionID, r.expectedLoginName = connectionID, expectedLoginName
	r.traceID, _ = receivedTraceIDFromContext(ctx)
	return DeliveryStatus_DELIVERY_STATUS_DELIVERED, nil
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

func (r *recordingReceiver) BroadcastRoom(ctx context.Context, message BroadcastMessage) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.broadcast = message
	r.traceID, _ = receivedTraceIDFromContext(ctx)
	return 2, nil
}

func receivedTraceIDFromContext(ctx context.Context) (string, bool) {
	value, ok := RequestContextFrom(ctx)
	return value.TraceID, ok
}
