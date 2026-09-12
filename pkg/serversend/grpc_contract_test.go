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
	"github.com/NeoJay0705/gaming-core-casino/pkg/logging"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestGRPCTransportContractForwardsOpaqueCommand(t *testing.T) {
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
	ctx, err := logging.ContinueOrNew(context.Background(), "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
	if err != nil {
		t.Fatal(err)
	}

	payload := []byte("opaque-command")
	if err := transport.Forward(ctx, endpoint, Message{CommandID: 12, Payload: payload}); err != nil {
		t.Fatalf("forward command: %v", err)
	}
	payload[0] = 'X'

	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	if receiver.remoteCommandID != 12 || string(receiver.remotePayload) != "opaque-command" {
		t.Fatalf("receiver remote command = id:%d payload:%q", receiver.remoteCommandID, receiver.remotePayload)
	}
	if receiver.traceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("receiver trace id = %q, want W3C trace ID", receiver.traceID)
	}
}

func TestGRPCTransportContractForwardsPlayerBatchEnvelope(t *testing.T) {
	receiver := &recordingReceiver{}
	server, err := NewReceiverServer(ReceiverConfig{ListenAddr: "127.0.0.1:0"}, receiver, PlayerDeliveryCommandID)
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
	commandPayload, err := proto.Marshal(&SendPlayersCommand{Messages: []*PlayerDelivery{{
		LoginName: "alice", ClientCommandId: 11, ClientPayload: []byte("opaque-client-payload"),
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.Forward(context.Background(), GateEndpoint{GateID: "gate-a", Address: server.Addr()}, Message{CommandID: PlayerDeliveryCommandID, Payload: commandPayload}); err != nil {
		t.Fatalf("forward player batch: %v", err)
	}
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	if receiver.remoteCommandID != PlayerDeliveryCommandID || string(receiver.remotePayload) != string(commandPayload) {
		t.Fatalf("receiver player command = id:%d payload:%q", receiver.remoteCommandID, receiver.remotePayload)
	}
}

func TestGateDeliveryForwardRejectsPayloadAboveProtocolLimit(t *testing.T) {
	dispatcher := dispatcher.New()
	if err := dispatcher.Register(RemoteCommandChannel, 1, func(context.Context, []byte) error { return nil }); err != nil {
		t.Fatal(err)
	}
	service, err := NewGateDeliveryService(dispatcher)
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.Forward(context.Background(), &gatelink.GateRequest{CommandId: 1, Payload: make([]byte, DefaultMaxPayloadBytes+1)})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), ErrPayloadTooLarge.Error()) {
		t.Fatalf("oversized Forward payload = response:%v error:%v, want InvalidArgument/ErrPayloadTooLarge", response, err)
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
	commandDispatcher := dispatcher.New()
	var calls int
	if err := commandDispatcher.Register(RemoteCommandChannel, 77, func(_ context.Context, payload []byte) error {
		calls++
		if string(payload) != "same-payload" {
			t.Fatalf("handler payload = %q, want same-payload", payload)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	service, err := NewGateDeliveryService(commandDispatcher)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Forward(context.Background(), &gatelink.GateRequest{CommandId: 77, Payload: []byte("same-payload")}); err != nil {
		t.Fatalf("gRPC Forward: %v", err)
	}
	encoded, err := marshalBroadcastCommand(77, []byte("same-payload"))
	if err != nil {
		t.Fatal(err)
	}
	subscriber := &RedisBroadcastSubscriber{commandDispatcher: commandDispatcher}
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
	service, err := NewGateDeliveryService(commandDispatcher)
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

	remoteCommandID uint32
	remotePayload   []byte
	traceID         string
}

func (r *recordingReceiver) HandleRemote(ctx context.Context, commandID uint32, payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.remoteCommandID = commandID
	r.remotePayload = append([]byte(nil), payload...)
	r.traceID, _, _ = logging.IDsFromContext(ctx)
	return nil
}
