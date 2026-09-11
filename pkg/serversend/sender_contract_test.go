package serversend

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
)

func TestDirectRequestPlayerSenderContractUsesForwardReplySlot(t *testing.T) {
	sender, err := NewDirectRequestPlayerSender()
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("reply")
	var secondErr error
	server := newTestGateRequestServer(t, gatelink.RequestHandlerFunc(func(ctx context.Context, _ gatelink.Request) error {
		if _, err := sender.SendToRequestPlayer(ctx, RequestPlayerMessage{ExpectedLoginName: "alice", Message: Message{CommandID: 1, Payload: payload}}); err != nil {
			return err
		}
		secondErr = func() error {
			_, err := sender.SendToRequestPlayer(ctx, RequestPlayerMessage{Message: Message{CommandID: 2}})
			return err
		}()
		return nil
	}))
	response, err := server.Forward(context.Background(), &gatelink.GateRequest{CommandId: 99})
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	payload[0] = 'X'
	if !errors.Is(secondErr, ErrRequestReplyAlreadySet) {
		t.Fatalf("second reply error = %v, want ErrRequestReplyAlreadySet", secondErr)
	}
	if response.GetReply().GetCommandId() != 1 || string(response.GetReply().GetPayload()) != "reply" || response.GetReply().GetExpectedLoginName() != "alice" {
		t.Fatalf("forward reply = %#v, want first message", response.GetReply())
	}
}

func TestDirectRequestPlayerSenderContractRequiresActiveReplyAndPayloadBound(t *testing.T) {
	sender, err := NewDirectRequestPlayerSender()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.SendToRequestPlayer(context.Background(), RequestPlayerMessage{Message: Message{CommandID: 0}}); !errors.Is(err, ErrMessageInvalid) {
		t.Fatalf("invalid message error = %v, want ErrMessageInvalid", err)
	}
	if _, err := sender.SendToRequestPlayer(context.Background(), RequestPlayerMessage{Message: Message{CommandID: 1}}); !errors.Is(err, ErrRequestRouteUnavailable) {
		t.Fatalf("missing reply slot error = %v, want ErrRequestRouteUnavailable", err)
	}
	var senderErr error
	server := newTestGateRequestServer(t, gatelink.RequestHandlerFunc(func(ctx context.Context, _ gatelink.Request) error {
		_, senderErr = sender.SendToRequestPlayer(ctx, RequestPlayerMessage{Message: Message{CommandID: 1, Payload: make([]byte, DefaultMaxPayloadBytes+1)}})
		return nil
	}))
	response, err := server.Forward(context.Background(), &gatelink.GateRequest{CommandId: 99})
	if err != nil || response.GetReply() != nil {
		t.Fatalf("oversized reply forward = response:%#v error:%v, want no reply/nil", response, err)
	}
	if !errors.Is(senderErr, ErrPayloadTooLarge) {
		t.Fatalf("oversized reply sender error = %v, want ErrPayloadTooLarge", senderErr)
	}
}

func TestRoutedPlayerSenderContractReturnsPrimaryRouteError(t *testing.T) {
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	listCalls := 0
	directory := testDirectory{byID: map[GateID]GateEndpoint{}}
	directory.listHook = func() { listCalls++ }
	sender, err := NewRoutedPlayerSender(presenceResolverFunc(func(context.Context, LoginName) (Presence, error) {
		return Presence{}, ErrRouteStoreUnavailable
	}), directory, transport, nil)
	if err != nil {
		t.Fatal(err)
	}
	if receipt, err := sender.SendToPlayer(context.Background(), PlayerMessage{LoginName: "alice", Message: Message{CommandID: 2}}); !errors.Is(err, ErrRouteStoreUnavailable) || !receipt.AcceptedAt.IsZero() {
		t.Fatalf("route error = receipt:%#v error:%v, want empty/ErrRouteStoreUnavailable", receipt, err)
	}
	if listCalls != 0 {
		t.Fatalf("route error enumerated fan-out directory %d times, want 0", listCalls)
	}
}

func TestRoutedPlayerSenderContractDoesNotFallbackInvalidMessage(t *testing.T) {
	delivered, endpoint := newTestReceiverEndpoint(t, "gate-a", DeliveryStatus_DELIVERY_STATUS_DELIVERED)
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	directory := testDirectory{byID: map[GateID]GateEndpoint{endpoint.GateID: endpoint}, endpoints: []GateEndpoint{endpoint}}
	sender, err := NewRoutedPlayerSender(presenceResolverFunc(func(context.Context, LoginName) (Presence, error) {
		t.Fatal("invalid message reached primary presence resolver")
		return Presence{}, nil
	}), directory, transport, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.SendToPlayer(context.Background(), PlayerMessage{Message: Message{CommandID: 5}}); !errors.Is(err, ErrDestinationInvalid) {
		t.Fatalf("invalid player message error = %v, want ErrDestinationInvalid", err)
	}
	delivered.mu.Lock()
	defer delivered.mu.Unlock()
	if delivered.player.CommandID != 0 {
		t.Fatalf("invalid player message reached delivery path: %#v", delivered.player)
	}
}

func TestRoutedPlayerSenderContractFallsBackToAllGatesAfterPrimaryFailure(t *testing.T) {
	receiver, endpoint := newTestReceiverEndpoint(t, "gate-a", DeliveryStatus_DELIVERY_STATUS_DELIVERED, 6)
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	directory := testDirectory{endpoints: []GateEndpoint{endpoint}}
	fanout, err := NewFanoutSender(directory, transport)
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewRoutedPlayerSender(presenceResolverFunc(func(context.Context, LoginName) (Presence, error) {
		return Presence{}, ErrRouteStoreUnavailable
	}), directory, transport, fanout)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.SendToPlayer(context.Background(), PlayerMessage{LoginName: "alice", Message: Message{CommandID: 6, Payload: []byte("fallback")}}); err != nil {
		t.Fatalf("fallback player send: %v", err)
	}
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	if receiver.player.LoginName != "alice" || string(receiver.player.Payload) != "fallback" {
		t.Fatalf("fallback receiver message = %#v", receiver.player)
	}
}

func TestRoutedPlayerSenderContractDoesNotFallbackCanceledContext(t *testing.T) {
	_, endpoint := newTestReceiverEndpoint(t, "gate-a", DeliveryStatus_DELIVERY_STATUS_DELIVERED, 7)
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	listCalls := 0
	directory := testDirectory{endpoints: []GateEndpoint{endpoint}, listHook: func() { listCalls++ }}
	fanout, err := NewFanoutSender(directory, transport)
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewRoutedPlayerSender(presenceResolverFunc(func(ctx context.Context, _ LoginName) (Presence, error) {
		return Presence{}, ctx.Err()
	}), directory, transport, fanout)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sender.SendToPlayer(ctx, PlayerMessage{LoginName: "alice", Message: Message{CommandID: 7}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled player send error = %v, want context.Canceled", err)
	}
	if listCalls != 0 {
		t.Fatalf("canceled player send enumerated fallback directory %d times", listCalls)
	}
}

func TestFanoutSenderContractUsesEveryUniqueEndpointExactlyOnce(t *testing.T) {
	first, firstEndpoint := newTestReceiverEndpoint(t, "gate-a", DeliveryStatus_DELIVERY_STATUS_DELIVERED)
	second, secondEndpoint := newTestReceiverEndpoint(t, "gate-b", DeliveryStatus_DELIVERY_STATUS_DELIVERED)
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	directory, err := newStaticTestDirectory([]GateEndpoint{firstEndpoint, secondEndpoint, firstEndpoint})
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewFanoutSender(directory, transport)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.Broadcast(context.Background(), Message{CommandID: 4}); err != nil {
		t.Fatalf("fan-out broadcast: %v", err)
	}
	first.mu.Lock()
	firstCommand := first.remoteCommandID
	first.mu.Unlock()
	second.mu.Lock()
	secondCommand := second.remoteCommandID
	second.mu.Unlock()
	if firstCommand != 4 || secondCommand != 4 {
		t.Fatalf("broadcast commands = first:%d second:%d, want 4/4", firstCommand, secondCommand)
	}
}

func TestFanoutSenderContractRejectsEndpointLimitBeforeDelivery(t *testing.T) {
	first, firstEndpoint := newTestReceiverEndpoint(t, "gate-a", DeliveryStatus_DELIVERY_STATUS_DELIVERED)
	second, secondEndpoint := newTestReceiverEndpoint(t, "gate-b", DeliveryStatus_DELIVERY_STATUS_DELIVERED)
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	directory := testDirectory{endpoints: []GateEndpoint{firstEndpoint, secondEndpoint}}
	sender, err := NewFanoutSender(directory, transport, FanoutConfig{MaxEndpoints: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.Broadcast(context.Background(), Message{CommandID: 5}); !errors.Is(err, ErrFanoutLimitExceeded) {
		t.Fatalf("fan-out limit error = %v, want ErrFanoutLimitExceeded", err)
	}
	first.mu.Lock()
	firstDelivered := first.remoteCommandID
	first.mu.Unlock()
	second.mu.Lock()
	secondDelivered := second.remoteCommandID
	second.mu.Unlock()
	if firstDelivered != 0 || secondDelivered != 0 {
		t.Fatalf("fan-out limit sent before rejection: first:%d second:%d", firstDelivered, secondDelivered)
	}
}

func TestFallbackBroadcastSenderContractUsesGRPCAfterRedisFailure(t *testing.T) {
	primary := &recordingBroadcastSender{err: ErrRouteStoreUnavailable}
	fallback := &recordingBroadcastSender{receipt: Receipt{AcceptedAt: time.Now()}}
	sender, err := NewFallbackBroadcastSender(primary, fallback)
	if err != nil {
		t.Fatal(err)
	}
	message := Message{CommandID: 8}
	receipt, err := sender.Broadcast(context.Background(), message)
	if err != nil || receipt.AcceptedAt.IsZero() {
		t.Fatalf("fallback broadcast = receipt:%#v error:%v, want accepted/nil", receipt, err)
	}
	if primary.calls != 1 || fallback.calls != 1 {
		t.Fatalf("fallback broadcast calls = primary:%d fallback:%d, want 1/1", primary.calls, fallback.calls)
	}

	successPrimary := &recordingBroadcastSender{receipt: newReceipt()}
	successFallback := &recordingBroadcastSender{receipt: newReceipt()}
	successSender, err := NewFallbackBroadcastSender(successPrimary, successFallback)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := successSender.Broadcast(context.Background(), message); err != nil {
		t.Fatalf("successful primary broadcast = %v", err)
	}
	if successPrimary.calls != 1 || successFallback.calls != 0 {
		t.Fatalf("successful primary fallback calls = primary:%d fallback:%d, want 1/0", successPrimary.calls, successFallback.calls)
	}
}

func TestFallbackBroadcastSenderContractUsesGRPCAfterZeroRedisSubscribers(t *testing.T) {
	keys, err := NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	primary, err := NewRedisBroadcastSender(&recordingBroadcastPublisher{returnZero: true}, keys)
	if err != nil {
		t.Fatal(err)
	}
	fallback := &recordingBroadcastSender{receipt: newReceipt()}
	sender, err := NewFallbackBroadcastSender(primary, fallback)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.Broadcast(context.Background(), Message{CommandID: 9}); err != nil {
		t.Fatalf("zero-subscriber fallback = %v", err)
	}
	if fallback.calls != 1 {
		t.Fatalf("zero-subscriber fallback calls = %d, want 1", fallback.calls)
	}
}

func newTestReceiverEndpoint(t *testing.T, gateID GateID, playerStatus DeliveryStatus, commandIDs ...uint32) (*recordingReceiver, GateEndpoint) {
	t.Helper()
	receiver := &recordingReceiver{playerStatus: playerStatus}
	if len(commandIDs) == 0 {
		commandIDs = []uint32{4, 5}
	}
	server, err := NewReceiverServer(ReceiverConfig{ListenAddr: "127.0.0.1:0"}, receiver, commandIDs...)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })
	return receiver, GateEndpoint{GateID: gateID, Address: server.Addr()}
}

type presenceResolverFunc func(context.Context, LoginName) (Presence, error)

func (f presenceResolverFunc) Resolve(ctx context.Context, loginName LoginName) (Presence, error) {
	return f(ctx, loginName)
}

type testDirectory struct {
	byID      map[GateID]GateEndpoint
	endpoints []GateEndpoint
	listHook  func()
}

func (d testDirectory) Resolve(_ context.Context, gateID GateID) (GateEndpoint, error) {
	endpoint, ok := d.byID[gateID]
	if !ok {
		return GateEndpoint{}, ErrGateEndpointNotFound
	}
	return endpoint, nil
}

func (d testDirectory) List(context.Context) ([]GateEndpoint, error) {
	if d.listHook != nil {
		d.listHook()
	}
	return append([]GateEndpoint(nil), d.endpoints...), nil
}

type staticTestDirectory struct{ endpoints []GateEndpoint }

func newStaticTestDirectory(endpoints []GateEndpoint) (*staticTestDirectory, error) {
	return &staticTestDirectory{endpoints: endpoints}, nil
}

func (d *staticTestDirectory) Resolve(context.Context, GateID) (GateEndpoint, error) {
	return GateEndpoint{}, ErrGateEndpointNotFound
}

func (d *staticTestDirectory) List(context.Context) ([]GateEndpoint, error) {
	return append([]GateEndpoint(nil), d.endpoints...), nil
}

type recordingBroadcastSender struct {
	calls   int
	receipt Receipt
	err     error
}

func (s *recordingBroadcastSender) Broadcast(context.Context, Message) (Receipt, error) {
	s.calls++
	return s.receipt, s.err
}
