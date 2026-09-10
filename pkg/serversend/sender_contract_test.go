package serversend

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
)

func TestDirectRequestPlayerSenderContractUsesForwardReplySlot(t *testing.T) {
	sender, err := NewDirectRequestPlayerSender(0)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("reply")
	var secondErr error
	server, err := gatelink.NewServer(gatelink.ServerConfig{ListenAddr: "127.0.0.1:0"}, gatelink.RequestHandlerFunc(func(ctx context.Context, _ gatelink.Request) error {
		if _, err := sender.SendToRequestPlayer(ctx, RequestPlayerMessage{ExpectedLoginName: "alice", Message: Message{CommandID: 1, Payload: payload}}); err != nil {
			return err
		}
		secondErr = func() error {
			_, err := sender.SendToRequestPlayer(ctx, RequestPlayerMessage{Message: Message{CommandID: 2}})
			return err
		}()
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
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
	sender, err := NewDirectRequestPlayerSender(3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.SendToRequestPlayer(context.Background(), RequestPlayerMessage{Message: Message{CommandID: 0}}); !errors.Is(err, ErrMessageInvalid) {
		t.Fatalf("invalid message error = %v, want ErrMessageInvalid", err)
	}
	if _, err := sender.SendToRequestPlayer(context.Background(), RequestPlayerMessage{Message: Message{CommandID: 1}}); !errors.Is(err, ErrRequestRouteUnavailable) {
		t.Fatalf("missing reply slot error = %v, want ErrRequestRouteUnavailable", err)
	}
	if _, err := NewDirectRequestPlayerSender(-1); err == nil || !strings.Contains(err.Error(), "max payload") {
		t.Fatalf("negative payload limit error = %v, want payload validation", err)
	}
	var senderErr error
	server, err := gatelink.NewServer(gatelink.ServerConfig{ListenAddr: "127.0.0.1:0"}, gatelink.RequestHandlerFunc(func(ctx context.Context, _ gatelink.Request) error {
		_, senderErr = sender.SendToRequestPlayer(ctx, RequestPlayerMessage{Message: Message{CommandID: 1, Payload: []byte("1234")}})
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.Forward(context.Background(), &gatelink.GateRequest{CommandId: 99})
	if err != nil || response.GetReply() != nil {
		t.Fatalf("oversized reply forward = response:%#v error:%v, want no reply/nil", response, err)
	}
	if !errors.Is(senderErr, ErrPayloadTooLarge) {
		t.Fatalf("oversized reply sender error = %v, want ErrPayloadTooLarge", senderErr)
	}
}

func TestRoutedPlayerSenderContractFansOutAfterPrimaryRouteErrors(t *testing.T) {
	ignored, ignoredEndpoint := newTestReceiverEndpoint(t, "gate-a", DeliveryStatus_DELIVERY_STATUS_IGNORED)
	delivered, deliveredEndpoint := newTestReceiverEndpoint(t, "gate-b", DeliveryStatus_DELIVERY_STATUS_DELIVERED)
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	directory := testDirectory{byID: map[GateID]GateEndpoint{ignoredEndpoint.GateID: ignoredEndpoint, deliveredEndpoint.GateID: deliveredEndpoint}, endpoints: []GateEndpoint{ignoredEndpoint, deliveredEndpoint}}
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
	if receipt, err := sender.SendToPlayer(context.Background(), PlayerMessage{LoginName: "alice", Message: Message{CommandID: 2, Payload: []byte("private")}}); err != nil || receipt.AcceptedAt.IsZero() {
		t.Fatalf("route store fallback = receipt:%#v error:%v, want accepted/nil", receipt, err)
	}
	ignored.mu.Lock()
	ignoredCount := ignored.player.CommandID
	ignored.mu.Unlock()
	delivered.mu.Lock()
	deliveredCount := delivered.player.CommandID
	delivered.mu.Unlock()
	if ignoredCount != 2 || deliveredCount != 2 {
		t.Fatalf("private route fallback commands = ignored:%d delivered:%d, want 2/2", ignoredCount, deliveredCount)
	}

	missing, err := NewRoutedPlayerSender(presenceResolverFunc(func(context.Context, LoginName) (Presence, error) {
		return Presence{}, ErrPresenceNotFound
	}), directory, transport, fanout)
	if err != nil {
		t.Fatal(err)
	}
	if receipt, err := missing.SendToPlayer(context.Background(), PlayerMessage{LoginName: "missing", Message: Message{CommandID: 3}}); err != nil || receipt.AcceptedAt.IsZero() {
		t.Fatalf("missing presence fallback = receipt:%#v error:%v, want accepted/nil", receipt, err)
	}

	endpointFailure, err := NewRoutedPlayerSender(presenceResolverFunc(func(context.Context, LoginName) (Presence, error) {
		return Presence{GateID: "gate-a"}, nil
	}), testDirectory{byID: map[GateID]GateEndpoint{}, endpoints: []GateEndpoint{ignoredEndpoint, deliveredEndpoint}}, transport, fanout)
	if err != nil {
		t.Fatal(err)
	}
	if receipt, err := endpointFailure.SendToPlayer(context.Background(), PlayerMessage{LoginName: "alice", Message: Message{CommandID: 4}}); err != nil || receipt.AcceptedAt.IsZero() {
		t.Fatalf("endpoint route fallback = receipt:%#v error:%v, want accepted/nil", receipt, err)
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
	fanout, err := NewFanoutSender(directory, transport)
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewRoutedPlayerSender(presenceResolverFunc(func(context.Context, LoginName) (Presence, error) {
		t.Fatal("invalid message reached primary presence resolver")
		return Presence{}, nil
	}), directory, transport, fanout)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.SendToPlayer(context.Background(), PlayerMessage{Message: Message{CommandID: 5}}); !errors.Is(err, ErrDestinationInvalid) {
		t.Fatalf("invalid player message error = %v, want ErrDestinationInvalid", err)
	}
	delivered.mu.Lock()
	defer delivered.mu.Unlock()
	if delivered.player.CommandID != 0 {
		t.Fatalf("invalid player message reached fallback: %#v", delivered.player)
	}
}

func TestRoutedPlayerSenderContractFansOutAfterPrimaryTransportError(t *testing.T) {
	ignored, ignoredEndpoint := newTestReceiverEndpoint(t, "gate-a", DeliveryStatus_DELIVERY_STATUS_IGNORED)
	delivered, deliveredEndpoint := newTestReceiverEndpoint(t, "gate-b", DeliveryStatus_DELIVERY_STATUS_DELIVERED)
	downListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	downAddress := downListener.Addr().String()
	if err := downListener.Close(); err != nil {
		t.Fatal(err)
	}
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	directory := testDirectory{
		byID:      map[GateID]GateEndpoint{"gate-down": {GateID: "gate-down", Address: downAddress}},
		endpoints: []GateEndpoint{ignoredEndpoint, deliveredEndpoint},
	}
	fanout, err := NewFanoutSender(directory, transport)
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewRoutedPlayerSender(presenceResolverFunc(func(context.Context, LoginName) (Presence, error) {
		return Presence{GateID: "gate-down"}, nil
	}), directory, transport, fanout)
	if err != nil {
		t.Fatal(err)
	}
	if receipt, err := sender.SendToPlayer(context.Background(), PlayerMessage{LoginName: "alice", Message: Message{CommandID: 6}}); err != nil || receipt.AcceptedAt.IsZero() {
		t.Fatalf("transport fallback = receipt:%#v error:%v, want accepted/nil", receipt, err)
	}
	ignored.mu.Lock()
	ignoredCommand := ignored.player.CommandID
	ignored.mu.Unlock()
	delivered.mu.Lock()
	deliveredCommand := delivered.player.CommandID
	delivered.mu.Unlock()
	if ignoredCommand != 6 || deliveredCommand != 6 {
		t.Fatalf("transport fallback commands = ignored:%d delivered:%d, want 6/6", ignoredCommand, deliveredCommand)
	}
}

func TestFanoutSenderContractPlayerAllIgnoredReturnsNotConnected(t *testing.T) {
	first, firstEndpoint := newTestReceiverEndpoint(t, "gate-a", DeliveryStatus_DELIVERY_STATUS_IGNORED)
	second, secondEndpoint := newTestReceiverEndpoint(t, "gate-b", DeliveryStatus_DELIVERY_STATUS_IGNORED)
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	directory := testDirectory{endpoints: []GateEndpoint{firstEndpoint, secondEndpoint}}
	sender, err := NewFanoutSender(directory, transport)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := sender.SendToPlayer(context.Background(), PlayerMessage{LoginName: "alice", Message: Message{CommandID: 7}})
	if !errors.Is(err, ErrTargetNotConnected) || !receipt.AcceptedAt.IsZero() {
		t.Fatalf("all ignored fan-out = receipt:%#v error:%v, want empty/ErrTargetNotConnected", receipt, err)
	}
	first.mu.Lock()
	firstCommand := first.player.CommandID
	first.mu.Unlock()
	second.mu.Lock()
	secondCommand := second.player.CommandID
	second.mu.Unlock()
	if firstCommand != 7 || secondCommand != 7 {
		t.Fatalf("all ignored attempts = first:%d second:%d, want 7/7", firstCommand, secondCommand)
	}
}

func TestFanoutSenderContractPlayerPartialDeliveryKeepsReceiptAndErrors(t *testing.T) {
	delivered, deliveredEndpoint := newTestReceiverEndpoint(t, "gate-live", DeliveryStatus_DELIVERY_STATUS_DELIVERED)
	downListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	downAddress := downListener.Addr().String()
	if err := downListener.Close(); err != nil {
		t.Fatal(err)
	}
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	directory := testDirectory{endpoints: []GateEndpoint{deliveredEndpoint, {GateID: "gate-down", Address: downAddress}}}
	sender, err := NewFanoutSender(directory, transport)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := sender.SendToPlayer(context.Background(), PlayerMessage{LoginName: "alice", Message: Message{CommandID: 8}})
	if err == nil || receipt.AcceptedAt.IsZero() || !strings.Contains(err.Error(), "gate-down") {
		t.Fatalf("partial player fan-out = receipt:%#v error:%v, want receipt and gate-down error", receipt, err)
	}
	delivered.mu.Lock()
	defer delivered.mu.Unlock()
	if delivered.player.CommandID != 8 {
		t.Fatalf("partial fan-out delivered command = %d, want 8", delivered.player.CommandID)
	}
}

func TestFallbackBroadcastSenderContractUsesFallbackOnlyAfterPrimaryError(t *testing.T) {
	primaryErr := errors.New("redis publish failed")
	fallbackErr := errors.New("fan-out failed")
	primary := &recordingBroadcastSender{err: primaryErr}
	fallback := &recordingBroadcastSender{receipt: newReceipt(), err: fallbackErr}
	sender, err := NewFallbackBroadcastSender(primary, fallback)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := sender.Broadcast(context.Background(), BroadcastMessage{RoomID: "room-a", Message: Message{CommandID: 9}})
	if !errors.Is(err, primaryErr) || !errors.Is(err, fallbackErr) || receipt.AcceptedAt.IsZero() {
		t.Fatalf("fallback errors = receipt:%#v error:%v, want joined errors and fallback receipt", receipt, err)
	}
	if primary.calls != 1 || fallback.calls != 1 {
		t.Fatalf("fallback calls = primary:%d fallback:%d, want 1/1", primary.calls, fallback.calls)
	}

	primary = &recordingBroadcastSender{receipt: newReceipt()}
	fallback = &recordingBroadcastSender{}
	sender, err = NewFallbackBroadcastSender(primary, fallback)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err = sender.Broadcast(context.Background(), BroadcastMessage{RoomID: "room-a", Message: Message{CommandID: 10}})
	if err != nil || receipt.AcceptedAt.IsZero() || fallback.calls != 0 {
		t.Fatalf("successful primary = receipt:%#v error:%v fallback calls:%d, want primary receipt/nil/0", receipt, err, fallback.calls)
	}
}

func TestFallbackBroadcastSenderContractValidatesBeforeBothPaths(t *testing.T) {
	primary := &recordingBroadcastSender{}
	fallback := &recordingBroadcastSender{}
	sender, err := NewFallbackBroadcastSender(primary, fallback)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.Broadcast(context.Background(), BroadcastMessage{Message: Message{CommandID: 11}}); !errors.Is(err, ErrDestinationInvalid) {
		t.Fatalf("invalid broadcast error = %v, want ErrDestinationInvalid", err)
	}
	if primary.calls != 0 || fallback.calls != 0 {
		t.Fatalf("invalid broadcast calls = primary:%d fallback:%d, want 0/0", primary.calls, fallback.calls)
	}

	primary = &recordingBroadcastSender{}
	fallback = &recordingBroadcastSender{}
	sender, err = NewFallbackBroadcastSender(primary, fallback, BroadcastFallbackConfig{MaxPayloadBytes: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.Broadcast(context.Background(), BroadcastMessage{RoomID: "room-a", Message: Message{CommandID: 12, Payload: []byte("1234")}}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversized broadcast error = %v, want ErrPayloadTooLarge", err)
	}
	if primary.calls != 0 || fallback.calls != 0 {
		t.Fatalf("oversized broadcast calls = primary:%d fallback:%d, want 0/0", primary.calls, fallback.calls)
	}
}

func TestFallbackBroadcastSenderContractUsesRedisPrimaryAndGRPCFallback(t *testing.T) {
	first, firstEndpoint := newTestReceiverEndpoint(t, "gate-a", DeliveryStatus_DELIVERY_STATUS_DELIVERED)
	second, secondEndpoint := newTestReceiverEndpoint(t, "gate-b", DeliveryStatus_DELIVERY_STATUS_DELIVERED)
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	directory := testDirectory{endpoints: []GateEndpoint{firstEndpoint, secondEndpoint}}
	fanout, err := NewFanoutSender(directory, transport)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	primary, err := NewRedisBroadcastSender(&recordingRoomPublisher{err: errors.New("Redis unavailable")}, keys)
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewFallbackBroadcastSender(primary, fanout)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := sender.Broadcast(context.Background(), BroadcastMessage{RoomID: "room-a", Message: Message{CommandID: 13, Payload: []byte("fallback")}})
	if err != nil || receipt.AcceptedAt.IsZero() {
		t.Fatalf("Redis-to-gRPC fallback = receipt:%#v error:%v, want accepted/nil", receipt, err)
	}
	first.mu.Lock()
	firstCommand := first.broadcast.CommandID
	first.mu.Unlock()
	second.mu.Lock()
	secondCommand := second.broadcast.CommandID
	second.mu.Unlock()
	if firstCommand != 13 || secondCommand != 13 {
		t.Fatalf("fallback broadcast commands = first:%d second:%d, want 13/13", firstCommand, secondCommand)
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
	if _, err := sender.Broadcast(context.Background(), BroadcastMessage{RoomID: "room-a", Message: Message{CommandID: 4}}); err != nil {
		t.Fatalf("fan-out broadcast: %v", err)
	}
	first.mu.Lock()
	firstCommand := first.broadcast.CommandID
	first.mu.Unlock()
	second.mu.Lock()
	secondCommand := second.broadcast.CommandID
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
	if _, err := sender.Broadcast(context.Background(), BroadcastMessage{RoomID: "room-a", Message: Message{CommandID: 5}}); !errors.Is(err, ErrFanoutLimitExceeded) {
		t.Fatalf("fan-out limit error = %v, want ErrFanoutLimitExceeded", err)
	}
	first.mu.Lock()
	firstDelivered := first.broadcast.CommandID
	first.mu.Unlock()
	second.mu.Lock()
	secondDelivered := second.broadcast.CommandID
	second.mu.Unlock()
	if firstDelivered != 0 || secondDelivered != 0 {
		t.Fatalf("fan-out limit sent before rejection: first:%d second:%d", firstDelivered, secondDelivered)
	}
}

func newTestReceiverEndpoint(t *testing.T, gateID GateID, playerStatus DeliveryStatus) (*recordingReceiver, GateEndpoint) {
	t.Helper()
	receiver := &recordingReceiver{playerStatus: playerStatus}
	server, err := NewReceiverServer(ReceiverConfig{ListenAddr: "127.0.0.1:0"}, receiver)
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
}

func (d testDirectory) Resolve(_ context.Context, gateID GateID) (GateEndpoint, error) {
	endpoint, ok := d.byID[gateID]
	if !ok {
		return GateEndpoint{}, ErrGateEndpointNotFound
	}
	return endpoint, nil
}

func (d testDirectory) List(context.Context) ([]GateEndpoint, error) {
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

func (s *recordingBroadcastSender) Broadcast(context.Context, BroadcastMessage) (Receipt, error) {
	s.calls++
	return s.receipt, s.err
}
