package serversend

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"google.golang.org/protobuf/proto"
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

func TestBatchPlayerSenderGroupsMessagesByEndpoint(t *testing.T) {
	first := &recordingReceiver{}
	firstServer, err := NewReceiverServer(ReceiverConfig{ListenAddr: "127.0.0.1:0"}, first, PlayerDeliveryCommandID)
	if err != nil {
		t.Fatal(err)
	}
	if err := firstServer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = firstServer.Stop(context.Background()) })
	second := &recordingReceiver{}
	secondServer, err := NewReceiverServer(ReceiverConfig{ListenAddr: "127.0.0.1:0"}, second, PlayerDeliveryCommandID)
	if err != nil {
		t.Fatal(err)
	}
	if err := secondServer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondServer.Stop(context.Background()) })
	firstEndpoint := GateEndpoint{GateID: "gate-a", Address: firstServer.Addr()}
	secondEndpoint := GateEndpoint{GateID: "gate-b", Address: secondServer.Addr()}
	presence := batchPresenceResolverFunc(func(_ context.Context, names []LoginName) (map[LoginName]Presence, error) {
		if len(names) != 3 {
			t.Fatalf("presence names = %v, want 3", names)
		}
		return map[LoginName]Presence{
			"alice": {LoginName: "alice", GateID: "gate-a", ConnectionID: "a"},
			"bob":   {LoginName: "bob", GateID: "gate-a", ConnectionID: "b"},
			"carol": {LoginName: "carol", GateID: "gate-b", ConnectionID: "c"},
		}, nil
	})
	directory := batchGateResolverFunc(func(_ context.Context, ids []GateID) (map[GateID]GateEndpoint, error) {
		return map[GateID]GateEndpoint{"gate-a": firstEndpoint, "gate-b": secondEndpoint}, nil
	})
	fallback := &staticTestDirectory{endpoints: nil}
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	sender, err := NewBatchPlayerSender(presence, directory, fallback, transport)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.SendToPlayers(context.Background(), []PlayerMessage{
		{LoginName: "alice", Message: Message{CommandID: 11, Payload: []byte("a")}},
		{LoginName: "bob", Message: Message{CommandID: 12, Payload: []byte("b")}},
		{LoginName: "carol", Message: Message{CommandID: 13, Payload: []byte("c")}},
	}); err != nil {
		t.Fatalf("batch player send: %v", err)
	}
	assertPlayerBatch(t, first, []string{"alice", "bob"})
	assertPlayerBatch(t, second, []string{"carol"})
}

func TestBatchPlayerSenderFallsBackOnlyForRedisRouteFailure(t *testing.T) {
	receiver := &recordingReceiver{}
	server, err := NewReceiverServer(ReceiverConfig{ListenAddr: "127.0.0.1:0"}, receiver, PlayerDeliveryCommandID)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })
	fallback := &staticTestDirectory{endpoints: []GateEndpoint{{GateID: "gate-a", Address: server.Addr()}}}
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	var fallbackCalls int
	fallback.listHook = func() { fallbackCalls++ }
	presence := batchPresenceResolverFunc(func(context.Context, []LoginName) (map[LoginName]Presence, error) {
		return nil, ErrRouteStoreUnavailable
	})
	directory := batchGateResolverFunc(func(context.Context, []GateID) (map[GateID]GateEndpoint, error) {
		t.Fatal("exact endpoint resolver called after Redis failure")
		return nil, nil
	})
	sender, err := NewBatchPlayerSender(presence, directory, fallback, transport)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.SendToPlayers(context.Background(), []PlayerMessage{{LoginName: "alice", Message: Message{CommandID: 14}}}); err != nil {
		t.Fatalf("Redis fallback send: %v", err)
	}
	if fallbackCalls != 1 {
		t.Fatalf("fallback list calls = %d, want 1", fallbackCalls)
	}
	assertPlayerBatch(t, receiver, []string{"alice"})
}

func TestBatchPlayerSenderDoesNotFallbackMissingPresenceOrGRPCFailure(t *testing.T) {
	fallback := &staticTestDirectory{}
	var fallbackCalls int
	fallback.listHook = func() { fallbackCalls++ }
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	directory := batchGateResolverFunc(func(context.Context, []GateID) (map[GateID]GateEndpoint, error) {
		return map[GateID]GateEndpoint{"gate-a": {GateID: "gate-a", Address: "127.0.0.1:1"}}, nil
	})
	missingPresence := batchPresenceResolverFunc(func(context.Context, []LoginName) (map[LoginName]Presence, error) {
		return map[LoginName]Presence{}, nil
	})
	sender, err := NewBatchPlayerSender(missingPresence, directory, fallback, transport)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.SendToPlayers(context.Background(), []PlayerMessage{{LoginName: "offline", Message: Message{CommandID: 1}}}); !errors.Is(err, ErrPresenceNotFound) {
		t.Fatalf("missing presence error = %v, want ErrPresenceNotFound", err)
	}
	if fallbackCalls != 0 {
		t.Fatalf("missing presence fallback calls = %d, want 0", fallbackCalls)
	}
	presence := batchPresenceResolverFunc(func(context.Context, []LoginName) (map[LoginName]Presence, error) {
		return map[LoginName]Presence{"alice": {LoginName: "alice", GateID: "gate-a", ConnectionID: "a"}}, nil
	})
	sender, err = NewBatchPlayerSender(presence, directory, fallback, transport)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.SendToPlayers(context.Background(), []PlayerMessage{{LoginName: "alice", Message: Message{CommandID: 1}}}); err == nil {
		t.Fatal("gRPC failure returned nil error")
	}
	if fallbackCalls != 0 {
		t.Fatalf("gRPC failure fallback calls = %d, want 0", fallbackCalls)
	}
}

func TestSplitPlayerMessagesCountsProtobufEnvelopeBytes(t *testing.T) {
	messages := []PlayerMessage{
		{LoginName: "alice", Message: Message{CommandID: 1, Payload: make([]byte, 700_000)}},
		{LoginName: "bob", Message: Message{CommandID: 2, Payload: make([]byte, 700_000)}},
	}
	chunks, err := splitPlayerMessages(messages)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 2 {
		t.Fatalf("chunk count = %d, want 2", len(chunks))
	}
	for index, chunk := range chunks {
		if len(chunk.Payload) > DefaultMaxPayloadBytes {
			t.Fatalf("chunk %d payload = %d, exceeds %d", index, len(chunk.Payload), DefaultMaxPayloadBytes)
		}
		var command SendPlayersCommand
		if err := proto.Unmarshal(chunk.Payload, &command); err != nil {
			t.Fatalf("decode chunk %d: %v", index, err)
		}
	}
}

func TestFanoutSenderContractUsesEveryUniqueEndpointExactlyOnce(t *testing.T) {
	first := &recordingReceiver{}
	firstEndpoint := testReceiverEndpoint(t, "gate-a", first, 4)
	second := &recordingReceiver{}
	secondEndpoint := testReceiverEndpoint(t, "gate-b", second, 4)
	directory := &staticTestDirectory{endpoints: []GateEndpoint{firstEndpoint, secondEndpoint, firstEndpoint}}
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
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
	first := &recordingReceiver{}
	firstEndpoint := testReceiverEndpoint(t, "gate-a", first, 5)
	second := &recordingReceiver{}
	secondEndpoint := testReceiverEndpoint(t, "gate-b", second, 5)
	directory := &staticTestDirectory{endpoints: []GateEndpoint{firstEndpoint, secondEndpoint}}
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
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

func testReceiverEndpoint(t *testing.T, gateID GateID, receiver *recordingReceiver, commandID uint32) GateEndpoint {
	t.Helper()
	server, err := NewReceiverServer(ReceiverConfig{ListenAddr: "127.0.0.1:0"}, receiver, commandID)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })
	return GateEndpoint{GateID: gateID, Address: server.Addr()}
}

func assertPlayerBatch(t *testing.T, receiver *recordingReceiver, wantLogins []string) {
	t.Helper()
	receiver.mu.Lock()
	payload := append([]byte(nil), receiver.remotePayload...)
	commandID := receiver.remoteCommandID
	receiver.mu.Unlock()
	if commandID != PlayerDeliveryCommandID {
		t.Fatalf("player batch command = %d, want %d", commandID, PlayerDeliveryCommandID)
	}
	var command SendPlayersCommand
	if err := proto.Unmarshal(payload, &command); err != nil {
		t.Fatalf("decode player batch: %v", err)
	}
	if len(command.Messages) != len(wantLogins) {
		t.Fatalf("player batch size = %d, want %d", len(command.Messages), len(wantLogins))
	}
	for index, message := range command.Messages {
		if message.GetLoginName() != wantLogins[index] {
			t.Fatalf("player batch login %d = %q, want %q", index, message.GetLoginName(), wantLogins[index])
		}
	}
}

type batchPresenceResolverFunc func(context.Context, []LoginName) (map[LoginName]Presence, error)

func (f batchPresenceResolverFunc) ResolveMany(ctx context.Context, names []LoginName) (map[LoginName]Presence, error) {
	return f(ctx, names)
}

type batchGateResolverFunc func(context.Context, []GateID) (map[GateID]GateEndpoint, error)

func (f batchGateResolverFunc) ResolveMany(ctx context.Context, ids []GateID) (map[GateID]GateEndpoint, error) {
	return f(ctx, ids)
}

type staticTestDirectory struct {
	endpoints []GateEndpoint
	listHook  func()
}

func (d *staticTestDirectory) List(context.Context) ([]GateEndpoint, error) {
	if d.listHook != nil {
		d.listHook()
	}
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
