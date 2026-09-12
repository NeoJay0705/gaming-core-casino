package serversend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

func TestBatchPlayerSenderRejectsOversizedEnvelopeBeforeRouting(t *testing.T) {
	var presenceCalls, endpointCalls, fallbackCalls int
	presence := batchPresenceResolverFunc(func(context.Context, []LoginName) (map[LoginName]Presence, error) {
		presenceCalls++
		return nil, nil
	})
	directory := batchGateResolverFunc(func(context.Context, []GateID) (map[GateID]GateEndpoint, error) {
		endpointCalls++
		return nil, nil
	})
	fallback := &staticTestDirectory{listHook: func() { fallbackCalls++ }}
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	sender, err := NewBatchPlayerSender(presence, directory, fallback, transport)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sender.SendToPlayers(context.Background(), []PlayerMessage{{
		LoginName: "alice",
		Message:   Message{CommandID: 1, Payload: make([]byte, DefaultMaxPayloadBytes)},
	}})
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversized envelope error = %v, want ErrPayloadTooLarge", err)
	}
	if presenceCalls != 0 || endpointCalls != 0 || fallbackCalls != 0 {
		t.Fatalf("oversized envelope performed routing I/O: presence=%d endpoint=%d fallback=%d", presenceCalls, endpointCalls, fallbackCalls)
	}
}

func TestBatchPlayerSenderPreservesMalformedRouteClassification(t *testing.T) {
	fallback := &staticTestDirectory{}
	var fallbackCalls int
	fallback.listHook = func() { fallbackCalls++ }
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	presence := batchPresenceResolverFunc(func(context.Context, []LoginName) (map[LoginName]Presence, error) {
		return nil, ErrDestinationInvalid
	})
	directory := batchGateResolverFunc(func(context.Context, []GateID) (map[GateID]GateEndpoint, error) {
		t.Fatal("endpoint resolver called for malformed presence")
		return nil, nil
	})
	sender, err := NewBatchPlayerSender(presence, directory, fallback, transport)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sender.SendToPlayers(context.Background(), []PlayerMessage{{LoginName: "alice", Message: Message{CommandID: 1}}})
	if !errors.Is(err, ErrDestinationInvalid) || errors.Is(err, ErrPresenceNotFound) {
		t.Fatalf("malformed presence error = %v, want only ErrDestinationInvalid classification", err)
	}
	if fallbackCalls != 0 {
		t.Fatalf("malformed presence fallback calls = %d, want 0", fallbackCalls)
	}

	presence = batchPresenceResolverFunc(func(context.Context, []LoginName) (map[LoginName]Presence, error) {
		return map[LoginName]Presence{"alice": {LoginName: "alice", GateID: "gate-a", ConnectionID: "a", Epoch: 1}}, nil
	})
	directory = batchGateResolverFunc(func(context.Context, []GateID) (map[GateID]GateEndpoint, error) {
		return nil, ErrDestinationInvalid
	})
	sender, err = NewBatchPlayerSender(presence, directory, fallback, transport)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sender.SendToPlayers(context.Background(), []PlayerMessage{{LoginName: "alice", Message: Message{CommandID: 1}}})
	if !errors.Is(err, ErrDestinationInvalid) || errors.Is(err, ErrGateEndpointNotFound) {
		t.Fatalf("malformed endpoint error = %v, want only ErrDestinationInvalid classification", err)
	}
	if fallbackCalls != 0 {
		t.Fatalf("malformed endpoint fallback calls = %d, want 0", fallbackCalls)
	}
}

func TestBatchPlayerSenderFallsBackAfterEndpointRedisFailure(t *testing.T) {
	first := &recordingReceiver{}
	firstEndpoint := testReceiverEndpoint(t, "gate-a", first, PlayerDeliveryCommandID)
	second := &recordingReceiver{}
	secondEndpoint := testReceiverEndpoint(t, "gate-b", second, PlayerDeliveryCommandID)
	fallback := &staticTestDirectory{endpoints: []GateEndpoint{firstEndpoint, secondEndpoint}}
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	presence := batchPresenceResolverFunc(func(context.Context, []LoginName) (map[LoginName]Presence, error) {
		return map[LoginName]Presence{
			"alice": {LoginName: "alice", GateID: "gate-a", ConnectionID: "a", Epoch: 1},
		}, nil
	})
	directory := batchGateResolverFunc(func(context.Context, []GateID) (map[GateID]GateEndpoint, error) {
		return nil, ErrRouteStoreUnavailable
	})
	sender, err := NewBatchPlayerSender(presence, directory, fallback, transport)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.SendToPlayers(context.Background(), []PlayerMessage{
		{LoginName: "alice", Message: Message{CommandID: 1, Payload: []byte("a")}},
		{LoginName: "bob", Message: Message{CommandID: 2, Payload: []byte("b")}},
	}); err != nil {
		t.Fatalf("endpoint Redis fallback: %v", err)
	}
	assertPlayerBatch(t, first, []string{"alice", "bob"})
	assertPlayerBatch(t, second, []string{"alice", "bob"})
}

func TestBatchPlayerSenderDoesNotFallbackAfterContextCancellation(t *testing.T) {
	fallback := &staticTestDirectory{}
	var fallbackCalls int
	fallback.listHook = func() { fallbackCalls++ }
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	ctx, cancel := context.WithCancel(context.Background())
	presence := batchPresenceResolverFunc(func(ctx context.Context, _ []LoginName) (map[LoginName]Presence, error) {
		cancel()
		return nil, ErrRouteStoreUnavailable
	})
	directory := batchGateResolverFunc(func(context.Context, []GateID) (map[GateID]GateEndpoint, error) {
		t.Fatal("endpoint resolver called after context cancellation")
		return nil, nil
	})
	sender, err := NewBatchPlayerSender(presence, directory, fallback, transport)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sender.SendToPlayers(ctx, []PlayerMessage{{LoginName: "alice", Message: Message{CommandID: 1}}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled route error = %v, want context.Canceled", err)
	}
	if fallbackCalls != 0 {
		t.Fatalf("canceled route fallback calls = %d, want 0", fallbackCalls)
	}
}

func TestBatchPlayerSenderReturnsPartialReceiptAndError(t *testing.T) {
	live := &recordingReceiver{}
	liveEndpoint := testReceiverEndpoint(t, "gate-live", live, PlayerDeliveryCommandID)
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	presence := batchPresenceResolverFunc(func(context.Context, []LoginName) (map[LoginName]Presence, error) {
		return map[LoginName]Presence{
			"alice": {LoginName: "alice", GateID: "gate-live", ConnectionID: "a", Epoch: 1},
			"bob":   {LoginName: "bob", GateID: "gate-down", ConnectionID: "b", Epoch: 1},
		}, nil
	})
	down := GateEndpoint{GateID: "gate-down", Address: "127.0.0.1:1"}
	directory := batchGateResolverFunc(func(_ context.Context, ids []GateID) (map[GateID]GateEndpoint, error) {
		return map[GateID]GateEndpoint{"gate-live": liveEndpoint, "gate-down": down}, nil
	})
	fallback := &staticTestDirectory{}
	sender, err := NewBatchPlayerSender(presence, directory, fallback, transport)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := sender.SendToPlayers(context.Background(), []PlayerMessage{
		{LoginName: "alice", Message: Message{CommandID: 1, Payload: []byte("a")}},
		{LoginName: "bob", Message: Message{CommandID: 2, Payload: []byte("b")}},
	})
	if err == nil || receipt.AcceptedAt.IsZero() {
		t.Fatalf("partial player send = receipt:%#v error:%v, want accepted and error", receipt, err)
	}
	assertPlayerBatch(t, live, []string{"alice"})
}

func TestBatchPlayerSenderClonesPayloadBeforeRouting(t *testing.T) {
	receiver := &recordingReceiver{}
	endpoint := testReceiverEndpoint(t, "gate-a", receiver, PlayerDeliveryCommandID)
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	payload := []byte("original")
	presence := batchPresenceResolverFunc(func(context.Context, []LoginName) (map[LoginName]Presence, error) {
		payload[0] = 'X'
		return map[LoginName]Presence{"alice": {LoginName: "alice", GateID: "gate-a", ConnectionID: "a", Epoch: 1}}, nil
	})
	directory := batchGateResolverFunc(func(context.Context, []GateID) (map[GateID]GateEndpoint, error) {
		return map[GateID]GateEndpoint{"gate-a": endpoint}, nil
	})
	sender, err := NewBatchPlayerSender(presence, directory, &staticTestDirectory{}, transport)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.SendToPlayers(context.Background(), []PlayerMessage{{LoginName: "alice", Message: Message{CommandID: 1, Payload: payload}}}); err != nil {
		t.Fatalf("cloned player send: %v", err)
	}
	receiver.mu.Lock()
	encoded := append([]byte(nil), receiver.remotePayload...)
	receiver.mu.Unlock()
	var command SendPlayersCommand
	if err := proto.Unmarshal(encoded, &command); err != nil {
		t.Fatal(err)
	}
	if got := string(command.Messages[0].GetClientPayload()); got != "original" {
		t.Fatalf("cloned payload = %q, want original", got)
	}
}

func TestSplitPlayerMessagesAcceptsExactEnvelopeLimit(t *testing.T) {
	message := PlayerMessage{LoginName: "alice", Message: Message{CommandID: 1}}
	low, high := 0, DefaultMaxPayloadBytes
	for low < high {
		middle := (low + high + 1) / 2
		message.Payload = make([]byte, middle)
		if playerDeliveryWireSize(message) <= DefaultMaxPayloadBytes {
			low = middle
		} else {
			high = middle - 1
		}
	}
	message.Payload = make([]byte, low)
	if got := playerDeliveryWireSize(message); got != DefaultMaxPayloadBytes {
		t.Fatalf("maximum fitting envelope = %d, want %d", got, DefaultMaxPayloadBytes)
	}
	chunks, err := splitPlayerMessages([]PlayerMessage{message})
	if err != nil || len(chunks) != 1 || len(chunks[0].Payload) != DefaultMaxPayloadBytes {
		t.Fatalf("exact-limit chunk = %#v error:%v, want one %d-byte chunk", chunks, err, DefaultMaxPayloadBytes)
	}
}

func TestBatchPlayerSenderKeepsChunksSequentialPerEndpoint(t *testing.T) {
	receiver := &orderedBatchReceiver{}
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
	sender := &BatchPlayerSender{transport: transport}
	plan := playerEndpointPlan{
		endpoint: GateEndpoint{GateID: "gate-a", Address: server.Addr()},
		chunks: []Message{
			{CommandID: PlayerDeliveryCommandID, Payload: []byte("first")},
			{CommandID: PlayerDeliveryCommandID, Payload: []byte("second")},
		},
	}
	accepted, err := sender.sendPlan(context.Background(), plan)
	if err != nil || !accepted {
		t.Fatalf("sequential chunks = accepted:%v error:%v", accepted, err)
	}
	receiver.mu.Lock()
	got := append([]string(nil), receiver.payloads...)
	receiver.mu.Unlock()
	if fmt.Sprint(got) != "[first second]" {
		t.Fatalf("chunk order = %v, want [first second]", got)
	}
}

func TestBatchPlayerSenderBoundsEndpointConcurrency(t *testing.T) {
	receiver := newBlockingBatchReceiver(defaultFanoutConcurrency)
	server, err := NewReceiverServer(ReceiverConfig{ListenAddr: "127.0.0.1:0"}, receiver, PlayerDeliveryCommandID)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		receiver.releaseAll()
		_ = server.Stop(context.Background())
	})
	transport, err := NewGRPCTransport(TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	sender := &BatchPlayerSender{transport: transport}
	plans := make([]playerEndpointPlan, defaultFanoutConcurrency+1)
	for index := range plans {
		plans[index] = playerEndpointPlan{
			endpoint: GateEndpoint{GateID: GateID(fmt.Sprintf("gate-%d", index)), Address: server.Addr()},
			chunks:   []Message{{CommandID: PlayerDeliveryCommandID, Payload: []byte("payload")}},
		}
	}
	done := make(chan struct{})
	go func() {
		_, _ = sender.sendPlans(context.Background(), plans)
		close(done)
	}()
	select {
	case <-receiver.reached:
	case <-time.After(3 * time.Second):
		t.Fatal("endpoint concurrency did not reach configured bound")
	}
	receiver.mu.Lock()
	maxActive := receiver.maxActive
	receiver.mu.Unlock()
	if maxActive != defaultFanoutConcurrency {
		t.Fatalf("maximum endpoint concurrency = %d, want %d", maxActive, defaultFanoutConcurrency)
	}
	receiver.releaseAll()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("endpoint plans did not finish after release")
	}
}

type orderedBatchReceiver struct {
	mu       sync.Mutex
	payloads []string
}

func (r *orderedBatchReceiver) HandleRemote(_ context.Context, _ uint32, payload []byte) error {
	r.mu.Lock()
	r.payloads = append(r.payloads, string(payload))
	r.mu.Unlock()
	return nil
}

type blockingBatchReceiver struct {
	mu          sync.Mutex
	active      int
	maxActive   int
	reached     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
	bound       int
}

func newBlockingBatchReceiver(bound int) *blockingBatchReceiver {
	return &blockingBatchReceiver{
		reached: make(chan struct{}),
		release: make(chan struct{}),
		bound:   bound,
	}
}

func (r *blockingBatchReceiver) HandleRemote(_ context.Context, _ uint32, _ []byte) error {
	r.mu.Lock()
	r.active++
	if r.active > r.maxActive {
		r.maxActive = r.active
	}
	if r.maxActive >= r.bound {
		r.once.Do(func() { close(r.reached) })
	}
	r.mu.Unlock()
	<-r.release
	r.mu.Lock()
	r.active--
	r.mu.Unlock()
	return nil
}

func (r *blockingBatchReceiver) releaseAll() {
	r.releaseOnce.Do(func() { close(r.release) })
}
