package serversend

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

func TestRedisBroadcastSenderContractPublishesOpaqueRoomEnvelope(t *testing.T) {
	keys, err := NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	publisher := &recordingRoomPublisher{}
	sender, err := NewRedisBroadcastSender(publisher, keys)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("room-message")
	ctx := WithRequestContext(context.Background(), RequestContext{TraceID: "trace-123"})
	if _, err := sender.Broadcast(ctx, BroadcastMessage{RoomID: "room-a", Message: Message{CommandID: 9, Payload: payload}}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	payload[0] = 'X'
	if publisher.channel != keys.roomChannel("room-a") {
		t.Fatalf("channel = %q, want %q", publisher.channel, keys.roomChannel("room-a"))
	}
	var envelope RedisBroadcastEnvelope
	if err := proto.Unmarshal(publisher.payload, &envelope); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if envelope.GetTraceId() != "trace-123" || envelope.GetMessage().GetCommandId() != 9 || string(envelope.GetMessage().GetPayload()) != "room-message" {
		t.Fatalf("published envelope = trace:%q command:%d payload:%q", envelope.GetTraceId(), envelope.GetMessage().GetCommandId(), envelope.GetMessage().GetPayload())
	}
	publisher.err = errors.New("redis down")
	if _, err := sender.Broadcast(context.Background(), BroadcastMessage{RoomID: "room-a", Message: Message{CommandID: 9}}); !errors.Is(err, ErrRouteStoreUnavailable) {
		t.Fatalf("publish failure = %v, want ErrRouteStoreUnavailable", err)
	}
	publisher.err = context.Canceled
	if _, err := sender.Broadcast(context.Background(), BroadcastMessage{RoomID: "room-a", Message: Message{CommandID: 9}}); !errors.Is(err, ErrRouteStoreUnavailable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled publish failure = %v, want route-store and context cancellation", err)
	}
}

func TestRedisBroadcastSubscriberContractPreservesSubscribeCancellationCause(t *testing.T) {
	subscriber := &RedisBroadcastSubscriber{
		receiver: &recordingReceiver{},
		subscribe: func(context.Context) (roomSubscription, error) {
			return nil, context.DeadlineExceeded
		},
	}
	if err := subscriber.Start(context.Background()); !errors.Is(err, ErrRouteStoreUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled subscribe error = %v, want route-store and deadline", err)
	}
}

func TestRedisBroadcastSubscriberContractRestoresTraceAndDeliversLocally(t *testing.T) {
	receiver := &recordingReceiver{}
	subscriber := &RedisBroadcastSubscriber{receiver: receiver}
	encoded, err := proto.Marshal(&RedisBroadcastEnvelope{TraceId: "trace-456", Message: &BroadcastRoomRequest{RoomId: "room-a", CommandId: 10, Payload: []byte("payload")}})
	if err != nil {
		t.Fatal(err)
	}
	if err := subscriber.handle(context.Background(), encoded); err != nil {
		t.Fatalf("handle broadcast: %v", err)
	}
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	if receiver.broadcast.RoomID != "room-a" || receiver.broadcast.CommandID != 10 || receiver.traceID != "trace-456" {
		t.Fatalf("local broadcast = room:%q command:%d trace:%q", receiver.broadcast.RoomID, receiver.broadcast.CommandID, receiver.traceID)
	}
}

func TestRedisBroadcastContractRejectsPayloadAboveConfiguredLimit(t *testing.T) {
	keys, err := NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	publisher := &recordingRoomPublisher{}
	sender, err := NewRedisBroadcastSender(publisher, keys, RedisBroadcastConfig{MaxPayloadBytes: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.Broadcast(context.Background(), BroadcastMessage{RoomID: "room-a", Message: Message{CommandID: 1, Payload: []byte("1234")}}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversized Redis payload = %v, want ErrPayloadTooLarge", err)
	}
	if publisher.channel != "" || publisher.payload != nil {
		t.Fatal("oversized payload was published to Redis")
	}

	receiver := &recordingReceiver{}
	subscriber := &RedisBroadcastSubscriber{receiver: receiver, maxPayloadBytes: 3}
	encoded, err := proto.Marshal(&RedisBroadcastEnvelope{Message: &BroadcastRoomRequest{RoomId: "room-a", CommandId: 1, Payload: []byte("1234")}})
	if err != nil {
		t.Fatal(err)
	}
	if err := subscriber.handle(context.Background(), encoded); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversized subscriber payload = %v, want ErrPayloadTooLarge", err)
	}
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	if receiver.broadcast.CommandID != 0 {
		t.Fatal("oversized Redis payload reached local receiver")
	}
}

func TestRedisBroadcastSubscriberContractReconnectsAndHonorsStopDeadline(t *testing.T) {
	keys, err := NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	receiver := &recordingReceiver{}
	encoded, err := proto.Marshal(&RedisBroadcastEnvelope{Message: &BroadcastRoomRequest{RoomId: "room-a", CommandId: 10}})
	if err != nil {
		t.Fatal(err)
	}
	first := &testRoomSubscription{receive: func(context.Context) (*redis.Message, error) {
		return nil, errors.New("temporary Redis disconnect")
	}}
	var secondOnce sync.Once
	second := &testRoomSubscription{receive: func(ctx context.Context) (*redis.Message, error) {
		var message *redis.Message
		secondOnce.Do(func() { message = &redis.Message{Payload: string(encoded)} })
		if message != nil {
			return message, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	var mu sync.Mutex
	connections := 0
	subscriber := &RedisBroadcastSubscriber{
		keys:          keys,
		receiver:      receiver,
		retryInterval: time.Millisecond,
		subscribe: func(context.Context) (roomSubscription, error) {
			mu.Lock()
			defer mu.Unlock()
			connections++
			if connections == 1 {
				return first, nil
			}
			return second, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := subscriber.Start(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		receiver.mu.Lock()
		delivered := receiver.broadcast.RoomID == "room-a"
		receiver.mu.Unlock()
		if delivered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("subscriber did not deliver after reconnect")
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	gotConnections := connections
	mu.Unlock()
	if gotConnections < 2 {
		t.Fatalf("subscription attempts = %d, want reconnect", gotConnections)
	}
	if err := subscriber.Stop(context.Background()); err != nil {
		t.Fatalf("stop subscriber: %v", err)
	}

	blocked := &RedisBroadcastSubscriber{started: true, done: make(chan struct{}), sub: &testRoomSubscription{}}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer stopCancel()
	if err := blocked.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stop deadline error = %v, want context.DeadlineExceeded", err)
	}
}

type recordingRoomPublisher struct {
	channel string
	payload []byte
	err     error
}

type testRoomSubscription struct {
	receive func(context.Context) (*redis.Message, error)
}

func (s *testRoomSubscription) ReceiveMessage(ctx context.Context) (*redis.Message, error) {
	if s.receive == nil {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return s.receive(ctx)
}

func (*testRoomSubscription) Close() error { return nil }

func (p *recordingRoomPublisher) Publish(_ context.Context, channel string, message any) *redis.IntCmd {
	if p.err != nil {
		return redis.NewIntResult(0, p.err)
	}
	p.channel = channel
	p.payload = append([]byte(nil), message.([]byte)...)
	return redis.NewIntResult(1, nil)
}
