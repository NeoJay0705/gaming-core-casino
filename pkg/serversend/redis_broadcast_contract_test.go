package serversend

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

func TestRedisBroadcastSenderContractPublishesGenericGateRequest(t *testing.T) {
	keys, err := NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	publisher := &recordingBroadcastPublisher{}
	sender, err := NewRedisBroadcastSender(publisher, keys)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("opaque-command")
	if _, err := sender.Broadcast(context.Background(), Message{CommandID: 9, Payload: payload}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	payload[0] = 'X'
	if publisher.channel != keys.broadcastChannel() {
		t.Fatalf("channel = %q, want %q", publisher.channel, keys.broadcastChannel())
	}
	var request gatelink.GateRequest
	if err := proto.Unmarshal(publisher.payload, &request); err != nil {
		t.Fatalf("unmarshal command: %v", err)
	}
	if request.GetCommandId() != 9 || string(request.GetPayload()) != "opaque-command" {
		t.Fatalf("published command = id:%d payload:%q", request.GetCommandId(), request.GetPayload())
	}
	publisher.err = errors.New("redis down")
	if _, err := sender.Broadcast(context.Background(), Message{CommandID: 9}); !errors.Is(err, ErrRouteStoreUnavailable) {
		t.Fatalf("publish failure = %v, want ErrRouteStoreUnavailable", err)
	}
	publisher.err = context.Canceled
	if _, err := sender.Broadcast(context.Background(), Message{CommandID: 9}); !errors.Is(err, ErrRouteStoreUnavailable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled publish failure = %v, want route-store and context cancellation", err)
	}
}

func TestRedisBroadcastSenderContractTreatsZeroSubscribersAsPrimaryFailure(t *testing.T) {
	keys, err := NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	publisher := &recordingBroadcastPublisher{returnZero: true}
	sender, err := NewRedisBroadcastSender(publisher, keys)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.Broadcast(context.Background(), Message{CommandID: 1}); !errors.Is(err, ErrRouteStoreUnavailable) {
		t.Fatalf("zero-subscriber publish error = %v, want ErrRouteStoreUnavailable", err)
	}
}

func TestRedisBroadcastSubscriberContractPreservesSubscribeCancellationCause(t *testing.T) {
	commandDispatcher, err := newTestDeliveryDispatcher(&recordingReceiver{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	subscriber := &RedisBroadcastSubscriber{
		commandDispatcher: commandDispatcher,
		subscribe: func(context.Context) (broadcastSubscription, error) {
			return nil, context.DeadlineExceeded
		},
	}
	if err := subscriber.Start(context.Background()); !errors.Is(err, ErrRouteStoreUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled subscribe error = %v, want route-store and deadline", err)
	}
}

func TestRedisBroadcastSubscriberContractDispatchesOpaqueCommand(t *testing.T) {
	receiver := &recordingReceiver{}
	commandDispatcher, err := newTestDeliveryDispatcher(receiver, 10)
	if err != nil {
		t.Fatal(err)
	}
	subscriber := &RedisBroadcastSubscriber{commandDispatcher: commandDispatcher}
	encoded, err := proto.Marshal(&gatelink.GateRequest{CommandId: 10, Payload: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}
	if err := subscriber.handle(context.Background(), encoded); err != nil {
		t.Fatalf("handle broadcast: %v", err)
	}
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	if receiver.remoteCommandID != 10 || string(receiver.remotePayload) != "payload" {
		t.Fatalf("local command = id:%d payload:%q", receiver.remoteCommandID, receiver.remotePayload)
	}
}

func TestRedisBroadcastSubscriberContinuesAfterUnregisteredCommand(t *testing.T) {
	commandDispatcher := dispatcher.New()
	handled := make(chan struct{}, 1)
	if err := commandDispatcher.Register(RemoteCommandChannel, 11, func(_ context.Context, payload []byte) error {
		if string(payload) != "valid" {
			return fmt.Errorf("payload = %q, want valid", payload)
		}
		handled <- struct{}{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	invalid, err := proto.Marshal(&gatelink.GateRequest{CommandId: 10})
	if err != nil {
		t.Fatal(err)
	}
	valid, err := proto.Marshal(&gatelink.GateRequest{CommandId: 11, Payload: []byte("valid")})
	if err != nil {
		t.Fatal(err)
	}
	messages := []*redis.Message{{Payload: string(invalid)}, {Payload: string(valid)}}
	index := 0
	subscription := &testBroadcastSubscription{receive: func(ctx context.Context) (*redis.Message, error) {
		if index < len(messages) {
			message := messages[index]
			index++
			return message, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	subscriber := &RedisBroadcastSubscriber{
		commandDispatcher: commandDispatcher,
		subscribe:         func(context.Context) (broadcastSubscription, error) { return subscription, nil },
	}
	if err := subscriber.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("subscriber stopped after unregistered command")
	}
	if err := subscriber.Stop(context.Background()); err != nil {
		t.Fatalf("stop subscriber: %v", err)
	}
	if index != len(messages) {
		t.Fatalf("consumed messages = %d, want %d", index, len(messages))
	}
}

func TestRedisBroadcastSenderDeliversToTwoSubscribers(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: miniRedis.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	keys, err := NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan string, 2)
	subscribers := make([]*RedisBroadcastSubscriber, 0, 2)
	t.Cleanup(func() {
		for _, subscriber := range subscribers {
			_ = subscriber.Stop(context.Background())
		}
	})
	for range 2 {
		commandDispatcher := dispatcher.New()
		if err := commandDispatcher.Register(RemoteCommandChannel, 12, func(_ context.Context, payload []byte) error {
			received <- string(payload)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		subscriber, err := NewRedisBroadcastSubscriber(client, keys, commandDispatcher)
		if err != nil {
			t.Fatal(err)
		}
		if err := subscriber.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		subscribers = append(subscribers, subscriber)
	}
	sender, err := NewRedisBroadcastSender(client, keys)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.Broadcast(context.Background(), Message{CommandID: 12, Payload: []byte("two-gates")}); err != nil {
		t.Fatalf("broadcast to two subscribers: %v", err)
	}
	for range 2 {
		select {
		case payload := <-received:
			if payload != "two-gates" {
				t.Fatalf("subscriber payload = %q, want two-gates", payload)
			}
		case <-time.After(time.Second):
			t.Fatal("second subscriber did not receive broadcast")
		}
	}
}

func TestRedisBroadcastSubscriberContractContainsHandlerPanic(t *testing.T) {
	commandDispatcher, err := newTestDeliveryDispatcher(panicReceiver{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	subscriber := &RedisBroadcastSubscriber{commandDispatcher: commandDispatcher}
	encoded, err := proto.Marshal(&gatelink.GateRequest{CommandId: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := subscriber.handle(context.Background(), encoded); err == nil || !strings.Contains(err.Error(), "handler panic") {
		t.Fatalf("panic handler error = %v, want contained error", err)
	}
}

func TestRedisBroadcastContractRejectsPayloadAboveProtocolLimit(t *testing.T) {
	keys, err := NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	publisher := &recordingBroadcastPublisher{}
	sender, err := NewRedisBroadcastSender(publisher, keys)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.Broadcast(context.Background(), Message{CommandID: 1, Payload: make([]byte, DefaultMaxPayloadBytes+1)}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversized Redis payload = %v, want ErrPayloadTooLarge", err)
	}
	if publisher.channel != "" || publisher.payload != nil {
		t.Fatal("oversized payload was published to Redis")
	}

	receiver := &recordingReceiver{}
	commandDispatcher, err := newTestDeliveryDispatcher(receiver, 1)
	if err != nil {
		t.Fatal(err)
	}
	subscriber := &RedisBroadcastSubscriber{commandDispatcher: commandDispatcher}
	encoded, err := proto.Marshal(&gatelink.GateRequest{CommandId: 1, Payload: make([]byte, DefaultMaxPayloadBytes+1)})
	if err != nil {
		t.Fatal(err)
	}
	if err := subscriber.handle(context.Background(), encoded); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversized subscriber payload = %v, want ErrPayloadTooLarge", err)
	}
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	if receiver.remoteCommandID != 0 {
		t.Fatal("oversized Redis payload reached local handler")
	}
}

func TestRedisBroadcastSubscriberContractReconnectsAndHonorsStopDeadline(t *testing.T) {
	keys, err := NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	receiver := &recordingReceiver{}
	commandDispatcher, err := newTestDeliveryDispatcher(receiver, 10)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := proto.Marshal(&gatelink.GateRequest{CommandId: 10})
	if err != nil {
		t.Fatal(err)
	}
	first := &testBroadcastSubscription{receive: func(context.Context) (*redis.Message, error) {
		return nil, errors.New("temporary Redis disconnect")
	}}
	var secondOnce sync.Once
	second := &testBroadcastSubscription{receive: func(ctx context.Context) (*redis.Message, error) {
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
		keys:              keys,
		commandDispatcher: commandDispatcher,
		retryInterval:     time.Millisecond,
		subscribe: func(context.Context) (broadcastSubscription, error) {
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
		delivered := receiver.remoteCommandID == 10
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

	blocked := &RedisBroadcastSubscriber{started: true, done: make(chan struct{}), sub: &testBroadcastSubscription{}}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer stopCancel()
	if err := blocked.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stop deadline error = %v, want context.DeadlineExceeded", err)
	}
}

type recordingBroadcastPublisher struct {
	channel    string
	payload    []byte
	err        error
	returnZero bool
}

type testBroadcastSubscription struct {
	receive func(context.Context) (*redis.Message, error)
}

func (s *testBroadcastSubscription) ReceiveMessage(ctx context.Context) (*redis.Message, error) {
	if s.receive == nil {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return s.receive(ctx)
}

func (*testBroadcastSubscription) Close() error { return nil }

func (p *recordingBroadcastPublisher) Publish(_ context.Context, channel string, message any) *redis.IntCmd {
	if p.err != nil {
		return redis.NewIntResult(0, p.err)
	}
	p.channel = channel
	p.payload = append([]byte(nil), message.([]byte)...)
	if p.returnZero {
		return redis.NewIntResult(0, nil)
	}
	return redis.NewIntResult(1, nil)
}

type panicReceiver struct{}

func (panicReceiver) HandleRemote(context.Context, uint32, []byte) error {
	panic("test handler panic")
}
