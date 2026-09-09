package serversend

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

// RoomPublisher is the narrow Redis Pub/Sub publish dependency.
type RoomPublisher interface {
	Publish(context.Context, string, any) *redis.IntCmd
}

// RedisBroadcastSender publishes a room message once. Gate subscribers apply
// it only to their local room members. Redis Pub/Sub is intentionally
// best-effort: it has no replay for a Gate that was disconnected.
type RedisBroadcastSender struct {
	publisher       RoomPublisher
	keys            Keyspace
	maxPayloadBytes int
}

type RedisBroadcastConfig struct{ MaxPayloadBytes int }

func NewRedisBroadcastSender(publisher RoomPublisher, keys Keyspace, configs ...RedisBroadcastConfig) (*RedisBroadcastSender, error) {
	if publisher == nil {
		return nil, fmt.Errorf("%w: Redis room publisher is required", ErrRouteStoreUnavailable)
	}
	if keys.Prefix() == "" {
		return nil, fmt.Errorf("%w: redis key prefix is required", ErrDestinationInvalid)
	}
	maxPayloadBytes, err := payloadLimitFromConfig(configs)
	if err != nil {
		return nil, err
	}
	return &RedisBroadcastSender{publisher: publisher, keys: keys, maxPayloadBytes: maxPayloadBytes}, nil
}

func (s *RedisBroadcastSender) Broadcast(ctx context.Context, message BroadcastMessage) (Receipt, error) {
	if s == nil || s.publisher == nil {
		return Receipt{}, fmt.Errorf("%w: Redis room publisher is not configured", ErrRouteStoreUnavailable)
	}
	if err := message.validatePayload(s.maxPayloadBytes); err != nil {
		return Receipt{}, err
	}
	return s.broadcastValidated(ctx, message)
}

func (s *RedisBroadcastSender) broadcastValidated(ctx context.Context, message BroadcastMessage) (Receipt, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	message = message.clone()
	traceID := traceIDFromContext(ctx)
	encoded, err := proto.Marshal(&RedisBroadcastEnvelope{Message: &BroadcastRoomRequest{RoomId: string(message.RoomID), CommandId: message.CommandID, Payload: message.Payload}, TraceId: traceID})
	if err != nil {
		return Receipt{}, fmt.Errorf("server send: encode Redis room broadcast: %w", err)
	}
	if err := s.publisher.Publish(ctx, s.keys.roomChannel(message.RoomID), encoded).Err(); err != nil {
		return Receipt{}, routeStoreError(fmt.Sprintf("publish room %q", message.RoomID), err)
	}
	return newReceipt(), nil
}

// RoomSubscriptionStore is the narrow Redis Pub/Sub subscription dependency.
type RoomSubscriptionStore interface {
	PSubscribe(context.Context, ...string) *redis.PubSub
}

type roomSubscription interface {
	ReceiveMessage(context.Context) (*redis.Message, error)
	Close() error
}

const (
	defaultSubscriptionRetryInterval = 100 * time.Millisecond
	defaultSubscriptionRetryMax      = 30 * time.Second
)

// RedisBroadcastSubscriber receives every room channel under one Keyspace and
// applies each message to local Gate state. A broken subscription is reopened
// with bounded exponential backoff and jitter; Pub/Sub still provides no replay
// for messages missed while the subscriber was disconnected.
type RedisBroadcastSubscriber struct {
	store    RoomSubscriptionStore
	keys     Keyspace
	receiver LocalReceiver

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	sub     roomSubscription
	started bool

	// subscribe and retryInterval make lifecycle behaviour contract-testable
	// without exposing Redis Pub/Sub implementation details.
	subscribe       func(context.Context) (roomSubscription, error)
	retryInterval   time.Duration
	retryMax        time.Duration
	maxPayloadBytes int
}

type RedisSubscriberConfig struct {
	MaxPayloadBytes int
}

func NewRedisBroadcastSubscriber(store RoomSubscriptionStore, keys Keyspace, receiver LocalReceiver, configs ...RedisSubscriberConfig) (*RedisBroadcastSubscriber, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: Redis room subscription store is required", ErrRouteStoreUnavailable)
	}
	if keys.Prefix() == "" {
		return nil, fmt.Errorf("%w: redis key prefix is required", ErrDestinationInvalid)
	}
	if isNilLocalReceiver(receiver) {
		return nil, errors.New("server send: local receiver is required")
	}
	maxPayloadBytes, retryMax, err := subscriberConfigValues(configs)
	if err != nil {
		return nil, err
	}
	return &RedisBroadcastSubscriber{store: store, keys: keys, receiver: receiver, maxPayloadBytes: maxPayloadBytes, retryMax: retryMax}, nil
}

func (s *RedisBroadcastSubscriber) Start(ctx context.Context) error {
	if s == nil {
		return errors.New("server send: Redis broadcast subscriber is nil")
	}
	if ctx == nil {
		return errors.New("server send: Redis broadcast subscriber context is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return errors.New("server send: Redis broadcast subscriber is already started")
	}
	sub, err := s.openSubscription(ctx)
	if err != nil {
		return err
	}
	loopCtx, cancel := context.WithCancel(context.Background())
	s.cancel, s.done, s.sub, s.started = cancel, make(chan struct{}), sub, true
	go s.consume(loopCtx, s.done, sub)
	return nil
}

func (s *RedisBroadcastSubscriber) Stop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return nil
	}
	cancel, done, sub := s.cancel, s.done, s.sub
	s.cancel, s.done, s.sub, s.started = nil, nil, nil, false
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if sub != nil {
		_ = sub.Close()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *RedisBroadcastSubscriber) openSubscription(ctx context.Context) (roomSubscription, error) {
	if s.subscribe != nil {
		sub, err := s.subscribe(ctx)
		if err != nil {
			return nil, routeStoreError("subscribe room broadcasts", err)
		}
		if sub == nil {
			return nil, routeStoreError("subscribe room broadcasts", errors.New("subscription is nil"))
		}
		return sub, nil
	}
	if s.store == nil {
		return nil, routeStoreError("subscribe room broadcasts", errors.New("Redis subscription store is not configured"))
	}
	pubsub := s.store.PSubscribe(ctx, s.keys.roomChannelPattern())
	if pubsub == nil {
		return nil, routeStoreError("subscribe room broadcasts", errors.New("Redis subscription is nil"))
	}
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, routeStoreError("subscribe room broadcasts", err)
	}
	return pubsub, nil
}

func (s *RedisBroadcastSubscriber) consume(ctx context.Context, done chan struct{}, sub roomSubscription) {
	defer close(done)
	current := sub
	attempt := 0
	for {
		message, err := current.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[server send] Redis room broadcast subscriber reconnecting: %v", err)
			_ = current.Close()
			attempt++
			if !waitForSubscriptionRetry(ctx, s.subscriptionRetryDelay(attempt)) {
				return
			}
			next, openErr := s.openSubscription(ctx)
			if openErr != nil {
				log.Printf("[server send] Redis room broadcast resubscribe failed: %v", openErr)
				continue
			}
			if !s.replaceSubscription(done, next) {
				_ = next.Close()
				return
			}
			current = next
			attempt = 0
			continue
		}
		if err := s.handle(ctx, []byte(message.Payload)); err != nil {
			log.Printf("[server send] Redis room broadcast ignored: %v", err)
		}
	}
}

func (s *RedisBroadcastSubscriber) replaceSubscription(done chan struct{}, next roomSubscription) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started || s.done != done {
		return false
	}
	s.sub = next
	return true
}

func (s *RedisBroadcastSubscriber) subscriptionRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if s.retryInterval > 0 {
		base := s.retryInterval
		maxDelay := s.retryMax
		if maxDelay <= 0 {
			maxDelay = defaultSubscriptionRetryMax
		}
		for i := 1; i < attempt && base < maxDelay/2; i++ {
			base *= 2
		}
		if base > maxDelay {
			base = maxDelay
		}
		return jitterDuration(base, maxDelay)
	}
	base := defaultSubscriptionRetryInterval
	maxDelay := s.retryMax
	if maxDelay <= 0 {
		maxDelay = defaultSubscriptionRetryMax
	}
	for i := 1; i < attempt && base < maxDelay/2; i++ {
		base *= 2
	}
	if base > maxDelay {
		base = maxDelay
	}
	return jitterDuration(base, maxDelay)
}

func jitterDuration(base, maxDelay time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	jittered := time.Duration(float64(base) * (0.8 + rand.Float64()*0.4))
	if jittered <= 0 {
		jittered = time.Nanosecond
	}
	if jittered > maxDelay {
		return maxDelay
	}
	return jittered
}

func waitForSubscriptionRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *RedisBroadcastSubscriber) handle(ctx context.Context, encoded []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var envelope RedisBroadcastEnvelope
	if err := proto.Unmarshal(encoded, &envelope); err != nil {
		return fmt.Errorf("decode broadcast envelope: %w", err)
	}
	request := envelope.GetMessage()
	if request == nil {
		return errors.New("broadcast envelope message is required")
	}
	message := BroadcastMessage{RoomID: RoomID(request.GetRoomId()), Message: Message{CommandID: request.GetCommandId(), Payload: append([]byte(nil), request.GetPayload()...)}}
	maxPayloadBytes := s.maxPayloadBytes
	if maxPayloadBytes == 0 {
		maxPayloadBytes = DefaultMaxPayloadBytes
	}
	if err := message.validatePayload(maxPayloadBytes); err != nil {
		return err
	}
	if envelope.GetTraceId() != "" {
		ctx = WithRequestContext(ctx, RequestContext{TraceID: envelope.GetTraceId()})
	}
	delivered, err := s.receiver.BroadcastRoom(ctx, message)
	if delivered < 0 {
		return errors.New("receiver returned invalid delivery count")
	}
	return err
}

var _ BroadcastSender = (*RedisBroadcastSender)(nil)
var _ interface {
	Start(context.Context) error
	Stop(context.Context) error
} = (*RedisBroadcastSubscriber)(nil)

func payloadLimitFromConfig(configs []RedisBroadcastConfig) (int, error) {
	if len(configs) > 1 {
		return 0, fmt.Errorf("%w: only one Redis broadcast config is supported", ErrDestinationInvalid)
	}
	value := 0
	if len(configs) == 1 {
		value = configs[0].MaxPayloadBytes
	}
	return normalizedPayloadLimit(value)
}

func subscriberConfigValues(configs []RedisSubscriberConfig) (int, time.Duration, error) {
	if len(configs) > 1 {
		return 0, 0, fmt.Errorf("%w: only one Redis subscriber config is supported", ErrDestinationInvalid)
	}
	value := RedisSubscriberConfig{}
	if len(configs) == 1 {
		value = configs[0]
	}
	maxPayload, err := normalizedPayloadLimit(value.MaxPayloadBytes)
	if err != nil {
		return 0, 0, err
	}
	return maxPayload, defaultSubscriptionRetryMax, nil
}
