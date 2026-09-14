package serversend

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"github.com/NeoJay0705/gaming-core-casino/pkg/logging"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

// BroadcastPublisher 是窄型 Redis Pub/Sub publish dependency。
type BroadcastPublisher interface {
	Publish(context.Context, string, any) *redis.IntCmd
}

// RedisBroadcastSender 對 generic command publish 一次。Redis Pub/Sub 刻意
// 維持 best-effort，斷線 Gate 不會 replay 遺失訊息。
type RedisBroadcastSender struct {
	publisher BroadcastPublisher
	keys      Keyspace
	observer  AsyncMetricsObserver
}

var errBroadcastNoSubscribers = errors.New("server send: broadcast publish has no subscribers")

func NewRedisBroadcastSender(publisher BroadcastPublisher, keys Keyspace) (*RedisBroadcastSender, error) {
	if publisher == nil {
		return nil, fmt.Errorf("%w: Redis broadcast publisher is required", ErrRouteStoreUnavailable)
	}
	if keys.Prefix() == "" {
		return nil, fmt.Errorf("%w: redis key prefix is required", ErrDestinationInvalid)
	}
	return &RedisBroadcastSender{publisher: publisher, keys: keys}, nil
}

func (s *RedisBroadcastSender) Broadcast(ctx context.Context, message Message) (Receipt, error) {
	if s == nil || s.publisher == nil {
		return Receipt{}, fmt.Errorf("%w: Redis broadcast publisher is not configured", ErrRouteStoreUnavailable)
	}
	if err := message.validatePayload(DefaultMaxPayloadBytes); err != nil {
		return Receipt{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	message = message.clone()
	traced, err := logging.Child(ctx)
	if err != nil {
		return Receipt{}, err
	}
	traceparent, ok := logging.TraceParentFromContext(traced)
	if !ok {
		return Receipt{}, errors.New("server send: Redis broadcast trace is unavailable")
	}
	encoded, err := proto.Marshal(&RedisBroadcastEnvelope{
		Traceparent: traceparent,
		Command:     &gatelink.GateRequest{CommandId: message.CommandID, Payload: message.Payload},
	})
	if err != nil {
		return Receipt{}, fmt.Errorf("server send: encode Redis broadcast command: %w", err)
	}
	started := time.Now()
	count, err := s.publisher.Publish(traced, s.keys.broadcastChannel(), encoded).Result()
	if s.observer != nil {
		s.observer.ObserveDependency("broadcast", "redis_publish", asyncDependencyResult(err), time.Since(started))
	}
	if err != nil {
		return Receipt{}, routeStoreError("publish broadcast command", err)
	}
	if count == 0 {
		return Receipt{}, fmt.Errorf("%w: %w", ErrRouteStoreUnavailable, errBroadcastNoSubscribers)
	}
	return newReceipt(), nil
}

// SetAsyncMetricsObserver attaches the product metrics adapter to Redis
// publish latency without coupling this package to a metrics implementation.
func (s *RedisBroadcastSender) SetAsyncMetricsObserver(observer AsyncMetricsObserver) {
	if s != nil {
		s.observer = observer
	}
}

// BroadcastSubscriptionStore 是窄型 Redis Pub/Sub subscription dependency。
type BroadcastSubscriptionStore interface {
	PSubscribe(context.Context, ...string) *redis.PubSub
}

type broadcastSubscription interface {
	ReceiveMessage(context.Context) (*redis.Message, error)
	Close() error
}

const (
	defaultSubscriptionRetryInterval = 100 * time.Millisecond
	defaultSubscriptionRetryMax      = 30 * time.Second
)

// RedisBroadcastSubscriber 透過固定 channel 接收 generic command，並將每筆
// 訊息交給 Gate product handler。subscription 中斷時以 bounded exponential
// backoff 與 jitter 重新建立；Pub/Sub 對中斷期間遺失的訊息仍不 replay。
type RedisBroadcastSubscriber struct {
	store             BroadcastSubscriptionStore
	keys              Keyspace
	commandDispatcher *dispatcher.Dispatcher

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	sub     broadcastSubscription
	started bool
	logger  *logging.Logger

	// subscribe 與 retryInterval 讓 lifecycle 行為可做 contract test，避免
	// 暴露 Redis Pub/Sub 實作細節。
	subscribe     func(context.Context) (broadcastSubscription, error)
	retryInterval time.Duration
	retryMax      time.Duration
}

func NewRedisBroadcastSubscriber(store BroadcastSubscriptionStore, keys Keyspace, commandDispatcher *dispatcher.Dispatcher, loggers ...*logging.Logger) (*RedisBroadcastSubscriber, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: Redis broadcast subscription store is required", ErrRouteStoreUnavailable)
	}
	if keys.Prefix() == "" {
		return nil, fmt.Errorf("%w: redis key prefix is required", ErrDestinationInvalid)
	}
	if commandDispatcher == nil {
		return nil, errors.New("server send: dispatcher is required")
	}
	var logger *logging.Logger
	if len(loggers) > 1 {
		return nil, errors.New("server send: at most one Redis broadcast logger is allowed")
	}
	if len(loggers) == 1 {
		logger = loggers[0]
	}
	return &RedisBroadcastSubscriber{store: store, keys: keys, commandDispatcher: commandDispatcher, retryMax: defaultSubscriptionRetryMax, logger: logger}, nil
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

func (s *RedisBroadcastSubscriber) openSubscription(ctx context.Context) (broadcastSubscription, error) {
	if s.subscribe != nil {
		sub, err := s.subscribe(ctx)
		if err != nil {
			return nil, routeStoreError("subscribe broadcast commands", err)
		}
		if sub == nil {
			return nil, routeStoreError("subscribe broadcast commands", errors.New("subscription is nil"))
		}
		return sub, nil
	}
	if s.store == nil {
		return nil, routeStoreError("subscribe broadcast commands", errors.New("Redis subscription store is not configured"))
	}
	pubsub := s.store.PSubscribe(ctx, s.keys.broadcastChannel())
	if pubsub == nil {
		return nil, routeStoreError("subscribe broadcast commands", errors.New("Redis subscription is nil"))
	}
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, routeStoreError("subscribe broadcast commands", err)
	}
	return pubsub, nil
}

func (s *RedisBroadcastSubscriber) consume(ctx context.Context, done chan struct{}, sub broadcastSubscription) {
	defer close(done)
	current := sub
	attempt := 0
	for {
		message, err := current.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if s.logger != nil {
				s.logger.Warn(ctx, "redis_broadcast_receive", "Redis broadcast subscriber reconnecting", slog.String("cause", err.Error()))
			}
			_ = current.Close()
			attempt++
			if !waitForSubscriptionRetry(ctx, s.subscriptionRetryDelay(attempt)) {
				return
			}
			next, openErr := s.openSubscription(ctx)
			if openErr != nil {
				if s.logger != nil {
					s.logger.Error(ctx, "redis_broadcast_subscribe", "Redis broadcast resubscribe failed", openErr)
				}
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
		_ = s.handle(ctx, []byte(message.Payload))
	}
}

func (s *RedisBroadcastSubscriber) replaceSubscription(done chan struct{}, next broadcastSubscription) bool {
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

func (s *RedisBroadcastSubscriber) handle(ctx context.Context, encoded []byte) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	logCtx := ctx
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("Redis broadcast handler panic: %v", recovered)
		}
		if err != nil && s != nil && s.logger != nil {
			s.logger.Error(logCtx, "redis_broadcast_dispatch", "Redis broadcast command ignored", err)
		}
	}()
	var envelope RedisBroadcastEnvelope
	if err := proto.Unmarshal(encoded, &envelope); err != nil {
		return fmt.Errorf("decode broadcast command: %w", err)
	}
	traced, traceErr := logging.ContinueOrNew(ctx, envelope.GetTraceparent())
	if traceErr != nil {
		return traceErr
	}
	logCtx = traced
	if envelope.GetCommand() == nil {
		return errors.New("decode broadcast command: command is required")
	}
	_, err = dispatchRemoteCommand(traced, s.commandDispatcher, Message{
		CommandID: envelope.GetCommand().GetCommandId(),
		Payload:   append([]byte(nil), envelope.GetCommand().GetPayload()...),
	})
	return err
}

var _ BroadcastSender = (*RedisBroadcastSender)(nil)
var _ interface {
	Start(context.Context) error
	Stop(context.Context) error
} = (*RedisBroadcastSubscriber)(nil)
