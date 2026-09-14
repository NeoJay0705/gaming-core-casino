package serversend

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/logging"
)

type asyncPlayerLifecycle uint8

const (
	asyncPlayerNew asyncPlayerLifecycle = iota
	asyncPlayerRunning
	asyncPlayerStopping
	asyncPlayerStopped
)

// AsyncPlayerSender turns the existing routing sender into a bounded,
// process-local async acceptance boundary. It owns one queue and one worker.
type AsyncPlayerSender struct {
	delegate PlayerSender
	config   AsyncPlayerConfig
	queue    *asyncQueue
	observer AsyncMetricsObserver
	logger   *logging.Logger

	mu     sync.Mutex
	state  asyncPlayerLifecycle
	cancel context.CancelFunc
	done   chan struct{}
}

// NewAsyncPlayerSender creates a managed async Player sender. The optional
// logger is component-scoped by the product and receives only bounded errors.
func NewAsyncPlayerSender(delegate PlayerSender, config AsyncPlayerConfig, observer AsyncMetricsObserver, loggers ...*logging.Logger) (*AsyncPlayerSender, error) {
	if delegate == nil {
		return nil, errors.New("server send: async player delegate is required")
	}
	normalized, err := NormalizeAsyncPlayerConfig(config)
	if err != nil {
		return nil, err
	}
	if len(loggers) > 1 {
		return nil, errors.New("server send: at most one async player logger is allowed")
	}
	queue, err := newAsyncQueue("player", normalized.AsyncQueueConfig, observer)
	if err != nil {
		return nil, err
	}
	var logger *logging.Logger
	if len(loggers) == 1 {
		logger = loggers[0]
	}
	return &AsyncPlayerSender{
		delegate: delegate,
		config:   normalized,
		queue:    queue,
		observer: asyncMetricsObserver(observer),
		logger:   logger,
		state:    asyncPlayerNew,
	}, nil
}

// SendToPlayers validates and copies the complete request before attempting
// one atomic queue admission. It never waits for Redis or gRPC.
func (s *AsyncPlayerSender) SendToPlayers(ctx context.Context, messages []PlayerMessage) (Receipt, error) {
	if s == nil || s.delegate == nil || s.queue == nil {
		return Receipt{}, ErrSenderNotRunning
	}
	if err := validatePlayerMessages(messages); err != nil {
		return Receipt{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	itemBytes, totalBytes, err := playerQueueAccounting(messages, s.config.QueueCapacityMessages, s.config.QueueCapacityBytes)
	if err != nil {
		if errors.Is(err, ErrEnqueueTooLarge) {
			s.observer.ObserveQueueRejected("player", "too_large")
		}
		return Receipt{}, err
	}
	cloned := clonePlayerMessages(messages)
	job, err := newPlayerJob(ctx, cloned, itemBytes, totalBytes)
	if err != nil {
		return Receipt{}, err
	}
	if err := s.queue.enqueue(ctx, job); err != nil {
		return Receipt{}, err
	}
	return newReceipt(), nil
}

// Start starts exactly one worker and performs no transport I/O.
func (s *AsyncPlayerSender) Start(ctx context.Context) error {
	if s == nil || s.queue == nil {
		return ErrSenderNotRunning
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != asyncPlayerNew {
		return errors.New("server send: async player sender is already started")
	}
	if err := s.queue.start(); err != nil {
		return err
	}
	workerCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})
	s.state = asyncPlayerRunning
	go s.run(workerCtx, s.done)
	return nil
}

// Stop closes admission, drains accepted work, and cancels an in-flight
// dependency only when the supplied shutdown context expires.
func (s *AsyncPlayerSender) Stop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	switch s.state {
	case asyncPlayerNew:
		s.mu.Unlock()
		return nil
	case asyncPlayerStopped:
		s.mu.Unlock()
		return nil
	case asyncPlayerStopping:
		done := s.done
		cancel := s.cancel
		s.mu.Unlock()
		return waitAsyncWorker(ctx, done, cancel)
	case asyncPlayerRunning:
		s.state = asyncPlayerStopping
		done := s.done
		cancel := s.cancel
		s.queue.beginStop()
		s.mu.Unlock()
		return waitAsyncWorker(ctx, done, cancel)
	default:
		s.mu.Unlock()
		return fmt.Errorf("server send: async player sender has unknown state")
	}
}

type playerJob struct {
	accepted   time.Time
	trace      context.Context
	messages   []PlayerMessage
	itemBytes  []int
	offset     int
	consumed   int
	totalBytes int
	waitSeen   bool
}

func playerQueueAccounting(messages []PlayerMessage, capacityMessages, capacityBytes int) ([]int, int, error) {
	if capacityMessages <= 0 || capacityBytes <= 0 || len(messages) > capacityMessages {
		return nil, 0, fmt.Errorf("%w: player queue input exceeds configured capacity", ErrEnqueueTooLarge)
	}
	itemBytes := make([]int, len(messages))
	totalBytes := 0
	for index, message := range messages {
		itemSize := playerDeliveryWireSize(message)
		if itemSize > DefaultMaxPayloadBytes {
			return nil, 0, fmt.Errorf("player message %d: %w: encoded envelope is %d bytes", index, ErrPayloadTooLarge, itemSize)
		}
		if itemSize > capacityBytes-totalBytes {
			return nil, 0, fmt.Errorf("%w: player queue input is %d bytes", ErrEnqueueTooLarge, totalBytes+itemSize)
		}
		itemBytes[index] = itemSize
		totalBytes += itemSize
	}
	return itemBytes, totalBytes, nil
}

func newPlayerJob(ctx context.Context, messages []PlayerMessage, itemBytes []int, totalBytes int) (*playerJob, error) {
	if len(messages) == 0 {
		return nil, fmt.Errorf("%w: player queue job is empty", ErrMessageInvalid)
	}
	if len(itemBytes) != len(messages) || totalBytes < 0 {
		return nil, fmt.Errorf("%w: invalid player queue accounting", ErrEnqueueTooLarge)
	}
	trace, err := detachedTrace(ctx)
	if err != nil {
		return nil, err
	}
	return &playerJob{accepted: time.Now(), trace: trace, messages: messages, itemBytes: itemBytes, totalBytes: totalBytes}, nil
}

func (j *playerJob) queuedMessages() int {
	if j == nil || j.offset >= len(j.messages) {
		return 0
	}
	return len(j.messages) - j.offset
}

func (j *playerJob) queuedBytes() int {
	if j == nil {
		return 0
	}
	return j.totalBytes - j.consumed
}

func (j *playerJob) acceptedAt() time.Time {
	if j == nil {
		return time.Time{}
	}
	return j.accepted
}

func (j *playerJob) claimQueueWaitObservation() bool {
	if j == nil || j.waitSeen {
		return false
	}
	j.waitSeen = true
	return true
}

func (j *playerJob) takePrefix(maxMessages, maxBytes int) ([]PlayerMessage, int, int) {
	if j == nil || maxMessages <= 0 || maxBytes <= 0 {
		return nil, 0, 0
	}
	start := j.offset
	bytes := 0
	for j.offset < len(j.messages) && j.offset-start < maxMessages {
		itemBytes := j.itemBytes[j.offset]
		if j.offset > start && bytes > maxBytes-itemBytes {
			break
		}
		if j.offset == start && itemBytes > maxBytes {
			break
		}
		bytes += itemBytes
		j.offset++
	}
	if j.offset == start {
		return nil, 0, 0
	}
	j.consumed += bytes
	return j.messages[start:j.offset], j.offset - start, bytes
}

func detachedTrace(ctx context.Context) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := logging.TraceParentFromContext(ctx); !ok {
		traced, err := logging.NewRoot(ctx)
		if err != nil {
			return nil, err
		}
		ctx = traced
	}
	return logging.Detach(ctx), nil
}

func (s *AsyncPlayerSender) run(ctx context.Context, done chan struct{}) {
	defer s.finishWorker(done)
	var pending *playerJob
	for {
		first, ok := s.nextPlayerJob(ctx, pending)
		if !ok {
			if ctx.Err() != nil {
				messages, bytes := s.discardPlayerJobs(nil, "shutdown_timeout")
				s.logDiscard(ctx, messages, bytes)
			}
			return
		}
		pending = nil
		batch, trace, bytes, remainder := s.takePlayerBatch(first)
		pending = remainder
		if len(batch) == 0 {
			if ctx.Err() != nil {
				messages, bytes := s.discardPlayerJobs(pending, "shutdown_timeout")
				s.logDiscard(trace, messages, bytes)
				return
			}
			// A validated single item always fits the normalized batch bound.
			continue
		}
		s.observer.ObservePlayerBatch(len(batch), bytes)
		started := time.Now()
		receipt, err := s.delegate.SendToPlayers(withAsyncWorkerCancellation(ctx, trace), batch)
		result := asyncResult(receipt, err)
		s.observer.ObserveWorker("player", result, time.Since(started))
		if err != nil && s.logger != nil {
			s.logger.Error(trace, "send_to_players", "async Player delivery failed", asyncLogError(err), slog.String("result", result))
		}
		if ctx.Err() != nil {
			messages, bytes := s.discardPlayerJobs(pending, "shutdown_timeout")
			s.logDiscard(trace, messages, bytes)
			return
		}
	}
}

func (s *AsyncPlayerSender) finishWorker(done chan struct{}) {
	s.queue.markStopped()
	s.mu.Lock()
	if s.state != asyncPlayerNew {
		s.state = asyncPlayerStopped
	}
	s.mu.Unlock()
	close(done)
}

func (s *AsyncPlayerSender) nextPlayerJob(ctx context.Context, pending *playerJob) (*playerJob, bool) {
	if pending != nil {
		return pending, true
	}
	job, ok := s.queue.receive(ctx)
	if !ok {
		return nil, false
	}
	player, ok := job.(*playerJob)
	if !ok || player == nil {
		return nil, false
	}
	return player, true
}

func (s *AsyncPlayerSender) takePlayerBatch(first *playerJob) ([]PlayerMessage, context.Context, int, *playerJob) {
	batch := make([]PlayerMessage, 0, s.config.BatchMaxMessages)
	trace := first.trace
	bytes := 0
	current := first
	var pending *playerJob
	for current != nil {
		availableBytes := s.config.BatchMaxBytes - bytes
		messages, count, itemBytes := current.takePrefix(s.config.BatchMaxMessages-len(batch), availableBytes)
		if count == 0 {
			pending = current
			break
		}
		s.queue.markStarted(current, count, itemBytes)
		batch = append(batch, messages...)
		bytes += itemBytes
		if current.queuedMessages() > 0 || len(batch) >= s.config.BatchMaxMessages || bytes >= s.config.BatchMaxBytes {
			if current.queuedMessages() > 0 {
				pending = current
			}
			break
		}
		select {
		case item, ok := <-s.queue.items:
			if !ok {
				current = nil
				continue
			}
			candidate, ok := item.(*playerJob)
			if !ok || candidate == nil {
				continue
			}
			if !sameAsyncTrace(trace, candidate.trace) {
				pending = candidate
				current = nil
				continue
			}
			current = candidate
		default:
			current = nil
		}
	}
	return batch, trace, bytes, pending
}

func sameAsyncTrace(left, right context.Context) bool {
	leftTrace, leftOK := logging.TraceParentFromContext(left)
	rightTrace, rightOK := logging.TraceParentFromContext(right)
	return leftOK && rightOK && leftTrace == rightTrace
}

func asyncResult(receipt Receipt, err error) string {
	if err == nil {
		return "success"
	}
	if !receipt.AcceptedAt.IsZero() {
		return "partial"
	}
	return "error"
}

func (s *AsyncPlayerSender) discardPlayerJobs(pending *playerJob, reason string) (messages, bytes int) {
	if pending != nil {
		messages, bytes = s.queue.discard(pending, reason)
	}
	remainingMessages, remainingBytes := s.queue.discardRemaining(reason)
	return messages + remainingMessages, bytes + remainingBytes
}

func (s *AsyncPlayerSender) logDiscard(ctx context.Context, messages, bytes int) {
	if s == nil || s.logger == nil || messages <= 0 {
		return
	}
	if _, ok := logging.TraceParentFromContext(ctx); !ok {
		if traced, err := detachedTrace(context.Background()); err == nil {
			ctx = traced
		}
	}
	s.logger.Warn(ctx, "shutdown_timeout", "async Player queue discarded work", slog.Int("messages", messages), slog.Int("bytes", bytes))
}

var _ PlayerSender = (*AsyncPlayerSender)(nil)
