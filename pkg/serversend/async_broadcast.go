package serversend

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"github.com/NeoJay0705/gaming-core-casino/pkg/logging"
	"google.golang.org/protobuf/proto"
)

type asyncBroadcastLifecycle uint8

const (
	asyncBroadcastNew asyncBroadcastLifecycle = iota
	asyncBroadcastRunning
	asyncBroadcastStopping
	asyncBroadcastStopped
)

// AsyncBroadcastSender turns one business Broadcast command into a bounded
// async acceptance boundary. Business-level repeated messages belong in the
// command protobuf; this wrapper never parses or merges payloads.
type AsyncBroadcastSender struct {
	delegate BroadcastSender
	queue    *asyncQueue
	observer AsyncMetricsObserver
	logger   *logging.Logger

	mu     sync.Mutex
	state  asyncBroadcastLifecycle
	cancel context.CancelFunc
	done   chan struct{}
}

// NewAsyncBroadcastSender creates a managed async Broadcast sender. The
// optional logger receives bounded worker failures only.
func NewAsyncBroadcastSender(delegate BroadcastSender, config AsyncQueueConfig, observer AsyncMetricsObserver, loggers ...*logging.Logger) (*AsyncBroadcastSender, error) {
	if delegate == nil {
		return nil, errors.New("server send: async broadcast delegate is required")
	}
	if len(loggers) > 1 {
		return nil, errors.New("server send: at most one async broadcast logger is allowed")
	}
	queue, err := newAsyncQueue("broadcast", config, observer)
	if err != nil {
		return nil, err
	}
	var logger *logging.Logger
	if len(loggers) == 1 {
		logger = loggers[0]
	}
	return &AsyncBroadcastSender{
		delegate: delegate,
		queue:    queue,
		observer: asyncMetricsObserver(observer),
		logger:   logger,
		state:    asyncBroadcastNew,
	}, nil
}

// Broadcast validates and clones the complete outer command before one
// atomic queue admission. It does not wait for Redis or gRPC.
func (s *AsyncBroadcastSender) Broadcast(ctx context.Context, message Message) (Receipt, error) {
	if s == nil || s.delegate == nil || s.queue == nil {
		return Receipt{}, ErrSenderNotRunning
	}
	if err := message.validatePayload(DefaultMaxPayloadBytes); err != nil {
		return Receipt{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	job, err := newBroadcastJob(ctx, message.clone())
	if err != nil {
		return Receipt{}, err
	}
	if err := s.queue.enqueue(ctx, job); err != nil {
		return Receipt{}, err
	}
	return newReceipt(), nil
}

// Start starts exactly one worker and performs no transport I/O.
func (s *AsyncBroadcastSender) Start(ctx context.Context) error {
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
	if s.state != asyncBroadcastNew {
		return errors.New("server send: async broadcast sender is already started")
	}
	if err := s.queue.start(); err != nil {
		return err
	}
	workerCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})
	s.state = asyncBroadcastRunning
	go s.run(workerCtx, s.done)
	return nil
}

// Stop closes admission, drains accepted commands, and cancels an in-flight
// dependency only when the supplied shutdown context expires.
func (s *AsyncBroadcastSender) Stop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	switch s.state {
	case asyncBroadcastNew:
		s.mu.Unlock()
		return nil
	case asyncBroadcastStopped:
		s.mu.Unlock()
		return nil
	case asyncBroadcastStopping:
		done, cancel := s.done, s.cancel
		s.mu.Unlock()
		return waitAsyncWorker(ctx, done, cancel)
	case asyncBroadcastRunning:
		s.state = asyncBroadcastStopping
		done, cancel := s.done, s.cancel
		s.queue.beginStop()
		s.mu.Unlock()
		return waitAsyncWorker(ctx, done, cancel)
	default:
		s.mu.Unlock()
		return fmt.Errorf("server send: async broadcast sender has unknown state")
	}
}

type broadcastJob struct {
	accepted time.Time
	trace    context.Context
	message  Message
	bytes    int
}

func newBroadcastJob(ctx context.Context, message Message) (*broadcastJob, error) {
	request := &gatelink.GateRequest{CommandId: message.CommandID, Payload: message.Payload}
	// A fixed canonical traceparent length makes queue accounting deterministic;
	// the actual detached trace is still carried by the job context.
	encodedBytes := proto.Size(&RedisBroadcastEnvelope{
		Traceparent: "00-00000000000000000000000000000000-0000000000000000-00",
		Command:     request,
	})
	trace, err := detachedTrace(ctx)
	if err != nil {
		return nil, err
	}
	return &broadcastJob{accepted: time.Now(), trace: trace, message: message, bytes: encodedBytes}, nil
}

func (j *broadcastJob) queuedMessages() int {
	if j == nil {
		return 0
	}
	return 1
}

func (j *broadcastJob) queuedBytes() int {
	if j == nil {
		return 0
	}
	return j.bytes
}

func (j *broadcastJob) acceptedAt() time.Time {
	if j == nil {
		return time.Time{}
	}
	return j.accepted
}

func (s *AsyncBroadcastSender) run(ctx context.Context, done chan struct{}) {
	defer s.finishWorker(done)
	for {
		item, ok := s.queue.receive(ctx)
		if !ok {
			if ctx.Err() != nil {
				messages, bytes := s.queue.discardRemaining("shutdown_timeout")
				s.logDiscard(ctx, messages, bytes)
			}
			return
		}
		job, ok := item.(*broadcastJob)
		if !ok || job == nil {
			continue
		}
		s.queue.markStarted(job, 1, job.bytes)
		started := time.Now()
		receipt, err := s.delegate.Broadcast(withAsyncWorkerCancellation(ctx, job.trace), job.message)
		result := asyncResult(receipt, err)
		s.observer.ObserveWorker("broadcast", result, time.Since(started))
		if err != nil && s.logger != nil {
			s.logger.Error(job.trace, "broadcast", "async Broadcast delivery failed", asyncLogError(err), slog.String("result", result))
		}
		if ctx.Err() != nil {
			messages, bytes := s.queue.discardRemaining("shutdown_timeout")
			s.logDiscard(job.trace, messages, bytes)
			return
		}
	}
}

func (s *AsyncBroadcastSender) finishWorker(done chan struct{}) {
	s.queue.markStopped()
	s.mu.Lock()
	if s.state != asyncBroadcastNew {
		s.state = asyncBroadcastStopped
	}
	s.mu.Unlock()
	close(done)
}

func (s *AsyncBroadcastSender) logDiscard(ctx context.Context, messages, bytes int) {
	if s == nil || s.logger == nil || messages <= 0 {
		return
	}
	if _, ok := logging.TraceParentFromContext(ctx); !ok {
		if traced, err := detachedTrace(context.Background()); err == nil {
			ctx = traced
		}
	}
	s.logger.Warn(ctx, "shutdown_timeout", "async Broadcast queue discarded work", slog.Int("messages", messages), slog.Int("bytes", bytes))
}

var _ BroadcastSender = (*AsyncBroadcastSender)(nil)
