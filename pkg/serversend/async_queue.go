package serversend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	// DefaultAsyncQueueCapacityMessages is the default number of logical
	// messages a sender may retain in memory.
	DefaultAsyncQueueCapacityMessages = 10_000
	// DefaultAsyncQueueCapacityBytes bounds the deterministic encoded bytes
	// retained by one async sender.
	DefaultAsyncQueueCapacityBytes = 64 << 20
	// DefaultAsyncPlayerBatchMaxMessages is the default Player worker batch
	// count. Broadcast does not use a transport batch limit.
	DefaultAsyncPlayerBatchMaxMessages = 10_000
	// DefaultAsyncPlayerBatchMaxBytes is the default Player worker batch byte
	// limit. Each resulting generic command is still limited to 1 MiB.
	DefaultAsyncPlayerBatchMaxBytes = 8 << 20
)

// AsyncQueueConfig bounds one process-local async sender queue. Zero selects
// the documented default; negative values are invalid.
type AsyncQueueConfig struct {
	QueueCapacityMessages int `config:"queue_capacity_messages" yaml:"queue_capacity_messages"`
	QueueCapacityBytes    int `config:"queue_capacity_bytes" yaml:"queue_capacity_bytes"`
}

// AsyncPlayerConfig adds the immediate Player coalescing limits to a bounded
// queue. Broadcast intentionally uses only AsyncQueueConfig.
type AsyncPlayerConfig struct {
	AsyncQueueConfig
	BatchMaxMessages int `config:"batch_max_messages" yaml:"batch_max_messages"`
	BatchMaxBytes    int `config:"batch_max_bytes" yaml:"batch_max_bytes"`
}

// NormalizeAsyncQueueConfig applies safe defaults and validates queue bounds.
func NormalizeAsyncQueueConfig(cfg AsyncQueueConfig) (AsyncQueueConfig, error) {
	if cfg.QueueCapacityMessages < 0 {
		return AsyncQueueConfig{}, fmt.Errorf("server send: queue capacity messages cannot be negative")
	}
	if cfg.QueueCapacityBytes < 0 {
		return AsyncQueueConfig{}, fmt.Errorf("server send: queue capacity bytes cannot be negative")
	}
	if cfg.QueueCapacityMessages == 0 {
		cfg.QueueCapacityMessages = DefaultAsyncQueueCapacityMessages
	}
	if cfg.QueueCapacityBytes == 0 {
		cfg.QueueCapacityBytes = DefaultAsyncQueueCapacityBytes
	}
	if cfg.QueueCapacityMessages <= 0 || cfg.QueueCapacityBytes <= 0 {
		return AsyncQueueConfig{}, fmt.Errorf("server send: queue capacities must be positive")
	}
	return cfg, nil
}

// NormalizeAsyncPlayerConfig applies queue and Player batch defaults.
func NormalizeAsyncPlayerConfig(cfg AsyncPlayerConfig) (AsyncPlayerConfig, error) {
	queue, err := NormalizeAsyncQueueConfig(cfg.AsyncQueueConfig)
	if err != nil {
		return AsyncPlayerConfig{}, err
	}
	cfg.AsyncQueueConfig = queue
	if cfg.BatchMaxMessages < 0 {
		return AsyncPlayerConfig{}, fmt.Errorf("server send: player batch max messages cannot be negative")
	}
	if cfg.BatchMaxBytes < 0 {
		return AsyncPlayerConfig{}, fmt.Errorf("server send: player batch max bytes cannot be negative")
	}
	if cfg.BatchMaxMessages == 0 {
		cfg.BatchMaxMessages = DefaultAsyncPlayerBatchMaxMessages
	}
	if cfg.BatchMaxBytes == 0 {
		cfg.BatchMaxBytes = DefaultAsyncPlayerBatchMaxBytes
	}
	if cfg.BatchMaxMessages <= 0 || cfg.BatchMaxBytes <= 0 {
		return AsyncPlayerConfig{}, fmt.Errorf("server send: player batch limits must be positive")
	}
	if cfg.BatchMaxMessages > cfg.QueueCapacityMessages {
		return AsyncPlayerConfig{}, fmt.Errorf("server send: player batch max messages cannot exceed queue capacity messages")
	}
	if cfg.BatchMaxBytes > cfg.QueueCapacityBytes {
		return AsyncPlayerConfig{}, fmt.Errorf("server send: player batch max bytes cannot exceed queue capacity bytes")
	}
	if cfg.BatchMaxBytes < DefaultMaxPayloadBytes {
		return AsyncPlayerConfig{}, fmt.Errorf("server send: player batch max bytes must be at least %d", DefaultMaxPayloadBytes)
	}
	return cfg, nil
}

// AsyncMetricsObserver is the small, Prometheus-independent observation
// boundary used by async senders. Implementations must use bounded labels.
type AsyncMetricsObserver interface {
	SetQueueCapacity(operation string, messages, bytes int)
	ObserveQueue(operation string, messages, bytes int)
	ObserveQueueRejected(operation, reason string)
	ObserveQueueWait(operation string, elapsed time.Duration)
	ObserveWorker(operation, result string, elapsed time.Duration)
	ObservePlayerBatch(messages, bytes int)
	ObserveDependency(operation, dependency, result string, elapsed time.Duration)
	ObserveFallback(operation, reason string)
	ObserveDiscarded(operation, reason string, messages, bytes int)
}

type noopAsyncMetricsObserver struct{}

func (noopAsyncMetricsObserver) SetQueueCapacity(string, int, int) {}
func (noopAsyncMetricsObserver) ObserveQueue(string, int, int)     {}
func (noopAsyncMetricsObserver) ObserveQueueRejected(string, string) {
}
func (noopAsyncMetricsObserver) ObserveQueueWait(string, time.Duration) {}
func (noopAsyncMetricsObserver) ObserveWorker(string, string, time.Duration) {
}
func (noopAsyncMetricsObserver) ObservePlayerBatch(int, int) {}
func (noopAsyncMetricsObserver) ObserveDependency(string, string, string, time.Duration) {
}
func (noopAsyncMetricsObserver) ObserveFallback(string, string) {}
func (noopAsyncMetricsObserver) ObserveDiscarded(string, string, int, int) {
}

func asyncMetricsObserver(observer AsyncMetricsObserver) AsyncMetricsObserver {
	if observer == nil {
		return noopAsyncMetricsObserver{}
	}
	return observer
}

func asyncDependencyResult(err error) string {
	if err == nil {
		return "success"
	}
	return "error"
}

// asyncLogError 只輸出 bounded failure category。既有 routing error 可能含
// login name、endpoint 或 Redis 原始訊息，不能直接交給 structured logger。
func asyncLogError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return errors.New("context canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return errors.New("context deadline exceeded")
	case errors.Is(err, ErrRouteStoreUnavailable):
		return errors.New("route store unavailable")
	case errors.Is(err, ErrPresenceNotFound):
		return errors.New("player presence not found")
	case errors.Is(err, ErrGateEndpointNotFound):
		return errors.New("Gate endpoint not found")
	case errors.Is(err, ErrPayloadTooLarge):
		return errors.New("payload too large")
	default:
		return errors.New("downstream delivery failed")
	}
}

type asyncQueueJob interface {
	queuedMessages() int
	queuedBytes() int
	acceptedAt() time.Time
}

type asyncQueueWaitTracker interface {
	claimQueueWaitObservation() bool
}

// asyncWorkerContext 保留 queue job 的 detached logging values，同時把
// worker 的取消訊號傳給 Redis／gRPC delegate。直接把兩個 context 其中一個
// 傳入會遺失另一側的 trace 或 cancellation，因此以最小 Context wrapper 合併。
type asyncWorkerContext struct {
	trace  context.Context
	worker context.Context
}

func (c asyncWorkerContext) Deadline() (time.Time, bool) {
	return c.worker.Deadline()
}

func (c asyncWorkerContext) Done() <-chan struct{} {
	return c.worker.Done()
}

func (c asyncWorkerContext) Err() error {
	return c.worker.Err()
}

func (c asyncWorkerContext) Value(key any) any {
	return c.trace.Value(key)
}

func withAsyncWorkerCancellation(worker, trace context.Context) context.Context {
	if worker == nil {
		worker = context.Background()
	}
	if trace == nil {
		trace = context.Background()
	}
	return asyncWorkerContext{trace: trace, worker: worker}
}

// waitAsyncWorker waits for normal completion, but never extends the caller's
// shutdown deadline after cancellation. The worker owns its eventual cleanup;
// Stop callers only need to request cancellation and report the context error.
func waitAsyncWorker(ctx context.Context, done <-chan struct{}, cancel context.CancelFunc) error {
	if done == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		if cancel != nil {
			cancel()
		}
		return ctx.Err()
	}
}

type asyncQueueState uint8

const (
	asyncQueueNew asyncQueueState = iota
	asyncQueueRunning
	asyncQueueStopping
	asyncQueueStopped
)

// asyncQueue is shared by Player and Broadcast wrappers. A mutex protects
// lifecycle and accounting; the channel itself only carries immutable jobs.
type asyncQueue struct {
	operation       string
	capacityEntries int
	capacityBytes   int
	items           chan asyncQueueJob
	observer        AsyncMetricsObserver

	mu             sync.Mutex
	state          asyncQueueState
	queuedMessages int
	queuedBytes    int
}

func newAsyncQueue(operation string, cfg AsyncQueueConfig, observer AsyncMetricsObserver) (*asyncQueue, error) {
	normalized, err := NormalizeAsyncQueueConfig(cfg)
	if err != nil {
		return nil, err
	}
	if operation == "" {
		return nil, errors.New("server send: async queue operation is required")
	}
	q := &asyncQueue{
		operation:       operation,
		capacityEntries: normalized.QueueCapacityMessages,
		capacityBytes:   normalized.QueueCapacityBytes,
		items:           make(chan asyncQueueJob, normalized.QueueCapacityMessages),
		observer:        asyncMetricsObserver(observer),
		state:           asyncQueueNew,
	}
	q.observer.SetQueueCapacity(operation, normalized.QueueCapacityMessages, normalized.QueueCapacityBytes)
	return q, nil
}

func (q *asyncQueue) start() error {
	if q == nil {
		return errors.New("server send: async queue is nil")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.state != asyncQueueNew {
		return errors.New("server send: async queue is already started")
	}
	q.state = asyncQueueRunning
	return nil
}

func (q *asyncQueue) beginStop() {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.state != asyncQueueRunning {
		return
	}
	q.state = asyncQueueStopping
	close(q.items)
}

func (q *asyncQueue) markStopped() {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.state = asyncQueueStopped
	q.mu.Unlock()
}

func (q *asyncQueue) enqueue(ctx context.Context, job asyncQueueJob) error {
	if q == nil {
		return ErrSenderNotRunning
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if job == nil || job.queuedMessages() <= 0 || job.queuedBytes() < 0 {
		return fmt.Errorf("%w: invalid async queue job", ErrEnqueueTooLarge)
	}
	messages, bytes := job.queuedMessages(), job.queuedBytes()
	q.mu.Lock()
	// 複製工作可能花費時間；進入 admission critical section 後再次檢查
	// caller context，避免取消剛好發生在 lock 等待期間仍被接受。
	if err := ctx.Err(); err != nil {
		q.mu.Unlock()
		return err
	}
	if q.state != asyncQueueRunning {
		q.mu.Unlock()
		q.observer.ObserveQueueRejected(q.operation, "not_running")
		return ErrSenderNotRunning
	}
	if messages > q.capacityEntries || bytes > q.capacityBytes {
		q.mu.Unlock()
		q.observer.ObserveQueueRejected(q.operation, "too_large")
		return ErrEnqueueTooLarge
	}
	if messages > q.capacityEntries-q.queuedMessages || bytes > q.capacityBytes-q.queuedBytes {
		q.mu.Unlock()
		q.observer.ObserveQueueRejected(q.operation, "full")
		return ErrQueueFull
	}
	// Admission and close are serialized by the mutex, so a successful send
	// cannot race a channel close and panic.
	q.queuedMessages += messages
	q.queuedBytes += bytes
	q.items <- job
	queuedMessages, queuedBytes := q.queuedMessages, q.queuedBytes
	// Keep the gauge update in the same critical section as accounting. If it
	// happened after unlock, a concurrent dequeue could publish an older value
	// after a newer enqueue and leave the exported gauge stale.
	q.observer.ObserveQueue(q.operation, queuedMessages, queuedBytes)
	q.mu.Unlock()
	return nil
}

func (q *asyncQueue) receive(ctx context.Context) (asyncQueueJob, bool) {
	if q == nil {
		return nil, false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case job, ok := <-q.items:
		return job, ok
	case <-ctx.Done():
		return nil, false
	}
}

func (q *asyncQueue) markStarted(job asyncQueueJob, messages, bytes int) {
	if q == nil || job == nil || messages <= 0 || bytes < 0 {
		return
	}
	q.mu.Lock()
	q.queuedMessages -= messages
	q.queuedBytes -= bytes
	if q.queuedMessages < 0 {
		q.queuedMessages = 0
	}
	if q.queuedBytes < 0 {
		q.queuedBytes = 0
	}
	queuedMessages, queuedBytes := q.queuedMessages, q.queuedBytes
	// Serialize the gauge publication with counter mutation; see enqueue.
	q.observer.ObserveQueue(q.operation, queuedMessages, queuedBytes)
	q.mu.Unlock()
	observeWait := true
	if tracker, ok := job.(asyncQueueWaitTracker); ok {
		observeWait = tracker.claimQueueWaitObservation()
	}
	if observeWait {
		if acceptedAt := job.acceptedAt(); !acceptedAt.IsZero() {
			q.observer.ObserveQueueWait(q.operation, time.Since(acceptedAt))
		}
	}
}

func (q *asyncQueue) discard(job asyncQueueJob, reason string) (int, int) {
	if q == nil || job == nil {
		return 0, 0
	}
	messages, bytes := job.queuedMessages(), job.queuedBytes()
	if messages <= 0 || bytes < 0 {
		return 0, 0
	}
	q.mu.Lock()
	q.queuedMessages -= messages
	q.queuedBytes -= bytes
	if q.queuedMessages < 0 {
		q.queuedMessages = 0
	}
	if q.queuedBytes < 0 {
		q.queuedBytes = 0
	}
	queuedMessages, queuedBytes := q.queuedMessages, q.queuedBytes
	q.observer.ObserveQueue(q.operation, queuedMessages, queuedBytes)
	q.mu.Unlock()
	q.observer.ObserveDiscarded(q.operation, reason, messages, bytes)
	return messages, bytes
}

func (q *asyncQueue) discardRemaining(reason string) (messages, bytes int) {
	if q == nil {
		return 0, 0
	}
	for job := range q.items {
		jobMessages, jobBytes := q.discard(job, reason)
		messages += jobMessages
		bytes += jobBytes
	}
	return messages, bytes
}

func (q *asyncQueue) snapshot() (messages, bytes int) {
	if q == nil {
		return 0, 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queuedMessages, q.queuedBytes
}
