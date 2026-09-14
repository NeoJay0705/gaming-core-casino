package serversend

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/logging"
)

func TestNormalizeAsyncConfigsUsesDefaultsAndRejectsUnsafeBounds(t *testing.T) {
	queue, err := NormalizeAsyncQueueConfig(AsyncQueueConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if queue.QueueCapacityMessages != DefaultAsyncQueueCapacityMessages || queue.QueueCapacityBytes != DefaultAsyncQueueCapacityBytes {
		t.Fatalf("queue defaults = %#v", queue)
	}
	player, err := NormalizeAsyncPlayerConfig(AsyncPlayerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if player.BatchMaxMessages != DefaultAsyncPlayerBatchMaxMessages || player.BatchMaxBytes != DefaultAsyncPlayerBatchMaxBytes {
		t.Fatalf("player defaults = %#v", player)
	}

	for name, cfg := range map[string]AsyncPlayerConfig{
		"negative queue messages": {AsyncQueueConfig: AsyncQueueConfig{QueueCapacityMessages: -1}},
		"negative batch bytes":    {BatchMaxBytes: -1},
		"batch messages over queue": {
			AsyncQueueConfig: AsyncQueueConfig{QueueCapacityMessages: 1, QueueCapacityBytes: DefaultMaxPayloadBytes},
			BatchMaxMessages: 2,
			BatchMaxBytes:    DefaultMaxPayloadBytes,
		},
		"batch bytes under payload limit": {
			AsyncQueueConfig: AsyncQueueConfig{QueueCapacityMessages: 1, QueueCapacityBytes: DefaultMaxPayloadBytes},
			BatchMaxMessages: 1,
			BatchMaxBytes:    DefaultMaxPayloadBytes - 1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NormalizeAsyncPlayerConfig(cfg); err == nil {
				t.Fatal("unsafe async configuration was accepted")
			}
		})
	}
}

func TestAsyncPlayerQueueCoalescesOnlyAdjacentSameTrace(t *testing.T) {
	config := AsyncPlayerConfig{
		AsyncQueueConfig: AsyncQueueConfig{QueueCapacityMessages: 8, QueueCapacityBytes: 2 * DefaultMaxPayloadBytes},
		BatchMaxMessages: 8,
		BatchMaxBytes:    DefaultMaxPayloadBytes,
	}
	normalized, err := NormalizeAsyncPlayerConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	trace, err := logging.ContinueOrNew(context.Background(), "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
	if err != nil {
		t.Fatal(err)
	}
	otherTrace, err := logging.ContinueOrNew(context.Background(), "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b8-00")
	if err != nil {
		t.Fatal(err)
	}

	newQueue := func(t *testing.T) *asyncQueue {
		t.Helper()
		queue, err := newAsyncQueue("player", normalized.AsyncQueueConfig, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := queue.start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			queue.beginStop()
			queue.discardRemaining("test_cleanup")
			queue.markStopped()
		})
		return queue
	}
	newJob := func(t *testing.T, ctx context.Context, commandID uint32) *playerJob {
		t.Helper()
		messages := []PlayerMessage{{LoginName: LoginName("player"), Message: Message{CommandID: commandID, Payload: []byte("payload")}}}
		itemBytes, totalBytes, err := playerQueueAccounting(messages, normalized.QueueCapacityMessages, normalized.QueueCapacityBytes)
		if err != nil {
			t.Fatal(err)
		}
		job, err := newPlayerJob(ctx, messages, itemBytes, totalBytes)
		if err != nil {
			t.Fatal(err)
		}
		return job
	}

	t.Run("same trace is flattened", func(t *testing.T) {
		queue := newQueue(t)
		sender := &AsyncPlayerSender{queue: queue, config: normalized}
		if err := queue.enqueue(context.Background(), newJob(t, trace, 1)); err != nil {
			t.Fatal(err)
		}
		if err := queue.enqueue(context.Background(), newJob(t, trace, 2)); err != nil {
			t.Fatal(err)
		}
		first, ok := queue.receive(context.Background())
		if !ok {
			t.Fatal("first player job was not received")
		}
		batch, _, _, pending := sender.takePlayerBatch(first.(*playerJob))
		if len(batch) != 2 || pending != nil {
			t.Fatalf("same-trace batch = %d pending:%v, want 2/nil", len(batch), pending != nil)
		}
	})

	t.Run("different trace remains pending", func(t *testing.T) {
		queue := newQueue(t)
		sender := &AsyncPlayerSender{queue: queue, config: normalized}
		if err := queue.enqueue(context.Background(), newJob(t, trace, 1)); err != nil {
			t.Fatal(err)
		}
		if err := queue.enqueue(context.Background(), newJob(t, otherTrace, 2)); err != nil {
			t.Fatal(err)
		}
		first, ok := queue.receive(context.Background())
		if !ok {
			t.Fatal("first player job was not received")
		}
		batch, _, _, pending := sender.takePlayerBatch(first.(*playerJob))
		if len(batch) != 1 || pending == nil || pending.messages[0].CommandID != 2 {
			t.Fatalf("different-trace batch = %d pending:%v, want 1/command 2", len(batch), pending)
		}
		queue.discard(pending, "test_cleanup")
	})
}

func TestAsyncPlayerAdmissionIsAtomicAndDefensivelyCopies(t *testing.T) {
	delegate := &recordingAsyncPlayerDelegate{started: make(chan struct{}, 1), release: make(chan struct{})}
	sender, err := NewAsyncPlayerSender(delegate, AsyncPlayerConfig{
		AsyncQueueConfig: AsyncQueueConfig{QueueCapacityMessages: 4, QueueCapacityBytes: 2 * DefaultMaxPayloadBytes},
		BatchMaxMessages: 4,
		BatchMaxBytes:    DefaultMaxPayloadBytes,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = sender.Stop(stopCtx)
	})
	payload := []byte("original")
	receipt, err := sender.SendToPlayers(context.Background(), []PlayerMessage{{
		LoginName: "alice",
		Message:   Message{CommandID: 1, Payload: payload},
	}})
	if err != nil || receipt.AcceptedAt.IsZero() {
		t.Fatalf("admission = receipt:%#v error:%v", receipt, err)
	}
	payload[0] = 'X'
	select {
	case <-delegate.started:
	case <-time.After(time.Second):
		t.Fatal("async Player worker did not start")
	}
	delegate.releaseOnce.Do(func() { close(delegate.release) })
	if err := sender.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	delegate.mu.Lock()
	defer delegate.mu.Unlock()
	if len(delegate.calls) != 1 || string(delegate.calls[0].messages[0].Payload) != "original" {
		t.Fatalf("delegated messages = %#v, want one unchanged payload", delegate.calls)
	}
}

func TestAsyncPlayerAdmissionPreflightsQueueCapacityBeforeClone(t *testing.T) {
	delegate := &recordingAsyncPlayerDelegate{}
	observer := &recordingAsyncMetricsObserver{}
	sender, err := NewAsyncPlayerSender(delegate, AsyncPlayerConfig{
		AsyncQueueConfig: AsyncQueueConfig{QueueCapacityMessages: 2, QueueCapacityBytes: DefaultMaxPayloadBytes},
		BatchMaxMessages: 1,
		BatchMaxBytes:    DefaultMaxPayloadBytes,
	}, observer)
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sender.Stop(context.Background()) }()

	tooMany := []PlayerMessage{
		{LoginName: "alice", Message: Message{CommandID: 1}},
		{LoginName: "bob", Message: Message{CommandID: 1}},
		{LoginName: "carol", Message: Message{CommandID: 1}},
	}
	if _, err := sender.SendToPlayers(context.Background(), tooMany); !errors.Is(err, ErrEnqueueTooLarge) {
		t.Fatalf("count preflight error = %v, want ErrEnqueueTooLarge", err)
	}

	largePayload := make([]byte, 600*1024)
	tooBytes := []PlayerMessage{
		{LoginName: "alice", Message: Message{CommandID: 1, Payload: largePayload}},
		{LoginName: "bob", Message: Message{CommandID: 1, Payload: largePayload}},
	}
	if _, err := sender.SendToPlayers(context.Background(), tooBytes); !errors.Is(err, ErrEnqueueTooLarge) {
		t.Fatalf("bytes preflight error = %v, want ErrEnqueueTooLarge", err)
	}
	if messages, bytes := sender.queue.snapshot(); messages != 0 || bytes != 0 {
		t.Fatalf("queue after preflight rejection = messages:%d bytes:%d, want 0/0", messages, bytes)
	}
	delegate.mu.Lock()
	if len(delegate.calls) != 0 {
		t.Fatalf("delegate calls after preflight rejection = %d, want 0", len(delegate.calls))
	}
	delegate.mu.Unlock()
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if got := observer.rejected["player:too_large"]; got != 2 {
		t.Fatalf("too_large observations = %d, want 2", got)
	}
}

func TestAsyncPlayerAdmissionStillReportsCurrentQueueFull(t *testing.T) {
	delegate := &recordingAsyncPlayerDelegate{started: make(chan struct{}, 1), release: make(chan struct{})}
	observer := &recordingAsyncMetricsObserver{}
	sender, err := NewAsyncPlayerSender(delegate, AsyncPlayerConfig{
		AsyncQueueConfig: AsyncQueueConfig{QueueCapacityMessages: 1, QueueCapacityBytes: 2 * DefaultMaxPayloadBytes},
		BatchMaxMessages: 1,
		BatchMaxBytes:    DefaultMaxPayloadBytes,
	}, observer)
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(delegate.release)
		_ = sender.Stop(context.Background())
	}()
	if _, err := sender.SendToPlayers(context.Background(), []PlayerMessage{{LoginName: "alice", Message: Message{CommandID: 1}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-delegate.started:
	case <-time.After(time.Second):
		t.Fatal("first Player delegate call did not start")
	}
	if _, err := sender.SendToPlayers(context.Background(), []PlayerMessage{{LoginName: "bob", Message: Message{CommandID: 1}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := sender.SendToPlayers(context.Background(), []PlayerMessage{{LoginName: "carol", Message: Message{CommandID: 1}}}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("current queue admission error = %v, want ErrQueueFull", err)
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if got := observer.rejected["player:full"]; got != 1 {
		t.Fatalf("full observations = %d, want 1", got)
	}
}

func TestAsyncBroadcastAdmissionIsImmediateAndCopiesOuterCommand(t *testing.T) {
	delegate := &recordingAsyncBroadcastDelegate{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	sender, err := NewAsyncBroadcastSender(delegate, AsyncQueueConfig{
		QueueCapacityMessages: 4,
		QueueCapacityBytes:    2 * DefaultMaxPayloadBytes,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = sender.Stop(stopCtx)
	})
	payload := []byte("original")
	receipt, err := sender.Broadcast(context.Background(), Message{CommandID: 7, Payload: payload})
	if err != nil || receipt.AcceptedAt.IsZero() {
		t.Fatalf("admission = receipt:%#v error:%v", receipt, err)
	}
	payload[0] = 'X'
	select {
	case <-delegate.started:
	case <-time.After(time.Second):
		t.Fatal("async Broadcast worker did not start")
	}
	delegate.releaseOnce.Do(func() { close(delegate.release) })
	if err := sender.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	delegate.mu.Lock()
	defer delegate.mu.Unlock()
	if len(delegate.calls) != 1 || delegate.calls[0].CommandID != 7 || string(delegate.calls[0].Payload) != "original" {
		t.Fatalf("delegated commands = %#v, want one unchanged command", delegate.calls)
	}
}

func TestAsyncSenderLifecycleAndQueueFullErrors(t *testing.T) {
	delegate := &recordingAsyncBroadcastDelegate{}
	sender, err := NewAsyncBroadcastSender(delegate, AsyncQueueConfig{
		QueueCapacityMessages: 1,
		QueueCapacityBytes:    2 * DefaultMaxPayloadBytes,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.Broadcast(context.Background(), Message{CommandID: 1}); !errors.Is(err, ErrSenderNotRunning) {
		t.Fatalf("before Start error = %v, want ErrSenderNotRunning", err)
	}
	if err := sender.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := sender.Start(context.Background()); err == nil {
		t.Fatal("duplicate Start was accepted")
	}
	// Use the queue directly to make the full condition deterministic while the
	// worker is not consuming an item.
	queue, err := newAsyncQueue("broadcast", AsyncQueueConfig{QueueCapacityMessages: 1, QueueCapacityBytes: 1024}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.start(); err != nil {
		t.Fatal(err)
	}
	job := newBroadcastTestJob(t, context.Background(), 1)
	if err := queue.enqueue(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if err := queue.enqueue(context.Background(), newBroadcastTestJob(t, context.Background(), 2)); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full queue error = %v, want ErrQueueFull", err)
	}
	queue.beginStop()
	queue.discardRemaining("test_cleanup")
	queue.markStopped()
	tooSmall, err := NewAsyncBroadcastSender(&recordingAsyncBroadcastDelegate{}, AsyncQueueConfig{
		QueueCapacityMessages: 4,
		QueueCapacityBytes:    1,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tooSmall.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := tooSmall.Broadcast(context.Background(), Message{CommandID: 4}); !errors.Is(err, ErrEnqueueTooLarge) {
		t.Fatalf("oversized queue admission error = %v, want ErrEnqueueTooLarge", err)
	}
	if err := tooSmall.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := sender.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := sender.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := sender.Broadcast(context.Background(), Message{CommandID: 3}); !errors.Is(err, ErrSenderNotRunning) {
		t.Fatalf("after Stop error = %v, want ErrSenderNotRunning", err)
	}
}

func TestAsyncBroadcastStopTimeoutDiscardsPendingCommands(t *testing.T) {
	delegate := &recordingAsyncBroadcastDelegate{started: make(chan struct{}, 1)}
	observer := &recordingAsyncMetricsObserver{}
	sender, err := NewAsyncBroadcastSender(delegate, AsyncQueueConfig{
		QueueCapacityMessages: 4,
		QueueCapacityBytes:    2 * DefaultMaxPayloadBytes,
	}, observer)
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = sender.Stop(stopCtx)
	})
	if _, err := sender.Broadcast(context.Background(), Message{CommandID: 1}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-delegate.started:
	case <-time.After(time.Second):
		t.Fatal("broadcast worker did not start")
	}
	if _, err := sender.Broadcast(context.Background(), Message{CommandID: 2}); err != nil {
		t.Fatal(err)
	}
	stopCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sender.Stop(stopCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("timeout Stop error = %v, want context.Canceled", err)
	}
	if err := sender.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _ := sender.queue.snapshot(); got != 0 {
		t.Fatalf("queue messages after timeout = %d, want 0", got)
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.discarded != 1 {
		t.Fatalf("discarded commands = %d, want 1", observer.discarded)
	}
}

func TestAsyncBroadcastStopHonorsDeadlineWithNonCooperativeDelegate(t *testing.T) {
	delegate := &nonCooperativeAsyncBroadcastDelegate{started: make(chan struct{}), release: make(chan struct{})}
	sender, err := NewAsyncBroadcastSender(delegate, AsyncQueueConfig{
		QueueCapacityMessages: 2,
		QueueCapacityBytes:    2 * DefaultMaxPayloadBytes,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		delegate.releaseOnce.Do(func() { close(delegate.release) })
		_ = sender.Stop(context.Background())
	}()
	if _, err := sender.Broadcast(context.Background(), Message{CommandID: 1}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-delegate.started:
	case <-time.After(time.Second):
		t.Fatal("broadcast delegate did not start")
	}
	stopContext, cancel := context.WithCancel(context.Background())
	cancel()
	stopResult := make(chan error, 1)
	go func() { stopResult <- sender.Stop(stopContext) }()
	select {
	case err := <-stopResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Stop error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop waited for a non-cooperative delegate after deadline")
	}
	if _, err := sender.Broadcast(context.Background(), Message{CommandID: 2}); !errors.Is(err, ErrSenderNotRunning) {
		t.Fatalf("admission after deadline Stop = %v, want ErrSenderNotRunning", err)
	}
	delegate.releaseOnce.Do(func() { close(delegate.release) })
	if err := sender.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := sender.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncPlayerStopHonorsDeadlineWithNonCooperativeDelegate(t *testing.T) {
	delegate := &nonCooperativeAsyncPlayerDelegate{started: make(chan struct{}), release: make(chan struct{})}
	sender, err := NewAsyncPlayerSender(delegate, AsyncPlayerConfig{
		AsyncQueueConfig: AsyncQueueConfig{QueueCapacityMessages: 2, QueueCapacityBytes: 2 * DefaultMaxPayloadBytes},
		BatchMaxMessages: 1,
		BatchMaxBytes:    DefaultMaxPayloadBytes,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		delegate.releaseOnce.Do(func() { close(delegate.release) })
		_ = sender.Stop(context.Background())
	}()
	if _, err := sender.SendToPlayers(context.Background(), []PlayerMessage{{LoginName: "alice", Message: Message{CommandID: 1}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-delegate.started:
	case <-time.After(time.Second):
		t.Fatal("Player delegate did not start")
	}
	stopContext, cancel := context.WithCancel(context.Background())
	cancel()
	stopResult := make(chan error, 1)
	go func() { stopResult <- sender.Stop(stopContext) }()
	select {
	case err := <-stopResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Stop error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop waited for a non-cooperative delegate after deadline")
	}
	if _, err := sender.SendToPlayers(context.Background(), []PlayerMessage{{LoginName: "bob", Message: Message{CommandID: 2}}}); !errors.Is(err, ErrSenderNotRunning) {
		t.Fatalf("admission after deadline Stop = %v, want ErrSenderNotRunning", err)
	}
	delegate.releaseOnce.Do(func() { close(delegate.release) })
	if err := sender.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := sender.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type recordingAsyncPlayerDelegate struct {
	mu          sync.Mutex
	calls       []asyncPlayerCall
	started     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

type nonCooperativeAsyncPlayerDelegate struct {
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
}

func (d *nonCooperativeAsyncPlayerDelegate) SendToPlayers(context.Context, []PlayerMessage) (Receipt, error) {
	d.startedOnce.Do(func() { close(d.started) })
	<-d.release
	return newReceipt(), nil
}

type asyncPlayerCall struct {
	trace    string
	messages []PlayerMessage
}

func (d *recordingAsyncPlayerDelegate) SendToPlayers(ctx context.Context, messages []PlayerMessage) (Receipt, error) {
	if d.started != nil {
		select {
		case d.started <- struct{}{}:
		default:
		}
	}
	if d.release != nil {
		select {
		case <-d.release:
		case <-ctx.Done():
		}
	}
	cloned := clonePlayerMessages(messages)
	trace, _ := logging.TraceParentFromContext(ctx)
	d.mu.Lock()
	d.calls = append(d.calls, asyncPlayerCall{trace: trace, messages: cloned})
	d.mu.Unlock()
	return newReceipt(), nil
}

type recordingAsyncBroadcastDelegate struct {
	mu          sync.Mutex
	calls       []Message
	started     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

type nonCooperativeAsyncBroadcastDelegate struct {
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
}

func (d *nonCooperativeAsyncBroadcastDelegate) Broadcast(context.Context, Message) (Receipt, error) {
	d.startedOnce.Do(func() { close(d.started) })
	<-d.release
	return newReceipt(), nil
}

func (d *recordingAsyncBroadcastDelegate) Broadcast(ctx context.Context, message Message) (Receipt, error) {
	if d.started != nil {
		select {
		case d.started <- struct{}{}:
		default:
		}
		if d.release == nil {
			<-ctx.Done()
		} else {
			select {
			case <-d.release:
			case <-ctx.Done():
			}
		}
	}
	d.mu.Lock()
	d.calls = append(d.calls, message.clone())
	d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	return newReceipt(), nil
}

type recordingAsyncMetricsObserver struct {
	mu        sync.Mutex
	discarded int
	fallbacks int
	rejected  map[string]int
}

func (o *recordingAsyncMetricsObserver) SetQueueCapacity(string, int, int) {}
func (o *recordingAsyncMetricsObserver) ObserveQueue(string, int, int)     {}
func (o *recordingAsyncMetricsObserver) ObserveQueueRejected(operation, reason string) {
	o.mu.Lock()
	if o.rejected == nil {
		o.rejected = make(map[string]int)
	}
	o.rejected[operation+":"+reason]++
	o.mu.Unlock()
}
func (o *recordingAsyncMetricsObserver) ObserveQueueWait(string, time.Duration) {}
func (o *recordingAsyncMetricsObserver) ObserveWorker(string, string, time.Duration) {
}
func (o *recordingAsyncMetricsObserver) ObservePlayerBatch(int, int) {}
func (o *recordingAsyncMetricsObserver) ObserveDependency(string, string, string, time.Duration) {
}
func (o *recordingAsyncMetricsObserver) ObserveFallback(string, string) {
	o.mu.Lock()
	o.fallbacks++
	o.mu.Unlock()
}
func (o *recordingAsyncMetricsObserver) ObserveDiscarded(_ string, _ string, messages, _ int) {
	o.mu.Lock()
	o.discarded += messages
	o.mu.Unlock()
}

func newBroadcastTestJob(t *testing.T, ctx context.Context, commandID uint32) *broadcastJob {
	t.Helper()
	job, err := newBroadcastJob(ctx, Message{CommandID: commandID, Payload: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}
	return job
}
