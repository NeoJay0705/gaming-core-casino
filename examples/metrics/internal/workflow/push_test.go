package workflow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/protocol"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
)

func TestNormalizePushWorkloadValidatesBoundedContract(t *testing.T) {
	valid := &protocol.StartPushRequest{
		RunId:          "run-1",
		Mode:           protocol.PushMode_PUSH_MODE_PLAYER,
		LoginNames:     []string{"alice", "bob"},
		IntervalMillis: 33,
		DurationMillis: 1000,
		PayloadBytes:   32,
	}
	workload, err := normalizePushWorkload(valid)
	if err != nil {
		t.Fatalf("normalize valid request: %v", err)
	}
	if workload.planned != 30 || len(workload.loginNames) != 2 {
		t.Fatalf("normalized workload = %+v, want planned=30 and 2 targets", workload)
	}
	roomOnlyBroadcast := &protocol.StartPushRequest{
		RunId:          "room-only",
		Mode:           protocol.PushMode_PUSH_MODE_BROADCAST,
		RoomId:         "room-a",
		IntervalMillis: 33,
		DurationMillis: 1000,
		PayloadBytes:   32,
	}
	if workload, err := normalizePushWorkload(roomOnlyBroadcast); err != nil || len(workload.loginNames) != 0 {
		t.Fatalf("room-only broadcast = %+v, error = %v; want accepted without login names", workload, err)
	}

	tests := []struct {
		name string
		edit func(*protocol.StartPushRequest)
	}{
		{name: "mode", edit: func(request *protocol.StartPushRequest) { request.Mode = protocol.PushMode_PUSH_MODE_UNSPECIFIED }},
		{name: "interval", edit: func(request *protocol.StartPushRequest) { request.IntervalMillis = 20 }},
		{name: "duration", edit: func(request *protocol.StartPushRequest) { request.DurationMillis = 0 }},
		{name: "duplicate target", edit: func(request *protocol.StartPushRequest) { request.LoginNames = []string{"alice", "alice"} }},
		{name: "missing target", edit: func(request *protocol.StartPushRequest) { request.LoginNames = nil }},
		{name: "oversized payload", edit: func(request *protocol.StartPushRequest) { request.PayloadBytes = maxPushPayloadBytes }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := protoCloneStartPushRequest(valid)
			test.edit(request)
			if _, err := normalizePushWorkload(request); err == nil {
				t.Fatal("normalize invalid request error = nil")
			}
		})
	}
}

func TestPushRunnerUsesFixedRateWithoutOverlap(t *testing.T) {
	clock := newPushTestClock(time.Unix(1_700_000_000, 0))
	sender := &recordingPushBroadcastSender{calls: make(chan serversend.Message, 8)}
	registry := prometheus.NewRegistry()
	metrics, err := newPushRunnerMetrics(registry)
	if err != nil {
		t.Fatalf("newPushRunnerMetrics: %v", err)
	}
	runner := &pushRunner{broadcast: sender, player: &recordingPushPlayerSender{}, metrics: metrics, clock: clock}
	if err := runner.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	request := &protocol.StartPushRequest{
		RunId:          "fixed-rate",
		Mode:           protocol.PushMode_PUSH_MODE_BROADCAST,
		RoomId:         "room-a",
		LoginNames:     []string{"alice"},
		IntervalMillis: 16,
		DurationMillis: 64,
		PayloadBytes:   4,
	}
	planned, err := runner.Submit(context.Background(), request)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if planned != 4 {
		t.Fatalf("planned ticks = %d, want 4", planned)
	}
	for tick := 0; tick < 4; tick++ {
		select {
		case <-sender.calls:
		case <-time.After(time.Second):
			t.Fatalf("sender call %d did not arrive", tick+1)
		}
		if tick < 3 {
			clock.Advance(16 * time.Millisecond)
		}
	}
	if err := runner.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got := sender.maxActiveValue(); got > 1 {
		t.Fatalf("maximum concurrent sender calls = %d, want 1", got)
	}
	if got := counterValue(t, registry, "gaming_core_example_push_missed_ticks_total", "broadcast"); got != 0 {
		t.Fatalf("missed ticks = %v, want 0", got)
	}
}

func TestPushRunnerSkipsMissedTicksInsteadOfBursting(t *testing.T) {
	clock := newPushTestClock(time.Unix(1_700_000_000, 0))
	sender := &recordingPushBroadcastSender{calls: make(chan serversend.Message, 8)}
	var callCount int
	sender.onCall = func() {
		callCount++
		if callCount == 1 {
			clock.Advance(40 * time.Millisecond)
		}
	}
	registry := prometheus.NewRegistry()
	metrics, err := newPushRunnerMetrics(registry)
	if err != nil {
		t.Fatalf("newPushRunnerMetrics: %v", err)
	}
	runner := &pushRunner{broadcast: sender, player: &recordingPushPlayerSender{}, metrics: metrics, clock: clock}
	if err := runner.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	_, err = runner.Submit(context.Background(), &protocol.StartPushRequest{
		RunId:          "skip",
		Mode:           protocol.PushMode_PUSH_MODE_BROADCAST,
		RoomId:         "room-a",
		LoginNames:     []string{"alice"},
		IntervalMillis: 16,
		DurationMillis: 64,
		PayloadBytes:   1,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	select {
	case <-sender.calls:
	case <-time.After(time.Second):
		t.Fatal("first sender call did not arrive")
	}
	// runner 可能尚未安裝下一個 waiter，fake clock 就已被推進；反覆小幅
	// 推進可排除 goroutine 排程差異，仍保留 missed-tick 情境。
	deadline := time.Now().Add(time.Second)
	for {
		clock.Advance(time.Millisecond)
		select {
		case <-sender.calls:
			break
		default:
			if time.Now().After(deadline) {
				t.Fatal("second sender call did not arrive")
			}
			time.Sleep(time.Millisecond)
			continue
		}
		break
	}
	if err := runner.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got := counterValue(t, registry, "gaming_core_example_push_missed_ticks_total", "broadcast"); got != 2 {
		t.Fatalf("missed ticks = %v, want 2", got)
	}
}

func TestPushRunnerPlayerUsesOneBatchPerTick(t *testing.T) {
	clock := newPushTestClock(time.Unix(1_700_000_000, 0))
	sender := &recordingPushPlayerSender{calls: make(chan []serversend.PlayerMessage, 4)}
	runner := &pushRunner{broadcast: &recordingPushBroadcastSender{}, player: sender, clock: clock}
	if err := runner.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	_, err := runner.Submit(context.Background(), &protocol.StartPushRequest{
		RunId:          "players",
		Mode:           protocol.PushMode_PUSH_MODE_PLAYER,
		LoginNames:     []string{"alice", "bob", "carol"},
		IntervalMillis: 33,
		DurationMillis: 33,
		PayloadBytes:   2,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	select {
	case messages := <-sender.calls:
		if len(messages) != 3 {
			t.Fatalf("player batch size = %d, want 3", len(messages))
		}
		if messages[0].CommandID != protocol.PushMessageCommandID || messages[1].CommandID != protocol.PushMessageCommandID || messages[2].CommandID != protocol.PushMessageCommandID {
			t.Fatalf("player command IDs = %#x, %#x, %#x", messages[0].CommandID, messages[1].CommandID, messages[2].CommandID)
		}
	case <-time.After(time.Second):
		t.Fatal("player sender call did not arrive")
	}
	if err := runner.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestPushRunnerStopCancelsActiveSender(t *testing.T) {
	sender := &blockingPushBroadcastSender{entered: make(chan struct{})}
	runner := &pushRunner{broadcast: sender, player: &recordingPushPlayerSender{}, clock: realPushClock{}}
	if err := runner.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	_, err := runner.Submit(context.Background(), &protocol.StartPushRequest{
		RunId:          "cancel",
		Mode:           protocol.PushMode_PUSH_MODE_BROADCAST,
		RoomId:         "room-a",
		LoginNames:     []string{"alice"},
		IntervalMillis: 16,
		DurationMillis: 1600,
		PayloadBytes:   1,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	select {
	case <-sender.entered:
	case <-time.After(time.Second):
		t.Fatal("blocking sender was not called")
	}
	stopContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runner.Stop(stopContext); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !sender.cancelled() {
		t.Fatal("active sender did not observe cancellation")
	}
}

func TestPushRunnerRejectsSecondActiveWorkload(t *testing.T) {
	sender := &blockingPushBroadcastSender{entered: make(chan struct{})}
	runner := &pushRunner{broadcast: sender, player: &recordingPushPlayerSender{}, clock: realPushClock{}}
	if err := runner.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	request := &protocol.StartPushRequest{
		RunId:          "active",
		Mode:           protocol.PushMode_PUSH_MODE_BROADCAST,
		RoomId:         "room-a",
		LoginNames:     []string{"alice"},
		IntervalMillis: 16,
		DurationMillis: 1600,
		PayloadBytes:   1,
	}
	if _, err := runner.Submit(context.Background(), request); err != nil {
		t.Fatalf("first Submit: %v", err)
	}
	select {
	case <-sender.entered:
	case <-time.After(time.Second):
		t.Fatal("first sender call did not start")
	}
	if _, err := runner.Submit(context.Background(), request); !errors.Is(err, errPushRunnerActive) {
		t.Fatalf("second Submit error = %v, want %v", err, errPushRunnerActive)
	}
	if err := runner.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

type pushTestClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []pushTestWaiter
}

type pushTestWaiter struct {
	deadline time.Time
	channel  chan time.Time
}

func newPushTestClock(now time.Time) *pushTestClock { return &pushTestClock{now: now} }

func (c *pushTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *pushTestClock) After(duration time.Duration) <-chan time.Time {
	channel := make(chan time.Time, 1)
	c.mu.Lock()
	deadline := c.now.Add(duration)
	if !deadline.After(c.now) {
		channel <- c.now
	} else {
		c.waiters = append(c.waiters, pushTestWaiter{deadline: deadline, channel: channel})
	}
	c.mu.Unlock()
	return channel
}

func (c *pushTestClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	ready := make([]pushTestWaiter, 0, len(c.waiters))
	pending := c.waiters[:0]
	for _, waiter := range c.waiters {
		if !waiter.deadline.After(c.now) {
			ready = append(ready, waiter)
		} else {
			pending = append(pending, waiter)
		}
	}
	c.waiters = pending
	now := c.now
	c.mu.Unlock()
	for _, waiter := range ready {
		waiter.channel <- now
	}
}

type recordingPushBroadcastSender struct {
	calls     chan serversend.Message
	onCall    func()
	mu        sync.Mutex
	active    int
	maxActive int
}

func (s *recordingPushBroadcastSender) Broadcast(context.Context, serversend.Message) (serversend.Receipt, error) {
	s.mu.Lock()
	s.active++
	if s.active > s.maxActive {
		s.maxActive = s.active
	}
	onCall := s.onCall
	s.mu.Unlock()
	if onCall != nil {
		onCall()
	}
	s.mu.Lock()
	s.active--
	s.mu.Unlock()
	if s.calls != nil {
		s.calls <- serversend.Message{}
	}
	return serversend.Receipt{AcceptedAt: time.Now()}, nil
}

func (s *recordingPushBroadcastSender) maxActiveValue() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxActive
}

type recordingPushPlayerSender struct {
	calls chan []serversend.PlayerMessage
}

func (s *recordingPushPlayerSender) SendToPlayers(_ context.Context, messages []serversend.PlayerMessage) (serversend.Receipt, error) {
	if s.calls != nil {
		s.calls <- append([]serversend.PlayerMessage(nil), messages...)
	}
	return serversend.Receipt{AcceptedAt: time.Now()}, nil
}

type blockingPushBroadcastSender struct {
	entered  chan struct{}
	mu       sync.Mutex
	canceled bool
}

func (s *blockingPushBroadcastSender) Broadcast(ctx context.Context, _ serversend.Message) (serversend.Receipt, error) {
	close(s.entered)
	<-ctx.Done()
	s.mu.Lock()
	s.canceled = true
	s.mu.Unlock()
	return serversend.Receipt{}, ctx.Err()
}

func (s *blockingPushBroadcastSender) cancelled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.canceled
}

func protoCloneStartPushRequest(request *protocol.StartPushRequest) *protocol.StartPushRequest {
	return proto.Clone(request).(*protocol.StartPushRequest)
}

func counterValue(t *testing.T, gatherer prometheus.Gatherer, name, mode string) float64 {
	t.Helper()
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("gather %s: %v", name, err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "mode" && label.GetValue() == mode {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}
