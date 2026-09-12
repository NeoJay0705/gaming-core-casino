package gateproduct

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
)

func TestSessionPresenceRenewalSchedulerContractDistributesTenThousandLeases(t *testing.T) {
	config := serversend.PresenceConfig{
		LeaseTTL: time.Minute,
		Renewal:  serversend.PresenceRenewalConfig{Interval: 30 * time.Second, Buckets: 100},
	}
	metrics := &renewalMetricsFake{}
	scheduler := newSessionPresenceRenewalSchedulerWithRenewer(&renewalBatchFake{}, config, nil, metrics)
	now := time.Unix(100, 0)
	scheduler.clock = func() time.Time { return now }

	const leaseCount = 10_000
	for index := 0; index < leaseCount; index++ {
		presence := serversend.Presence{
			LoginName:    serversend.LoginName("login-" + strconv.Itoa(index)),
			GateID:       "gate-a",
			ConnectionID: serversend.ConnectionID("connection-" + strconv.Itoa(index)),
			Epoch:        uint64(index + 1),
		}
		if err := scheduler.Schedule(presence); err != nil {
			t.Fatalf("schedule lease %d: %v", index, err)
		}
	}

	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if len(scheduler.entries) != leaseCount {
		t.Fatalf("active entries = %d, want %d", len(scheduler.entries), leaseCount)
	}
	if got := metrics.activeValue(); got != leaseCount {
		t.Fatalf("active lease metric = %v, want %d", got, leaseCount)
	}
	seen := make(map[serversend.ConnectionID]int, leaseCount)
	for bucketIndex, bucket := range scheduler.buckets {
		if len(bucket) == 0 {
			t.Fatalf("bucket %d is empty for %d leases", bucketIndex, leaseCount)
		}
		for connectionID, generation := range bucket {
			entry, ok := scheduler.entries[connectionID]
			if !ok || entry.generation != generation || entry.bucket != bucketIndex {
				t.Fatalf("bucket %d contains stale lease %q", bucketIndex, connectionID)
			}
			seen[connectionID]++
		}
	}
	if len(seen) != leaseCount {
		t.Fatalf("bucket references = %d, want %d unique leases", len(seen), leaseCount)
	}
	for connectionID, count := range seen {
		if count != 1 {
			t.Fatalf("lease %q appears in %d buckets, want once", connectionID, count)
		}
	}
}

func TestSessionPresenceRenewalSchedulerContractRejectsTickAboveRetryMaximum(t *testing.T) {
	validConfig := serversend.PresenceConfig{
		LeaseTTL: time.Minute * 5,
		Renewal:  serversend.PresenceRenewalConfig{Interval: 100 * time.Second, Buckets: 20},
	}
	scheduler, err := newSessionPresenceRenewalScheduler(&renewalBatchFake{}, validConfig, nil, nil)
	if err != nil {
		t.Fatalf("valid five-second tick: %v", err)
	}
	if scheduler.tick != 5*time.Second {
		t.Fatalf("validated tick = %s, want 5s", scheduler.tick)
	}
	invalidConfig := validConfig
	invalidConfig.Renewal.Buckets = 19
	if _, err := newSessionPresenceRenewalScheduler(&renewalBatchFake{}, invalidConfig, nil, nil); !errors.Is(err, serversend.ErrDestinationInvalid) {
		t.Fatalf("tick above retry maximum error = %v, want ErrDestinationInvalid", err)
	}
}

func TestSessionPresenceRenewalSchedulerContractRetryRoundingStaysWithinMaximum(t *testing.T) {
	config := serversend.PresenceConfig{
		LeaseTTL: time.Second * 10,
		Renewal:  serversend.PresenceRenewalConfig{Interval: time.Second * 2, Buckets: 2},
	}
	var connectionID serversend.ConnectionID
	for index := 0; index < 10_000; index++ {
		candidate := serversend.ConnectionID("retry-rounding-" + strconv.Itoa(index))
		if presenceRetryDelay(4, time.Second, candidate) >= defaultPresenceRetryMax-time.Second {
			connectionID = candidate
			break
		}
	}
	if connectionID == "" {
		t.Fatal("could not find deterministic retry jitter fixture")
	}

	now := time.Unix(275, 0)
	var firstTick time.Time
	transientErr := errors.New("temporary Redis failure")
	renewer := &renewalBatchFake{responses: []renewalBatchResponse{
		func(_ context.Context, presences []serversend.Presence) []serversend.PresenceRenewResult {
			now = firstTick.Add(time.Nanosecond)
			results := successfulRenewalResults(presences)
			results[0].Err = transientErr
			return results
		},
	}}
	scheduler, err := newSessionPresenceRenewalScheduler(renewer, config, nil, nil)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}
	scheduler.clock = func() time.Time { return now }
	presence := serversend.Presence{
		LoginName:    "alice",
		GateID:       "gate-a",
		ConnectionID: connectionID,
		Epoch:        1,
	}
	if err := scheduler.Schedule(presence); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	scheduler.mu.Lock()
	entry := scheduler.entries[connectionID]
	entry.attempt = 3
	firstTick = entry.nextDue
	scheduler.mu.Unlock()
	now = firstTick
	scheduler.processAt(context.Background(), firstTick)
	if got := renewer.callSizes(); len(got) != 1 {
		t.Fatalf("initial renewal calls = %v, want one call", got)
	}

	for step := 1; step < 5; step++ {
		now = firstTick.Add(time.Duration(step) * time.Second)
		scheduler.processAt(context.Background(), now)
		if got := renewer.callSizes(); len(got) != 1 {
			t.Fatalf("retry executed before maximum delay at step %d: calls = %v", step, got)
		}
	}
	now = firstTick.Add(5 * time.Second)
	scheduler.processAt(context.Background(), now)
	if got := renewer.callSizes(); len(got) != 2 {
		t.Fatalf("retry calls by maximum delay = %v, want retry within 5s", got)
	}
}

func TestSessionPresenceRenewalSchedulerContractSplitsSequentialBatches(t *testing.T) {
	config := serversend.PresenceConfig{
		LeaseTTL: time.Second,
		Renewal:  serversend.PresenceRenewalConfig{Interval: 300 * time.Millisecond, Buckets: 3},
	}
	renewer := &renewalBatchFake{}
	metrics := &renewalMetricsFake{}
	scheduler := newSessionPresenceRenewalSchedulerWithRenewer(renewer, config, nil, metrics)
	now := time.Unix(200, 0)
	scheduler.clock = func() time.Time { return now }

	for index := 0; index < presenceRenewalBatchMax+44; index++ {
		presence := serversend.Presence{
			LoginName:    serversend.LoginName("login-" + strconv.Itoa(index)),
			GateID:       "gate-a",
			ConnectionID: serversend.ConnectionID("connection-" + strconv.Itoa(index)),
			Epoch:        uint64(index + 1),
		}
		if err := scheduler.Schedule(presence); err != nil {
			t.Fatalf("schedule lease %d: %v", index, err)
		}
	}

	// 延遲 wake-up 必須在一次處理中收集所有 overdue bucket。
	scheduler.processAt(context.Background(), now.Add(time.Second))
	if got := renewer.callSizes(); len(got) != 2 || got[0] != presenceRenewalBatchMax || got[1] != 44 {
		t.Fatalf("renewal batch sizes = %v, want [%d 44]", got, presenceRenewalBatchMax)
	}
	if got := metrics.batchSizesSnapshot(); len(got) != 2 || got[0] != presenceRenewalBatchMax || got[1] != 44 {
		t.Fatalf("recorded batch sizes = %v, want [%d 44]", got, presenceRenewalBatchMax)
	}
	if got := metrics.overdueValue(); got != 0 {
		t.Fatalf("overdue lease metric after processing = %v, want 0", got)
	}
}

func TestSessionPresenceRenewalSchedulerContractRecordsZeroAndSequentialLag(t *testing.T) {
	config := serversend.PresenceConfig{
		LeaseTTL: time.Second,
		Renewal:  serversend.PresenceRenewalConfig{Interval: 300 * time.Millisecond, Buckets: 1},
	}
	metrics := &renewalMetricsFake{}
	now := time.Unix(250, 0)
	var calls int
	renewer := &renewalBatchFake{responses: []renewalBatchResponse{
		func(_ context.Context, presences []serversend.Presence) []serversend.PresenceRenewResult {
			calls++
			if calls == 1 {
				now = now.Add(2 * time.Second)
			}
			return successfulRenewalResults(presences)
		},
	}}
	scheduler := newSessionPresenceRenewalSchedulerWithRenewer(renewer, config, nil, metrics)
	scheduler.clock = func() time.Time { return now }
	for index := 0; index < presenceRenewalBatchMax+1; index++ {
		presence := serversend.Presence{
			LoginName:    serversend.LoginName("login-" + strconv.Itoa(index)),
			GateID:       "gate-a",
			ConnectionID: serversend.ConnectionID("connection-" + strconv.Itoa(index)),
			Epoch:        uint64(index + 1),
		}
		if err := scheduler.Schedule(presence); err != nil {
			t.Fatalf("schedule lease %d: %v", index, err)
		}
	}
	scheduler.mu.Lock()
	dueAt := scheduler.nextTick
	scheduler.mu.Unlock()
	now = dueAt
	scheduler.processAt(context.Background(), dueAt)
	if got := renewer.callSizes(); len(got) != 2 || got[0] != presenceRenewalBatchMax || got[1] != 1 {
		t.Fatalf("renewal batch sizes = %v, want [%d 1]", got, presenceRenewalBatchMax)
	}
	lags := metrics.schedulerLagSnapshot()
	if len(lags) != presenceRenewalBatchMax+1 {
		t.Fatalf("scheduler lag samples = %d, want %d", len(lags), presenceRenewalBatchMax+1)
	}
	var sawZero, sawSequentialWait bool
	for _, lag := range lags {
		if lag == 0 {
			sawZero = true
		}
		if lag >= 2*time.Second {
			sawSequentialWait = true
		}
	}
	if !sawZero || !sawSequentialWait {
		t.Fatalf("scheduler lag samples = %v, want zero and at least 2s sequential wait", lags)
	}
}

func TestSessionPresenceRenewalSchedulerContractClassifiesOutcomesAndRetries(t *testing.T) {
	config := serversend.PresenceConfig{
		LeaseTTL: time.Second,
		Renewal:  serversend.PresenceRenewalConfig{Interval: 300 * time.Millisecond, Buckets: 1},
	}
	transientErr := errors.New("temporary Redis failure")
	renewer := &renewalBatchFake{responses: []renewalBatchResponse{
		func(_ context.Context, presences []serversend.Presence) []serversend.PresenceRenewResult {
			results := successfulRenewalResults(presences)
			for index, presence := range presences {
				switch presence.LoginName {
				case "alice":
					results[index].Err = transientErr
				case "bob":
					results[index].Err = errors.Join(serversend.ErrPresenceNotOwner, errors.New("stale epoch"))
				}
			}
			return results
		},
		func(_ context.Context, presences []serversend.Presence) []serversend.PresenceRenewResult {
			return successfulRenewalResults(presences)
		},
	}}
	metrics := &renewalMetricsFake{}
	scheduler := newSessionPresenceRenewalSchedulerWithRenewer(renewer, config, nil, metrics)
	now := time.Unix(300, 0)
	scheduler.clock = func() time.Time { return now }
	presences := []serversend.Presence{
		{LoginName: "alice", GateID: "gate-a", ConnectionID: "connection-alice", Epoch: 1},
		{LoginName: "bob", GateID: "gate-a", ConnectionID: "connection-bob", Epoch: 1},
		{LoginName: "carol", GateID: "gate-a", ConnectionID: "connection-carol", Epoch: 1},
	}
	for _, presence := range presences {
		if err := scheduler.Schedule(presence); err != nil {
			t.Fatalf("schedule %q: %v", presence.LoginName, err)
		}
	}

	scheduler.processAt(context.Background(), now.Add(time.Second))
	scheduler.processAt(context.Background(), now.Add(2*time.Second))

	scheduler.mu.Lock()
	_, bobActive := scheduler.entries["connection-bob"]
	aliceEntry := scheduler.entries["connection-alice"]
	carolEntry := scheduler.entries["connection-carol"]
	scheduler.mu.Unlock()
	if bobActive {
		t.Fatal("not-owner lease was retained")
	}
	if aliceEntry == nil || aliceEntry.attempt != 0 || carolEntry == nil || carolEntry.attempt != 0 {
		t.Fatalf("retry state = alice:%#v carol:%#v, want successful reset", aliceEntry, carolEntry)
	}
	counts := metrics.renewalCountsSnapshot()
	if counts["success"] != 3 || counts["error"] != 1 || counts["not_owner"] != 1 {
		t.Fatalf("renewal outcome counts = %#v, want success:3 error:1 not_owner:1", counts)
	}
}

func TestSessionPresenceRenewalSchedulerContractRemoveDuringInFlightCannotReinsert(t *testing.T) {
	config := serversend.PresenceConfig{
		LeaseTTL: time.Second,
		Renewal:  serversend.PresenceRenewalConfig{Interval: 300 * time.Millisecond, Buckets: 1},
	}
	renewer := &renewalBatchFake{started: make(chan struct{}), block: make(chan struct{})}
	scheduler := newSessionPresenceRenewalSchedulerWithRenewer(renewer, config, nil, nil)
	now := time.Unix(400, 0)
	scheduler.clock = func() time.Time { return now }
	presence := serversend.Presence{LoginName: "alice", GateID: "gate-a", ConnectionID: "connection-alice", Epoch: 1}
	if err := scheduler.Schedule(presence); err != nil {
		t.Fatal(err)
	}
	processed := make(chan struct{})
	go func() {
		scheduler.processAt(context.Background(), now.Add(time.Second))
		close(processed)
	}()
	select {
	case <-renewer.started:
	case <-time.After(time.Second):
		t.Fatal("renewal batch did not start")
	}
	scheduler.Remove(presence)
	close(renewer.block)
	select {
	case <-processed:
	case <-time.After(time.Second):
		t.Fatal("renewal batch did not finish")
	}
	scheduler.mu.Lock()
	_, active := scheduler.entries[presence.ConnectionID]
	scheduler.mu.Unlock()
	if active {
		t.Fatal("stale in-flight renewal reinserted a removed lease")
	}
}

func TestSessionPresenceRenewalSchedulerContractStartStopLifecycle(t *testing.T) {
	config := serversend.PresenceConfig{
		LeaseTTL: time.Second,
		Renewal:  serversend.PresenceRenewalConfig{Interval: 300 * time.Millisecond, Buckets: 3},
	}
	ticker := &renewalTestTicker{channel: make(chan time.Time), stopped: make(chan struct{})}
	scheduler := newSessionPresenceRenewalSchedulerWithRenewer(&renewalBatchFake{}, config, nil, nil)
	scheduler.newTicker = func(time.Duration) schedulerTicker { return ticker }
	if err := scheduler.Start(context.Background()); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := scheduler.Start(context.Background()); !errors.Is(err, errPresenceSchedulerStarted) {
		t.Fatalf("second Start error = %v, want errPresenceSchedulerStarted", err)
	}
	if err := scheduler.Stop(context.Background()); err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	if err := scheduler.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	if err := scheduler.Schedule(serversend.Presence{LoginName: "alice", GateID: "gate-a", ConnectionID: "connection-alice", Epoch: 1}); !errors.Is(err, errPresenceSchedulerStopped) {
		t.Fatalf("Schedule after Stop error = %v, want errPresenceSchedulerStopped", err)
	}
}

func TestSessionPresenceRenewalSchedulerContractStopTimeoutSharesTermination(t *testing.T) {
	config := serversend.PresenceConfig{
		LeaseTTL: time.Second,
		Renewal:  serversend.PresenceRenewalConfig{Interval: 300 * time.Millisecond, Buckets: 1},
	}
	ticker := &renewalTestTicker{channel: make(chan time.Time), stopped: make(chan struct{})}
	renewer := &renewalBatchFake{started: make(chan struct{}), block: make(chan struct{}), ignoreContext: true}
	metrics := &renewalMetricsFake{}
	scheduler := newSessionPresenceRenewalSchedulerWithRenewer(renewer, config, nil, metrics)
	scheduler.newTicker = func(time.Duration) schedulerTicker { return ticker }
	now := time.Unix(500, 0)
	scheduler.clock = func() time.Time { return now }
	if err := scheduler.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	presence := serversend.Presence{LoginName: "alice", GateID: "gate-a", ConnectionID: "connection-alice", Epoch: 1}
	if err := scheduler.Schedule(presence); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	now = now.Add(time.Second)
	ticker.channel <- now
	select {
	case <-renewer.started:
	case <-time.After(time.Second):
		t.Fatal("renewal batch did not start")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := scheduler.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Stop error = %v, want context deadline", err)
	}
	secondStop := make(chan error, 1)
	go func() { secondStop <- scheduler.Stop(context.Background()) }()
	select {
	case err := <-secondStop:
		t.Fatalf("second Stop returned before loop termination: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(renewer.block)
	select {
	case err := <-secondStop:
		if err != nil {
			t.Fatalf("second Stop after termination: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second Stop did not observe loop termination")
	}
	scheduler.mu.Lock()
	active := len(scheduler.entries)
	scheduler.mu.Unlock()
	if active != 0 {
		t.Fatalf("entries after Stop = %d, want 0", active)
	}
	if got := metrics.activeValue(); got != 0 {
		t.Fatalf("active lease metric after Stop = %v, want 0", got)
	}
	if got := metrics.overdueValue(); got != 0 {
		t.Fatalf("overdue lease metric after Stop = %v, want 0", got)
	}
	select {
	case <-ticker.stopped:
	default:
		t.Fatal("ticker was not stopped")
	}
}

func successfulRenewalResults(presences []serversend.Presence) []serversend.PresenceRenewResult {
	results := make([]serversend.PresenceRenewResult, len(presences))
	for index, presence := range presences {
		results[index] = serversend.PresenceRenewResult{Presence: presence}
	}
	return results
}

type renewalBatchResponse func(context.Context, []serversend.Presence) []serversend.PresenceRenewResult

type renewalBatchFake struct {
	mu            sync.Mutex
	calls         [][]serversend.Presence
	responses     []renewalBatchResponse
	started       chan struct{}
	startOnce     sync.Once
	block         chan struct{}
	ignoreContext bool
}

func (f *renewalBatchFake) RenewMany(ctx context.Context, presences []serversend.Presence) []serversend.PresenceRenewResult {
	input := append([]serversend.Presence(nil), presences...)
	f.mu.Lock()
	f.calls = append(f.calls, input)
	responseIndex := len(f.calls) - 1
	var response renewalBatchResponse
	if responseIndex < len(f.responses) {
		response = f.responses[responseIndex]
	}
	started := f.started
	block := f.block
	f.mu.Unlock()
	if started != nil {
		f.startOnce.Do(func() { close(started) })
	}
	if block != nil {
		if f.ignoreContext {
			<-block
		} else {
			select {
			case <-block:
			case <-ctx.Done():
			}
		}
	}
	if response != nil {
		return response(ctx, input)
	}
	return successfulRenewalResults(input)
}

func (f *renewalBatchFake) callSizes() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	sizes := make([]int, len(f.calls))
	for index, call := range f.calls {
		sizes[index] = len(call)
	}
	return sizes
}

type renewalMetricsFake struct {
	mu             sync.Mutex
	active         float64
	overdue        float64
	renewals       map[string]int
	batchSizes     []int
	batchDurations []time.Duration
	schedulerLag   []time.Duration
}

func (m *renewalMetricsFake) SetSessionOwnershipActiveLeases(value float64) {
	m.mu.Lock()
	m.active = value
	m.mu.Unlock()
}

func (m *renewalMetricsFake) ObserveSessionOwnershipRenewal(result string) {
	m.mu.Lock()
	if m.renewals == nil {
		m.renewals = make(map[string]int)
	}
	m.renewals[result]++
	m.mu.Unlock()
}

func (m *renewalMetricsFake) ObserveSessionOwnershipBatch(duration time.Duration, size int) {
	m.mu.Lock()
	m.batchDurations = append(m.batchDurations, duration)
	m.batchSizes = append(m.batchSizes, size)
	m.mu.Unlock()
}

func (m *renewalMetricsFake) ObserveSessionOwnershipSchedulerLag(duration time.Duration) {
	m.mu.Lock()
	m.schedulerLag = append(m.schedulerLag, duration)
	m.mu.Unlock()
}

func (m *renewalMetricsFake) SetSessionOwnershipOverdueLeases(value float64) {
	m.mu.Lock()
	m.overdue = value
	m.mu.Unlock()
}

func (m *renewalMetricsFake) batchSizesSnapshot() []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]int(nil), m.batchSizes...)
}

func (m *renewalMetricsFake) activeValue() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active
}

func (m *renewalMetricsFake) overdueValue() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.overdue
}

func (m *renewalMetricsFake) schedulerLagSnapshot() []time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]time.Duration(nil), m.schedulerLag...)
}

func (m *renewalMetricsFake) renewalCountsSnapshot() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return map[string]int{"success": m.renewals["success"], "error": m.renewals["error"], "not_owner": m.renewals["not_owner"]}
}

type renewalTestTicker struct {
	channel  chan time.Time
	stopped  chan struct{}
	stopOnce sync.Once
}

func (t *renewalTestTicker) C() <-chan time.Time { return t.channel }

func (t *renewalTestTicker) Stop() {
	t.stopOnce.Do(func() { close(t.stopped) })
}
