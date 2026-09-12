package gateproduct

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/logging"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
)

const (
	defaultPresenceRetryMax = 5 * time.Second
	presenceRenewalBatchMax = 256
)

var (
	errPresenceSchedulerStopped = errors.New("gate session: presence renewal scheduler is stopped")
	errPresenceSchedulerStarted = errors.New("gate session: presence renewal scheduler is already started")
)

type sessionPresenceBatchRenewer interface {
	RenewMany(context.Context, []serversend.Presence) []serversend.PresenceRenewResult
}

// sessionPresenceScheduler 刻意維持 package-private。續租是 Gate session lifecycle
// concern，不是通用 framework scheduler API。
type sessionPresenceScheduler interface {
	Start(context.Context) error
	Stop(context.Context) error
	Schedule(serversend.Presence) error
	Remove(serversend.Presence)
}

type sessionPresenceRenewalMetrics interface {
	SetSessionOwnershipActiveLeases(float64)
	ObserveSessionOwnershipRenewal(string)
	ObserveSessionOwnershipBatch(time.Duration, int)
	ObserveSessionOwnershipSchedulerLag(time.Duration)
	SetSessionOwnershipOverdueLeases(float64)
}

type schedulerTicker interface {
	C() <-chan time.Time
	Stop()
}

type realSchedulerTicker struct{ ticker *time.Ticker }

func (t *realSchedulerTicker) C() <-chan time.Time { return t.ticker.C }
func (t *realSchedulerTicker) Stop()               { t.ticker.Stop() }

type renewalEntry struct {
	presence   serversend.Presence
	generation uint64
	nextDue    time.Time
	attempt    uint32
	bucket     int
	inFlight   bool
}

// sessionPresenceRenewalScheduler 為所有 Gate session lease 共用一個 timer。
// Redis I/O 一律在 mu 外執行；generation check 讓 in-flight batch 中的 Remove
// 與 replacement 保持安全。
type sessionPresenceRenewalScheduler struct {
	renewer  sessionPresenceBatchRenewer
	leaseTTL time.Duration
	interval time.Duration
	bucketsN int
	tick     time.Duration
	logger   *logging.Logger
	metrics  sessionPresenceRenewalMetrics

	clock     func() time.Time
	newTicker func(time.Duration) schedulerTicker

	mu         sync.Mutex
	buckets    []map[serversend.ConnectionID]uint64
	entries    map[serversend.ConnectionID]*renewalEntry
	cursor     int
	nextTick   time.Time
	generation uint64

	cancel  context.CancelFunc
	done    chan struct{}
	started bool
	stopped bool
}

func newSessionPresenceRenewalScheduler(renewer sessionPresenceBatchRenewer, cfg serversend.PresenceConfig, logger *logging.Logger, metrics sessionPresenceRenewalMetrics) (*sessionPresenceRenewalScheduler, error) {
	if renewer == nil {
		return nil, errors.New("gate session: presence batch renewer is required")
	}
	normalized, err := serversend.NormalizePresenceConfig(cfg)
	if err != nil {
		return nil, err
	}
	tick := normalized.Renewal.Interval / time.Duration(normalized.Renewal.Buckets)
	if tick > defaultPresenceRetryMax {
		return nil, fmt.Errorf("%w: presence renewal tick %s exceeds retry maximum %s", serversend.ErrDestinationInvalid, tick, defaultPresenceRetryMax)
	}
	return newSessionPresenceRenewalSchedulerWithRenewer(renewer, normalized, logger, metrics), nil
}

func newSessionPresenceRenewalSchedulerWithRenewer(renewer sessionPresenceBatchRenewer, cfg serversend.PresenceConfig, logger *logging.Logger, metrics sessionPresenceRenewalMetrics) *sessionPresenceRenewalScheduler {
	interval := cfg.Renewal.Interval
	bucketsN := cfg.Renewal.Buckets
	tick := interval / time.Duration(bucketsN)
	scheduler := &sessionPresenceRenewalScheduler{
		renewer: renewer, leaseTTL: cfg.LeaseTTL, interval: interval, bucketsN: bucketsN, tick: tick,
		logger: logger, metrics: metrics,
		clock: time.Now,
		newTicker: func(duration time.Duration) schedulerTicker {
			return &realSchedulerTicker{ticker: time.NewTicker(duration)}
		},
		buckets: make([]map[serversend.ConnectionID]uint64, bucketsN),
		entries: make(map[serversend.ConnectionID]*renewalEntry),
	}
	for index := range scheduler.buckets {
		scheduler.buckets[index] = make(map[serversend.ConnectionID]uint64)
	}
	return scheduler
}

func (s *sessionPresenceRenewalScheduler) Start(ctx context.Context) error {
	if s == nil {
		return errors.New("gate session: presence renewal scheduler is nil")
	}
	if ctx == nil {
		return errors.New("gate session: presence renewal scheduler context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return errPresenceSchedulerStarted
	}
	if s.stopped {
		s.mu.Unlock()
		return errPresenceSchedulerStopped
	}
	now := s.clock()
	if s.nextTick.IsZero() {
		s.nextTick = now.Add(s.tick)
	}
	loopCtx, cancel := context.WithCancel(context.Background())
	ticker := s.newTicker(s.tick)
	s.cancel = cancel
	s.done = make(chan struct{})
	s.started = true
	done := s.done
	s.mu.Unlock()
	go s.loop(loopCtx, done, ticker)
	return nil
}

func (s *sessionPresenceRenewalScheduler) Stop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if !s.started {
		if !s.stopped {
			s.stopped = true
			s.clearLocked()
		}
		s.mu.Unlock()
		return nil
	}
	firstStop := !s.stopped
	s.stopped = true
	cancel, done := s.cancel, s.done
	s.mu.Unlock()
	if firstStop && cancel != nil {
		cancel()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *sessionPresenceRenewalScheduler) Schedule(presence serversend.Presence) error {
	if err := validateRenewalPresence(presence); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return errPresenceSchedulerStopped
	}
	if existing, ok := s.entries[presence.ConnectionID]; ok {
		if existing.presence == presence {
			return nil
		}
		return fmt.Errorf("gate session: connection %q already has a different presence lease", presence.ConnectionID)
	}
	now := s.clock()
	if s.nextTick.IsZero() {
		s.nextTick = now.Add(s.tick)
	}
	s.generation++
	entry := &renewalEntry{presence: presence, generation: s.generation, bucket: -1}
	s.entries[presence.ConnectionID] = entry
	offset := int(stablePresenceHash(presence.ConnectionID) % uint64(s.bucketsN))
	s.scheduleAtLocked(entry, s.nextTick.Add(time.Duration(offset)*s.tick))
	s.observeActiveLocked()
	return nil
}

func (s *sessionPresenceRenewalScheduler) Remove(presence serversend.Presence) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[presence.ConnectionID]
	if !ok || entry.presence != presence {
		return
	}
	s.removeLocked(entry)
	s.observeActiveLocked()
}

func (s *sessionPresenceRenewalScheduler) loop(ctx context.Context, done chan struct{}, ticker schedulerTicker) {
	defer s.finishLoop(done)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			// ticker 的 timestamp 可能在 Redis I/O 或 GC pause 期間已過時；
			// 每次 wake-up 都讀取目前 clock，才能一次補齊所有 overdue leases。
			s.processAt(ctx, s.clock())
		}
	}
}

func (s *sessionPresenceRenewalScheduler) finishLoop(done chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done != done {
		return
	}
	s.started = false
	s.cancel = nil
	s.clearLocked()
	close(done)
}

// processAt 保留 clock seam，供 deterministic contract test 驅動。
func (s *sessionPresenceRenewalScheduler) processAt(ctx context.Context, now time.Time) {
	if s == nil || s.renewer == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if now.IsZero() {
		now = s.clock()
	}
	s.mu.Lock()
	due := s.advanceLocked(now)
	s.mu.Unlock()
	if len(due) == 0 {
		if s.metrics != nil {
			s.metrics.SetSessionOwnershipOverdueLeases(0)
		}
		return
	}
	if s.metrics != nil {
		s.metrics.SetSessionOwnershipOverdueLeases(float64(len(due)))
		defer s.metrics.SetSessionOwnershipOverdueLeases(0)
	}
	for start := 0; start < len(due); start += presenceRenewalBatchMax {
		if err := ctx.Err(); err != nil {
			s.requeueCancelled(due[start:])
			return
		}
		end := start + presenceRenewalBatchMax
		if end > len(due) {
			end = len(due)
		}
		batch := due[start:end]
		batchNow := s.clock()
		presences := make([]serversend.Presence, len(batch))
		for index, entry := range batch {
			presences[index] = entry.presence
			if s.metrics != nil {
				lag := batchNow.Sub(entry.nextDue)
				if lag < 0 {
					lag = 0
				}
				s.metrics.ObserveSessionOwnershipSchedulerLag(lag)
			}
		}
		started := s.clock()
		operationCtx, cancel := sessionPresenceRenewalOperationContext(ctx, s.leaseTTL)
		results := s.renewer.RenewMany(operationCtx, presences)
		cancel()
		elapsed := s.clock().Sub(started)
		if elapsed < 0 {
			elapsed = 0
		}
		if s.metrics != nil {
			s.metrics.ObserveSessionOwnershipBatch(elapsed, len(batch))
		}
		if err := ctx.Err(); err != nil {
			s.requeueCancelled(batch)
			return
		}
		errorCount, notOwnerCount := 0, 0
		for index, entry := range batch {
			result := serversend.PresenceRenewResult{Presence: entry.presence, Err: fmt.Errorf("gate session: renewal result missing")}
			if index < len(results) {
				result = results[index]
			}
			if result.Err != nil {
				if errors.Is(result.Err, serversend.ErrPresenceNotOwner) {
					notOwnerCount++
				} else {
					errorCount++
				}
			}
			s.commitResult(entry, result)
		}
		if s.logger != nil && (errorCount > 0 || notOwnerCount > 0) {
			attrs := []slog.Attr{
				slog.Int("batch_size", len(batch)),
				slog.Int("success_count", len(batch)-errorCount-notOwnerCount),
				slog.Int("error_count", errorCount),
				slog.Int("not_owner_count", notOwnerCount),
				slog.Duration("duration", elapsed),
			}
			if errorCount > 0 {
				s.logger.Error(ctx, "presence_renewal", "session ownership renewal batch had failures", errors.New("one or more renewal commands failed"), attrs...)
			} else {
				s.logger.Warn(ctx, "presence_renewal", "session ownership leases lost ownership", attrs...)
			}
		}
	}
}

func (s *sessionPresenceRenewalScheduler) requeueCancelled(entries []*renewalEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range entries {
		current, ok := s.entries[entry.presence.ConnectionID]
		if !ok || current != entry || current.generation != entry.generation {
			continue
		}
		entry.inFlight = false
		s.scheduleAtLocked(entry, entry.nextDue)
	}
}

func sessionPresenceRenewalOperationContext(parent context.Context, leaseTTL time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	timeout := leaseTTL / 3
	if timeout <= 0 {
		timeout = time.Millisecond
	}
	if timeout > 5*time.Second {
		timeout = 5 * time.Second
	}
	return context.WithTimeout(parent, timeout)
}

func (s *sessionPresenceRenewalScheduler) advanceLocked(now time.Time) []*renewalEntry {
	if s.nextTick.IsZero() {
		s.nextTick = now.Add(s.tick)
		return nil
	}
	if now.Before(s.nextTick) {
		return nil
	}
	var due []*renewalEntry
	if now.Sub(s.nextTick) >= s.interval {
		steps := int64(now.Sub(s.nextTick)/s.tick) + 1
		s.cursor = (s.cursor + int(steps%int64(s.bucketsN))) % s.bucketsN
		s.nextTick = now.Add(s.tick)
		for _, entry := range s.entries {
			if !entry.nextDue.After(now) {
				s.markDueLocked(entry, &due)
			}
		}
		return due
	}
	for !now.Before(s.nextTick) {
		s.cursor = (s.cursor + 1) % s.bucketsN
		s.nextTick = s.nextTick.Add(s.tick)
		due = append(due, s.takeBucketLocked(s.cursor, now)...)
	}
	return due
}

func (s *sessionPresenceRenewalScheduler) takeBucketLocked(bucket int, now time.Time) []*renewalEntry {
	ids := s.buckets[bucket]
	var due []*renewalEntry
	for connectionID, generation := range ids {
		delete(ids, connectionID)
		entry, ok := s.entries[connectionID]
		if !ok || entry.generation != generation || entry.inFlight {
			continue
		}
		entry.bucket = -1
		if entry.nextDue.After(now) {
			s.scheduleAtLocked(entry, entry.nextDue)
			continue
		}
		entry.inFlight = true
		due = append(due, entry)
	}
	return due
}

func (s *sessionPresenceRenewalScheduler) markDueLocked(entry *renewalEntry, due *[]*renewalEntry) {
	if entry.bucket >= 0 {
		delete(s.buckets[entry.bucket], entry.presence.ConnectionID)
	}
	entry.bucket = -1
	if !entry.inFlight {
		entry.inFlight = true
		*due = append(*due, entry)
	}
}

func (s *sessionPresenceRenewalScheduler) commitResult(entry *renewalEntry, result serversend.PresenceRenewResult) {
	if s.metrics != nil {
		resultName := "success"
		if result.Err != nil {
			if errors.Is(result.Err, serversend.ErrPresenceNotOwner) {
				resultName = "not_owner"
			} else {
				resultName = "error"
			}
		}
		s.metrics.ObserveSessionOwnershipRenewal(resultName)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.entries[entry.presence.ConnectionID]
	if !ok || current != entry || current.generation != entry.generation || current.presence != entry.presence {
		return
	}
	if result.Err == nil {
		entry.attempt = 0
		entry.inFlight = false
		s.scheduleAtLocked(entry, s.clock().Add(s.interval))
		return
	}
	if errors.Is(result.Err, serversend.ErrPresenceNotOwner) {
		s.removeLocked(entry)
		s.observeActiveLocked()
		return
	}
	entry.attempt++
	entry.inFlight = false
	now := s.clock()
	due := now.Add(presenceRetryDelay(entry.attempt, s.tick, entry.presence.ConnectionID))
	// scheduleAtLocked 會向上 rounding 一個 tick；預留該 tick 才能履行 retry 上限。
	latestDue := now.Add(defaultPresenceRetryMax - s.tick)
	if due.After(latestDue) {
		due = latestDue
	}
	s.scheduleAtLocked(entry, due)
}

func (s *sessionPresenceRenewalScheduler) scheduleAtLocked(entry *renewalEntry, due time.Time) {
	if entry.bucket >= 0 {
		delete(s.buckets[entry.bucket], entry.presence.ConnectionID)
	}
	if due.Before(s.nextTick) {
		due = s.nextTick
	}
	steps := int64(0)
	if delta := due.Sub(s.nextTick); delta > 0 {
		steps = int64((delta + s.tick - 1) / s.tick)
	}
	entry.nextDue = due
	entry.bucket = (s.cursor + 1 + int(steps%int64(s.bucketsN))) % s.bucketsN
	s.buckets[entry.bucket][entry.presence.ConnectionID] = entry.generation
}

func (s *sessionPresenceRenewalScheduler) removeLocked(entry *renewalEntry) {
	if entry.bucket >= 0 {
		delete(s.buckets[entry.bucket], entry.presence.ConnectionID)
	}
	delete(s.entries, entry.presence.ConnectionID)
	entry.bucket = -1
	entry.inFlight = false
}

func (s *sessionPresenceRenewalScheduler) clearLocked() {
	s.entries = make(map[serversend.ConnectionID]*renewalEntry)
	for index := range s.buckets {
		s.buckets[index] = make(map[serversend.ConnectionID]uint64)
	}
	if s.metrics != nil {
		s.metrics.SetSessionOwnershipActiveLeases(0)
		s.metrics.SetSessionOwnershipOverdueLeases(0)
	}
}

func (s *sessionPresenceRenewalScheduler) observeActiveLocked() {
	if s.metrics != nil {
		s.metrics.SetSessionOwnershipActiveLeases(float64(len(s.entries)))
	}
}

func validateRenewalPresence(presence serversend.Presence) error {
	if presence.LoginName == "" || presence.GateID == "" || presence.ConnectionID == "" || presence.Epoch == 0 {
		return fmt.Errorf("%w: complete presence identity and epoch are required", serversend.ErrDestinationInvalid)
	}
	return nil
}

func stablePresenceHash(connectionID serversend.ConnectionID) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(connectionID))
	return hasher.Sum64()
}

func presenceRetryDelay(attempt uint32, base time.Duration, connectionID serversend.ConnectionID) time.Duration {
	if base <= 0 {
		base = time.Millisecond
	}
	delay := base
	for index := uint32(1); index < attempt && delay < defaultPresenceRetryMax/2; index++ {
		delay *= 2
	}
	if delay > defaultPresenceRetryMax {
		delay = defaultPresenceRetryMax
	}
	// Stable bounded jitter 避免失敗 bucket 內的 entry 在同一時刻重試，
	// 同時不引入隨機測試 seam。
	factor := 80 + stablePresenceHash(connectionID)%41
	delay = time.Duration(int64(delay) * int64(factor) / 100)
	if delay > defaultPresenceRetryMax {
		delay = defaultPresenceRetryMax
	}
	if delay <= 0 {
		delay = time.Nanosecond
	}
	return delay
}

var _ sessionPresenceScheduler = (*sessionPresenceRenewalScheduler)(nil)
