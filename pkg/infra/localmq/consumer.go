package localmq

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"time"

	infraredis "github.com/NeoJay0705/gaming-core-casino/pkg/infra/redis"
)

type consumerState uint8

const (
	consumerStateNew consumerState = iota
	consumerStateStarting
	consumerStateStarted
	consumerStateStopping
	consumerStateStopped
	consumerStateFailed
)

// Consumer 以 lane 為 ownership 單位執行 at-least-once delivery。Redis 只
// 保存短暫 membership，不保存 WAL 或 checkpoint truth。
type Consumer struct {
	client       *Client
	membershipFn membershipFactory
	subscription Subscription
	handler      Handler
	cfg          ConsumerConfig

	mu                   sync.Mutex
	state                consumerState
	instanceID           string
	membership           *membership
	ctx                  context.Context
	cancel               context.CancelFunc
	workers              map[string]*laneWorker
	laneOrder            []string
	scheduleAt           int
	schedulerWake        chan struct{}
	runtimeWG            sync.WaitGroup
	lastBeat             time.Time
	membershipReady      bool
	membershipGeneration uint64
	startDone            chan struct{}
	stopDone             chan struct{}
	stopErr              error

	errMu   sync.Mutex
	lastErr error
}

// NewConsumer 建立一個尚未啟動的 consumer。它不會連線 Redis，也不會
// 讀取 handler 以外的 downstream resource。
func NewConsumer(client *Client, redisClient *infraredis.Client, subscription Subscription, handler Handler) (*Consumer, error) {
	if redisClient == nil {
		return nil, errors.New("localmq consumer Redis client is nil")
	}
	return newConsumerWithMembershipFactory(client, subscription, handler, redisMembershipFactory(redisClient))
}

func newConsumerWithMembershipFactory(client *Client, subscription Subscription, handler Handler, membershipFn membershipFactory) (*Consumer, error) {
	if client == nil {
		return nil, errors.New("localmq consumer client is nil")
	}
	if membershipFn == nil {
		return nil, errors.New("localmq consumer membership factory is nil")
	}
	if err := validateTopicAndGroup(subscription.Topic, subscription.Group); err != nil {
		return nil, err
	}
	if handler == nil || (isNilHandler(handler)) {
		return nil, errors.New("localmq consumer handler is required")
	}
	instanceID, err := newUUIDv7()
	if err != nil {
		return nil, err
	}
	return &Consumer{
		client:        client,
		membershipFn:  membershipFn,
		subscription:  subscription,
		handler:       handler,
		cfg:           client.cfg.Consumer,
		state:         consumerStateNew,
		instanceID:    instanceID,
		workers:       make(map[string]*laneWorker),
		schedulerWake: make(chan struct{}, 1),
	}, nil
}

func isNilHandler(handler Handler) bool {
	if handler == nil {
		return true
	}
	value := reflect.ValueOf(handler)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// Start 先確認 Group 已 durable ACTIVE，再以 Redis server-time heartbeat
// 建立本次 process incarnation 的 membership。
func (c *Consumer) Start(ctx context.Context) error {
	if c == nil {
		return errors.New("localmq consumer is nil")
	}
	if ctx == nil {
		return errors.New("localmq consumer start context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	if c.state != consumerStateNew {
		state := c.state
		c.mu.Unlock()
		return fmt.Errorf("localmq consumer start is not allowed in state %d", state)
	}
	c.state = consumerStateStarting
	c.startDone = make(chan struct{})
	startDone := c.startDone
	c.mu.Unlock()
	committed := false
	defer func() {
		if committed {
			return
		}
		c.mu.Lock()
		if c.state == consumerStateStarting {
			c.state = consumerStateNew
		}
		close(startDone)
		c.mu.Unlock()
	}()
	if c.client.stateValue() != clientStateStarted {
		return errors.New("localmq consumer client is not started")
	}
	manifest, err := c.client.storage.groupManifest(c.subscription.Topic, c.subscription.Group)
	if err != nil {
		return fmt.Errorf("localmq consumer group: %w", err)
	}
	control, err := c.client.storage.groupControl(c.subscription.Topic, c.subscription.Group)
	if err != nil {
		return fmt.Errorf("localmq consumer group control: %w", err)
	}
	if control.State != string(GroupActive) {
		return fmt.Errorf("localmq consumer group is %s", control.State)
	}
	if manifest.Topic != c.subscription.Topic || manifest.Group != c.subscription.Group {
		return errors.New("localmq consumer group identity mismatch")
	}
	m, err := c.membershipFn(c.subscription.Topic, c.subscription.Group, c.instanceID, c.cfg.MembershipTimeout)
	if err != nil {
		return err
	}
	if err := m.heartbeat(ctx); err != nil {
		return fmt.Errorf("localmq consumer membership heartbeat: %w", err)
	}
	consumerCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	if c.state != consumerStateStarting {
		c.mu.Unlock()
		cancel()
		_ = m.remove(context.Background())
		return errors.New("localmq consumer start was interrupted")
	}
	c.membership = m
	c.ctx = consumerCtx
	c.cancel = cancel
	c.lastBeat = c.client.clock()
	c.membershipReady = true
	c.membershipGeneration++
	c.state = consumerStateStarted
	initialReconcile := make(chan error, 1)
	c.runtimeWG.Add(c.cfg.WorkerConcurrency + 2)
	for index := 0; index < c.cfg.WorkerConcurrency; index++ {
		go c.schedulerLoop()
	}
	go c.heartbeatLoop()
	go c.reconcileLoop(initialReconcile)
	committed = true
	close(startDone)
	c.mu.Unlock()
	select {
	case err := <-initialReconcile:
		if err == nil {
			c.mu.Lock()
			started := c.state == consumerStateStarted
			c.mu.Unlock()
			if started {
				return nil
			}
			return errors.New("localmq consumer start was interrupted")
		}
		c.setError(err)
		_ = c.Stop(context.Background())
		return err
	case <-consumerCtx.Done():
		_ = c.Stop(context.Background())
		return consumerCtx.Err()
	}
}

// Stop 停止新 handler attempt、flush worker checkpoint、移除 membership。
func (c *Consumer) Stop(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("localmq consumer stop context is nil")
	}
	c.mu.Lock()
	if c.state == consumerStateStarting {
		done := c.startDone
		c.mu.Unlock()
		select {
		case <-done:
			return c.Stop(ctx)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if c.state == consumerStateNew {
		c.mu.Unlock()
		return nil
	}
	if c.state == consumerStateStopped {
		err := c.stopErr
		c.mu.Unlock()
		return err
	}
	if c.state == consumerStateStopping {
		done := c.stopDone
		c.mu.Unlock()
		select {
		case <-done:
			c.mu.Lock()
			err := c.stopErr
			c.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c.state = consumerStateStopping
	c.stopDone = make(chan struct{})
	stopDone := c.stopDone
	cancel := c.cancel
	m := c.membership
	if cancel != nil {
		cancel()
	}
	for _, worker := range c.workers {
		worker.cancel()
	}
	c.mu.Unlock()

	finished := make(chan struct{})
	go func() {
		c.runtimeWG.Wait()
		close(finished)
	}()
	var stopErr error
	select {
	case <-finished:
	case <-ctx.Done():
		stopErr = ctx.Err()
	}
	if stopErr == nil {
		c.mu.Lock()
		workers := make([]*laneWorker, 0, len(c.workers))
		for _, worker := range c.workers {
			workers = append(workers, worker)
		}
		c.mu.Unlock()
		for _, worker := range workers {
			if err := worker.flushForShutdown(); err != nil {
				stopErr = errors.Join(stopErr, err)
			}
		}
	}
	if stopErr == nil && m != nil {
		if err := m.remove(ctx); err != nil {
			stopErr = err
		}
	}
	c.mu.Lock()
	c.stopErr = stopErr
	c.state = consumerStateStopped
	close(stopDone)
	c.mu.Unlock()
	return stopErr
}

// LastError 回傳 background reconcile/heartbeat/worker 最近一次錯誤，供
// deployment readiness/structured logging adapter 觀測；不暴露 payload。
func (c *Consumer) LastError() error {
	if c == nil {
		return nil
	}
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.lastErr
}

func (c *Consumer) setError(err error) {
	if err == nil {
		return
	}
	c.errMu.Lock()
	c.lastErr = err
	c.errMu.Unlock()
}

func (c *Consumer) heartbeatLoop() {
	defer c.runtimeWG.Done()
	c.mu.Lock()
	ctx := c.ctx
	c.mu.Unlock()
	ticker := time.NewTicker(c.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			m := c.membership
			c.mu.Unlock()
			if m == nil {
				return
			}
			if err := m.heartbeat(ctx); err != nil {
				c.setError(fmt.Errorf("localmq consumer heartbeat: %w", err))
				now := c.client.clock()
				c.mu.Lock()
				lastBeat := c.lastBeat
				timedOut := now.Sub(lastBeat) >= c.cfg.MembershipTimeout
				if timedOut && c.membershipReady {
					c.membershipReady = false
					c.membershipGeneration++
				}
				c.mu.Unlock()
				if timedOut {
					c.pauseWorkers()
				}
				continue
			}
			c.mu.Lock()
			c.lastBeat = c.client.clock()
			if !c.membershipReady {
				c.membershipReady = true
				c.membershipGeneration++
			}
			c.mu.Unlock()
		}
	}
}

func (c *Consumer) reconcileLoop(initialResult chan<- error) {
	defer c.runtimeWG.Done()
	c.mu.Lock()
	ctx := c.ctx
	c.mu.Unlock()
	if err := c.reconcile(ctx); err != nil {
		initialResult <- fmt.Errorf("localmq consumer initial reconcile: %w", err)
		return
	}
	initialResult <- nil
	ticker := time.NewTicker(c.cfg.ReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.reconcile(ctx); err != nil {
				c.setError(fmt.Errorf("localmq consumer reconcile: %w", err))
			}
		}
	}
}

func (c *Consumer) pauseWorkers() {
	c.mu.Lock()
	for _, worker := range c.workers {
		worker.cancel()
		worker.draining = true
	}
	c.mu.Unlock()
	c.flushDrainingWorkers()
	c.notifyScheduler()
}

func (c *Consumer) reconcile(ctx context.Context) error {
	c.mu.Lock()
	m := c.membership
	state := c.state
	membershipReady := c.membershipReady
	membershipGeneration := c.membershipGeneration
	c.mu.Unlock()
	if state != consumerStateStarted || m == nil || !membershipReady {
		return nil
	}
	control, err := c.client.storage.groupControl(c.subscription.Topic, c.subscription.Group)
	if err != nil {
		return err
	}
	if control.State != string(GroupActive) {
		c.pauseWorkers()
		c.mu.Lock()
		if c.cancel != nil {
			c.cancel()
		}
		c.mu.Unlock()
		return m.remove(ctx)
	}
	members, err := m.activeMembers(ctx)
	if err != nil {
		return err
	}
	lanes, err := c.client.storage.listLanes(c.subscription.Topic)
	if err != nil {
		return err
	}
	owned := make(map[string]struct{}, len(lanes))
	for _, laneID := range lanes {
		if rendezvousOwner(c.subscription.Topic, laneID, members) == c.instanceID {
			owned[laneID] = struct{}{}
		}
	}
	c.client.metricRebalance(c.subscription.Topic, c.subscription.Group)
	changed := false
	c.mu.Lock()
	if c.state != consumerStateStarted || c.ctx == nil || c.ctx.Err() != nil || !c.membershipReady || c.membershipGeneration != membershipGeneration {
		c.mu.Unlock()
		return nil
	}
	for laneID, worker := range c.workers {
		if _, ok := owned[laneID]; !ok {
			worker.cancel()
			worker.draining = true
			changed = true
		}
	}
	for laneID := range owned {
		if _, ok := c.workers[laneID]; !ok {
			workerCtx, cancel := context.WithCancel(c.ctx)
			c.workers[laneID] = &laneWorker{
				consumer: c,
				laneID:   laneID,
				ctx:      workerCtx,
				cancel:   cancel,
				nextRun:  time.Now(),
				backoff:  newBackoff(),
			}
			changed = true
		}
	}
	c.rebuildLaneOrderLocked()
	c.mu.Unlock()
	c.flushDrainingWorkers()
	if changed {
		c.notifyScheduler()
	}
	return nil
}

// rebuildLaneOrderLocked 讓 map 與 round-robin order 保持一致；caller 必須持有 c.mu。
func (c *Consumer) rebuildLaneOrderLocked() {
	c.laneOrder = c.laneOrder[:0]
	for laneID := range c.workers {
		c.laneOrder = append(c.laneOrder, laneID)
	}
	sort.Strings(c.laneOrder)
	if len(c.laneOrder) == 0 {
		c.scheduleAt = 0
	} else if c.scheduleAt >= len(c.laneOrder) {
		c.scheduleAt %= len(c.laneOrder)
	}
}

type laneWorker struct {
	consumer        *Consumer
	laneID          string
	ctx             context.Context
	cancel          context.CancelFunc
	nextRun         time.Time
	inFlight        bool
	draining        bool
	flushInProgress bool
	initialized     bool
	groupManifest   groupManifest
	checkpoint      *checkpointTracker
	reader          *laneReader
	backoff         *backoff
}

// flushDrainingWorkers 在 lock 外完成被取消 worker 的 bounded checkpoint flush；
// flush 完成前保留 worker pointer，避免同一 instance/lane 建立 replacement。
func (c *Consumer) flushDrainingWorkers() {
	c.mu.Lock()
	toFlush := make([]*laneWorker, 0, len(c.workers))
	for _, worker := range c.workers {
		if worker.draining && !worker.inFlight && !worker.flushInProgress {
			worker.flushInProgress = true
			toFlush = append(toFlush, worker)
		}
	}
	c.mu.Unlock()

	removed := false
	for _, worker := range toFlush {
		err := worker.flushForShutdown()
		c.mu.Lock()
		worker.flushInProgress = false
		current, exists := c.workers[worker.laneID]
		if err == nil && exists && current == worker && worker.draining && !worker.inFlight {
			delete(c.workers, worker.laneID)
			c.rebuildLaneOrderLocked()
			removed = true
		}
		c.mu.Unlock()
		if err != nil {
			c.setError(err)
		}
	}
	if removed {
		c.notifyScheduler()
	}
}

func (c *Consumer) notifyScheduler() {
	select {
	case c.schedulerWake <- struct{}{}:
	default:
	}
}

func (c *Consumer) schedulerLoop() {
	defer c.runtimeWG.Done()
	for {
		worker, wait, running := c.claimLane(time.Now())
		if !running {
			return
		}
		if worker == nil {
			c.waitForScheduler(wait)
			continue
		}
		delay := worker.step()
		if worker.ctx.Err() != nil {
			if err := worker.flushForShutdown(); err != nil {
				c.setError(err)
				delay = c.cfg.ReconcileInterval
			} else {
				delay = -1
			}
		}
		c.releaseLane(worker, delay)
	}
}

func (c *Consumer) claimLane(now time.Time) (*laneWorker, time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != consumerStateStarted {
		return nil, 0, false
	}
	if !c.membershipReady {
		return nil, c.cfg.ReconcileInterval, true
	}
	if len(c.laneOrder) == 0 {
		return nil, c.cfg.ReconcileInterval, true
	}
	var earliest time.Time
	for offset := 0; offset < len(c.laneOrder); offset++ {
		index := (c.scheduleAt + offset) % len(c.laneOrder)
		worker := c.workers[c.laneOrder[index]]
		if worker == nil || worker.inFlight || worker.draining || (worker.ctx != nil && worker.ctx.Err() != nil) {
			continue
		}
		if now.Before(worker.nextRun) {
			if earliest.IsZero() || worker.nextRun.Before(earliest) {
				earliest = worker.nextRun
			}
			continue
		}
		worker.inFlight = true
		c.scheduleAt = (index + 1) % len(c.laneOrder)
		return worker, 0, true
	}
	if earliest.IsZero() {
		return nil, c.cfg.ReconcileInterval, true
	}
	wait := time.Until(earliest)
	if wait <= 0 {
		wait = time.Millisecond
	}
	return nil, wait, true
}

func (c *Consumer) waitForScheduler(wait time.Duration) {
	if wait <= 0 {
		wait = c.cfg.ReconcileInterval
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	c.mu.Lock()
	ctx := c.ctx
	c.mu.Unlock()
	select {
	case <-ctx.Done():
	case <-c.schedulerWake:
	case <-timer.C:
	}
}

func (c *Consumer) releaseLane(worker *laneWorker, delay time.Duration) {
	removed := false
	c.mu.Lock()
	current, exists := c.workers[worker.laneID]
	if exists && current == worker {
		if delay < 0 && c.state != consumerStateStopping {
			delete(c.workers, worker.laneID)
			removed = true
		} else {
			worker.inFlight = false
			if delay >= 0 {
				worker.nextRun = time.Now().Add(delay)
			}
		}
		if removed {
			c.rebuildLaneOrderLocked()
		}
	}
	c.mu.Unlock()
	if delay == 0 || removed {
		c.notifyScheduler()
	}
}

func (w *laneWorker) step() time.Duration {
	c := w.consumer
	if err := w.ctx.Err(); err != nil {
		return -1
	}
	if w.backoff == nil {
		w.backoff = newBackoff()
	}
	if !w.initialized {
		groupManifest, err := c.client.storage.groupManifest(c.subscription.Topic, c.subscription.Group)
		if err != nil {
			return w.retry(err)
		}
		next, err := w.startingCheckpoint()
		if err != nil {
			return w.retry(err)
		}
		w.groupManifest = groupManifest
		w.checkpoint = &checkpointTracker{consumer: c, laneID: w.laneID, next: next, persisted: next, lastPersisted: c.client.clock()}
		w.reader = newLaneReader(c.client.storage, c.subscription.Topic, w.laneID)
		w.initialized = true
	}
	checkpoint := w.checkpoint
	if checkpoint.mustFlush {
		if err := checkpoint.flush(w.ctx); err != nil {
			return w.retry(err)
		}
	}
	control, err := c.client.storage.groupControl(c.subscription.Topic, c.subscription.Group)
	if err != nil {
		return w.retry(err)
	}
	if control.State != string(GroupActive) {
		w.flushForShutdown()
		return -1
	}
	effective, err := c.client.storage.effectiveCheckpoint(c.subscription.Topic, c.subscription.Group, w.laneID)
	if err != nil {
		return w.retry(err)
	}
	index, err := c.client.storage.retention(c.subscription.Topic, w.laneID)
	if err != nil {
		return w.retry(err)
	}
	if effective < index.EarliestRetainedSeq {
		if w.groupManifest.GroupType == string(GroupTypeProtected) {
			c.setError(fmt.Errorf("protected group checkpoint %d is below retention floor %d", effective, index.EarliestRetainedSeq))
			w.flushForShutdown()
			return -1
		}
		floor := index.EarliestRetainedSeq
		skipped := floor - effective
		c.client.metricBestEffortSkipped(c.subscription.Topic, c.subscription.Group, skipped, 0)
		if floor > checkpoint.next {
			if err := checkpoint.advance(floor); err != nil {
				return w.retry(err)
			}
		}
		if err := checkpoint.flush(w.ctx); err != nil {
			return w.retry(err)
		}
		effective = floor
	}
	if effective > checkpoint.next {
		checkpoint.next = effective
		if checkpoint.persisted < effective {
			checkpoint.persisted = effective
		}
		checkpoint.pendingCount = 0
	}
	blocked, exists, err := c.client.storage.earliestBlocked(c.subscription.Topic, c.subscription.Group, w.laneID, checkpoint.next)
	if err != nil {
		return w.retry(err)
	}
	if exists {
		c.setError(fmt.Errorf("lane %s blocked at sequence %d: %s", w.laneID, blocked.Sequence, blocked.Error))
		c.client.metricBlocked(c.subscription.Topic, c.subscription.Group, w.laneID, true)
		return c.cfg.ReconcileInterval
	}
	c.client.metricBlocked(c.subscription.Topic, c.subscription.Group, w.laneID, false)
	capacity, _, err := c.client.storage.capacity()
	if err != nil {
		return w.retry(err)
	}
	if capacity == capacityRecoveryOnly {
		return c.cfg.ReconcileInterval
	}
	read, err := c.client.storage.readLaneBatchWithCursor(w.reader, checkpoint.next, c.cfg.BatchMaxMessages, c.cfg.BatchMaxBytes)
	if err != nil {
		w.reader.cursor = laneCursor{}
		return w.retry(err)
	}
	if len(read.Batch.Messages) == 0 {
		if checkpoint.next > read.DurableEnd {
			c.setError(fmt.Errorf("checkpoint %d exceeds durable end %d", checkpoint.next, read.DurableEnd))
			w.flushForShutdown()
			return -1
		}
		c.client.metricConsumerState(c.subscription.Topic, c.subscription.Group, 0, 0)
		w.backoff.reset()
		return c.cfg.BatchMaxWait
	}
	owned, err := c.validHandlerOwnership(w.ctx, w.laneID)
	if err != nil {
		return w.retry(err)
	}
	if !owned {
		w.cancel()
		c.mu.Lock()
		if c.state == consumerStateStarted {
			w.draining = true
		}
		c.mu.Unlock()
		return -1
	}
	handlerErr := c.callHandler(w.ctx, read.Batch)
	if handlerErr == nil {
		if checkpoint.next > read.DurableEnd {
			c.setError(fmt.Errorf("checkpoint %d exceeds durable end %d", checkpoint.next, read.DurableEnd))
			w.flushForShutdown()
			return -1
		}
		c.client.metricBlocked(c.subscription.Topic, c.subscription.Group, w.laneID, false)
		oldestAge := c.client.clock().Sub(read.Batch.Messages[0].AppendTime)
		if oldestAge < 0 {
			oldestAge = 0
		}
		c.client.metricConsumerState(c.subscription.Topic, c.subscription.Group, read.DurableEnd-checkpoint.next, oldestAge)
		lastSequence := read.Batch.Messages[len(read.Batch.Messages)-1].Sequence
		if lastSequence == ^uint64(0) {
			c.setError(errors.New("consumer checkpoint sequence is exhausted"))
			w.flushForShutdown()
			return -1
		}
		if err := checkpoint.advance(lastSequence + 1); err != nil {
			c.setError(err)
			w.flushForShutdown()
			return -1
		}
		if checkpoint.due() {
			if err := checkpoint.flush(w.ctx); err != nil {
				return w.retry(err)
			}
		}
		w.backoff.reset()
		return 0
	}
	if !IsPermanent(handlerErr) {
		return w.retry(handlerErr)
	}
	prefix, poison, findErr := c.findPermanent(w.ctx, read.Batch)
	if findErr != nil {
		return w.retry(findErr)
	}
	if prefix > 0 {
		lastSequence := read.Batch.Messages[prefix-1].Sequence
		if lastSequence == ^uint64(0) {
			c.setError(errors.New("consumer checkpoint sequence is exhausted"))
			w.flushForShutdown()
			return -1
		}
		if err := checkpoint.advance(lastSequence + 1); err != nil {
			c.setError(err)
			w.flushForShutdown()
			return -1
		}
	}
	if err := checkpoint.flush(w.ctx); err != nil {
		return w.retry(err)
	}
	marker := blockedRecord{
		Topic:              c.subscription.Topic,
		Group:              c.subscription.Group,
		LaneID:             w.laneID,
		Sequence:           poison.Sequence,
		ConsumerInstanceID: c.instanceID,
		MessageID:          fmt.Sprintf("%x", poison.MessageID),
		ContentHash:        contentHash(walRecord{MessageID: poison.MessageID, EventType: poison.EventType, Payload: poison.Payload}),
		CreatedAtUnixNano:  c.client.clock().UnixNano(),
		Error:              handlerErr.Error(),
	}
	if err := c.client.storage.writeBlocked(w.ctx, marker); err != nil {
		return w.retry(err)
	}
	w.backoff.reset()
	return c.cfg.ReconcileInterval
}

func (w *laneWorker) retry(err error) time.Duration {
	if err != nil {
		w.consumer.setError(err)
	}
	if w.backoff == nil {
		w.backoff = newBackoff()
	}
	return w.backoff.next()
}

func (w *laneWorker) flushForShutdown() error {
	if w.checkpoint == nil {
		return nil
	}
	if err := w.checkpoint.flushForShutdown(); err != nil {
		w.consumer.setError(err)
		return err
	}
	return nil
}

func (c *Consumer) validHandlerOwnership(ctx context.Context, laneID string) (bool, error) {
	c.mu.Lock()
	m := c.membership
	ready := c.membershipReady
	generation := c.membershipGeneration
	lastBeat := c.lastBeat
	c.mu.Unlock()
	if m == nil {
		return false, errors.New("localmq consumer membership is nil")
	}
	if !ready {
		return false, nil
	}
	if c.client.clock().Sub(lastBeat) >= c.cfg.MembershipTimeout {
		return false, nil
	}
	control, err := c.client.storage.groupControl(c.subscription.Topic, c.subscription.Group)
	if err != nil {
		return false, err
	}
	if control.State != string(GroupActive) {
		return false, nil
	}
	members, err := m.activeMembers(ctx)
	if err != nil {
		return false, err
	}
	c.mu.Lock()
	valid := c.state == consumerStateStarted && c.membershipReady && c.membershipGeneration == generation && c.ctx != nil && c.ctx.Err() == nil && c.client.clock().Sub(c.lastBeat) < c.cfg.MembershipTimeout
	c.mu.Unlock()
	if !valid {
		return false, nil
	}
	return rendezvousOwner(c.subscription.Topic, laneID, members) == c.instanceID, nil
}

func (w *laneWorker) startingCheckpoint() (uint64, error) {
	c := w.consumer
	base, err := c.client.storage.readCheckpointBase(c.subscription.Topic, c.subscription.Group)
	if err != nil {
		return 0, err
	}
	if _, exists := base.NextSequence[w.laneID]; exists {
		return c.client.storage.effectiveCheckpoint(c.subscription.Topic, c.subscription.Group, w.laneID)
	}
	index, err := c.client.storage.retention(c.subscription.Topic, w.laneID)
	if err != nil {
		return 0, err
	}
	sequence := index.EarliestRetainedSeq
	checkpoint := checkpointRecord{
		Topic:              c.subscription.Topic,
		Group:              c.subscription.Group,
		LaneID:             w.laneID,
		NextSequence:       sequence,
		ConsumerInstanceID: c.instanceID,
	}
	if err := c.client.storage.writeMemberCheckpoint(w.ctx, checkpoint); err != nil {
		return 0, err
	}
	return sequence, nil
}

func (c *Consumer) callHandler(ctx context.Context, batch Batch) error {
	handlerCtx, cancel := context.WithTimeout(ctx, c.cfg.HandlerTimeout)
	defer cancel()
	return c.handler.Handle(handlerCtx, batch)
}

func (c *Consumer) findPermanent(ctx context.Context, batch Batch) (int, DeliveredMessage, error) {
	if len(batch.Messages) == 0 {
		return 0, DeliveredMessage{}, errors.New("cannot locate poison record in empty batch")
	}
	var locate func(Batch) (int, error)
	locate = func(candidate Batch) (int, error) {
		if len(candidate.Messages) == 1 {
			err := c.callHandler(ctx, candidate)
			if err == nil {
				return 1, nil
			}
			if IsPermanent(err) {
				return 0, nil
			}
			return -1, err
		}
		middle := len(candidate.Messages) / 2
		left := candidate
		left.Messages = candidate.Messages[:middle]
		leftResult, err := locate(left)
		if err != nil {
			return -1, err
		}
		if leftResult < middle {
			return leftResult, nil
		}
		right := candidate
		right.Messages = candidate.Messages[middle:]
		rightResult, err := locate(right)
		if err != nil {
			return -1, err
		}
		if rightResult < len(right.Messages) {
			return middle + rightResult, nil
		}
		return len(candidate.Messages), nil
	}
	prefix, err := locate(batch)
	if err != nil {
		return 0, DeliveredMessage{}, err
	}
	if prefix < 0 || prefix >= len(batch.Messages) {
		return 0, DeliveredMessage{}, errors.New("permanent handler error did not identify a poison record")
	}
	return prefix, batch.Messages[prefix], nil
}

type checkpointTracker struct {
	consumer      *Consumer
	laneID        string
	next          uint64
	persisted     uint64
	pendingCount  int
	mustFlush     bool
	lastPersisted time.Time
}

func (t *checkpointTracker) advance(next uint64) error {
	if next <= t.next {
		if next < t.next {
			return errors.New("checkpoint watermark regressed")
		}
		return nil
	}
	delta := next - t.next
	maxInt := uint64(^uint(0) >> 1)
	if delta > maxInt || t.pendingCount > int(maxInt-delta) {
		t.pendingCount = int(maxInt)
	} else {
		t.pendingCount += int(delta)
	}
	t.next = next
	return nil
}

func (t *checkpointTracker) due() bool {
	now := time.Now()
	if t.consumer != nil && t.consumer.client != nil && t.consumer.client.clock != nil {
		now = t.consumer.client.clock()
	}
	return t.next > t.persisted && (t.pendingCount >= t.consumer.cfg.CheckpointMaxRecords || now.Sub(t.lastPersisted) >= t.consumer.cfg.CheckpointMaxDelay)
}

func (t *checkpointTracker) flush(ctx context.Context) error {
	if t.next <= t.persisted {
		t.mustFlush = false
		return nil
	}
	err := t.consumer.client.storage.writeMemberCheckpoint(ctx, checkpointRecord{
		Topic:              t.consumer.subscription.Topic,
		Group:              t.consumer.subscription.Group,
		LaneID:             t.laneID,
		NextSequence:       t.next,
		ConsumerInstanceID: t.consumer.instanceID,
	})
	if err != nil {
		t.mustFlush = true
		t.consumer.client.metricCheckpointFailure(t.consumer.subscription.Topic, t.consumer.subscription.Group)
		return err
	}
	t.persisted = t.next
	t.pendingCount = 0
	t.mustFlush = false
	t.lastPersisted = t.consumer.client.clock()
	return nil
}

func (t *checkpointTracker) flushForShutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), t.consumer.cfg.HandlerTimeout)
	defer cancel()
	return t.flush(ctx)
}

type backoff struct {
	current time.Duration
}

func newBackoff() *backoff { return &backoff{} }

func (b *backoff) reset() { b.current = 0 }

func (b *backoff) next() time.Duration {
	if b.current == 0 {
		b.current = 25 * time.Millisecond
	} else {
		b.current *= 2
		if b.current > 5*time.Second {
			b.current = 5 * time.Second
		}
	}
	jitter := time.Duration(rand.Int63n(int64(b.current/2 + 1)))
	return b.current/2 + jitter
}

func waitWithContext(ctx context.Context, duration time.Duration) bool {
	if duration <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

var _ interface {
	Start(context.Context) error
	Stop(context.Context) error
} = (*Consumer)(nil)
