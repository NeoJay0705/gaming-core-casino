package localmq

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const requiredTestGroup = "workers"

func writeRequiredGroup(t testing.TB, s *storage, groupType GroupType, baseline uint64) {
	t.Helper()
	groupDir := s.groupDir(requiredTestTopic, requiredTestGroup)
	if err := os.MkdirAll(filepath.Join(groupDir, blockedDirectory, requiredTestLane), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(groupDir, membersDirectory), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := writeMetadataAtomic(filepath.Join(groupDir, groupManifestFile), metadataKindGroupManifest, groupManifest{
		FormatVersion:   formatVersion,
		Topic:           requiredTestTopic,
		Group:           requiredTestGroup,
		GroupType:       string(groupType),
		InitialPosition: string(InitialEarliest),
	}, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := writeMetadataAtomic(filepath.Join(groupDir, groupControlFile), metadataKindGroupControl, groupControl{
		FormatVersion: formatVersion,
		State:         string(GroupActive),
	}, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := s.writeCheckpointBase(context.Background(), requiredTestTopic, requiredTestGroup, map[string]uint64{requiredTestLane: baseline}); err != nil {
		t.Fatal(err)
	}
}

func requiredClient(s *storage) *Client {
	return &Client{
		cfg:     s.cfg,
		storage: s,
		clock:   s.now,
		state:   clientStateStarted,
		lanes:   make(map[string]*writerLane),
		blocked: make(map[string]bool),
	}
}

func requiredRecordsFrom(base uint64, count int) []walRecord {
	records := make([]walRecord, count)
	for index := range records {
		sequence := base + uint64(index)
		records[index] = walRecord{
			Sequence:   sequence,
			AppendTime: time.Unix(1000+int64(sequence), 0).UTC(),
			MessageID:  []byte{byte(sequence + 1)},
			EventType:  "event",
			Payload:    []byte{byte(sequence)},
		}
	}
	return records
}

func writeRequiredTopicMetadata(t testing.TB, s *storage, topic string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(s.topicDir(topic), lanesDirectory), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(s.topicDir(topic), gcStagingDirectory), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := writeMetadataAtomic(filepath.Join(s.topicDir(topic), topicManifestFile), metadataKindTopicManifest, topicManifest{
		FormatVersion: formatVersion, Topic: topic, MaxRecordBytes: 1024,
	}, 0o640); err != nil {
		t.Fatal(err)
	}
}

func writeRequiredTopicControl(t testing.TB, s *storage, topic string, retentionSeconds int64) {
	t.Helper()
	if err := writeMetadataAtomic(filepath.Join(s.topicDir(topic), topicControlFile), metadataKindTopicControl, topicControl{
		FormatVersion: formatVersion, RetentionTimeSeconds: retentionSeconds,
	}, 0o640); err != nil {
		t.Fatal(err)
	}
}

func writeRequiredLaneMetadata(t testing.TB, s *storage, topic, laneID string) {
	t.Helper()
	if err := os.MkdirAll(s.segmentsDir(topic, laneID), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := writeMetadataAtomic(filepath.Join(s.laneDir(topic, laneID), laneManifestFile), metadataKindLaneManifest, laneManifest{
		FormatVersion: formatVersion, StorageID: s.cfg.StorageID, Topic: topic, LaneID: laneID,
	}, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := writeMetadataAtomic(filepath.Join(s.laneDir(topic, laneID), retentionIndexFile), metadataKindRetentionIndex, retentionIndex{
		FormatVersion: formatVersion, Topic: topic, LaneID: laneID,
	}, 0o640); err != nil {
		t.Fatal(err)
	}
}

func writeRequiredHeaderOnly(t testing.TB, s *storage, filenameTopic, filenameLane string, filenameBase uint64, headerTopic, headerLane string, headerBase uint64) int64 {
	t.Helper()
	header, err := encodeSegmentHeader(headerTopic, headerLane, headerBase, time.Unix(1000, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.segmentsDir(filenameTopic, filenameLane), formatSegmentName(filenameBase, true))
	if err := os.WriteFile(path, header, 0o640); err != nil {
		t.Fatal(err)
	}
	return int64(len(header))
}

func updateRequiredAtomicMax(max *atomic.Int64, current int64) {
	for {
		previous := max.Load()
		if current <= previous || max.CompareAndSwap(previous, current) {
			return
		}
	}
}

func writeRequiredBlocked(t testing.TB, s *storage, sequence uint64) {
	t.Helper()
	if err := s.writeBlocked(context.Background(), blockedRecord{
		Topic:              requiredTestTopic,
		Group:              requiredTestGroup,
		LaneID:             requiredTestLane,
		Sequence:           sequence,
		ConsumerInstanceID: "consumera",
		Error:              "poison",
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRequiredAcknowledgeUsesDurableBaselineAsRetryProof(t *testing.T) {
	t.Run("first operation advances baseline", func(t *testing.T) {
		s, topic, laneID := newRequiredTestStorage(t)
		records := requiredRecordsFrom(0, 10)
		dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, records, nil)
		writeRequiredFrontier(t, s, topic, laneID, 0, 10, dataEnd)
		writeRequiredGroup(t, s, GroupTypeBestEffort, 2)
		writeRequiredBlocked(t, s, 3)
		writeRequiredBlocked(t, s, 6)

		maintainer := &Maintainer{client: requiredClient(s)}
		err := maintainer.acknowledgeRecords(context.Background(), AcknowledgeRequest{
			Topic: topic, Group: requiredTestGroup, LaneID: laneID, UpToSequence: 5,
			Operator: "operator", Reason: "confirmed poison", RequestID: "ack-first",
		})
		if err != nil {
			t.Fatal(err)
		}
		base, err := s.readCheckpointBase(topic, requiredTestGroup)
		if err != nil {
			t.Fatal(err)
		}
		if got := base.NextSequence[laneID]; got != 5 {
			t.Fatalf("baseline = %d, want 5", got)
		}
		markers, err := s.listBlocked(topic, requiredTestGroup, laneID)
		if err != nil {
			t.Fatal(err)
		}
		if len(markers) != 1 || markers[0].Sequence != 6 {
			t.Fatalf("remaining markers = %+v", markers)
		}
	})

	t.Run("member checkpoint alone is not retry proof", func(t *testing.T) {
		s, topic, laneID := newRequiredTestStorage(t)
		records := requiredRecordsFrom(0, 10)
		dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, records, nil)
		writeRequiredFrontier(t, s, topic, laneID, 0, 10, dataEnd)
		writeRequiredGroup(t, s, GroupTypeBestEffort, 2)
		writeRequiredBlocked(t, s, 3)
		if err := s.writeMemberCheckpoint(context.Background(), checkpointRecord{
			Topic: topic, Group: requiredTestGroup, LaneID: laneID,
			ConsumerInstanceID: "consumer-b", NextSequence: 8,
		}); err != nil {
			t.Fatal(err)
		}

		maintainer := &Maintainer{client: requiredClient(s)}
		err := maintainer.acknowledgeRecords(context.Background(), AcknowledgeRequest{
			Topic: topic, Group: requiredTestGroup, LaneID: laneID, UpToSequence: 5,
			Operator: "operator", Reason: "retry", RequestID: "ack-unsafe-retry",
		})
		if err == nil {
			t.Fatal("acknowledge accepted a non-durable baseline retry")
		}
		markers, listErr := s.listBlocked(topic, requiredTestGroup, laneID)
		if listErr != nil || len(markers) != 1 {
			t.Fatalf("blocked marker changed: markers=%+v err=%v", markers, listErr)
		}
	})

	t.Run("stale marker below effective cannot authorize a larger skip", func(t *testing.T) {
		s, topic, laneID := newRequiredTestStorage(t)
		records := requiredRecordsFrom(0, 250)
		dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, records, nil)
		writeRequiredFrontier(t, s, topic, laneID, 0, 250, dataEnd)
		writeRequiredGroup(t, s, GroupTypeBestEffort, 0)
		writeRequiredBlocked(t, s, 50)
		if err := s.writeMemberCheckpoint(context.Background(), checkpointRecord{
			Topic: topic, Group: requiredTestGroup, LaneID: laneID,
			ConsumerInstanceID: "consumer-member", NextSequence: 100,
		}); err != nil {
			t.Fatal(err)
		}
		maintainer := &Maintainer{client: requiredClient(s)}
		if err := maintainer.acknowledgeRecords(context.Background(), AcknowledgeRequest{
			Topic: topic, Group: requiredTestGroup, LaneID: laneID, UpToSequence: 200,
			Operator: "operator", Reason: "invalid skip", RequestID: "ack-stale-marker",
		}); err == nil {
			t.Fatal("stale marker authorized records after the effective checkpoint")
		}
		base, err := s.readCheckpointBase(topic, requiredTestGroup)
		if err != nil || base.NextSequence[laneID] != 0 {
			t.Fatalf("baseline changed after rejected acknowledge: %+v err=%v", base, err)
		}
	})

	t.Run("retry cleans through published baseline", func(t *testing.T) {
		s, topic, laneID := newRequiredTestStorage(t)
		records := requiredRecordsFrom(0, 10)
		dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, records, nil)
		writeRequiredFrontier(t, s, topic, laneID, 0, 10, dataEnd)
		writeRequiredGroup(t, s, GroupTypeBestEffort, 8)
		writeRequiredBlocked(t, s, 3)
		writeRequiredBlocked(t, s, 7)

		maintainer := &Maintainer{client: requiredClient(s)}
		if err := maintainer.acknowledgeRecords(context.Background(), AcknowledgeRequest{
			Topic: topic, Group: requiredTestGroup, LaneID: laneID, UpToSequence: 5,
			Operator: "operator", Reason: "retry cleanup", RequestID: "ack-safe-retry",
		}); err != nil {
			t.Fatal(err)
		}
		markers, err := s.listBlocked(topic, requiredTestGroup, laneID)
		if err != nil {
			t.Fatal(err)
		}
		if len(markers) != 0 {
			t.Fatalf("stale markers remain below baseline: %+v", markers)
		}
	})
}

func TestRequiredBestEffortFloorRequiresCheckpointDurability(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	records := requiredRecordsFrom(0, 5)
	dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, records, nil)
	writeRequiredFrontier(t, s, topic, laneID, 0, 5, dataEnd)
	writeRequiredGroup(t, s, GroupTypeBestEffort, 0)
	if err := writeMetadataAtomic(filepath.Join(s.laneDir(topic, laneID), retentionIndexFile), metadataKindRetentionIndex, retentionIndex{
		FormatVersion: formatVersion, Topic: topic, LaneID: laneID, EarliestRetainedSeq: 5,
	}, 0o640); err != nil {
		t.Fatal(err)
	}
	ops := newStorageOps()
	failCheckpoint := true
	ops.fault = func(point string) error {
		if point == faultCheckpointRename && failCheckpoint {
			return errors.New("injected checkpoint failure")
		}
		return nil
	}
	s.ops = ops
	client := requiredClient(s)
	handler := &countingRequiredHandler{}
	consumer := &Consumer{
		client: client, subscription: Subscription{Topic: topic, Group: requiredTestGroup},
		handler: handler, cfg: client.cfg.Consumer, instanceID: "consumer-floor",
	}
	worker := &laneWorker{
		consumer: consumer, laneID: laneID, ctx: context.Background(), cancel: func() {}, initialized: true,
		groupManifest: groupManifest{GroupType: string(GroupTypeBestEffort)},
		checkpoint:    &checkpointTracker{consumer: consumer, laneID: laneID, lastPersisted: s.now()},
		reader:        newLaneReader(s, topic, laneID), backoff: newBackoff(),
	}
	worker.step()
	worker.step()
	if worker.checkpoint.next != 5 || worker.checkpoint.persisted != 0 || !worker.checkpoint.mustFlush {
		t.Fatalf("checkpoint tracker = %+v", worker.checkpoint)
	}
	if calls := handler.calls.Load(); calls != 0 {
		t.Fatalf("handler calls while checkpoint failed = %d, want 0", calls)
	}
	if _, exists, err := s.readCheckpoint(topic, requiredTestGroup, consumer.instanceID, laneID); err != nil || exists {
		t.Fatalf("failed floor checkpoint became durable: exists=%v err=%v", exists, err)
	}
	failCheckpoint = false
	if err := worker.checkpoint.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	restarted := newStorage(s.cfg)
	checkpoint, exists, err := restarted.readCheckpoint(topic, requiredTestGroup, consumer.instanceID, laneID)
	if err != nil || !exists || checkpoint.NextSequence != 5 {
		t.Fatalf("checkpoint retry = %+v exists=%v err=%v", checkpoint, exists, err)
	}
}

type permanentRequiredHandler struct{}

func (permanentRequiredHandler) Handle(context.Context, Batch) error {
	return Permanent(errors.New("poison"))
}

func TestRequiredPermanentMarkerWaitsForPendingCheckpoint(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	records := requiredRecordsFrom(0, 4)
	dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, records, nil)
	writeRequiredFrontier(t, s, topic, laneID, 0, 4, dataEnd)
	writeRequiredGroup(t, s, GroupTypeBestEffort, 0)
	ops := newStorageOps()
	failCheckpoint := true
	ops.fault = func(point string) error {
		if point == faultCheckpointRename && failCheckpoint {
			return errors.New("injected checkpoint failure")
		}
		return nil
	}
	s.ops = ops
	client := requiredClient(s)
	store := &requiredMembershipStore{members: []string{"consumer-poison"}}
	membership, err := newMembershipWithStore(store, "membership", "consumer-poison", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	consumer := &Consumer{
		client: client, subscription: Subscription{Topic: topic, Group: requiredTestGroup},
		handler: permanentRequiredHandler{}, cfg: client.cfg.Consumer,
		instanceID: "consumer-poison", membership: membership, state: consumerStateStarted,
		membershipReady: true, membershipGeneration: 1, lastBeat: s.now(),
		ctx: context.Background(),
	}
	worker := &laneWorker{
		consumer: consumer, laneID: laneID, ctx: context.Background(), cancel: func() {}, initialized: true,
		groupManifest: groupManifest{GroupType: string(GroupTypeBestEffort)},
		checkpoint: &checkpointTracker{
			consumer: consumer, laneID: laneID, next: 2, persisted: 0, pendingCount: 2, lastPersisted: s.now(),
		},
		reader: newLaneReader(s, topic, laneID), backoff: newBackoff(),
	}
	worker.step()
	markers, err := s.listBlocked(topic, requiredTestGroup, laneID)
	if err != nil {
		t.Fatal(err)
	}
	if len(markers) != 0 || !worker.checkpoint.mustFlush {
		t.Fatalf("blocked marker bypassed checkpoint: markers=%+v tracker=%+v", markers, worker.checkpoint)
	}
	failCheckpoint = false
	worker.step()
	checkpoint, exists, err := s.readCheckpoint(topic, requiredTestGroup, consumer.instanceID, laneID)
	if err != nil || !exists || checkpoint.NextSequence != 2 {
		t.Fatalf("pending checkpoint retry = %+v exists=%v err=%v", checkpoint, exists, err)
	}
	markers, err = s.listBlocked(topic, requiredTestGroup, laneID)
	if err != nil || len(markers) != 1 || markers[0].Sequence != 2 {
		t.Fatalf("blocked marker after durable checkpoint = %+v err=%v", markers, err)
	}
}

func TestRequiredStopReportsAllPendingFlushFailuresAndKeepsMembership(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	records := requiredRecordsFrom(0, 1)
	dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, records, nil)
	writeRequiredFrontier(t, s, topic, laneID, 0, 1, dataEnd)
	writeRequiredGroup(t, s, GroupTypeBestEffort, 0)
	ops := newStorageOps()
	ops.fault = func(point string) error {
		if point == faultCheckpointRename {
			return errors.New("injected shutdown checkpoint failure")
		}
		return nil
	}
	s.ops = ops
	client := requiredClient(s)
	store := &requiredMembershipStore{}
	membership, err := newMembershipWithStore(store, "membership", "consumer-stop", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	consumerCtx, cancel := context.WithCancel(context.Background())
	consumer := &Consumer{
		client: client, subscription: Subscription{Topic: topic, Group: requiredTestGroup},
		cfg: client.cfg.Consumer, state: consumerStateStarted, instanceID: "consumer-stop",
		ctx: consumerCtx, cancel: cancel, membership: membership,
		workers: map[string]*laneWorker{}, schedulerWake: make(chan struct{}, 1),
	}
	worker := &laneWorker{consumer: consumer, laneID: laneID, cancel: func() {}}
	worker.checkpoint = &checkpointTracker{consumer: consumer, laneID: laneID, next: 1, lastPersisted: s.now()}
	consumer.workers[laneID] = worker

	firstErr := consumer.Stop(context.Background())
	if firstErr == nil {
		t.Fatal("Stop reported success after checkpoint failure")
	}
	if store.removed != 0 {
		t.Fatalf("membership remove calls = %d, want 0", store.removed)
	}
	if secondErr := consumer.Stop(context.Background()); secondErr == nil || secondErr.Error() != firstErr.Error() {
		t.Fatalf("repeated Stop error = %v, want %v", secondErr, firstErr)
	}
}

func TestRequiredCancellationRetainsInflightWorkerUntilRelease(t *testing.T) {
	consumer := &Consumer{
		state: consumerStateStarted, cfg: ConsumerConfig{ReconcileInterval: time.Second},
		workers: map[string]*laneWorker{}, laneOrder: []string{requiredTestLane}, schedulerWake: make(chan struct{}, 1),
	}
	worker := &laneWorker{consumer: consumer, laneID: requiredTestLane, inFlight: true, cancel: func() {}}
	consumer.workers[requiredTestLane] = worker
	consumer.pauseWorkers()
	if current := consumer.workers[requiredTestLane]; current != worker || !worker.inFlight {
		t.Fatalf("in-flight worker was discarded: current=%p worker=%p inFlight=%v", current, worker, worker.inFlight)
	}
	consumer.state = consumerStateStopping
	consumer.releaseLane(worker, -1)
	if current := consumer.workers[requiredTestLane]; current != worker || worker.inFlight {
		t.Fatalf("stopping release did not retain terminal state: current=%p inFlight=%v", current, worker.inFlight)
	}
}

type countingRequiredHandler struct{ calls atomic.Int64 }

func (h *countingRequiredHandler) Handle(context.Context, Batch) error {
	h.calls.Add(1)
	return nil
}

func TestRequiredBlockedLaneUsesReconcileDelayWithoutHandlerAttempt(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	records := requiredRecordsFrom(0, 1)
	dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, records, nil)
	writeRequiredFrontier(t, s, topic, laneID, 0, 1, dataEnd)
	writeRequiredGroup(t, s, GroupTypeBestEffort, 0)
	writeRequiredBlocked(t, s, 0)
	client := requiredClient(s)
	handler := &countingRequiredHandler{}
	consumer := &Consumer{
		client: client, subscription: Subscription{Topic: topic, Group: requiredTestGroup},
		handler: handler, cfg: client.cfg.Consumer, instanceID: "consumer-blocked",
	}
	worker := &laneWorker{
		consumer: consumer, laneID: laneID, ctx: context.Background(), cancel: func() {}, initialized: true,
		groupManifest: groupManifest{GroupType: string(GroupTypeBestEffort)},
		checkpoint:    &checkpointTracker{consumer: consumer, laneID: laneID, lastPersisted: s.now()},
		reader:        newLaneReader(s, topic, laneID), backoff: newBackoff(),
	}
	if delay := worker.step(); delay != client.cfg.Consumer.ReconcileInterval {
		t.Fatalf("blocked lane delay = %v, want %v", delay, client.cfg.Consumer.ReconcileInterval)
	}
	if calls := handler.calls.Load(); calls != 0 {
		t.Fatalf("handler calls = %d, want 0", calls)
	}
}

type blockingMembershipStore struct {
	instance   string
	entered    chan struct{}
	release    chan struct{}
	enterOnce  sync.Once
	removeCall atomic.Int64
}

func (s *blockingMembershipStore) heartbeat(ctx context.Context, _, instance string) error {
	s.instance = instance
	s.enterOnce.Do(func() { close(s.entered) })
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *blockingMembershipStore) activeMembers(context.Context, string, time.Duration) ([]string, error) {
	return []string{s.instance}, nil
}

func (s *blockingMembershipStore) remove(context.Context, string, string) error {
	s.removeCall.Add(1)
	return nil
}

type blockingActiveMembershipStore struct {
	entered chan struct{}
	release chan struct{}
	members []string
}

func (s *blockingActiveMembershipStore) heartbeat(context.Context, string, string) error {
	return nil
}

func (s *blockingActiveMembershipStore) activeMembers(context.Context, string, time.Duration) ([]string, error) {
	close(s.entered)
	<-s.release
	return append([]string(nil), s.members...), nil
}

func (s *blockingActiveMembershipStore) remove(context.Context, string, string) error {
	return nil
}

type mutableRequiredMembershipStore struct {
	mu          sync.RWMutex
	members     []string
	fail        atomic.Bool
	failed      chan struct{}
	succeeded   chan struct{}
	failOnce    sync.Once
	successOnce sync.Once
	removed     atomic.Int64
}

func (s *mutableRequiredMembershipStore) setMembers(members ...string) {
	s.mu.Lock()
	s.members = append([]string(nil), members...)
	s.mu.Unlock()
}

func (s *mutableRequiredMembershipStore) heartbeat(context.Context, string, string) error {
	if s.fail.Load() {
		if s.failed != nil {
			s.failOnce.Do(func() { close(s.failed) })
		}
		return errors.New("injected membership heartbeat failure")
	}
	if s.succeeded != nil {
		s.successOnce.Do(func() { close(s.succeeded) })
	}
	return nil
}

func (s *mutableRequiredMembershipStore) activeMembers(context.Context, string, time.Duration) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]string(nil), s.members...), nil
}

func (s *mutableRequiredMembershipStore) remove(context.Context, string, string) error {
	s.removed.Add(1)
	return nil
}

type noopRequiredHandler struct{}

func (noopRequiredHandler) Handle(context.Context, Batch) error { return nil }

func TestRequiredConcurrentStartStopWaitsForMembershipPreflight(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	records := requiredRecordsFrom(0, 1)
	dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, records, nil)
	writeRequiredFrontier(t, s, topic, laneID, 0, 1, dataEnd)
	writeRequiredGroup(t, s, GroupTypeBestEffort, 0)
	client := requiredClient(s)
	client.cfg.Consumer.WorkerConcurrency = 1
	client.cfg.Consumer.HeartbeatInterval = time.Hour
	client.cfg.Consumer.ReconcileInterval = time.Hour
	store := &blockingMembershipStore{entered: make(chan struct{}), release: make(chan struct{})}
	factory := func(_, _, instanceID string, timeout time.Duration) (*membership, error) {
		return newMembershipWithStore(store, "membership", instanceID, timeout)
	}
	consumer, err := newConsumerWithMembershipFactory(client, Subscription{Topic: topic, Group: requiredTestGroup}, noopRequiredHandler{}, factory)
	if err != nil {
		t.Fatal(err)
	}
	startResult := make(chan error, 1)
	go func() { startResult <- consumer.Start(context.Background()) }()
	<-store.entered
	stopResult := make(chan error, 1)
	go func() { stopResult <- consumer.Stop(context.Background()) }()
	close(store.release)
	if err := <-startResult; err != nil && !errors.Is(err, context.Canceled) && err.Error() != "localmq consumer start was interrupted" {
		t.Fatal(err)
	}
	if err := <-stopResult; err != nil {
		t.Fatal(err)
	}
	if got := store.removeCall.Load(); got != 1 {
		t.Fatalf("membership remove calls = %d, want 1", got)
	}
}

func TestRequiredDrainingWorkerBlocksReplacementUntilFlush(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, requiredRecordsFrom(0, 1), nil)
	writeRequiredFrontier(t, s, topic, laneID, 0, 1, dataEnd)
	writeRequiredGroup(t, s, GroupTypeBestEffort, 0)

	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	var flushFail atomic.Bool
	ops := newStorageOps()
	ops.fault = func(point string) error {
		if point == faultCheckpointRename {
			enteredOnce.Do(func() { close(entered) })
			<-release
			if flushFail.Load() {
				return errors.New("injected draining checkpoint failure")
			}
		}
		return nil
	}
	s.ops = ops
	client := requiredClient(s)
	store := &requiredMembershipStore{members: []string{"consumer-drain"}}
	membership, err := newMembershipWithStore(store, "membership", "consumer-drain", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	consumerCtx := context.Background()
	consumer := &Consumer{
		client: client, subscription: Subscription{Topic: topic, Group: requiredTestGroup},
		cfg: client.cfg.Consumer, state: consumerStateStarted, instanceID: "consumer-drain",
		ctx: consumerCtx, membership: membership, membershipReady: true, membershipGeneration: 1,
		lastBeat: s.now(), workers: make(map[string]*laneWorker), schedulerWake: make(chan struct{}, 1),
	}
	workerCtx, workerCancel := context.WithCancel(consumerCtx)
	worker := &laneWorker{
		consumer: consumer, laneID: laneID, ctx: workerCtx, cancel: workerCancel,
		checkpoint: &checkpointTracker{consumer: consumer, laneID: laneID, next: 1, lastPersisted: s.now()},
	}
	consumer.workers[laneID] = worker
	consumer.laneOrder = []string{laneID}

	flushFail.Store(true)
	pauseDone := make(chan struct{})
	go func() {
		consumer.pauseWorkers()
		close(pauseDone)
	}()
	<-entered
	if err := consumer.reconcile(consumerCtx); err != nil {
		t.Fatal(err)
	}
	consumer.mu.Lock()
	current := consumer.workers[laneID]
	consumer.mu.Unlock()
	if current != worker {
		t.Fatalf("replacement worker was created before flush completed: current=%p old=%p", current, worker)
	}

	close(release)
	<-pauseDone
	consumer.mu.Lock()
	_, exists := consumer.workers[laneID]
	consumer.mu.Unlock()
	if !exists {
		t.Fatal("drained worker was removed after failed flush")
	}
	if err := consumer.reconcile(consumerCtx); err != nil {
		t.Fatal(err)
	}
	consumer.mu.Lock()
	current = consumer.workers[laneID]
	consumer.mu.Unlock()
	if current != worker {
		t.Fatalf("failed draining flush allowed replacement worker: current=%p old=%p", current, worker)
	}
	flushFail.Store(false)
	consumer.flushDrainingWorkers()
	consumer.mu.Lock()
	_, exists = consumer.workers[laneID]
	consumer.mu.Unlock()
	if exists {
		t.Fatal("drained worker was retained after successful retry")
	}
	if err := consumer.reconcile(consumerCtx); err != nil {
		t.Fatal(err)
	}
	consumer.mu.Lock()
	replacement := consumer.workers[laneID]
	consumer.mu.Unlock()
	if replacement == nil || replacement == worker {
		t.Fatalf("replacement worker was not created after successful flush: %p", replacement)
	}
}

func TestRequiredReconcileDropsStaleMembershipPlan(t *testing.T) {
	s, topic, _ := newRequiredTestStorage(t)
	writeRequiredGroup(t, s, GroupTypeBestEffort, 0)
	store := &blockingActiveMembershipStore{
		entered: make(chan struct{}), release: make(chan struct{}), members: []string{"consumer-generation"},
	}
	membership, err := newMembershipWithStore(store, "membership", "consumer-generation", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	consumerCtx := context.Background()
	consumer := &Consumer{
		client: requiredClient(s), subscription: Subscription{Topic: topic, Group: requiredTestGroup},
		cfg: DefaultConfig().Consumer, state: consumerStateStarted, instanceID: "consumer-generation",
		ctx: consumerCtx, membership: membership, membershipReady: true, membershipGeneration: 1,
		lastBeat: s.now(), workers: make(map[string]*laneWorker), schedulerWake: make(chan struct{}, 1),
	}
	reconcileDone := make(chan error, 1)
	go func() { reconcileDone <- consumer.reconcile(consumerCtx) }()
	<-store.entered
	consumer.mu.Lock()
	consumer.membershipReady = false
	consumer.membershipGeneration++
	consumer.mu.Unlock()
	close(store.release)
	if err := <-reconcileDone; err != nil {
		t.Fatal(err)
	}
	consumer.mu.Lock()
	workerCount := len(consumer.workers)
	consumer.mu.Unlock()
	if workerCount != 0 {
		t.Fatalf("stale membership plan created %d workers", workerCount)
	}
}

func TestRequiredMembershipTimeoutStopsSchedulerClaim(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	consumer := &Consumer{
		state:           consumerStateStarted,
		membershipReady: false,
		cfg:             ConsumerConfig{ReconcileInterval: time.Second},
		workers:         make(map[string]*laneWorker),
		laneOrder:       []string{requiredTestLane},
		schedulerWake:   make(chan struct{}, 1),
	}
	worker := &laneWorker{consumer: consumer, laneID: requiredTestLane, nextRun: now, ctx: context.Background()}
	consumer.workers[requiredTestLane] = worker

	claimed, _, running := consumer.claimLane(now)
	if !running || claimed != nil {
		t.Fatalf("scheduler claimed while membership was paused: worker=%p running=%v", claimed, running)
	}

	consumer.mu.Lock()
	consumer.membershipReady = true
	consumer.membershipGeneration++
	consumer.mu.Unlock()
	claimed, _, running = consumer.claimLane(now)
	if !running || claimed != worker {
		t.Fatalf("scheduler did not recover after membership resumed: worker=%p running=%v", claimed, running)
	}
}

func TestRequiredInflightOwnershipLossDoesNotOverlapReplacement(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, requiredRecordsFrom(0, 1), nil)
	writeRequiredFrontier(t, s, topic, laneID, 0, 1, dataEnd)
	writeRequiredGroup(t, s, GroupTypeBestEffort, 0)
	client := requiredClient(s)
	store := &mutableRequiredMembershipStore{}
	store.setMembers("consumer-inflight")
	membership, err := newMembershipWithStore(store, "membership", "consumer-inflight", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	handler := &blockingConcurrencyRequiredHandler{entered: make(chan struct{}), release: make(chan struct{})}
	consumer := &Consumer{
		client: client, subscription: Subscription{Topic: topic, Group: requiredTestGroup},
		handler: handler, cfg: client.cfg.Consumer, state: consumerStateStarted,
		instanceID: "consumer-inflight", membership: membership, membershipReady: true,
		membershipGeneration: 1, lastBeat: s.now(), ctx: context.Background(),
		workers: make(map[string]*laneWorker), schedulerWake: make(chan struct{}, 1),
	}
	workerCtx, workerCancel := context.WithCancel(consumer.ctx)
	worker := &laneWorker{
		consumer: consumer, laneID: laneID, ctx: workerCtx, cancel: workerCancel, initialized: true,
		groupManifest: groupManifest{GroupType: string(GroupTypeBestEffort)},
		checkpoint:    &checkpointTracker{consumer: consumer, laneID: laneID, lastPersisted: s.now()},
		reader:        newLaneReader(s, topic, laneID), backoff: newBackoff(), inFlight: true,
	}
	consumer.workers[laneID] = worker
	consumer.laneOrder = []string{laneID}

	stepDone := make(chan time.Duration, 1)
	go func() { stepDone <- worker.step() }()
	<-handler.entered
	store.setMembers("another-consumer")
	if err := consumer.reconcile(consumer.ctx); err != nil {
		t.Fatal(err)
	}
	store.setMembers(consumer.instanceID)
	if err := consumer.reconcile(consumer.ctx); err != nil {
		t.Fatal(err)
	}
	consumer.mu.Lock()
	current := consumer.workers[laneID]
	draining := worker.draining
	inFlight := worker.inFlight
	consumer.mu.Unlock()
	if current != worker || !draining || !inFlight {
		t.Fatalf("ownership loss replaced in-flight worker: current=%p old=%p draining=%v inFlight=%v", current, worker, draining, inFlight)
	}

	close(handler.release)
	<-stepDone
	if err := worker.flushForShutdown(); err != nil {
		t.Fatal(err)
	}
	consumer.releaseLane(worker, -1)
	if handler.maxActive.Load() != 1 {
		t.Fatalf("same-lane maximum handler concurrency = %d, want 1", handler.maxActive.Load())
	}
	if err := consumer.reconcile(consumer.ctx); err != nil {
		t.Fatal(err)
	}
	consumer.mu.Lock()
	replacement := consumer.workers[laneID]
	consumer.mu.Unlock()
	if replacement == nil || replacement == worker {
		t.Fatalf("replacement was not created after in-flight worker drained: %p", replacement)
	}
}

func TestRequiredOwnershipLossFlushesOncePerSchedulerCycle(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, requiredRecordsFrom(0, 2), nil)
	writeRequiredFrontier(t, s, topic, laneID, 0, 2, dataEnd)
	writeRequiredGroup(t, s, GroupTypeBestEffort, 0)

	var flushAttempts atomic.Int64
	ops := newStorageOps()
	ops.fault = func(point string) error {
		if point == faultCheckpointRename {
			flushAttempts.Add(1)
			return errors.New("injected terminal checkpoint failure")
		}
		return nil
	}
	s.ops = ops

	client := requiredClient(s)
	store := &requiredMembershipStore{members: []string{"consumer-other"}}
	membership, err := newMembershipWithStore(store, "membership", "consumer-ownership-loss", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	consumerCtx, cancel := context.WithCancel(context.Background())
	consumer := &Consumer{
		client: client, subscription: Subscription{Topic: topic, Group: requiredTestGroup},
		cfg: client.cfg.Consumer, state: consumerStateStarted, instanceID: "consumer-ownership-loss",
		ctx: consumerCtx, membership: membership, membershipReady: true, membershipGeneration: 1,
		lastBeat: s.now(), workers: make(map[string]*laneWorker), schedulerWake: make(chan struct{}, 1),
	}
	workerCtx, workerCancel := context.WithCancel(consumerCtx)
	worker := &laneWorker{
		consumer: consumer, laneID: laneID, ctx: workerCtx, cancel: workerCancel, initialized: true,
		groupManifest: groupManifest{GroupType: string(GroupTypeBestEffort)},
		checkpoint: &checkpointTracker{
			consumer: consumer, laneID: laneID, next: 1, lastPersisted: s.now(),
		},
		reader: newLaneReader(s, topic, laneID), backoff: newBackoff(),
	}
	consumer.workers[laneID] = worker
	consumer.laneOrder = []string{laneID}

	consumer.runtimeWG.Add(1)
	go consumer.schedulerLoop()
	defer func() {
		consumer.mu.Lock()
		consumer.state = consumerStateStopping
		consumer.mu.Unlock()
		cancel()
		consumer.notifyScheduler()
		consumer.runtimeWG.Wait()
	}()

	deadline := time.Now().Add(time.Second)
	for {
		consumer.mu.Lock()
		drained := worker.draining && !worker.inFlight
		consumer.mu.Unlock()
		if drained {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("scheduler did not finish ownership-loss cycle")
		}
		time.Sleep(time.Millisecond)
	}
	if got := flushAttempts.Load(); got != 1 {
		t.Fatalf("terminal checkpoint flush attempts = %d, want 1", got)
	}
}

type blockingConcurrencyRequiredHandler struct {
	active    atomic.Int64
	maxActive atomic.Int64
	entered   chan struct{}
	release   chan struct{}
	once      sync.Once
}

func (h *blockingConcurrencyRequiredHandler) Handle(context.Context, Batch) error {
	current := h.active.Add(1)
	updateRequiredAtomicMax(&h.maxActive, current)
	h.once.Do(func() { close(h.entered) })
	<-h.release
	h.active.Add(-1)
	return nil
}

func TestRequiredStopWaitsForDrainingFlushAndKeepsMembershipOnFailure(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, requiredRecordsFrom(0, 1), nil)
	writeRequiredFrontier(t, s, topic, laneID, 0, 1, dataEnd)
	writeRequiredGroup(t, s, GroupTypeBestEffort, 0)
	client := requiredClient(s)
	store := &requiredMembershipStore{}
	membership, err := newMembershipWithStore(store, "membership", "consumer-stop-drain", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce sync.Once
	var fail atomic.Bool
	ops := newStorageOps()
	ops.fault = func(point string) error {
		if point == faultCheckpointRename && fail.Load() {
			enterOnce.Do(func() { close(entered) })
			<-release
			return errors.New("injected draining checkpoint failure")
		}
		return nil
	}
	s.ops = ops
	consumerCtx, cancel := context.WithCancel(context.Background())
	consumer := &Consumer{
		client: client, subscription: Subscription{Topic: topic, Group: requiredTestGroup},
		cfg: client.cfg.Consumer, state: consumerStateStarted, instanceID: "consumer-stop-drain",
		ctx: consumerCtx, cancel: cancel, membership: membership, membershipReady: true,
		membershipGeneration: 1, lastBeat: s.now(), workers: make(map[string]*laneWorker),
		schedulerWake: make(chan struct{}, 1),
	}
	worker := &laneWorker{consumer: consumer, laneID: laneID, cancel: func() {}}
	worker.checkpoint = &checkpointTracker{consumer: consumer, laneID: laneID, next: 1, lastPersisted: s.now()}
	consumer.workers[laneID] = worker

	fail.Store(true)
	consumer.runtimeWG.Add(1)
	go func() {
		defer consumer.runtimeWG.Done()
		consumer.pauseWorkers()
	}()
	<-entered
	stopDone := make(chan error, 1)
	go func() { stopDone <- consumer.Stop(context.Background()) }()
	select {
	case err := <-stopDone:
		t.Fatalf("Stop completed while draining flush was blocked: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if err := <-stopDone; err == nil {
		t.Fatal("Stop reported success after draining checkpoint failure")
	}
	if store.removed != 0 {
		t.Fatalf("membership remove calls = %d, want 0", store.removed)
	}
	cancel()
}

type fairnessRequiredHandler struct {
	mu                    sync.Mutex
	calls                 map[string]int
	wanted                int
	hotLane               string
	hotCallsAtObservation int
	observed              chan struct{}
	once                  sync.Once
}

func (h *fairnessRequiredHandler) Handle(_ context.Context, batch Batch) error {
	h.mu.Lock()
	h.calls[batch.LaneID]++
	observedLanes := 0
	for _, calls := range h.calls {
		if calls > 0 {
			observedLanes++
		}
	}
	if observedLanes == h.wanted {
		h.hotCallsAtObservation = h.calls[h.hotLane]
		h.once.Do(func() { close(h.observed) })
	}
	h.mu.Unlock()
	return nil
}

func (h *fairnessRequiredHandler) snapshot() (map[string]int, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	calls := make(map[string]int, len(h.calls))
	for laneID, count := range h.calls {
		calls[laneID] = count
	}
	return calls, h.hotCallsAtObservation
}

func TestRequiredSchedulerFairnessWithHotLane(t *testing.T) {
	laneIDs := []string{
		requiredTestLane,
		"01993e11-8d3c-7c8f-a7ce-2ab21df8d411",
		"01993e11-8d3c-7c8f-a7ce-2ab21df8d412",
		"01993e11-8d3c-7c8f-a7ce-2ab21df8d413",
		"01993e11-8d3c-7c8f-a7ce-2ab21df8d414",
	}
	s, topic, _ := newRequiredTestStorage(t)
	for _, laneID := range laneIDs[1:] {
		writeRequiredLaneMetadata(t, s, topic, laneID)
	}
	checkpoints := make(map[string]uint64, len(laneIDs))
	for index, laneID := range laneIDs {
		count := 1
		if index == 0 {
			count = 128
		}
		dataEnd := writeRequiredSealedSegment(t, s, topic, laneID, 0, requiredRecordsFrom(0, count))
		writeRequiredFrontier(t, s, topic, laneID, 0, uint64(count), dataEnd)
		checkpoints[laneID] = 0
	}
	writeRequiredGroup(t, s, GroupTypeBestEffort, 0)
	if err := s.writeCheckpointBase(context.Background(), topic, requiredTestGroup, checkpoints); err != nil {
		t.Fatal(err)
	}
	client := requiredClient(s)
	client.cfg.Consumer.WorkerConcurrency = 2
	client.cfg.Consumer.BatchMaxMessages = 1
	client.cfg.Consumer.BatchMaxBytes = 1024
	client.cfg.Consumer.BatchMaxWait = time.Millisecond
	client.cfg.Consumer.CheckpointMaxRecords = 1
	client.cfg.Consumer.CheckpointMaxDelay = time.Hour
	client.cfg.Consumer.HeartbeatInterval = time.Hour
	client.cfg.Consumer.ReconcileInterval = time.Hour
	client.cfg.Consumer.HandlerTimeout = time.Second
	store := &mutableRequiredMembershipStore{}
	handler := &fairnessRequiredHandler{
		calls: make(map[string]int), wanted: len(laneIDs), hotLane: laneIDs[0], observed: make(chan struct{}),
	}
	factory := func(_, _, instanceID string, timeout time.Duration) (*membership, error) {
		store.setMembers(instanceID)
		return newMembershipWithStore(store, "membership", instanceID, timeout)
	}
	consumer, err := newConsumerWithMembershipFactory(client, Subscription{Topic: topic, Group: requiredTestGroup}, handler, factory)
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handler.observed:
	case <-time.After(5 * time.Second):
		_ = consumer.Stop(context.Background())
		t.Fatal("scheduler did not give every owned lane a handler opportunity")
	}
	if err := consumer.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	calls, hotCallsAtObservation := handler.snapshot()
	for _, laneID := range laneIDs {
		if calls[laneID] == 0 {
			t.Fatalf("lane %s never received a handler opportunity: %v", laneID, calls)
		}
	}
	if hotCallsAtObservation >= 128 {
		t.Fatalf("hot lane consumed all records before other lanes progressed: %d calls", hotCallsAtObservation)
	}
}

func TestRequiredHeartbeatRecoveryCreatesWorkerWithNewGeneration(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	writeRequiredGroup(t, s, GroupTypeBestEffort, 0)
	client := requiredClient(s)
	store := &mutableRequiredMembershipStore{
		failed: make(chan struct{}), succeeded: make(chan struct{}),
	}
	store.setMembers("consumer-heartbeat")
	store.fail.Store(true)
	membership, err := newMembershipWithStore(store, "membership", "consumer-heartbeat", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	consumerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer := &Consumer{
		client: client, subscription: Subscription{Topic: topic, Group: requiredTestGroup},
		cfg: client.cfg.Consumer, state: consumerStateStarted, instanceID: "consumer-heartbeat",
		ctx: consumerCtx, membership: membership, membershipReady: true, membershipGeneration: 1,
		lastBeat: time.Unix(0, 0).UTC(), workers: make(map[string]*laneWorker),
		schedulerWake: make(chan struct{}, 1),
	}
	consumer.cfg.HeartbeatInterval = time.Millisecond
	consumer.cfg.MembershipTimeout = time.Second
	consumer.runtimeWG.Add(1)
	heartbeatDone := make(chan struct{})
	go func() {
		consumer.heartbeatLoop()
		close(heartbeatDone)
	}()
	select {
	case <-store.failed:
	case <-time.After(time.Second):
		t.Fatal("heartbeat failure was not observed")
	}
	ready, generation := requiredMembershipStateEventually(t, consumer, false, 2)
	if ready || generation != 2 {
		t.Fatalf("membership timeout state = ready:%v generation:%d, want false:2", ready, generation)
	}

	store.fail.Store(false)
	select {
	case <-store.succeeded:
	case <-time.After(time.Second):
		t.Fatal("heartbeat recovery was not observed")
	}
	ready, generation = requiredMembershipStateEventually(t, consumer, true, 3)
	if !ready || generation != 3 {
		t.Fatalf("membership recovery state = ready:%v generation:%d, want true:3", ready, generation)
	}
	if err := consumer.reconcile(consumerCtx); err != nil {
		t.Fatal(err)
	}
	consumer.mu.Lock()
	worker := consumer.workers[laneID]
	consumer.mu.Unlock()
	if worker == nil {
		t.Fatal("reconcile did not create a worker after membership recovery")
	}
	cancel()
	<-heartbeatDone
	consumer.runtimeWG.Wait()
}

func requiredMembershipStateEventually(t *testing.T, consumer *Consumer, wantReady bool, wantGeneration uint64) (bool, uint64) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		consumer.mu.Lock()
		ready := consumer.membershipReady
		generation := consumer.membershipGeneration
		consumer.mu.Unlock()
		if ready == wantReady && generation == wantGeneration {
			return ready, generation
		}
		select {
		case <-deadline:
			t.Fatalf("membership state did not reach ready:%v generation:%d; got ready:%v generation:%d", wantReady, wantGeneration, ready, generation)
		case <-time.After(time.Millisecond):
		}
	}
}

func TestRequiredForceRetireRecoversEmptyMarkerBeforeCleanup(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, nil, []byte{1, 2, 3})
	writeRequiredFrontier(t, s, topic, laneID, 0, 0, dataEnd)
	evidenceHash := bytesHash([]byte("node-fenced"))
	baseOps := newStorageOps()
	ops := newStorageOps()
	failCleanup := true
	ops.remove = func(path string) error {
		if failCleanup && filepath.Ext(path) == ".open" {
			return errors.New("injected empty segment cleanup failure")
		}
		return baseOps.remove(path)
	}
	s.ops = ops

	err := forceRetireActiveLaneWithRequest(context.Background(), s, topic, laneID, "retire-empty", evidenceHash)
	if err == nil {
		t.Fatal("force retire unexpectedly completed through cleanup fault")
	}
	marker, retired, markerErr := s.retired(topic, laneID)
	if markerErr != nil || !retired {
		t.Fatalf("retired marker was not durable before cleanup: retired=%v err=%v", retired, markerErr)
	}
	if marker.RetireRequestID != "retire-empty" || marker.FencingEvidenceHash != evidenceHash {
		t.Fatalf("retired request identity = %+v", marker)
	}
	openPath := filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(0, true))
	if stat, statErr := os.Stat(openPath); statErr != nil || stat.Size() != dataEnd {
		t.Fatalf("header-only open was not retained: stat=%v err=%v", stat, statErr)
	}
	if _, frontierErr := s.durableEnd(topic, laneID); frontierErr != nil {
		t.Fatalf("durable frontier disappeared before cleanup: %v", frontierErr)
	}

	failCleanup = false
	if err := forceRetireActiveLaneWithRequest(context.Background(), s, topic, laneID, "retire-empty", evidenceHash); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(openPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty open remains after retry, err=%v", err)
	}
	if _, err := s.durableEnd(topic, laneID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("durable frontier remains after retry, err=%v", err)
	}

	maintainer := &Maintainer{client: requiredClient(s)}
	request := ForceRetireLaneRequest{
		Topic: topic, LaneID: laneID, FencingEvidence: "node-fenced",
		Operator: "operator", Reason: "replacement", RequestID: "retire-empty",
	}
	if err := maintainer.forceRetireLane(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	request.FencingEvidence = "different-fence"
	if err := maintainer.forceRetireLane(context.Background(), request); err == nil {
		t.Fatal("same request_id accepted different fencing evidence")
	}
}

func TestRequiredForceRetireEmptyRecoveryAcrossInterruptions(t *testing.T) {
	for _, interruption := range []string{"marker publication", "segments directory sync", "open remove", "durable-end remove"} {
		t.Run(interruption, func(t *testing.T) {
			s, topic, laneID := newRequiredTestStorage(t)
			dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, nil, nil)
			writeRequiredFrontier(t, s, topic, laneID, 0, 0, dataEnd)
			openPath := filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(0, true))
			retiredPath := filepath.Join(s.laneDir(topic, laneID), retiredMarkerFile)
			durableEndPath := filepath.Join(s.laneDir(topic, laneID), durableEndFile)
			fail := true
			ops := newStorageOps()
			switch interruption {
			case "marker publication":
				baseOpenFile := ops.openFile
				ops.openFile = func(path string, flag int, perm os.FileMode) (fileHandle, error) {
					if fail && path == retiredPath {
						return nil, errors.New("injected retired marker interruption")
					}
					return baseOpenFile(path, flag, perm)
				}
			case "segments directory sync":
				baseSyncDirectory := ops.syncDir
				ops.syncDir = func(path string) error {
					if fail && path == s.segmentsDir(topic, laneID) {
						return errors.New("injected segments directory interruption")
					}
					return baseSyncDirectory(path)
				}
			case "open remove":
				baseRemove := ops.remove
				ops.remove = func(path string) error {
					if fail && path == openPath {
						return errors.New("injected open removal interruption")
					}
					return baseRemove(path)
				}
			case "durable-end remove":
				baseRemove := ops.remove
				ops.remove = func(path string) error {
					if fail && path == durableEndPath {
						return errors.New("injected durable-end removal interruption")
					}
					return baseRemove(path)
				}
			}
			s.ops = ops
			maintainer := &Maintainer{client: requiredClient(s)}
			request := ForceRetireLaneRequest{
				Topic: topic, LaneID: laneID, FencingEvidence: "node-fenced",
				Operator: "operator", Reason: "restart", RequestID: "retire-interrupted",
			}
			if err := maintainer.forceRetireLane(context.Background(), request); err == nil {
				t.Fatal("interrupted empty force retire unexpectedly succeeded")
			}
			fail = false
			if err := maintainer.forceRetireLane(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			if _, retired, err := s.retired(topic, laneID); err != nil || !retired {
				t.Fatalf("retired marker = retired:%v err:%v", retired, err)
			}
			if _, err := s.durableEnd(topic, laneID); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("durable frontier remains after retry, err=%v", err)
			}
		})
	}
}

func TestRequiredForceRetireRejectsUnverifiableTopology(t *testing.T) {
	cases := []struct {
		name  string
		setup func(*testing.T, *storage, string, string)
	}{
		{
			name: "wrong topic identity",
			setup: func(t *testing.T, s *storage, topic, laneID string) {
				dataEnd := writeRequiredHeaderOnly(t, s, topic, laneID, 0, "other-topic", laneID, 0)
				writeRequiredFrontier(t, s, topic, laneID, 0, 0, dataEnd)
			},
		},
		{
			name: "wrong lane identity",
			setup: func(t *testing.T, s *storage, topic, laneID string) {
				dataEnd := writeRequiredHeaderOnly(t, s, topic, laneID, 0, topic, "other-lane", 0)
				writeRequiredFrontier(t, s, topic, laneID, 0, 0, dataEnd)
			},
		},
		{
			name: "wrong base identity",
			setup: func(t *testing.T, s *storage, topic, laneID string) {
				dataEnd := writeRequiredHeaderOnly(t, s, topic, laneID, 0, topic, laneID, 1)
				writeRequiredFrontier(t, s, topic, laneID, 0, 0, dataEnd)
			},
		},
		{
			name: "sequence gap",
			setup: func(t *testing.T, s *storage, topic, laneID string) {
				writeRequiredSealedSegment(t, s, topic, laneID, 0, requiredRecordsFrom(0, 1))
				dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 2, requiredRecordsFrom(2, 1), nil)
				writeRequiredFrontier(t, s, topic, laneID, 2, 3, dataEnd)
			},
		},
		{
			name: "append time regression",
			setup: func(t *testing.T, s *storage, topic, laneID string) {
				records := requiredRecordsFrom(0, 2)
				records[0].AppendTime = time.Unix(2000, 0).UTC()
				records[1].AppendTime = time.Unix(1999, 0).UTC()
				dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, records, nil)
				writeRequiredFrontier(t, s, topic, laneID, 0, 2, dataEnd)
			},
		},
		{
			name: "footer checksum",
			setup: func(t *testing.T, s *storage, topic, laneID string) {
				dataEnd := writeRequiredSealedSegment(t, s, topic, laneID, 0, requiredRecordsFrom(0, 2))
				writeRequiredFrontier(t, s, topic, laneID, 0, 2, dataEnd)
				path := filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(0, false))
				file, err := os.OpenFile(path, os.O_RDWR, 0o640)
				if err != nil {
					t.Fatal(err)
				}
				stat, err := file.Stat()
				if err != nil {
					_ = file.Close()
					t.Fatal(err)
				}
				if _, err := file.WriteAt([]byte{0xff}, stat.Size()-1); err != nil {
					_ = file.Close()
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "data checksum",
			setup: func(t *testing.T, s *storage, topic, laneID string) {
				dataEnd := writeRequiredSealedSegment(t, s, topic, laneID, 0, requiredRecordsFrom(0, 2))
				writeRequiredFrontier(t, s, topic, laneID, 0, 2, dataEnd)
				path := filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(0, false))
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				header, err := decodeSegmentHeader(data)
				if err != nil {
					t.Fatal(err)
				}
				file, err := os.OpenFile(path, os.O_RDWR, 0o640)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := file.WriteAt([]byte{0xff}, int64(header.Length+8)); err != nil {
					_ = file.Close()
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			s, topic, laneID := newRequiredTestStorage(t)
			testCase.setup(t, s, topic, laneID)
			if err := forceRetireActiveLaneWithRequest(context.Background(), s, topic, laneID, "invalid-topology", bytesHash([]byte("fenced"))); err == nil {
				t.Fatal("force retire accepted unverifiable lane state")
			}
			if _, retired, err := s.retired(topic, laneID); err != nil || retired {
				t.Fatalf("invalid lane produced retired marker: retired=%v err=%v", retired, err)
			}
		})
	}
}

func TestRequiredRetentionFooterRejectsWrongSegmentIdentity(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	records := requiredRecordsFrom(0, 2)
	writeRequiredSealedSegment(t, s, topic, laneID, 0, records)
	path := filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(0, false))
	if _, err := readSealedFooter(s, topic, laneID, segmentFile{base: 1, path: path}); err == nil {
		t.Fatal("retention accepted a filename/header base mismatch")
	}
	if _, err := readSealedFooter(s, topic, laneID, segmentFile{base: 0, path: path, open: true}); err == nil {
		t.Fatal("retention accepted an active segment")
	}
}

func TestRequiredCursorHandlesGrowthRotationAndFloorAdvance(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	firstRecords := requiredRecordsFrom(0, 1)
	firstDataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, firstRecords, nil)
	writeRequiredFrontier(t, s, topic, laneID, 0, 1, firstDataEnd)
	reader := newLaneReader(s, topic, laneID)

	result, err := s.readLaneBatchWithCursor(reader, 0, 1, 1024)
	if err != nil || len(result.Batch.Messages) != 1 || result.Batch.Messages[0].Sequence != 0 {
		t.Fatalf("initial read = %+v err=%v", result, err)
	}
	second := requiredRecordsFrom(1, 1)[0]
	encoded, err := encodeRecord(second, 1024)
	if err != nil {
		t.Fatal(err)
	}
	openPath := filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(0, true))
	file, err := os.OpenFile(openPath, os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(encoded); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	secondDataEnd := firstDataEnd + int64(len(encoded))
	writeRequiredFrontier(t, s, topic, laneID, 0, 2, secondDataEnd)
	result, err = s.readLaneBatchWithCursor(reader, 1, 1, 1024)
	if err != nil || len(result.Batch.Messages) != 1 || result.Batch.Messages[0].Sequence != 1 {
		t.Fatalf("active growth read = %+v err=%v", result, err)
	}

	data, err := os.ReadFile(openPath)
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, encodeSegmentFooter(segmentFooter{
		FileLength:     uint64(len(data) + segmentFooterBytes),
		NextSequence:   2,
		RecordCount:    2,
		MinAppendNanos: firstRecords[0].AppendTime.UnixNano(),
		MaxAppendNanos: second.AppendTime.UnixNano(),
		DataChecksum:   crc32c(data),
	})...)
	if err := os.WriteFile(openPath, data, 0o640); err != nil {
		t.Fatal(err)
	}
	sealedPath := filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(0, false))
	if err := os.Rename(openPath, sealedPath); err != nil {
		t.Fatal(err)
	}
	thirdRecords := requiredRecordsFrom(2, 1)
	thirdDataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 2, thirdRecords, nil)
	writeRequiredFrontier(t, s, topic, laneID, 2, 3, thirdDataEnd)
	result, err = s.readLaneBatchWithCursor(reader, 2, 1, 1024)
	if err != nil || len(result.Batch.Messages) != 1 || result.Batch.Messages[0].Sequence != 2 {
		t.Fatalf("rotation read = %+v err=%v", result, err)
	}

	if err := writeMetadataAtomic(filepath.Join(s.laneDir(topic, laneID), retentionIndexFile), metadataKindRetentionIndex, retentionIndex{
		FormatVersion: formatVersion, Topic: topic, LaneID: laneID, EarliestRetainedSeq: 2,
	}, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(sealedPath); err != nil {
		t.Fatal(err)
	}
	result, err = s.readLaneBatchWithCursor(reader, 0, 1, 1024)
	if err != nil || len(result.Batch.Messages) != 1 || result.Batch.Messages[0].Sequence != 2 || result.RetentionFloor != 2 {
		t.Fatalf("floor-clamped read = %+v err=%v", result, err)
	}
}

func TestRequiredCursorInvalidatesAcrossFloorAndGCNamespaceChange(t *testing.T) {
	for _, testCase := range []struct {
		name string
		move func(*testing.T, *storage, string, string, string)
	}{
		{
			name: "staging rename",
			move: func(t *testing.T, s *storage, topic, laneID, sealedPath string) {
				staging := filepath.Join(s.topicDir(topic), gcStagingDirectory, laneID)
				if err := os.MkdirAll(staging, 0o750); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(sealedPath, filepath.Join(staging, filepath.Base(sealedPath))); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "unlink",
			move: func(t *testing.T, _ *storage, _, _, sealedPath string) {
				if err := os.Remove(sealedPath); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			s, topic, laneID := newRequiredTestStorage(t)
			writeRequiredSealedSegment(t, s, topic, laneID, 0, requiredRecordsFrom(0, 2))
			activeDataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 2, requiredRecordsFrom(2, 1), nil)
			writeRequiredFrontier(t, s, topic, laneID, 2, 3, activeDataEnd)
			sealedPath := filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(0, false))
			reader := newLaneReader(s, topic, laneID)

			first, err := s.readLaneBatchWithCursor(reader, 0, 1, 1024)
			if err != nil || len(first.Batch.Messages) != 1 || first.Batch.Messages[0].Sequence != 0 {
				t.Fatalf("initial read = %+v err=%v", first, err)
			}
			if err := writeMetadataAtomic(filepath.Join(s.laneDir(topic, laneID), retentionIndexFile), metadataKindRetentionIndex, retentionIndex{
				FormatVersion: formatVersion, Topic: topic, LaneID: laneID, EarliestRetainedSeq: 2,
			}, 0o640); err != nil {
				t.Fatal(err)
			}
			testCase.move(t, s, topic, laneID, sealedPath)

			result, err := s.readLaneBatchWithCursor(reader, 1, 10, 1024)
			if err != nil {
				t.Fatal(err)
			}
			if result.RetentionFloor != 2 || len(result.Batch.Messages) != 1 || result.Batch.Messages[0].Sequence != 2 || result.NextSequence != 3 {
				t.Fatalf("same-reader GC recovery = %+v", result)
			}
		})
	}
}

func TestRequiredCursorCrossesSealedBoundaryWithoutDuplicate(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		maxMessages int
		maxBytes    int64
	}{
		{name: "message limit", maxMessages: 2, maxBytes: 1024},
		{name: "byte limit", maxMessages: 10, maxBytes: 2},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			s, topic, laneID := newRequiredTestStorage(t)
			writeRequiredSealedSegment(t, s, topic, laneID, 0, requiredRecordsFrom(0, 3))
			dataEnd := writeRequiredSealedSegment(t, s, topic, laneID, 3, requiredRecordsFrom(3, 3))
			writeRequiredFrontier(t, s, topic, laneID, 3, 6, dataEnd)

			reader := newLaneReader(s, topic, laneID)
			from := uint64(0)
			seen := make(map[uint64]struct{}, 6)
			for from < 6 {
				result, err := s.readLaneBatchWithCursor(reader, from, testCase.maxMessages, testCase.maxBytes)
				if err != nil {
					t.Fatal(err)
				}
				if len(result.Batch.Messages) == 0 || result.NextSequence <= from {
					t.Fatalf("cursor stopped at sequence %d: %+v", from, result)
				}
				expected := from
				for _, message := range result.Batch.Messages {
					if _, exists := seen[message.Sequence]; exists {
						t.Fatalf("sequence %d was returned more than once", message.Sequence)
					}
					seen[message.Sequence] = struct{}{}
					if message.Sequence != expected {
						t.Fatalf("batch returned sequence %d, want %d; batch=%+v", message.Sequence, expected, result.Batch.Messages)
					}
					expected++
				}
				if result.NextSequence != expected {
					t.Fatalf("next sequence = %d, want %d", result.NextSequence, expected)
				}
				from = result.NextSequence
			}
			if from != 6 || len(seen) != 6 {
				t.Fatalf("cursor coverage = next:%d seen:%d, want next:6 seen:6", from, len(seen))
			}
		})
	}
}

func TestRequiredCursorRejectsSealedDataCorruption(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	records := requiredRecordsFrom(0, 2)
	dataEnd := writeRequiredSealedSegment(t, s, topic, laneID, 0, records)
	writeRequiredFrontier(t, s, topic, laneID, 0, 2, dataEnd)
	path := filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(0, false))
	file, err := os.OpenFile(path, os.O_RDWR, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	header, err := encodeSegmentHeader(topic, laneID, 0, time.Unix(1000, 0).UTC())
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, int64(len(header)+8)); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.readLaneBatchWithCursor(newLaneReader(s, topic, laneID), 0, 1, 1024); err == nil {
		t.Fatal("cursor accepted corrupted sealed data")
	}
}

type countingMembershipFactory struct {
	calls atomic.Int64
	store membershipStore
}

func (f *countingMembershipFactory) create(_, _, instanceID string, timeout time.Duration) (*membership, error) {
	f.calls.Add(1)
	return newMembershipWithStore(f.store, "membership", instanceID, timeout)
}

func TestRequiredMaintainerUsesInjectedMembershipFactory(t *testing.T) {
	s, _, _ := newRequiredTestStorage(t)
	writeRequiredGroup(t, s, GroupTypeBestEffort, 0)
	client := requiredClient(s)
	client.cfg.Consumer.MembershipTimeout = time.Nanosecond
	store := &requiredMembershipStore{}
	factory := &countingMembershipFactory{store: store}
	maintainer, err := newMaintainerWithMembershipFactory(client, MaintainerConfig{Interval: time.Hour}, factory.create)
	if err != nil {
		t.Fatal(err)
	}
	if err := compactAllGroups(context.Background(), maintainer); err != nil {
		t.Fatal(err)
	}
	if err := maintainer.deleteGroup(context.Background(), DeleteGroupRequest{
		Topic: requiredTestTopic, Group: requiredTestGroup,
		Operator: "operator", Reason: "retired consumer", RequestID: "delete-group",
	}); err != nil {
		t.Fatal(err)
	}
	if got := factory.calls.Load(); got != 2 {
		t.Fatalf("membership factory calls = %d, want 2", got)
	}
}

func TestRequiredPressureRetentionBlocksWholeLaneBehindProtectedGroup(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	sealedRecords := requiredRecordsFrom(0, 2)
	writeRequiredSealedSegment(t, s, topic, laneID, 0, sealedRecords)
	activeDataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 2, nil, nil)
	writeRequiredFrontier(t, s, topic, laneID, 2, 2, activeDataEnd)
	writeRequiredGroup(t, s, GroupTypeProtected, 0)
	baseOps := newStorageOps()
	ops := newStorageOps()
	ops.statfs = func(string) (uint64, error) { return 0, nil }
	ops.remove = baseOps.remove
	s.ops = ops
	client := requiredClient(s)
	client.cfg.ProducerStopFreeBytes = 1
	maintainer := &Maintainer{client: client}

	err := runPressureRetention(context.Background(), maintainer, []string{topic})
	if err == nil {
		t.Fatal("pressure retention reported success despite protected barrier")
	}
	index, indexErr := s.retention(topic, laneID)
	if indexErr != nil {
		t.Fatal(indexErr)
	}
	if index.EarliestRetainedSeq != 0 {
		t.Fatalf("retention floor = %d, want 0", index.EarliestRetainedSeq)
	}
	if _, statErr := os.Stat(filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(0, false))); statErr != nil {
		t.Fatalf("protected segment was removed: %v", statErr)
	}
}

func TestRequiredPressureRetentionStopsAfterWatermarkRecovery(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	writeRequiredSealedSegment(t, s, topic, laneID, 0, requiredRecordsFrom(0, 2))
	writeRequiredSealedSegment(t, s, topic, laneID, 2, requiredRecordsFrom(2, 2))
	activeEnd := writeRequiredOpenSegment(t, s, topic, laneID, 4, nil, nil)
	writeRequiredFrontier(t, s, topic, laneID, 4, 4, activeEnd)
	if err := writeMetadataAtomic(filepath.Join(s.topicDir(topic), topicControlFile), metadataKindTopicControl, topicControl{
		FormatVersion: formatVersion,
	}, 0o640); err != nil {
		t.Fatal(err)
	}
	firstPath := filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(0, false))
	secondPath := filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(2, false))
	baseOps := newStorageOps()
	var statfsCalls atomic.Int64
	ops := newStorageOps()
	ops.statfs = func(string) (uint64, error) {
		statfsCalls.Add(1)
		if _, err := os.Stat(firstPath); errors.Is(err, os.ErrNotExist) {
			return 1, nil
		}
		return 0, nil
	}
	ops.remove = baseOps.remove
	s.ops = ops
	client := requiredClient(s)
	client.cfg.ProducerStopFreeBytes = 1
	maintainer := &Maintainer{client: client}
	if err := runPressureRetention(context.Background(), maintainer, []string{topic}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(firstPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oldest segment remains, err=%v", err)
	}
	if _, err := os.Stat(secondPath); err != nil {
		t.Fatalf("pressure GC deleted beyond recovered watermark: %v", err)
	}
	index, err := s.retention(topic, laneID)
	if err != nil || index.EarliestRetainedSeq != 2 {
		t.Fatalf("retention floor = %+v err=%v", index, err)
	}
	if calls := statfsCalls.Load(); calls < 3 {
		t.Fatalf("statfs calls = %d, expected post-deletion recheck", calls)
	}
}

func TestRequiredRetentionRevalidatesProtectedStateBeforePublishingFloor(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(*testing.T, *storage, string, string)
	}{
		{
			name: "checkpoint regressed",
			mutate: func(t *testing.T, s *storage, topic, laneID string) {
				if err := s.writeCheckpointBase(context.Background(), topic, requiredTestGroup, map[string]uint64{laneID: 0}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "group is not active",
			mutate: func(t *testing.T, s *storage, topic, _ string) {
				if err := writeMetadataAtomic(filepath.Join(s.groupDir(topic, requiredTestGroup), groupControlFile), metadataKindGroupControl, groupControl{
					FormatVersion: formatVersion, State: string(GroupDeleting),
				}, 0o640); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			s, topic, laneID := newRequiredTestStorage(t)
			writeRequiredSealedSegment(t, s, topic, laneID, 0, requiredRecordsFrom(0, 2))
			activeDataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 2, nil, nil)
			writeRequiredFrontier(t, s, topic, laneID, 2, 2, activeDataEnd)
			writeRequiredTopicControl(t, s, topic, 0)
			writeRequiredGroup(t, s, GroupTypeProtected, 2)
			candidatePath := filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(0, false))
			footer, err := readSealedFooter(s, topic, laneID, segmentFile{base: 0, path: candidatePath})
			if err != nil {
				t.Fatal(err)
			}
			testCase.mutate(t, s, topic, laneID)
			if err := deleteRetentionSegments(context.Background(), &Maintainer{client: requiredClient(s)}, topic, laneID, []pressureCandidate{{topic: topic, laneID: laneID, segment: segmentFile{base: 0, path: candidatePath}, footer: footer}}, "pressure"); err != nil {
				t.Fatal(err)
			}
			index, err := s.retention(topic, laneID)
			if err != nil {
				t.Fatal(err)
			}
			if index.EarliestRetainedSeq != 0 {
				t.Fatalf("retention floor = %d, want 0", index.EarliestRetainedSeq)
			}
			if _, err := os.Stat(filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(0, false))); err != nil {
				t.Fatalf("protected segment was removed after revalidation change: %v", err)
			}
		})
	}
}

func TestRequiredBestEffortBlockedMarkerPreventsRetiredLaneCollection(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, nil, nil)
	writeRequiredFrontier(t, s, topic, laneID, 0, 0, dataEnd)
	if err := forceRetireActiveLane(context.Background(), s, topic, laneID); err != nil {
		t.Fatal(err)
	}
	writeRequiredGroup(t, s, GroupTypeBestEffort, 0)
	writeRequiredBlocked(t, s, 0)
	if err := maybeCollectRetiredLaneIfEmpty(context.Background(), &Maintainer{client: requiredClient(s)}, topic, laneID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.laneDir(topic, laneID)); err != nil {
		t.Fatalf("retired lane with blocked marker was collected: %v", err)
	}
}

func TestRequiredNonActiveProtectedGroupPreventsRetiredLaneCollection(t *testing.T) {
	for _, state := range []GroupState{GroupInitializing, GroupDeleting} {
		t.Run(string(state), func(t *testing.T) {
			s, topic, laneID := newRequiredTestStorage(t)
			dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, nil, nil)
			writeRequiredFrontier(t, s, topic, laneID, 0, 0, dataEnd)
			if err := forceRetireActiveLane(context.Background(), s, topic, laneID); err != nil {
				t.Fatal(err)
			}
			writeRequiredGroup(t, s, GroupTypeProtected, 0)
			if err := writeMetadataAtomic(filepath.Join(s.groupDir(topic, requiredTestGroup), groupControlFile), metadataKindGroupControl, groupControl{
				FormatVersion: formatVersion, State: string(state),
			}, 0o640); err != nil {
				t.Fatal(err)
			}

			if err := maybeCollectRetiredLaneIfEmpty(context.Background(), &Maintainer{client: requiredClient(s)}, topic, laneID); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(s.laneDir(topic, laneID)); err != nil {
				t.Fatalf("retired lane was collected while group was %s: %v", state, err)
			}
			if _, retired, err := s.retired(topic, laneID); err != nil || !retired {
				t.Fatalf("retired marker = retired:%v err:%v", retired, err)
			}
			if _, err := s.retention(topic, laneID); err != nil {
				t.Fatalf("retention index was removed while group was %s: %v", state, err)
			}
		})
	}
}

func TestRequiredPressureRetentionUsesGlobalCandidateOrder(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	otherTopic := "audit"
	otherLane := "01993e11-8d3c-7c8f-a7ce-2ab21df8d415"
	writeRequiredTopicMetadata(t, s, otherTopic)
	writeRequiredTopicControl(t, s, topic, 0)
	writeRequiredTopicControl(t, s, otherTopic, 0)
	writeRequiredLaneMetadata(t, s, otherTopic, otherLane)
	writeRequiredGroup(t, s, GroupTypeBestEffort, 0)

	oldRecords := requiredRecordsFrom(0, 2)
	oldRecords[0].AppendTime = time.Unix(100, 0).UTC()
	oldRecords[1].AppendTime = time.Unix(101, 0).UTC()
	writeRequiredSealedSegment(t, s, topic, laneID, 0, oldRecords)
	oldActiveEnd := writeRequiredOpenSegment(t, s, topic, laneID, 2, nil, nil)
	writeRequiredFrontier(t, s, topic, laneID, 2, 2, oldActiveEnd)

	newRecords := requiredRecordsFrom(0, 2)
	newRecords[0].AppendTime = time.Unix(200, 0).UTC()
	newRecords[1].AppendTime = time.Unix(201, 0).UTC()
	writeRequiredSealedSegment(t, s, otherTopic, otherLane, 0, newRecords)
	newActiveEnd := writeRequiredOpenSegment(t, s, otherTopic, otherLane, 2, nil, nil)
	writeRequiredFrontier(t, s, otherTopic, otherLane, 2, 2, newActiveEnd)

	oldPath := filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(0, false))
	newPath := filepath.Join(s.segmentsDir(otherTopic, otherLane), formatSegmentName(0, false))
	ops := newStorageOps()
	ops.statfs = func(string) (uint64, error) {
		if _, err := os.Stat(oldPath); errors.Is(err, os.ErrNotExist) {
			return 1, nil
		}
		return 0, nil
	}
	s.ops = ops
	client := requiredClient(s)
	client.cfg.ProducerStopFreeBytes = 1
	if err := runPressureRetention(context.Background(), &Maintainer{client: client}, []string{topic, otherTopic}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oldPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("globally oldest segment was not collected, err=%v", err)
	}
	if _, err := os.Stat(newPath); err != nil {
		t.Fatalf("newer segment was collected before pressure recovered: %v", err)
	}
	oldIndex, err := s.retention(topic, laneID)
	if err != nil || oldIndex.EarliestRetainedSeq != 2 {
		t.Fatalf("old lane retention index = %+v err=%v", oldIndex, err)
	}
	newIndex, err := s.retention(otherTopic, otherLane)
	if err != nil || newIndex.EarliestRetainedSeq != 0 {
		t.Fatalf("new lane retention index = %+v err=%v", newIndex, err)
	}
}

func TestRequiredReaderUsesPublishedFloorDuringStagingRecovery(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	writeRequiredTopicControl(t, s, topic, 0)
	writeRequiredSealedSegment(t, s, topic, laneID, 0, requiredRecordsFrom(0, 2))
	activeDataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 2, requiredRecordsFrom(2, 1), nil)
	writeRequiredFrontier(t, s, topic, laneID, 2, 3, activeDataEnd)
	candidatePath := filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(0, false))
	footer, err := readSealedFooter(s, topic, laneID, segmentFile{base: 0, path: candidatePath})
	if err != nil {
		t.Fatal(err)
	}
	ops := newStorageOps()
	fail := true
	ops.fault = func(point string) error {
		if point == faultStagingRename && fail {
			fail = false
			return errors.New("injected staging interruption")
		}
		return nil
	}
	s.ops = ops
	candidate := pressureCandidate{topic: topic, laneID: laneID, segment: segmentFile{base: 0, path: candidatePath}, footer: footer}
	if err := deleteRetentionSegments(context.Background(), &Maintainer{client: requiredClient(s)}, topic, laneID, []pressureCandidate{candidate}, "pressure"); err == nil {
		t.Fatal("staging interruption unexpectedly succeeded")
	}
	restarted := newStorage(s.cfg)
	result, err := restarted.readLaneBatch(topic, laneID, 0, 10, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if result.RetentionFloor != 2 || len(result.Batch.Messages) != 1 || result.Batch.Messages[0].Sequence != 2 {
		t.Fatalf("reader delivered below published floor: result=%+v", result)
	}
	maintainer := &Maintainer{client: requiredClient(restarted)}
	if err := cleanupLogicallyDeletedSegments(context.Background(), maintainer, topic, laneID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(candidatePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging recovery left old segment, err=%v", err)
	}
}

func TestRequiredCheckpointFaultRestartDoesNotAdvancePastWatermark(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, requiredRecordsFrom(0, 1), nil)
	writeRequiredFrontier(t, s, topic, laneID, 0, 1, dataEnd)
	writeRequiredGroup(t, s, GroupTypeBestEffort, 0)
	client := requiredClient(s)
	consumer := &Consumer{
		client: client, subscription: Subscription{Topic: topic, Group: requiredTestGroup},
		instanceID: "consumer-checkpoint-restart", cfg: client.cfg.Consumer,
	}
	tracker := &checkpointTracker{consumer: consumer, laneID: laneID, next: 1, lastPersisted: s.now()}
	fail := true
	ops := newStorageOps()
	ops.fault = func(point string) error {
		if point == faultCheckpointRename && fail {
			return errors.New("injected checkpoint publication failure")
		}
		return nil
	}
	s.ops = ops
	if err := tracker.flush(context.Background()); err == nil {
		t.Fatal("checkpoint fault unexpectedly succeeded")
	}
	restarted := newStorage(s.cfg)
	if checkpoint, exists, err := restarted.readCheckpoint(topic, requiredTestGroup, consumer.instanceID, laneID); err != nil || exists {
		t.Fatalf("failed checkpoint became durable after restart: checkpoint=%+v exists=%v err=%v", checkpoint, exists, err)
	}
	fail = false
	if err := tracker.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	restarted = newStorage(s.cfg)
	checkpoint, exists, err := restarted.readCheckpoint(topic, requiredTestGroup, consumer.instanceID, laneID)
	if err != nil || !exists || checkpoint.NextSequence != 1 {
		t.Fatalf("checkpoint after retry = %+v exists=%v err=%v", checkpoint, exists, err)
	}
}

func TestRequiredPublishFaultsRestartAtDurableFrontier(t *testing.T) {
	points := []string{
		faultRecordAppend,
		faultWalSync,
		faultDurableEndTempSync,
		faultDurableEndRename,
		faultDurableEndParentSync,
	}
	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			s, topic, _ := newRequiredTestStorage(t)
			ops := newStorageOps()
			ops.statfs = func(string) (uint64, error) { return ^uint64(0), nil }
			activeFault := ""
			ops.fault = func(candidate string) error {
				if candidate == activeFault {
					return errors.New("injected " + candidate)
				}
				return nil
			}
			s.ops = ops
			s.mu.Lock()
			s.started = true
			s.mu.Unlock()
			client := requiredClient(s)
			writer, err := newWriterLane(client, s, s.cfg, "producer", topic, 1024, s.now, nil)
			if err != nil {
				t.Fatal(err)
			}
			activeFault = point
			request := &publishRequest{
				ctx:     context.Background(),
				message: Message{Topic: topic, MessageID: []byte("id"), EventType: "event", Payload: []byte("payload")},
				result:  make(chan publishResult, 1),
			}
			writer.flush([]*publishRequest{request})
			publish := <-request.result
			if publish.err == nil || !IsDurabilityUnknown(publish.err) {
				t.Fatalf("publish error = %v, want DurabilityUnknown", publish.err)
			}
			if writer.activeFile != nil {
				if err := writer.activeFile.Close(); err != nil {
					t.Fatal(err)
				}
				writer.activeFile = nil
			}

			restarted := newStorage(s.cfg)
			frontier, err := restarted.durableEnd(topic, writer.laneID)
			if err != nil {
				t.Fatal(err)
			}
			if frontier.NextSequence > 1 {
				t.Fatalf("durable frontier = %d, want 0 or 1", frontier.NextSequence)
			}
			read, err := restarted.readLaneBatch(topic, writer.laneID, 0, 2, 1024)
			if err != nil {
				t.Fatal(err)
			}
			if uint64(len(read.Batch.Messages)) != frontier.NextSequence {
				t.Fatalf("read count = %d, frontier = %d", len(read.Batch.Messages), frontier.NextSequence)
			}
		})
	}
}

func TestRequiredForceRetireFaultsAreRetryable(t *testing.T) {
	for _, point := range []string{faultFooterSync, faultSegmentRename} {
		t.Run(point, func(t *testing.T) {
			s, topic, laneID := newRequiredTestStorage(t)
			records := requiredRecordsFrom(0, 2)
			dataEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, records, []byte{1, 2})
			writeRequiredFrontier(t, s, topic, laneID, 0, 2, dataEnd)
			activeFault := point
			ops := newStorageOps()
			ops.fault = func(candidate string) error {
				if candidate == activeFault {
					return errors.New("injected " + candidate)
				}
				return nil
			}
			s.ops = ops
			if err := forceRetireActiveLaneWithRequest(context.Background(), s, topic, laneID, "retire-fault", bytesHash([]byte("fenced"))); err == nil {
				t.Fatal("faulted force retire unexpectedly succeeded")
			}
			activeFault = ""
			restarted := newStorage(s.cfg)
			if err := forceRetireActiveLaneWithRequest(context.Background(), restarted, topic, laneID, "retire-fault", bytesHash([]byte("fenced"))); err != nil {
				t.Fatal(err)
			}
			if _, retired, err := restarted.retired(topic, laneID); err != nil || !retired {
				t.Fatalf("retired=%v err=%v", retired, err)
			}
			segment := segmentFile{base: 0, path: filepath.Join(restarted.segmentsDir(topic, laneID), formatSegmentName(0, false))}
			if _, err := readSealedFooter(restarted, topic, laneID, segment); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRequiredRetentionFaultsRecoverPublishedFloor(t *testing.T) {
	for _, point := range []string{faultRetentionFloorRename, faultStagingRename} {
		t.Run(point, func(t *testing.T) {
			s, topic, laneID := newRequiredTestStorage(t)
			records := requiredRecordsFrom(0, 2)
			writeRequiredSealedSegment(t, s, topic, laneID, 0, records)
			activeEnd := writeRequiredOpenSegment(t, s, topic, laneID, 2, nil, nil)
			writeRequiredFrontier(t, s, topic, laneID, 2, 2, activeEnd)
			if err := writeMetadataAtomic(filepath.Join(s.topicDir(topic), topicControlFile), metadataKindTopicControl, topicControl{
				FormatVersion: formatVersion,
			}, 0o640); err != nil {
				t.Fatal(err)
			}
			segment := segmentFile{base: 0, path: filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(0, false))}
			footer, err := readSealedFooter(s, topic, laneID, segment)
			if err != nil {
				t.Fatal(err)
			}
			activeFault := point
			ops := newStorageOps()
			ops.fault = func(candidate string) error {
				if candidate == activeFault {
					return errors.New("injected " + candidate)
				}
				return nil
			}
			s.ops = ops
			maintainer := &Maintainer{client: requiredClient(s)}
			candidate := pressureCandidate{topic: topic, laneID: laneID, segment: segment, footer: footer}
			if err := deleteRetentionSegments(context.Background(), maintainer, topic, laneID, []pressureCandidate{candidate}, "pressure"); err == nil {
				t.Fatal("faulted retention unexpectedly succeeded")
			}

			restarted := newStorage(s.cfg)
			restartedMaintainer := &Maintainer{client: requiredClient(restarted)}
			if point == faultRetentionFloorRename {
				if err := deleteRetentionSegments(context.Background(), restartedMaintainer, topic, laneID, []pressureCandidate{candidate}, "pressure"); err != nil {
					t.Fatal(err)
				}
			} else if err := cleanupLogicallyDeletedSegments(context.Background(), restartedMaintainer, topic, laneID); err != nil {
				t.Fatal(err)
			}
			index, err := restarted.retention(topic, laneID)
			if err != nil {
				t.Fatal(err)
			}
			if index.EarliestRetainedSeq != 2 {
				t.Fatalf("retention floor = %d, want 2", index.EarliestRetainedSeq)
			}
			if _, err := os.Stat(segment.path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("logically deleted segment remains, err=%v", err)
			}
		})
	}
}

func TestRequiredGroupControlFaultsResumeInitialization(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(map[int]string{1: "initial", 2: "activate"}[failAt], func(t *testing.T) {
			s, topic, _ := newRequiredTestStorage(t)
			calls := 0
			ops := newStorageOps()
			ops.fault = func(point string) error {
				if point == faultGroupControlRename {
					calls++
					if calls == failAt {
						return errors.New("injected group control failure")
					}
				}
				return nil
			}
			s.ops = ops
			maintainer := &Maintainer{client: requiredClient(s)}
			request := CreateGroupRequest{
				Topic: topic, Group: requiredTestGroup,
				GroupType: GroupTypeBestEffort, InitialPosition: InitialEarliest,
			}
			if err := maintainer.createGroup(context.Background(), request); err == nil {
				t.Fatal("faulted group creation unexpectedly succeeded")
			}

			restarted := newStorage(s.cfg)
			restartedMaintainer := &Maintainer{client: requiredClient(restarted)}
			if err := restartedMaintainer.createGroup(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			control, err := restarted.groupControl(topic, requiredTestGroup)
			if err != nil || control.State != string(GroupActive) {
				t.Fatalf("group control = %+v err=%v", control, err)
			}
		})
	}
}
