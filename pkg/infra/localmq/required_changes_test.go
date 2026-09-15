package localmq

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const (
	requiredTestTopic = "events"
	requiredTestLane  = "01993e11-8d3c-7c8f-a7ce-2ab21df8d410"
)

func formatSegmentName(base uint64, open bool) string {
	extension := ".wal"
	if open {
		extension = ".open"
	}
	return fmt.Sprintf("%d%s", base, extension)
}

func newRequiredTestStorage(t testing.TB) (*storage, string, string) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.RootPath = t.TempDir()
	cfg.StorageID = "01993e11-8d3a-7c8f-a7ce-2ab21df8d410"
	s := newStorage(cfg)
	now := time.Unix(1000, 0).UTC()
	s.now = func() time.Time { return now }
	if err := os.MkdirAll(s.segmentsDir(requiredTestTopic, requiredTestLane), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := writeMetadataAtomic(filepath.Join(s.topicDir(requiredTestTopic), topicManifestFile), metadataKindTopicManifest, topicManifest{
		FormatVersion:  formatVersion,
		Topic:          requiredTestTopic,
		MaxRecordBytes: 1024,
	}, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := writeMetadataAtomic(filepath.Join(s.laneDir(requiredTestTopic, requiredTestLane), laneManifestFile), metadataKindLaneManifest, laneManifest{
		FormatVersion: formatVersion,
		StorageID:     cfg.StorageID,
		Topic:         requiredTestTopic,
		LaneID:        requiredTestLane,
	}, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := writeMetadataAtomic(filepath.Join(s.laneDir(requiredTestTopic, requiredTestLane), retentionIndexFile), metadataKindRetentionIndex, retentionIndex{
		FormatVersion: formatVersion,
		Topic:         requiredTestTopic,
		LaneID:        requiredTestLane,
	}, 0o640); err != nil {
		t.Fatal(err)
	}
	return s, requiredTestTopic, requiredTestLane
}

func requiredTestRecords(payloads ...string) []walRecord {
	records := make([]walRecord, 0, len(payloads))
	for sequence, payload := range payloads {
		records = append(records, walRecord{
			Sequence:   uint64(sequence),
			AppendTime: time.Unix(int64(1000+sequence), 0).UTC(),
			MessageID:  []byte{byte(sequence + 1)},
			EventType:  "event",
			Payload:    []byte(payload),
		})
	}
	return records
}

func writeRequiredOpenSegment(t testing.TB, s *storage, topic, laneID string, base uint64, records []walRecord, tail []byte) int64 {
	t.Helper()
	header, err := encodeSegmentHeader(topic, laneID, base, time.Unix(1000+int64(base), 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	data := append([]byte(nil), header...)
	for _, record := range records {
		encoded, encodeErr := encodeRecord(record, 1024)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		data = append(data, encoded...)
	}
	data = append(data, tail...)
	path := filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(base, true))
	if err := os.WriteFile(path, data, 0o640); err != nil {
		t.Fatal(err)
	}
	return int64(len(data) - len(tail))
}

func writeRequiredSealedSegment(t testing.TB, s *storage, topic, laneID string, base uint64, records []walRecord) int64 {
	t.Helper()
	header, err := encodeSegmentHeader(topic, laneID, base, time.Unix(1000+int64(base), 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	data := append([]byte(nil), header...)
	var minAppend, maxAppend int64
	for index, record := range records {
		encoded, encodeErr := encodeRecord(record, 1024)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		data = append(data, encoded...)
		if index == 0 {
			minAppend = record.AppendTime.UnixNano()
		}
		maxAppend = record.AppendTime.UnixNano()
	}
	dataEnd := int64(len(data))
	data = append(data, encodeSegmentFooter(segmentFooter{
		FileLength:     uint64(len(data) + segmentFooterBytes),
		NextSequence:   base + uint64(len(records)),
		RecordCount:    uint64(len(records)),
		MinAppendNanos: minAppend,
		MaxAppendNanos: maxAppend,
		DataChecksum:   crc32c(data),
	})...)
	path := filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(base, false))
	if err := os.WriteFile(path, data, 0o640); err != nil {
		t.Fatal(err)
	}
	return dataEnd
}

func writeRequiredFrontier(t testing.TB, s *storage, topic, laneID string, base, next uint64, byteEnd int64) {
	t.Helper()
	if err := writeMetadataAtomic(filepath.Join(s.laneDir(topic, laneID), durableEndFile), metadataKindDurableEnd, durableEnd{
		FormatVersion:     formatVersion,
		StorageID:         s.cfg.StorageID,
		Topic:             topic,
		LaneID:            laneID,
		ActiveSegmentBase: base,
		NextSequence:      next,
		DurableByteEnd:    byteEnd,
	}, 0o640); err != nil {
		t.Fatal(err)
	}
}

func TestRequiredReaderKeepsContiguousByteLimitedPrefix(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	records := requiredTestRecords(string(make([]byte, 60)), string(make([]byte, 60)), string(make([]byte, 20)))
	byteEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, records, nil)
	writeRequiredFrontier(t, s, topic, laneID, 0, 3, byteEnd)

	first, err := s.readLaneBatch(topic, laneID, 0, 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Batch.Messages) != 1 || first.Batch.Messages[0].Sequence != 0 || first.NextSequence != 1 {
		t.Fatalf("first batch = %+v, next=%d", first.Batch.Messages, first.NextSequence)
	}
	second, err := s.readLaneBatch(topic, laneID, 1, 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Batch.Messages) != 2 || second.Batch.Messages[0].Sequence != 1 || second.Batch.Messages[1].Sequence != 2 {
		t.Fatalf("second batch = %+v", second.Batch.Messages)
	}

	reader := newLaneReader(s, topic, laneID)
	first, err = s.readLaneBatchWithCursor(reader, 0, 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Batch.Messages) != 1 || first.Batch.Messages[0].Sequence != 0 || reader.cursor.sequence != 1 {
		t.Fatalf("cursor first batch = %+v, cursor=%+v", first.Batch.Messages, reader.cursor)
	}
	second, err = s.readLaneBatchWithCursor(reader, 1, 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Batch.Messages) != 2 || second.Batch.Messages[0].Sequence != 1 || second.Batch.Messages[1].Sequence != 2 {
		t.Fatalf("cursor second batch = %+v", second.Batch.Messages)
	}
}

func TestRequiredReaderDeliversSingleOversizedRecord(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	records := requiredTestRecords(string(make([]byte, 120)), "small")
	byteEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, records, nil)
	writeRequiredFrontier(t, s, topic, laneID, 0, 2, byteEnd)
	result, err := s.readLaneBatch(topic, laneID, 0, 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Batch.Messages) != 1 || result.Batch.Messages[0].Sequence != 0 {
		t.Fatalf("batch = %+v", result.Batch.Messages)
	}
}

func TestRequiredCheckpointTrackerCountsRecordsAndRejectsRegression(t *testing.T) {
	consumer := &Consumer{cfg: ConsumerConfig{CheckpointMaxRecords: 500, CheckpointMaxDelay: time.Hour}}
	tracker := &checkpointTracker{consumer: consumer, next: 0, persisted: 0, lastPersisted: time.Now()}
	if err := tracker.advance(500); err != nil {
		t.Fatal(err)
	}
	if tracker.pendingCount != 500 || !tracker.due() {
		t.Fatalf("tracker = %+v", tracker)
	}
	if err := tracker.advance(499); err == nil {
		t.Fatal("checkpoint regression was accepted")
	}
}

func TestRequiredForceRetireAcceptsSealedFrontierAndOrphanHeader(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	records := requiredTestRecords("one", "two")
	dataEnd := writeRequiredSealedSegment(t, s, topic, laneID, 0, records)
	if byteEnd := writeRequiredOpenSegment(t, s, topic, laneID, 2, nil, nil); byteEnd == 0 {
		t.Fatal("orphan header was not created")
	}
	writeRequiredFrontier(t, s, topic, laneID, 0, 2, dataEnd)
	if err := forceRetireActiveLane(context.Background(), s, topic, laneID); err != nil {
		t.Fatal(err)
	}
	if _, retired, err := s.retired(topic, laneID); err != nil || !retired {
		t.Fatalf("retired marker: retired=%v err=%v", retired, err)
	}
	if _, err := os.Stat(filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(2, true))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan open still exists, err=%v", err)
	}
	if _, err := s.durableEnd(topic, laneID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("durable frontier still exists, err=%v", err)
	}
}

func TestRequiredForceRetireTruncatesNonDurableTail(t *testing.T) {
	s, topic, laneID := newRequiredTestStorage(t)
	records := requiredTestRecords("one", "two")
	durableEnd := writeRequiredOpenSegment(t, s, topic, laneID, 0, records, []byte{1, 2, 3})
	writeRequiredFrontier(t, s, topic, laneID, 0, 2, durableEnd)
	if err := forceRetireActiveLane(context.Background(), s, topic, laneID); err != nil {
		t.Fatal(err)
	}
	footer, err := readSealedFooter(s, topic, laneID, segmentFile{base: 0, path: filepath.Join(s.segmentsDir(topic, laneID), formatSegmentName(0, false))})
	if err != nil {
		t.Fatal(err)
	}
	if footer.NextSequence != 2 || footer.RecordCount != 2 {
		t.Fatalf("footer = %+v", footer)
	}
}

func TestRequiredPressureHeapUsesGlobalAppendAge(t *testing.T) {
	candidates := pressureCandidateHeap{
		{topic: "z", laneID: "lane", segment: segmentFile{base: 0}, footer: segmentFooter{MaxAppendNanos: 20}},
		{topic: "a", laneID: "lane", segment: segmentFile{base: 0}, footer: segmentFooter{MaxAppendNanos: 10}},
	}
	// 直接使用 GC 實際採用的 heap，確認排序先看資料年齡，再看穩定 tie-breaker。
	heap.Init(&candidates)
	if got := heap.Pop(&candidates).(*pressureCandidate).footer.MaxAppendNanos; got != 10 {
		t.Fatalf("first pressure candidate age = %d, want 10", got)
	}
}

func TestRequiredSegmentAgeUsesCreationTime(t *testing.T) {
	created := time.Unix(1000, 0)
	now := created.Add(2 * time.Hour)
	writer := &writerLane{
		cfg:             Config{SegmentMaxBytes: 1 << 30, SegmentMaxAge: time.Hour},
		clock:           func() time.Time { return now },
		activeBytes:     1,
		activeCount:     1,
		lastAppend:      now,
		activeCreatedAt: created,
	}
	if !writer.shouldRotate() {
		t.Fatal("segment age was not measured from creation time")
	}
}

func TestRequiredFaultPointCanFailAtomicCheckpointRename(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "checkpoint")
	ops := newStorageOps()
	fail := true
	ops.fault = func(point string) error {
		if point == faultCheckpointRename && fail {
			return errors.New("injected checkpoint rename failure")
		}
		return nil
	}
	value := checkpointSnapshot{FormatVersion: formatVersion, NextSequence: map[string]uint64{requiredTestLane: 7}}
	if err := writeMetadataAtomicWithOps(path, metadataKindCheckpointBase, value, 0o640, ops, "", faultCheckpointRename, ""); err == nil {
		t.Fatal("faulted checkpoint rename unexpectedly succeeded")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed atomic write left target, err=%v", err)
	}
	fail = false
	if err := writeMetadataAtomicWithOps(path, metadataKindCheckpointBase, value, 0o640, ops, "", faultCheckpointRename, ""); err != nil {
		t.Fatal(err)
	}
}

func TestRequiredSchedulerRoundRobinReachesEveryOwnedLane(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	client := &Client{clock: func() time.Time { return now }}
	consumer := &Consumer{
		client:          client,
		state:           consumerStateStarted,
		membershipReady: true,
		cfg:             ConsumerConfig{ReconcileInterval: time.Second},
		workers:         make(map[string]*laneWorker),
		laneOrder:       []string{"lane-a", "lane-b", "lane-c", "lane-d", "lane-e"},
	}
	consumer.schedulerWake = make(chan struct{}, 1)
	for _, laneID := range consumer.laneOrder {
		consumer.workers[laneID] = &laneWorker{consumer: consumer, laneID: laneID, nextRun: now}
	}
	for _, want := range consumer.laneOrder {
		worker, _, running := consumer.claimLane(now)
		if !running || worker == nil || worker.laneID != want {
			t.Fatalf("claimed worker = %#v, running=%v, want lane %q", worker, running, want)
		}
		consumer.releaseLane(worker, 0)
	}
}

func TestRequiredSchedulerRemovesLaneAfterTerminalStep(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	consumer := &Consumer{
		state:         consumerStateStarted,
		cfg:           ConsumerConfig{ReconcileInterval: time.Second},
		workers:       map[string]*laneWorker{},
		laneOrder:     []string{"lane-a"},
		schedulerWake: make(chan struct{}, 1),
	}
	worker := &laneWorker{consumer: consumer, laneID: "lane-a", nextRun: now, inFlight: true}
	consumer.workers[worker.laneID] = worker
	consumer.releaseLane(worker, -1)
	if _, exists := consumer.workers[worker.laneID]; exists {
		t.Fatal("terminal lane state was retained")
	}
	if len(consumer.laneOrder) != 0 {
		t.Fatalf("lane order = %v", consumer.laneOrder)
	}
}

type requiredMembershipStore struct {
	heartbeats int
	removed    int
	members    []string
}

func (s *requiredMembershipStore) heartbeat(context.Context, string, string) error {
	s.heartbeats++
	return nil
}

func (s *requiredMembershipStore) activeMembers(context.Context, string, time.Duration) ([]string, error) {
	return append([]string(nil), s.members...), nil
}

func (s *requiredMembershipStore) remove(context.Context, string, string) error {
	s.removed++
	return nil
}

func TestRequiredMembershipStoreIsConstructorInjectable(t *testing.T) {
	store := &requiredMembershipStore{members: []string{"b", "a"}}
	membership, err := newMembershipWithStore(store, "localmq:test", "instance", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := membership.heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	members, err := membership.activeMembers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 || members[0] != "b" || members[1] != "a" {
		t.Fatalf("members = %v", members)
	}
	if err := membership.remove(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.heartbeats != 1 || store.removed != 1 {
		t.Fatalf("store calls = heartbeat:%d remove:%d", store.heartbeats, store.removed)
	}
}
