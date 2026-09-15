package localmq

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type publishRequest struct {
	ctx     context.Context
	message Message
	result  chan publishResult

	boundaryMu sync.Mutex
	started    bool
	cancelled  bool
}

type publishResult struct {
	receipt Receipt
	err     error
}

func (r *publishRequest) markWriteStarted() bool {
	r.boundaryMu.Lock()
	defer r.boundaryMu.Unlock()
	if r.cancelled || r.ctx.Err() != nil {
		return false
	}
	r.started = true
	return true
}

func (r *publishRequest) cancelBeforeWrite() bool {
	r.boundaryMu.Lock()
	defer r.boundaryMu.Unlock()
	if r.started {
		return false
	}
	r.cancelled = true
	return true
}

func (r *publishRequest) writeStarted() bool {
	r.boundaryMu.Lock()
	defer r.boundaryMu.Unlock()
	return r.started
}

type writerLaneState uint8

const (
	writerLaneRunning writerLaneState = iota
	writerLaneStopping
	writerLaneStopped
	writerLaneFailed
)

type writerLane struct {
	owner          *Client
	storage        *storage
	cfg            Config
	topic          string
	laneID         string
	maxRecordBytes int64
	clock          func() time.Time
	instanceID     string
	metrics        *clientMetrics

	requests chan *publishRequest
	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once

	mu               sync.Mutex
	state            writerLaneState
	accepting        bool
	failure          error
	stopErr          error
	activeFile       fileHandle
	activeBase       uint64
	nextSequence     uint64
	activeBytes      int64
	activeCount      uint64
	minAppend        int64
	maxAppend        int64
	lastAppend       time.Time
	activeCreatedAt  time.Time
	dataCRC          uint32
	activePublishers int
	publishersZero   chan struct{}
}

func newWriterLane(owner *Client, s *storage, cfg Config, instanceID, topic string, maxRecordBytes int64, clock func() time.Time, metrics *clientMetrics) (*writerLane, error) {
	if s == nil || !s.isStarted() {
		return nil, errors.New("localmq storage is not started")
	}
	laneID, err := newUUIDv7()
	if err != nil {
		return nil, err
	}
	lanePath, err := createLaneDirectory(s, cfg.StorageID, instanceID, topic, laneID, maxRecordBytes, clock())
	if err != nil {
		return nil, err
	}
	segmentPath := filepath.Join(lanePath, segmentsDirectory, "0.open")
	file, err := s.openFile(segmentPath, os.O_RDWR|os.O_APPEND, 0o640)
	if err != nil {
		return nil, fmt.Errorf("open new lane segment: %w", err)
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	headerBytes, err := s.readFile(segmentPath)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	header, err := decodeSegmentHeader(headerBytes)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	frontier, err := s.durableEnd(topic, laneID)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if frontier.ActiveSegmentBase != header.BaseSequence || frontier.DurableByteEnd != stat.Size() || frontier.NextSequence != header.BaseSequence {
		_ = file.Close()
		return nil, errors.New("new lane durable frontier does not match segment header")
	}
	return &writerLane{
		owner:           owner,
		storage:         s,
		cfg:             cfg,
		topic:           topic,
		laneID:          laneID,
		maxRecordBytes:  maxRecordBytes,
		clock:           clock,
		instanceID:      instanceID,
		metrics:         metrics,
		requests:        make(chan *publishRequest, cfg.PublishQueueCapacity),
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
		state:           writerLaneRunning,
		accepting:       true,
		activeFile:      file,
		activeBase:      header.BaseSequence,
		nextSequence:    frontier.NextSequence,
		activeBytes:     stat.Size(),
		lastAppend:      header.CreatedAt,
		activeCreatedAt: header.CreatedAt,
		dataCRC:         crc32.Checksum(headerBytes, crc32cTable),
		publishersZero:  closedSignal(),
	}, nil
}

func createLaneDirectory(s *storage, storageID, instanceID, topic, laneID string, maxRecordBytes int64, now time.Time) (string, error) {
	parent := s.lanesDir(topic)
	if err := s.mkdirAll(parent, 0o750); err != nil {
		return "", err
	}
	temporary, err := s.mkdirTemp(parent, ".lane-")
	if err != nil {
		return "", err
	}
	keep := false
	defer func() {
		if !keep {
			_ = s.removeAll(temporary)
		}
	}()
	if err := s.mkdir(filepath.Join(temporary, segmentsDirectory), 0o750); err != nil {
		return "", err
	}
	manifest := laneManifest{
		FormatVersion:      formatVersion,
		StorageID:          storageID,
		Topic:              topic,
		LaneID:             laneID,
		ProducerInstanceID: instanceID,
		CreatedAtUnixNano:  now.UnixNano(),
	}
	if err := writeMetadataAtomicWithOps(filepath.Join(temporary, laneManifestFile), metadataKindLaneManifest, manifest, 0o640, s.fsOps(), "", "", ""); err != nil {
		return "", err
	}
	if err := writeMetadataAtomicWithOps(filepath.Join(temporary, retentionIndexFile), metadataKindRetentionIndex, retentionIndex{
		FormatVersion:       formatVersion,
		Topic:               topic,
		LaneID:              laneID,
		EarliestRetainedSeq: 0,
		UpdatedAtUnixNano:   now.UnixNano(),
	}, 0o640, s.fsOps(), "", "", ""); err != nil {
		return "", err
	}
	header, err := encodeSegmentHeader(topic, laneID, 0, now)
	if err != nil {
		return "", err
	}
	segmentPath := filepath.Join(temporary, segmentsDirectory, "0.open")
	segment, err := s.openFile(segmentPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		return "", err
	}
	if _, err := segment.Write(header); err != nil {
		_ = segment.Close()
		return "", err
	}
	if err := s.fault(faultWalSync); err != nil {
		_ = segment.Close()
		return "", err
	}
	if err := segment.Sync(); err != nil {
		_ = segment.Close()
		return "", err
	}
	if err := segment.Close(); err != nil {
		return "", err
	}
	frontier := durableEnd{
		FormatVersion:     formatVersion,
		StorageID:         storageID,
		Topic:             topic,
		LaneID:            laneID,
		ActiveSegmentBase: 0,
		NextSequence:      0,
		DurableByteEnd:    int64(len(header)),
		UpdatedAtUnixNano: now.UnixNano(),
	}
	if err := writeMetadataAtomicWithOps(filepath.Join(temporary, durableEndFile), metadataKindDurableEnd, frontier, 0o640, s.fsOps(), faultDurableEndTempSync, faultDurableEndRename, faultDurableEndParentSync); err != nil {
		return "", err
	}
	if err := s.syncDirectory(filepath.Join(temporary, segmentsDirectory)); err != nil {
		return "", err
	}
	if err := s.syncDirectory(temporary); err != nil {
		return "", err
	}
	finalPath := s.laneDir(topic, laneID)
	if err := s.rename(temporary, finalPath); err != nil {
		return "", err
	}
	if err := s.syncDirectory(parent); err != nil {
		return "", err
	}
	keep = true
	return finalPath, nil
}

func (w *writerLane) publish(ctx context.Context, message Message) (Receipt, error) {
	request := &publishRequest{ctx: ctx, message: message, result: make(chan publishResult, 1)}
	w.mu.Lock()
	if !w.accepting || w.state != writerLaneRunning {
		err := w.failure
		if err == nil {
			err = errors.New("writer lane is not accepting requests")
		}
		w.mu.Unlock()
		return Receipt{}, &PublishError{Kind: PublishRejectedBeforeWrite, Cause: err}
	}
	w.activePublishers++
	if w.activePublishers == 1 {
		w.publishersZero = make(chan struct{})
	}
	w.mu.Unlock()
	defer w.endPublish()
	select {
	case w.requests <- request:
		if w.owner != nil {
			w.owner.metricQueue(w.topic, len(w.requests))
		}
	case <-ctx.Done():
		return Receipt{}, &PublishError{Kind: PublishRejectedBeforeWrite, Cause: ctx.Err()}
	}
	select {
	case result := <-request.result:
		return result.receipt, result.err
	case <-ctx.Done():
		if request.cancelBeforeWrite() {
			return Receipt{}, &PublishError{Kind: PublishRejectedBeforeWrite, Cause: ctx.Err()}
		}
		return Receipt{}, &PublishError{Kind: PublishDurabilityUnknown, Cause: ctx.Err()}
	}
}

func (w *writerLane) isAvailable() bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.accepting && w.state == writerLaneRunning
}

func (w *writerLane) stop(ctx context.Context) error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	if w.state == writerLaneStopped {
		err := w.stopErr
		w.mu.Unlock()
		return err
	}
	if w.state != writerLaneStopped {
		w.state = writerLaneStopping
		w.accepting = false
		w.signalStop()
	}
	w.mu.Unlock()
	select {
	case <-w.doneCh:
		w.mu.Lock()
		err := w.stopErr
		w.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *writerLane) run() {
	defer close(w.doneCh)
	for {
		select {
		case request := <-w.requests:
			batch, stopping := w.collectBatch(request)
			w.flush(batch)
			if w.isFailed() {
				w.rejectPending()
				w.markStopped()
				return
			}
			if stopping {
				w.drainAndRetire()
				return
			}
		case <-w.stopCh:
			w.drainAndRetire()
			return
		}
	}
}

func (w *writerLane) collectBatch(first *publishRequest) ([]*publishRequest, bool) {
	batch := []*publishRequest{first}
	if w.batchReachedLimit(batch) {
		return batch, w.stopRequested()
	}
	timer := time.NewTimer(w.cfg.GroupCommitMaxWait)
	defer timer.Stop()
	for {
		select {
		case request := <-w.requests:
			batch = append(batch, request)
			if w.batchReachedLimit(batch) {
				return batch, w.stopRequested()
			}
		case <-timer.C:
			return batch, w.stopRequested()
		case <-w.stopCh:
			return batch, true
		}
	}
}

func (w *writerLane) batchReachedLimit(batch []*publishRequest) bool {
	if len(batch) >= w.cfg.GroupCommitMaxMessages {
		return true
	}
	var bytes int64
	for _, request := range batch {
		bytes += int64(len(request.message.Payload) + len(request.message.MessageID) + len(request.message.EventType) + 32)
		if bytes >= w.cfg.GroupCommitMaxBytes {
			return true
		}
	}
	return false
}

func (w *writerLane) stopRequested() bool {
	select {
	case <-w.stopCh:
		return true
	default:
		return false
	}
}

func closedSignal() chan struct{} {
	signal := make(chan struct{})
	close(signal)
	return signal
}

func (w *writerLane) endPublish() {
	w.mu.Lock()
	if w.activePublishers > 0 {
		w.activePublishers--
		if w.activePublishers == 0 && w.publishersZero != nil {
			close(w.publishersZero)
		}
	}
	w.mu.Unlock()
}

func (w *writerLane) publisherState() (int, <-chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.activePublishers, w.publishersZero
}

type encodedPublish struct {
	request *publishRequest
	record  walRecord
	data    []byte
}

func (w *writerLane) flush(batch []*publishRequest) {
	if len(batch) == 0 {
		return
	}
	selected := make([]*publishRequest, 0, len(batch))
	for _, request := range batch {
		if request.ctx.Err() != nil || !request.isAvailableBeforeWrite() {
			w.reply(request, publishResult{err: &PublishError{Kind: PublishRejectedBeforeWrite, Cause: request.ctx.Err()}})
			continue
		}
		selected = append(selected, request)
	}
	if len(selected) == 0 {
		return
	}
	encoded := w.preEncode(selected)
	if len(encoded) == 0 {
		return
	}
	var totalBytes int64
	for _, item := range encoded {
		totalBytes += int64(len(item.data))
	}
	free, err := w.storage.freeBytes()
	if err != nil {
		for _, item := range encoded {
			w.reply(item.request, publishResult{err: &PublishError{Kind: PublishRejectedBeforeWrite, Cause: fmt.Errorf("filesystem capacity check: %w", err)}})
		}
		return
	}
	reserve := uint64(w.cfg.ProducerStopFreeBytes)
	if free < reserve || free-reserve < uint64(totalBytes) {
		cause := fmt.Errorf("filesystem free bytes %d are below producer stop watermark", free)
		for _, item := range encoded {
			w.reply(item.request, publishResult{err: &PublishError{Kind: PublishRejectedBeforeWrite, Cause: cause}})
		}
		return
	}
	if w.activeCount > 0 && w.ageExceeded() {
		if err := w.rotate(); err != nil {
			w.fail(err)
			for _, item := range encoded {
				w.reply(item.request, publishResult{err: &PublishError{Kind: PublishRejectedBeforeWrite, Cause: err}})
			}
			return
		}
	}

	started := make([]*encodedPublish, 0, len(encoded))
	for _, item := range encoded {
		if item.request.markWriteStarted() {
			started = append(started, item)
		} else {
			w.reply(item.request, publishResult{err: &PublishError{Kind: PublishRejectedBeforeWrite, Cause: item.request.ctx.Err()}})
		}
	}
	if len(started) == 0 {
		return
	}
	// markWriteStarted 與實際 Write 之間不應配置大型 buffer；編碼資料已在
	// bounded batch 內完成，這裡只重排 sequence 以消除已取消 request。
	if len(started) != len(encoded) {
		started = w.resequence(started)
	}
	writeData := make([]byte, 0, int(totalBytes))
	for _, item := range started {
		writeData = append(writeData, item.data...)
	}
	if err := w.storage.fault(faultRecordAppend); err != nil {
		w.fail(fmt.Errorf("append WAL segment: %w", err))
		for _, item := range started {
			w.reply(item.request, publishResult{err: &PublishError{Kind: PublishDurabilityUnknown, Cause: err}})
		}
		return
	}
	n, writeErr := w.activeFile.Write(writeData)
	if writeErr != nil || n != len(writeData) {
		if writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		w.fail(fmt.Errorf("append WAL segment: %w", writeErr))
		for _, item := range started {
			w.reply(item.request, publishResult{err: &PublishError{Kind: PublishDurabilityUnknown, Cause: writeErr}})
		}
		return
	}
	if err := w.storage.fault(faultWalSync); err != nil {
		w.fail(fmt.Errorf("sync WAL segment: %w", err))
		for _, item := range started {
			w.reply(item.request, publishResult{err: &PublishError{Kind: PublishDurabilityUnknown, Cause: err}})
		}
		return
	}
	if err := w.activeFile.Sync(); err != nil {
		w.fail(fmt.Errorf("sync WAL segment: %w", err))
		for _, item := range started {
			w.reply(item.request, publishResult{err: &PublishError{Kind: PublishDurabilityUnknown, Cause: err}})
		}
		return
	}
	for _, item := range started {
		w.dataCRC = crc32.Update(w.dataCRC, crc32cTable, item.data)
		w.activeCount++
		appendNanos := item.record.AppendTime.UnixNano()
		if w.activeCount == 1 {
			w.minAppend = appendNanos
		}
		w.maxAppend = appendNanos
		w.lastAppend = item.record.AppendTime
	}
	w.activeBytes += int64(n)
	w.nextSequence += uint64(len(started))
	frontier := durableEnd{
		FormatVersion:     formatVersion,
		StorageID:         w.cfg.StorageID,
		Topic:             w.topic,
		LaneID:            w.laneID,
		ActiveSegmentBase: w.activeBase,
		NextSequence:      w.nextSequence,
		DurableByteEnd:    w.activeBytes,
		UpdatedAtUnixNano: w.clock().UnixNano(),
	}
	if err := writeMetadataAtomicWithOps(filepath.Join(w.storage.laneDir(w.topic, w.laneID), durableEndFile), metadataKindDurableEnd, frontier, 0o640, w.storage.fsOps(), faultDurableEndTempSync, faultDurableEndRename, faultDurableEndParentSync); err != nil {
		w.fail(fmt.Errorf("publish durable-end: %w", err))
		for _, item := range started {
			w.reply(item.request, publishResult{err: &PublishError{Kind: PublishDurabilityUnknown, Cause: err}})
		}
		return
	}
	if err := w.storage.syncDirectory(w.storage.laneDir(w.topic, w.laneID)); err != nil {
		w.fail(fmt.Errorf("sync lane directory: %w", err))
		for _, item := range started {
			w.reply(item.request, publishResult{err: &PublishError{Kind: PublishDurabilityUnknown, Cause: err}})
		}
		return
	}
	if w.metrics != nil {
		w.metrics.groupCommitMessages.WithLabelValues(w.topic).Observe(float64(len(started)))
		w.metrics.groupCommitBytes.WithLabelValues(w.topic).Observe(float64(n))
		w.metrics.walBytes.WithLabelValues(w.topic).Set(float64(w.activeBytes))
		if free, freeErr := w.storage.freeBytes(); freeErr == nil {
			w.metrics.filesystemFree.Set(float64(free))
		}
	}
	for _, item := range started {
		err := error(nil)
		if item.request.ctx.Err() != nil || !item.request.writeStarted() {
			err = &PublishError{Kind: PublishDurabilityUnknown, Cause: item.request.ctx.Err()}
		}
		w.reply(item.request, publishResult{receipt: Receipt{Topic: w.topic, LaneID: w.laneID, Sequence: item.record.Sequence}, err: err})
	}
	if w.shouldRotate() {
		if err := w.rotate(); err != nil {
			w.fail(err)
		}
	}
}

func (r *publishRequest) isAvailableBeforeWrite() bool {
	r.boundaryMu.Lock()
	defer r.boundaryMu.Unlock()
	return !r.cancelled && r.ctx.Err() == nil
}

func (w *writerLane) preEncode(requests []*publishRequest) []*encodedPublish {
	result := make([]*encodedPublish, 0, len(requests))
	sequence := w.nextSequence
	lastAppend := w.lastAppend
	for _, request := range requests {
		if sequence == ^uint64(0) {
			w.reply(request, publishResult{err: &PublishError{Kind: PublishRejectedBeforeWrite, Cause: errors.New("WAL sequence is exhausted")}})
			continue
		}
		now := w.clock()
		if !lastAppend.IsZero() && now.Before(lastAppend) {
			now = lastAppend
		}
		record := walRecord{
			Sequence:      sequence,
			AppendTime:    now,
			MessageID:     append([]byte(nil), request.message.MessageID...),
			EventType:     request.message.EventType,
			SchemaVersion: request.message.SchemaVersion,
			Payload:       append([]byte(nil), request.message.Payload...),
		}
		data, err := encodeRecord(record, w.maxRecordBytes)
		if err != nil {
			w.reply(request, publishResult{err: &PublishError{Kind: PublishRejectedBeforeWrite, Cause: err}})
			continue
		}
		result = append(result, &encodedPublish{request: request, record: record, data: data})
		sequence++
		lastAppend = now
	}
	return result
}

func (w *writerLane) resequence(items []*encodedPublish) []*encodedPublish {
	sequence := w.nextSequence
	result := items[:0]
	for _, item := range items {
		if sequence == ^uint64(0) {
			w.reply(item.request, publishResult{err: &PublishError{Kind: PublishRejectedBeforeWrite, Cause: errors.New("WAL sequence is exhausted")}})
			continue
		}
		item.record.Sequence = sequence
		data, err := encodeRecord(item.record, w.maxRecordBytes)
		if err != nil {
			w.reply(item.request, publishResult{err: &PublishError{Kind: PublishRejectedBeforeWrite, Cause: err}})
			continue
		}
		item.data = data
		result = append(result, item)
		sequence++
	}
	return result
}

func (w *writerLane) shouldRotate() bool {
	return w.activeBytes >= w.cfg.SegmentMaxBytes || w.ageExceeded()
}

func (w *writerLane) ageExceeded() bool {
	return !w.activeCreatedAt.IsZero() && w.clock().Sub(w.activeCreatedAt) >= w.cfg.SegmentMaxAge
}

func (w *writerLane) rotate() error {
	if w.activeCount == 0 {
		return nil
	}
	if err := w.sealActive(); err != nil {
		return err
	}
	return w.openNextSegment()
}

func (w *writerLane) sealActive() error {
	footer := encodeSegmentFooter(segmentFooter{
		FileLength:     uint64(w.activeBytes + segmentFooterBytes),
		NextSequence:   w.nextSequence,
		RecordCount:    w.activeCount,
		MinAppendNanos: w.minAppend,
		MaxAppendNanos: w.maxAppend,
		DataChecksum:   w.dataCRC,
	})
	if _, err := w.activeFile.Write(footer); err != nil {
		return fmt.Errorf("append segment footer: %w", err)
	}
	if err := w.storage.fault(faultFooterSync); err != nil {
		return fmt.Errorf("sync segment footer: %w", err)
	}
	if err := w.activeFile.Sync(); err != nil {
		return fmt.Errorf("sync segment footer: %w", err)
	}
	if err := w.activeFile.Close(); err != nil {
		return err
	}
	oldPath := filepath.Join(w.storage.segmentsDir(w.topic, w.laneID), fmt.Sprintf("%d.open", w.activeBase))
	sealedPath := filepath.Join(w.storage.segmentsDir(w.topic, w.laneID), fmt.Sprintf("%d.wal", w.activeBase))
	if err := w.storage.fault(faultSegmentRename); err != nil {
		return fmt.Errorf("seal segment: %w", err)
	}
	if err := w.storage.rename(oldPath, sealedPath); err != nil {
		return fmt.Errorf("seal segment: %w", err)
	}
	if err := w.storage.syncDirectory(w.storage.segmentsDir(w.topic, w.laneID)); err != nil {
		return err
	}
	w.activeFile = nil
	return nil
}

func (w *writerLane) openNextSegment() error {
	path := filepath.Join(w.storage.segmentsDir(w.topic, w.laneID), fmt.Sprintf("%d.open", w.nextSequence))
	header, err := encodeSegmentHeader(w.topic, w.laneID, w.nextSequence, w.clock())
	if err != nil {
		return err
	}
	file, err := w.storage.openFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		return err
	}
	if _, err := file.Write(header); err != nil {
		_ = file.Close()
		return err
	}
	if err := w.storage.fault(faultWalSync); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := w.storage.syncDirectory(w.storage.segmentsDir(w.topic, w.laneID)); err != nil {
		return err
	}
	file, err = w.storage.openFile(path, os.O_RDWR|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	w.activeFile = file
	w.activeBase = w.nextSequence
	w.activeBytes = int64(len(header))
	w.activeCount = 0
	w.minAppend = 0
	w.maxAppend = 0
	decodedHeader, err := decodeSegmentHeader(header)
	if err != nil {
		_ = file.Close()
		return err
	}
	w.lastAppend = decodedHeader.CreatedAt
	w.activeCreatedAt = decodedHeader.CreatedAt
	w.dataCRC = crc32.Checksum(header, crc32cTable)
	frontier := durableEnd{
		FormatVersion:     formatVersion,
		StorageID:         w.cfg.StorageID,
		Topic:             w.topic,
		LaneID:            w.laneID,
		ActiveSegmentBase: w.activeBase,
		NextSequence:      w.nextSequence,
		DurableByteEnd:    w.activeBytes,
		UpdatedAtUnixNano: w.clock().UnixNano(),
	}
	if err := writeMetadataAtomicWithOps(filepath.Join(w.storage.laneDir(w.topic, w.laneID), durableEndFile), metadataKindDurableEnd, frontier, 0o640, w.storage.fsOps(), faultDurableEndTempSync, faultDurableEndRename, faultDurableEndParentSync); err != nil {
		_ = file.Close()
		return err
	}
	return w.storage.syncDirectory(w.storage.laneDir(w.topic, w.laneID))
}

func (w *writerLane) drainAndRetire() {
	if w.isFailed() {
		w.rejectPending()
		w.markStopped()
		return
	}
	for {
		batch := make([]*publishRequest, 0, w.cfg.GroupCommitMaxMessages)
		select {
		case request := <-w.requests:
			batch = append(batch, request)
		default:
			publishers, publishersZero := w.publisherState()
			if publishers == 0 {
				if err := w.retire(); err != nil {
					w.fail(err)
				}
				w.mu.Lock()
				w.state = writerLaneStopped
				w.mu.Unlock()
				return
			}
			select {
			case request := <-w.requests:
				batch = append(batch, request)
			case <-publishersZero:
				continue
			}
		}
		for len(batch) < w.cfg.GroupCommitMaxMessages {
			select {
			case request := <-w.requests:
				batch = append(batch, request)
			default:
				w.flush(batch)
				batch = nil
			}
			if batch == nil {
				break
			}
		}
		if len(batch) != 0 {
			w.flush(batch)
		}
	}
}

func (w *writerLane) retire() error {
	if w.activeFile == nil {
		return nil
	}
	emptyPath := ""
	if w.activeCount > 0 {
		if err := w.sealActive(); err != nil {
			return err
		}
	} else {
		emptyPath = filepath.Join(w.storage.segmentsDir(w.topic, w.laneID), fmt.Sprintf("%d.open", w.activeBase))
		if err := w.activeFile.Truncate(w.activeBytes); err != nil {
			return err
		}
		if err := w.storage.fault(faultFooterSync); err != nil {
			return err
		}
		if err := w.activeFile.Sync(); err != nil {
			return err
		}
		if err := w.activeFile.Close(); err != nil {
			return err
		}
		w.activeFile = nil
	}
	marker := retiredMarker{
		FormatVersion:     formatVersion,
		Topic:             w.topic,
		LaneID:            w.laneID,
		FinalNextSequence: w.nextSequence,
		RetiredAtUnixNano: w.clock().UnixNano(),
		RetireReason:      "graceful",
	}
	path := filepath.Join(w.storage.laneDir(w.topic, w.laneID), retiredMarkerFile)
	if err := writeMetadataExclusiveWithOps(path, metadataKindRetired, marker, 0o640, w.storage.fsOps()); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		var existing retiredMarker
		if readErr := readMetadataWithOps(path, metadataKindRetired, &existing, w.storage.fsOps()); readErr != nil {
			return readErr
		}
		if existing.FinalNextSequence != marker.FinalNextSequence {
			return errors.New("existing retired marker has a different final sequence")
		}
	}
	if emptyPath != "" {
		if err := w.storage.remove(emptyPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := w.storage.syncDirectory(w.storage.segmentsDir(w.topic, w.laneID)); err != nil {
			return err
		}
	}
	if err := w.storage.remove(filepath.Join(w.storage.laneDir(w.topic, w.laneID), durableEndFile)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return w.storage.syncDirectory(w.storage.laneDir(w.topic, w.laneID))
}

func (w *writerLane) fail(err error) {
	w.mu.Lock()
	if w.failure == nil {
		w.failure = err
	}
	if w.stopErr == nil {
		w.stopErr = err
	}
	w.accepting = false
	w.state = writerLaneFailed
	if w.activeFile != nil {
		_ = w.activeFile.Close()
		w.activeFile = nil
	}
	w.mu.Unlock()
	w.signalStop()
}

func (w *writerLane) signalStop() {
	w.stopOnce.Do(func() { close(w.stopCh) })
}

func (w *writerLane) isFailed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.state == writerLaneFailed
}

func (w *writerLane) markStopped() {
	w.mu.Lock()
	w.state = writerLaneStopped
	w.mu.Unlock()
}

func (w *writerLane) rejectPending() {
	for {
		select {
		case request := <-w.requests:
			cause := w.failure
			if cause == nil {
				cause = errors.New("writer lane stopped")
			}
			w.reply(request, publishResult{err: &PublishError{Kind: PublishRejectedBeforeWrite, Cause: cause}})
			continue
		default:
		}
		publishers, publishersZero := w.publisherState()
		if publishers == 0 {
			return
		}
		select {
		case request := <-w.requests:
			cause := w.failure
			if cause == nil {
				cause = errors.New("writer lane stopped")
			}
			w.reply(request, publishResult{err: &PublishError{Kind: PublishRejectedBeforeWrite, Cause: cause}})
		case <-publishersZero:
		}
	}
}

func (w *writerLane) reply(request *publishRequest, result publishResult) {
	if result.err != nil && request.ctx.Err() != nil && !IsDurabilityUnknown(result.err) {
		if request.writeStarted() {
			result.err = &PublishError{Kind: PublishDurabilityUnknown, Cause: request.ctx.Err()}
		}
	}
	request.result <- result
}

type segmentFile struct {
	base uint64
	path string
	open bool
}

func (s *storage) listSegments(topic, laneID string) ([]segmentFile, error) {
	entries, err := s.readDir(s.segmentsDir(topic, laneID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(entries) > maxStorageEntries {
		return nil, errors.New("segment directory exceeds configured bound")
	}
	segments := make([]segmentFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		extension := filepath.Ext(entry.Name())
		if extension != ".wal" && extension != ".open" {
			continue
		}
		base, err := strconv.ParseUint(strings.TrimSuffix(entry.Name(), extension), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("segment %q has invalid base sequence", entry.Name())
		}
		segments = append(segments, segmentFile{base: base, path: filepath.Join(s.segmentsDir(topic, laneID), entry.Name()), open: extension == ".open"})
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].base < segments[j].base })
	for i := 1; i < len(segments); i++ {
		if segments[i-1].base == segments[i].base {
			return nil, errors.New("lane contains duplicate segment base sequence")
		}
	}
	return segments, nil
}
