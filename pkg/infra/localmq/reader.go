package localmq

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"
)

type laneReadResult struct {
	Batch          Batch
	NextSequence   uint64
	DurableEnd     uint64
	RetentionFloor uint64
	AtEnd          bool
}

// laneReader 保存單一 consumer/lane 的短暫 byte cursor。cursor 不寫入
// storage；process restart 或 ownership change 時由 durable checkpoint 重建。
type laneReader struct {
	storage           *storage
	topic             string
	laneID            string
	manifestValidated bool
	retentionFloor    uint64
	durableEnd        uint64
	segments          []segmentFile
	boundaries        []segmentBoundary
	segmentCacheValid bool
	cursor            laneCursor
}

type laneCursor struct {
	valid          bool
	base           uint64
	path           string
	open           bool
	fileInfo       os.FileInfo
	offset         int64
	sequence       uint64
	lastAppend     time.Time
	validatedFile  bool
	recordCount    uint64
	minAppendNanos int64
	maxAppendNanos int64
	boundary       segmentBoundary
}

type cursorSegmentResult struct {
	messages     []DeliveredMessage
	bytes        int64
	batchFull    bool
	segmentAtEnd bool
}

// readLaneBatch 只讀 sealed record 與 active durable-end 以前的完整 record。
// 它每次 bounded 讀取，不把整條 lane 或整個 Topic 載入 memory。
func (s *storage) readLaneBatch(topic, laneID string, from uint64, maxMessages int, maxBytes int64) (laneReadResult, error) {
	if err := validateTopicAndLane(topic, laneID); err != nil {
		return laneReadResult{}, err
	}
	if maxMessages <= 0 || maxBytes <= 0 {
		return laneReadResult{}, errors.New("lane read limits must be greater than zero")
	}
	if _, err := s.laneManifest(topic, laneID); err != nil {
		return laneReadResult{}, err
	}
	floor, err := s.retention(topic, laneID)
	if err != nil {
		return laneReadResult{}, err
	}
	if from < floor.EarliestRetainedSeq {
		from = floor.EarliestRetainedSeq
	}
	frontier, frontierErr := s.durableEnd(topic, laneID)
	retired, isRetired, retiredErr := s.retired(topic, laneID)
	if retiredErr != nil {
		return laneReadResult{}, retiredErr
	}
	if frontierErr != nil && (!isRetired || !errors.Is(frontierErr, os.ErrNotExist)) {
		return laneReadResult{}, frontierErr
	}
	if frontierErr == nil && isRetired && frontier.NextSequence != retired.FinalNextSequence {
		return laneReadResult{}, errors.New("retired marker and durable frontier disagree")
	}
	durableEndSequence := uint64(0)
	if frontierErr == nil {
		durableEndSequence = frontier.NextSequence
	}
	if isRetired && retired.FinalNextSequence > durableEndSequence {
		durableEndSequence = retired.FinalNextSequence
	}
	if floor.EarliestRetainedSeq > durableEndSequence {
		return laneReadResult{}, errors.New("retention floor exceeds durable end")
	}
	segments, err := s.listSegments(topic, laneID)
	if err != nil {
		return laneReadResult{}, err
	}
	result := laneReadResult{
		Batch: Batch{
			Topic:    topic,
			LaneID:   laneID,
			Messages: make([]DeliveredMessage, 0, maxMessages),
		},
		DurableEnd:     durableEndSequence,
		RetentionFloor: floor.EarliestRetainedSeq,
	}
	batchBytes := int64(0)
	if len(segments) == 0 && !isRetired {
		return laneReadResult{}, errors.New("non-retired lane segments are missing")
	}
	if durableEndSequence > floor.EarliestRetainedSeq && len(segments) == 0 {
		return laneReadResult{}, errors.New("lane segments are missing below durable end")
	}
	if len(segments) > 0 && durableEndSequence > floor.EarliestRetainedSeq && segments[0].base > floor.EarliestRetainedSeq {
		return laneReadResult{}, errors.New("lane segment coverage starts after retention floor")
	}
	var expectedSegmentBase uint64
	seenSegment := false
	for _, segment := range segments {
		if seenSegment && segment.base != expectedSegmentBase {
			return laneReadResult{}, errors.New("segment base sequence regressed")
		}
		segmentResult, err := readSegment(s, topic, laneID, segment, frontier, from, maxMessages-len(result.Batch.Messages), maxBytes-batchBytes)
		if err != nil {
			return laneReadResult{}, err
		}
		if segmentResult.nextSequence < segment.base {
			return laneReadResult{}, errors.New("segment next sequence regressed")
		}
		expectedSegmentBase = segmentResult.nextSequence
		seenSegment = true
		if segmentResult.nextSequence > result.DurableEnd {
			return laneReadResult{}, errors.New("segment exceeds durable frontier")
		}
		for _, message := range segmentResult.messages {
			if message.Sequence < from {
				continue
			}
			if len(result.Batch.Messages) >= maxMessages || (len(result.Batch.Messages) > 0 && batchBytes+int64(len(message.Payload)) > maxBytes) {
				result.AtEnd = false
				if err := setLaneResultNext(&result, from); err != nil {
					return laneReadResult{}, err
				}
				return result, nil
			}
			result.Batch.Messages = append(result.Batch.Messages, message)
			batchBytes += int64(len(message.Payload))
		}
		if segmentResult.batchFull {
			result.AtEnd = false
			if err := setLaneResultNext(&result, from); err != nil {
				return laneReadResult{}, err
			}
			return result, nil
		}
		if len(result.Batch.Messages) >= maxMessages || batchBytes >= maxBytes {
			result.AtEnd = false
			if err := setLaneResultNext(&result, from); err != nil {
				return laneReadResult{}, err
			}
			return result, nil
		}
	}
	if seenSegment && expectedSegmentBase != result.DurableEnd {
		return laneReadResult{}, errors.New("lane segment coverage does not reach durable frontier")
	}
	if err := setLaneResultNext(&result, from); err != nil {
		return laneReadResult{}, err
	}
	result.AtEnd = from >= result.DurableEnd || len(result.Batch.Messages) == 0
	return result, nil
}

func setLaneResultNext(result *laneReadResult, from uint64) error {
	if len(result.Batch.Messages) == 0 {
		result.NextSequence = from
		return nil
	}
	last := result.Batch.Messages[len(result.Batch.Messages)-1].Sequence
	if last == ^uint64(0) {
		return errors.New("lane sequence is exhausted")
	}
	result.NextSequence = last + 1
	return nil
}

// readLaneBatchWithCursor 是 consumer hot path 使用的 bounded sequential
// reader。WAL 的 persisted coordinate 仍然只有 sequence；byte offset 只存在
// process memory，重新啟動時由 checkpoint 重新定位。
func (s *storage) readLaneBatchWithCursor(reader *laneReader, from uint64, maxMessages int, maxBytes int64) (laneReadResult, error) {
	if reader == nil || reader.storage != s {
		return laneReadResult{}, errors.New("lane reader does not belong to storage")
	}
	if err := validateTopicAndLane(reader.topic, reader.laneID); err != nil {
		return laneReadResult{}, err
	}
	if maxMessages <= 0 || maxBytes <= 0 {
		return laneReadResult{}, errors.New("lane read limits must be greater than zero")
	}
	if !reader.manifestValidated {
		if _, err := s.laneManifest(reader.topic, reader.laneID); err != nil {
			return laneReadResult{}, err
		}
		reader.manifestValidated = true
	}
	floorIndex, frontier, hasFrontier, isRetired, durableEndSequence, err := s.laneCoordinates(reader.topic, reader.laneID)
	if err != nil {
		reader.cursor = laneCursor{}
		return laneReadResult{}, err
	}
	floor := floorIndex.EarliestRetainedSeq
	if reader.cursor.valid && (floor < reader.retentionFloor || durableEndSequence < reader.durableEnd) {
		reader.cursor = laneCursor{}
		reader.clearSegmentCache()
		return laneReadResult{}, errors.New("lane durable coordinates regressed")
	}
	reader.retentionFloor = floor
	reader.durableEnd = durableEndSequence
	if from < floor {
		from = floor
	}
	result := laneReadResult{
		Batch:          Batch{Topic: reader.topic, LaneID: reader.laneID, Messages: make([]DeliveredMessage, 0, maxMessages)},
		DurableEnd:     durableEndSequence,
		RetentionFloor: floor,
	}
	batchBytes := int64(0)
	continueFromCursor := false
	if reader.cursor.valid && reader.cursor.sequence == from && reader.cursor.sequence <= durableEndSequence && (reader.cursor.open || reader.cursor.boundary.nextSequence <= durableEndSequence) {
		segment := segmentFile{base: reader.cursor.base, path: reader.cursor.path, open: reader.cursor.open}
		boundary, usable := reader.fastBoundary(frontier, hasFrontier)
		if usable {
			segmentResult, fastErr := readCursorSegment(s, reader.topic, reader.laneID, segment, boundary, from, maxMessages, maxBytes, &reader.cursor)
			if fastErr == nil {
				reader.updateCachedBoundary(boundary)
				result.Batch.Messages = append(result.Batch.Messages, segmentResult.messages...)
				batchBytes += segmentResult.bytes
				if segmentResult.batchFull || (segmentResult.segmentAtEnd && reader.cursor.sequence == durableEndSequence) {
					result.NextSequence = reader.cursor.sequence
					result.AtEnd = len(result.Batch.Messages) == 0 && from >= durableEndSequence
					return result, nil
				}
				if segmentResult.segmentAtEnd {
					// fast path 已經消費到 current segment 尾端；後續 slow
					// locate 必須從新的 cursor coordinate 接續，不能以原始
					// from 重掃並重複加入尾端 record。
					from = reader.cursor.sequence
					continueFromCursor = true
				}
			} else {
				reader.clearSegmentCache()
			}
		} else {
			reader.clearSegmentCache()
		}
		// Rotation、GC、identity變更或 fast path error 時才回到完整
		// 定位路徑；成功抵達 segment 尾端則保留 cursor 供下一段接續。
		if !continueFromCursor {
			reader.cursor = laneCursor{}
		}
	}
	segments := reader.segments
	boundaries := reader.boundaries
	cacheUsable := reader.segmentCacheValid && len(segments) == len(boundaries) && ((len(boundaries) == 0 && durableEndSequence == floor) || (len(boundaries) > 0 && boundaries[len(boundaries)-1].nextSequence == durableEndSequence))
	if !cacheUsable {
		segments, err = s.listSegments(reader.topic, reader.laneID)
		if err != nil {
			return laneReadResult{}, err
		}
		boundaries = make([]segmentBoundary, len(segments))
		for index, segment := range segments {
			boundary, boundaryErr := readSegmentBoundary(s, reader.topic, reader.laneID, segment, frontier, hasFrontier, false)
			if boundaryErr != nil {
				return laneReadResult{}, boundaryErr
			}
			if index > 0 && segment.base != boundaries[index-1].nextSequence {
				return laneReadResult{}, errors.New("segment base sequence regressed")
			}
			if boundary.nextSequence > durableEndSequence {
				return laneReadResult{}, errors.New("segment exceeds durable frontier")
			}
			boundaries[index] = boundary
		}
		reader.segments = append(reader.segments[:0], segments...)
		reader.boundaries = append(reader.boundaries[:0], boundaries...)
		reader.segmentCacheValid = true
	}
	if len(segments) == 0 && !isRetired {
		return laneReadResult{}, errors.New("non-retired lane segments are missing")
	}
	if durableEndSequence > floor && len(segments) == 0 {
		return laneReadResult{}, errors.New("lane segments are missing below durable end")
	}

	if len(boundaries) > 0 && durableEndSequence > floor && boundaries[0].base > floor {
		return laneReadResult{}, errors.New("lane segment coverage starts after retention floor")
	}
	if len(boundaries) > 0 && boundaries[len(boundaries)-1].nextSequence != durableEndSequence {
		return laneReadResult{}, errors.New("lane segment coverage does not reach durable frontier")
	}

	startIndex := 0
	if reader.cursor.valid {
		for index, segment := range segments {
			if segment.base == reader.cursor.base {
				if reader.cursor.fileInfo == nil || boundaries[index].fileInfo == nil || reader.cursor.sequence != from || reader.cursor.path != segment.path || reader.cursor.open != segment.open || !os.SameFile(reader.cursor.fileInfo, boundaries[index].fileInfo) {
					reader.cursor = laneCursor{}
					break
				}
				startIndex = index
				if reader.cursor.offset >= boundaries[index].dataEnd && index+1 < len(segments) {
					reader.cursor = laneCursor{}
					startIndex++
				}
				break
			}
		}
		if reader.cursor.valid && startIndex >= len(segments) {
			reader.cursor = laneCursor{}
		}
	}
	if !reader.cursor.valid {
		for index, boundary := range boundaries {
			if from < boundary.nextSequence {
				startIndex = index
				break
			}
			if index == len(boundaries)-1 {
				startIndex = len(boundaries)
			}
		}
	}
	for index := startIndex; index < len(segments); index++ {
		if err := reader.resetForSegment(segments[index], boundaries[index], from); err != nil {
			return laneReadResult{}, err
		}
		segmentResult, err := readCursorSegment(s, reader.topic, reader.laneID, segments[index], boundaries[index], from, maxMessages-len(result.Batch.Messages), maxBytes-batchBytes, &reader.cursor)
		if err != nil {
			reader.cursor = laneCursor{}
			reader.clearSegmentCache()
			return laneReadResult{}, err
		}
		result.Batch.Messages = append(result.Batch.Messages, segmentResult.messages...)
		batchBytes += segmentResult.bytes
		if segmentResult.batchFull {
			result.NextSequence = reader.cursor.sequence
			result.AtEnd = false
			return result, nil
		}
		if !segmentResult.segmentAtEnd {
			return laneReadResult{}, errors.New("cursor reader stopped before segment end")
		}
		if index+1 < len(segments) {
			reader.cursor = laneCursor{}
			continue
		}
	}
	if len(result.Batch.Messages) > 0 {
		last := result.Batch.Messages[len(result.Batch.Messages)-1].Sequence
		if last == ^uint64(0) {
			return laneReadResult{}, errors.New("lane sequence is exhausted")
		}
		result.NextSequence = last + 1
	} else {
		result.NextSequence = from
	}
	result.AtEnd = from >= durableEndSequence || len(result.Batch.Messages) == 0
	if isRetired && result.NextSequence > durableEndSequence {
		return laneReadResult{}, errors.New("cursor next sequence exceeds retired lane")
	}
	return result, nil
}

func newLaneReader(s *storage, topic, laneID string) *laneReader {
	return &laneReader{storage: s, topic: topic, laneID: laneID}
}

func (r *laneReader) resetForSegment(segment segmentFile, boundary segmentBoundary, from uint64) error {
	if r.cursor.valid && r.cursor.base == segment.base && r.cursor.sequence == from && r.cursor.offset <= boundary.dataEnd {
		r.cursor.boundary = boundary
		return nil
	}
	r.cursor = laneCursor{
		valid:    true,
		base:     segment.base,
		path:     segment.path,
		open:     segment.open,
		fileInfo: boundary.fileInfo,
		offset:   int64(boundary.header.Length),
		sequence: segment.base,
		boundary: boundary,
	}
	return nil
}

func (r *laneReader) fastBoundary(frontier durableEnd, hasFrontier bool) (segmentBoundary, bool) {
	if !r.cursor.valid || r.cursor.fileInfo == nil {
		return segmentBoundary{}, false
	}
	boundary := r.cursor.boundary
	if boundary.base != r.cursor.base || boundary.fileInfo == nil {
		return segmentBoundary{}, false
	}
	if !r.cursor.open {
		return boundary, boundary.hasFooter && r.cursor.validatedFile
	}
	if !hasFrontier || frontier.ActiveSegmentBase != r.cursor.base || frontier.DurableByteEnd < int64(boundary.header.Length) || frontier.NextSequence < r.cursor.sequence {
		return segmentBoundary{}, false
	}
	boundary.dataEnd = frontier.DurableByteEnd
	boundary.nextSequence = frontier.NextSequence
	r.cursor.boundary = boundary
	return boundary, true
}

func (r *laneReader) updateCachedBoundary(boundary segmentBoundary) {
	for index := range r.segments {
		if r.segments[index].base == boundary.base && r.segments[index].path == r.cursor.path {
			r.boundaries[index] = boundary
			return
		}
	}
}

func (r *laneReader) clearSegmentCache() {
	r.segments = nil
	r.boundaries = nil
	r.segmentCacheValid = false
}

type segmentBoundary struct {
	base         uint64
	nextSequence uint64
	dataEnd      int64
	header       segmentHeader
	footer       segmentFooter
	hasFooter    bool
	fileInfo     os.FileInfo
}

func (s *storage) laneCoordinates(topic, laneID string) (retentionIndex, durableEnd, bool, bool, uint64, error) {
	floor, err := s.retention(topic, laneID)
	if err != nil {
		return retentionIndex{}, durableEnd{}, false, false, 0, err
	}
	frontier, frontierErr := s.durableEnd(topic, laneID)
	marker, isRetired, retiredErr := s.retired(topic, laneID)
	if retiredErr != nil {
		return retentionIndex{}, durableEnd{}, false, false, 0, retiredErr
	}
	if frontierErr != nil && (!isRetired || !errors.Is(frontierErr, os.ErrNotExist)) {
		return retentionIndex{}, durableEnd{}, false, false, 0, frontierErr
	}
	if frontierErr == nil && isRetired && frontier.NextSequence != marker.FinalNextSequence {
		return retentionIndex{}, durableEnd{}, false, false, 0, errors.New("retired marker and durable frontier disagree")
	}
	durableEndSequence := uint64(0)
	if frontierErr == nil {
		durableEndSequence = frontier.NextSequence
	}
	if isRetired && marker.FinalNextSequence > durableEndSequence {
		durableEndSequence = marker.FinalNextSequence
	}
	if floor.EarliestRetainedSeq > durableEndSequence {
		return retentionIndex{}, durableEnd{}, false, false, 0, errors.New("retention floor exceeds durable end")
	}
	return floor, frontier, frontierErr == nil, isRetired, durableEndSequence, nil
}

func readSegmentBoundary(s *storage, topic, laneID string, segment segmentFile, frontier durableEnd, hasFrontier, validateData bool) (segmentBoundary, error) {
	stat, err := s.stat(segment.path)
	if err != nil {
		return segmentBoundary{}, err
	}
	if stat.Size() > s.maxSegmentFileBytes(topic) {
		return segmentBoundary{}, errors.New("segment exceeds configured size bound")
	}
	file, err := s.open(segment.path)
	if err != nil {
		return segmentBoundary{}, err
	}
	defer file.Close()
	header, err := readSegmentHeader(file, stat)
	if err != nil {
		return segmentBoundary{}, err
	}
	if header.Topic != topic || header.LaneID != laneID || header.BaseSequence != segment.base {
		return segmentBoundary{}, errors.New("segment identity does not match lane")
	}
	boundary := segmentBoundary{base: segment.base, header: header, fileInfo: stat}
	if segment.open {
		if !hasFrontier || frontier.ActiveSegmentBase != segment.base || frontier.DurableByteEnd < int64(header.Length) || frontier.DurableByteEnd > stat.Size() {
			return segmentBoundary{}, errors.New("active segment does not match durable frontier")
		}
		boundary.dataEnd = frontier.DurableByteEnd
		boundary.nextSequence = frontier.NextSequence
		return boundary, nil
	}
	if stat.Size() < int64(header.Length+segmentFooterBytes) {
		return segmentBoundary{}, errors.New("sealed segment footer is truncated")
	}
	footerBytes := make([]byte, segmentFooterBytes)
	if err := readAtFull(file, stat.Size()-segmentFooterBytes, footerBytes); err != nil {
		return segmentBoundary{}, err
	}
	footer, err := decodeSegmentFooter(footerBytes)
	if err != nil {
		return segmentBoundary{}, err
	}
	if footer.FileLength != uint64(stat.Size()) || footer.FileLength < uint64(header.Length+segmentFooterBytes) {
		return segmentBoundary{}, errors.New("sealed segment file length is invalid")
	}
	if footer.NextSequence < segment.base || footer.RecordCount == 0 || footer.NextSequence-segment.base != footer.RecordCount || footer.MaxAppendNanos < footer.MinAppendNanos {
		return segmentBoundary{}, errors.New("sealed segment sequence range is invalid")
	}
	if validateData {
		dataChecksum, err := crcFileRange(file, stat.Size()-segmentFooterBytes)
		if err != nil {
			return segmentBoundary{}, err
		}
		if footer.DataChecksum != dataChecksum {
			return segmentBoundary{}, errors.New("sealed segment data checksum mismatch")
		}
	}
	boundary.dataEnd = stat.Size() - segmentFooterBytes
	boundary.nextSequence = footer.NextSequence
	boundary.footer = footer
	boundary.hasFooter = true
	return boundary, nil
}

func readSegmentHeader(file fileHandle, stat os.FileInfo) (segmentHeader, error) {
	if stat.Size() < int64(segmentHeaderFixedBytes) {
		return segmentHeader{}, errors.New("segment header is truncated")
	}
	prefix := make([]byte, 8)
	if err := readAtFull(file, 0, prefix); err != nil {
		return segmentHeader{}, err
	}
	headerLength := int(binary.BigEndian.Uint16(prefix[6:8]))
	if headerLength < segmentHeaderFixedBytes || headerLength > maxRecordHeaderBytes || int64(headerLength) > stat.Size() {
		return segmentHeader{}, errors.New("segment header length is invalid")
	}
	data := make([]byte, headerLength)
	if err := readAtFull(file, 0, data); err != nil {
		return segmentHeader{}, err
	}
	return decodeSegmentHeader(data)
}

func readCursorSegment(s *storage, topic, laneID string, segment segmentFile, boundary segmentBoundary, from uint64, maxMessages int, maxBytes int64, cursor *laneCursor) (cursorSegmentResult, error) {
	if maxMessages < 0 || maxBytes < 0 {
		return cursorSegmentResult{}, errors.New("segment read limits are exhausted")
	}
	cursor.path = segment.path
	cursor.open = segment.open
	file, err := s.open(segment.path)
	if err != nil {
		return cursorSegmentResult{}, err
	}
	defer file.Close()
	actualStat, err := file.Stat()
	if err != nil {
		return cursorSegmentResult{}, err
	}
	if actualStat == nil || boundary.fileInfo == nil || !os.SameFile(actualStat, boundary.fileInfo) || (!segment.open && actualStat.Size() != boundary.fileInfo.Size()) || (segment.open && actualStat.Size() < boundary.dataEnd) {
		return cursorSegmentResult{}, errors.New("cursor segment file identity changed")
	}
	if actualStat.Size() > s.maxSegmentFileBytes(topic) || boundary.dataEnd > actualStat.Size() {
		return cursorSegmentResult{}, errors.New("cursor segment exceeds configured boundary")
	}
	if !segment.open {
		// sealed segment 的內容不可變；第一次成功開啟時完成整檔 CRC，後續
		// batch 只沿用同一個 file identity 的 cursor。
		if !cursor.validatedFile {
			dataChecksum, err := crcFileRange(file, boundary.dataEnd)
			if err != nil {
				return cursorSegmentResult{}, err
			}
			if boundary.footer.DataChecksum != dataChecksum {
				return cursorSegmentResult{}, errors.New("sealed segment data checksum mismatch")
			}
		}
		cursor.validatedFile = true
	}
	if cursor.base != segment.base || cursor.sequence < segment.base || cursor.offset < int64(boundary.header.Length) || cursor.offset > boundary.dataEnd {
		return cursorSegmentResult{}, errors.New("lane cursor is outside segment boundary")
	}
	if cursor.fileInfo == nil || !os.SameFile(cursor.fileInfo, boundary.fileInfo) {
		return cursorSegmentResult{}, errors.New("lane cursor file identity changed")
	}
	position := cursor.offset
	expectedSequence := cursor.sequence
	previousAppend := cursor.lastAppend
	batchBytes := int64(0)
	result := cursorSegmentResult{messages: make([]DeliveredMessage, 0, maxMessages)}
	if maxMessages == 0 || maxBytes == 0 {
		result.batchFull = true
		return result, nil
	}
	for position < boundary.dataEnd {
		recordStart := position
		if boundary.dataEnd-position < 4 {
			return cursorSegmentResult{}, errors.New("record length is truncated")
		}
		lengthBytes := make([]byte, 4)
		if err := readAtFull(file, position, lengthBytes); err != nil {
			return cursorSegmentResult{}, err
		}
		bodyLength := uint64(binary.BigEndian.Uint32(lengthBytes))
		maxRecordBytes := s.topicMaxRecordBytes(topic)
		if maxRecordBytes < 4 || bodyLength+4 > uint64(maxRecordBytes) || bodyLength > uint64(boundary.dataEnd-position-4) {
			return cursorSegmentResult{}, errors.New("record length is invalid")
		}
		recordBytes := make([]byte, 4+int(bodyLength))
		copy(recordBytes[:4], lengthBytes)
		if err := readAtFull(file, position+4, recordBytes[4:]); err != nil {
			return cursorSegmentResult{}, err
		}
		record, consumed, err := decodeRecord(recordBytes, maxRecordBytes)
		if err != nil {
			return cursorSegmentResult{}, err
		}
		if record.Sequence != expectedSequence {
			return cursorSegmentResult{}, errors.New("record sequence is not continuous")
		}
		if !previousAppend.IsZero() && record.AppendTime.Before(previousAppend) {
			return cursorSegmentResult{}, errors.New("record append time is not monotonic")
		}
		priorAppend := previousAppend
		priorRecordCount := cursor.recordCount
		priorMinAppendNanos := cursor.minAppendNanos
		priorMaxAppendNanos := cursor.maxAppendNanos
		if cursor.recordCount == 0 {
			cursor.minAppendNanos = record.AppendTime.UnixNano()
		}
		cursor.maxAppendNanos = record.AppendTime.UnixNano()
		cursor.recordCount++
		previousAppend = record.AppendTime
		if record.Sequence == ^uint64(0) {
			return cursorSegmentResult{}, errors.New("record sequence is exhausted")
		}
		expectedSequence++
		position += int64(consumed)
		if record.Sequence < from {
			continue
		}
		payloadBytes := int64(len(record.Payload))
		if len(result.messages) > 0 && (len(result.messages) >= maxMessages || batchBytes+payloadBytes > maxBytes) {
			cursor.offset = recordStart
			cursor.sequence = record.Sequence
			cursor.lastAppend = priorAppend
			cursor.recordCount = priorRecordCount
			cursor.minAppendNanos = priorMinAppendNanos
			cursor.maxAppendNanos = priorMaxAppendNanos
			result.batchFull = true
			return result, nil
		}
		result.messages = append(result.messages, DeliveredMessage{
			Sequence:      record.Sequence,
			AppendTime:    record.AppendTime,
			MessageID:     append([]byte(nil), record.MessageID...),
			EventType:     record.EventType,
			SchemaVersion: record.SchemaVersion,
			Payload:       append([]byte(nil), record.Payload...),
		})
		batchBytes += payloadBytes
		result.bytes = batchBytes
		cursor.offset = position
		cursor.sequence = expectedSequence
		cursor.lastAppend = record.AppendTime
		if len(result.messages) >= maxMessages {
			result.batchFull = true
			return result, nil
		}
	}
	if expectedSequence != boundary.nextSequence {
		return cursorSegmentResult{}, errors.New("segment sequence end does not match boundary")
	}
	cursor.offset = boundary.dataEnd
	cursor.sequence = boundary.nextSequence
	cursor.lastAppend = previousAppend
	cursor.validatedFile = cursor.validatedFile || !segment.open
	if boundary.hasFooter && (cursor.recordCount != boundary.footer.RecordCount || cursor.minAppendNanos != boundary.footer.MinAppendNanos || cursor.maxAppendNanos != boundary.footer.MaxAppendNanos) {
		return cursorSegmentResult{}, errors.New("sealed segment footer record summary is invalid")
	}
	result.segmentAtEnd = true
	result.bytes = batchBytes
	return result, nil
}

func validateTopicAndLane(topic, laneID string) error {
	if err := validateTopicName(topic); err != nil {
		return err
	}
	return validateLaneID(laneID)
}

type segmentReadResult struct {
	nextSequence uint64
	messages     []DeliveredMessage
	batchFull    bool
}

func readSegment(s *storage, topic, laneID string, segment segmentFile, frontier durableEnd, from uint64, maxMessages int, maxBytes int64) (segmentReadResult, error) {
	if maxMessages < 0 || maxBytes < 0 {
		return segmentReadResult{}, errors.New("segment read limits are exhausted")
	}
	stat, err := s.stat(segment.path)
	if err != nil {
		return segmentReadResult{}, err
	}
	maxFileBytes := s.maxSegmentFileBytes(topic)
	if stat.Size() > maxFileBytes {
		return segmentReadResult{}, errors.New("segment exceeds configured size bound")
	}
	file, err := s.open(segment.path)
	if err != nil {
		return segmentReadResult{}, err
	}
	defer file.Close()
	headerPrefix := make([]byte, 8)
	if err := readAtFull(file, 0, headerPrefix); err != nil {
		return segmentReadResult{}, err
	}
	headerLength := int(binary.BigEndian.Uint16(headerPrefix[6:8]))
	if headerLength < segmentHeaderFixedBytes || headerLength > maxRecordHeaderBytes || int64(headerLength) > stat.Size() {
		return segmentReadResult{}, errors.New("segment header length is invalid")
	}
	headerBytes := make([]byte, headerLength)
	if err := readAtFull(file, 0, headerBytes); err != nil {
		return segmentReadResult{}, err
	}
	header, err := decodeSegmentHeader(headerBytes)
	if err != nil {
		return segmentReadResult{}, fmt.Errorf("segment %d header: %w", segment.base, err)
	}
	if header.Topic != topic || header.LaneID != laneID || header.BaseSequence != segment.base {
		return segmentReadResult{}, errors.New("segment identity does not match lane")
	}
	dataEnd := stat.Size()
	nextSequence := segment.base
	var footer segmentFooter
	hasFooter := false
	if segment.open {
		if frontier.ActiveSegmentBase != segment.base || frontier.DurableByteEnd < int64(header.Length) || frontier.DurableByteEnd > stat.Size() {
			return segmentReadResult{}, errors.New("active segment does not match durable frontier")
		}
		dataEnd = frontier.DurableByteEnd
		nextSequence = frontier.NextSequence
	} else {
		if stat.Size() < int64(header.Length+segmentFooterBytes) {
			return segmentReadResult{}, errors.New("sealed segment footer is truncated")
		}
		footerBytes := make([]byte, segmentFooterBytes)
		if err := readAtFull(file, stat.Size()-segmentFooterBytes, footerBytes); err != nil {
			return segmentReadResult{}, err
		}
		footer, err = decodeSegmentFooter(footerBytes)
		if err != nil {
			return segmentReadResult{}, err
		}
		hasFooter = true
		if footer.FileLength != uint64(stat.Size()) || footer.FileLength < uint64(segmentFooterBytes+header.Length) {
			return segmentReadResult{}, errors.New("sealed segment file length is invalid")
		}
		dataEnd = stat.Size() - segmentFooterBytes
		dataChecksum, err := crcFileRange(file, dataEnd)
		if err != nil {
			return segmentReadResult{}, err
		}
		if got, want := footer.DataChecksum, dataChecksum; got != want {
			return segmentReadResult{}, errors.New("sealed segment data checksum mismatch")
		}
		nextSequence = footer.NextSequence
	}
	if dataEnd < int64(header.Length) || dataEnd > stat.Size() {
		return segmentReadResult{}, errors.New("segment durable byte boundary is invalid")
	}
	position := int64(header.Length)
	expectedSequence := segment.base
	result := segmentReadResult{nextSequence: nextSequence, messages: make([]DeliveredMessage, 0, maxMessages)}
	var recordCount uint64
	var minAppendNanos, maxAppendNanos int64
	var previousAppend time.Time
	batchBytes := int64(0)
	batchFull := maxMessages == 0 || maxBytes == 0
	for position < dataEnd {
		if dataEnd-position < 4 {
			return segmentReadResult{}, errors.New("record length is truncated")
		}
		lengthBytes := make([]byte, 4)
		if err := readAtFull(file, position, lengthBytes); err != nil {
			return segmentReadResult{}, fmt.Errorf("segment %d record length at byte %d: %w", segment.base, position, err)
		}
		bodyLength := uint64(binary.BigEndian.Uint32(lengthBytes))
		maxRecordBytes := s.topicMaxRecordBytes(topic)
		if maxRecordBytes < 4 || bodyLength+4 > uint64(maxRecordBytes) || bodyLength > uint64(dataEnd-position-4) {
			return segmentReadResult{}, errors.New("record length is invalid")
		}
		recordBytes := make([]byte, 4+int(bodyLength))
		copy(recordBytes[:4], lengthBytes)
		if err := readAtFull(file, position+4, recordBytes[4:]); err != nil {
			return segmentReadResult{}, fmt.Errorf("segment %d record at byte %d: %w", segment.base, position, err)
		}
		record, consumed, err := decodeRecord(recordBytes, maxRecordBytes)
		if err != nil {
			return segmentReadResult{}, fmt.Errorf("segment %d record at byte %d: %w", segment.base, position, err)
		}
		if record.Sequence != expectedSequence {
			return segmentReadResult{}, errors.New("record sequence is not continuous")
		}
		if !previousAppend.IsZero() && record.AppendTime.Before(previousAppend) {
			return segmentReadResult{}, errors.New("record append time is not monotonic")
		}
		if recordCount == 0 {
			minAppendNanos = record.AppendTime.UnixNano()
		}
		maxAppendNanos = record.AppendTime.UnixNano()
		recordCount++
		previousAppend = record.AppendTime
		if record.Sequence == ^uint64(0) {
			return segmentReadResult{}, errors.New("record sequence is exhausted")
		}
		expectedSequence++
		position += int64(consumed)
		if record.Sequence >= from && !batchFull {
			payloadBytes := int64(len(record.Payload))
			if len(result.messages) > 0 && (len(result.messages) >= maxMessages || batchBytes+payloadBytes > maxBytes) {
				// Batch 只能回傳 contiguous prefix，不能跳過這筆再收後面的 record。
				batchFull = true
			} else {
				result.messages = append(result.messages, DeliveredMessage{
					Sequence:      record.Sequence,
					AppendTime:    record.AppendTime,
					MessageID:     append([]byte(nil), record.MessageID...),
					EventType:     record.EventType,
					SchemaVersion: record.SchemaVersion,
					Payload:       append([]byte(nil), record.Payload...),
				})
				batchBytes += payloadBytes
				if len(result.messages) >= maxMessages {
					batchFull = true
				}
			}
		}
	}
	if expectedSequence != nextSequence {
		return segmentReadResult{}, fmt.Errorf("segment sequence end %d does not match frontier %d", expectedSequence, nextSequence)
	}
	if hasFooter {
		if recordCount == 0 || footer.RecordCount != recordCount || footer.MinAppendNanos != minAppendNanos || footer.MaxAppendNanos != maxAppendNanos {
			return segmentReadResult{}, errors.New("sealed segment footer record summary is invalid")
		}
	}
	result.batchFull = batchFull
	return result, nil
}

func readAtFull(file fileHandle, offset int64, data []byte) error {
	position := 0
	for position < len(data) {
		read, err := file.ReadAt(data[position:], offset+int64(position))
		position += read
		if err != nil {
			if errors.Is(err, io.EOF) && position == len(data) {
				return nil
			}
			return err
		}
		if read == 0 {
			return io.ErrUnexpectedEOF
		}
	}
	return nil
}

func crcFileRange(file fileHandle, length int64) (uint32, error) {
	if length < 0 {
		return 0, errors.New("CRC range is negative")
	}
	buffer := make([]byte, 1<<20)
	var checksum uint32
	var offset int64
	for offset < length {
		readLength := int64(len(buffer))
		if remaining := length - offset; remaining < readLength {
			readLength = remaining
		}
		if err := readAtFull(file, offset, buffer[:int(readLength)]); err != nil {
			return 0, err
		}
		checksum = crc32.Update(checksum, crc32cTable, buffer[:int(readLength)])
		offset += readLength
	}
	return checksum, nil
}

func crc32c(data []byte) uint32 {
	return crc32.Checksum(data, crc32cTable)
}

func (s *storage) topicMaxRecordBytes(topic string) int64 {
	manifest, err := s.topicManifest(topic)
	if err != nil {
		return 0
	}
	return manifest.MaxRecordBytes
}
