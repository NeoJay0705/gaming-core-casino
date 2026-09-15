package localmq

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	formatVersion           uint16 = 1
	metadataMagic                  = "LMQM"
	segmentMagic                   = "LMQW"
	footerMagic                    = "LMQF"
	maxMetadataBytes               = 16 << 20
	maxMetadataKindBytes           = 128
	maxRecordHeaderBytes           = 64 << 10
	segmentHeaderFixedBytes        = 4 + 2 + 2 + 8 + 8 + 2 + 2 + 4
	segmentFooterBytes             = 4 + 4 + 8 + 8 + 8 + 8 + 8 + 4 + 4
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

const (
	metadataKindStorageControl = "storage-control"
	metadataKindTopicManifest  = "topic-manifest"
	metadataKindTopicControl   = "topic-control"
	metadataKindLaneManifest   = "lane-manifest"
	metadataKindRetired        = "retired"
	metadataKindDurableEnd     = "durable-end"
	metadataKindRetentionIndex = "retention-index"
	metadataKindGroupManifest  = "group-manifest"
	metadataKindGroupControl   = "group-control"
	metadataKindCheckpointBase = "checkpoint-base"
	metadataKindCheckpoint     = "checkpoint"
	metadataKindBlocked        = "blocked"
	metadataKindAudit          = "audit"
)

type metadataPayload struct {
	Kind  string          `json:"kind"`
	Value json.RawMessage `json:"value"`
}

func encodeMetadata(kind string, value any) ([]byte, error) {
	if kind == "" || len(kind) > maxMetadataKindBytes {
		return nil, fmt.Errorf("localmq metadata kind is invalid")
	}
	valueBytes, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode metadata value: %w", err)
	}
	payload, err := json.Marshal(metadataPayload{Kind: kind, Value: valueBytes})
	if err != nil {
		return nil, fmt.Errorf("encode metadata payload: %w", err)
	}
	if len(payload) > maxMetadataBytes {
		return nil, fmt.Errorf("metadata payload exceeds %d bytes", maxMetadataBytes)
	}

	result := make([]byte, 10+len(payload)+4)
	copy(result[:4], metadataMagic)
	binary.BigEndian.PutUint16(result[4:6], formatVersion)
	binary.BigEndian.PutUint32(result[6:10], uint32(len(payload)))
	copy(result[10:10+len(payload)], payload)
	binary.BigEndian.PutUint32(result[10+len(payload):], crc32.Checksum(result[:10+len(payload)], crc32cTable))
	return result, nil
}

func decodeMetadata(data []byte, expectedKind string, target any) error {
	if len(data) < 14 {
		return errors.New("metadata is truncated")
	}
	if string(data[:4]) != metadataMagic {
		return errors.New("metadata magic mismatch")
	}
	if version := binary.BigEndian.Uint16(data[4:6]); version != formatVersion {
		return fmt.Errorf("metadata version %d is unsupported", version)
	}
	payloadSize := binary.BigEndian.Uint32(data[6:10])
	if payloadSize > maxMetadataBytes || uint64(payloadSize)+14 != uint64(len(data)) {
		return errors.New("metadata length is invalid")
	}
	checksumOffset := 10 + int(payloadSize)
	if got, want := binary.BigEndian.Uint32(data[checksumOffset:]), crc32.Checksum(data[:checksumOffset], crc32cTable); got != want {
		return errors.New("metadata checksum mismatch")
	}
	var payload metadataPayload
	if err := json.Unmarshal(data[10:checksumOffset], &payload); err != nil {
		return fmt.Errorf("decode metadata payload: %w", err)
	}
	if payload.Kind != expectedKind {
		return fmt.Errorf("metadata kind %q does not match %q", payload.Kind, expectedKind)
	}
	if target == nil {
		return nil
	}
	if err := json.Unmarshal(payload.Value, target); err != nil {
		return fmt.Errorf("decode metadata value: %w", err)
	}
	return nil
}

func readMetadata(path, expectedKind string, target any) error {
	return readMetadataWithOps(path, expectedKind, target, nil)
}

func readMetadataWithOps(path, expectedKind string, target any, ops *storageOps) error {
	if ops == nil {
		ops = newStorageOps()
	}
	info, err := ops.stat(path)
	if err != nil {
		return err
	}
	if info.Size() < 14 || info.Size() > int64(maxMetadataBytes)+14 {
		return errors.New("metadata file exceeds configured bound")
	}
	data, err := ops.readFile(path)
	if err != nil {
		return err
	}
	return decodeMetadata(data, expectedKind, target)
}

func writeMetadataAtomic(path, kind string, value any, perm os.FileMode) error {
	return writeMetadataAtomicWithOps(path, kind, value, perm, nil, "", "", "")
}

func writeMetadataAtomicWithOps(path, kind string, value any, perm os.FileMode, ops *storageOps, tempSyncPoint, renamePoint, parentSyncPoint string) error {
	data, err := encodeMetadata(kind, value)
	if err != nil {
		return err
	}
	return writeFileAtomicWithOps(path, data, perm, ops, tempSyncPoint, renamePoint, parentSyncPoint)
}

func writeMetadataExclusive(path, kind string, value any, perm os.FileMode) error {
	return writeMetadataExclusiveWithOpsAtPoint(path, kind, value, perm, nil, "")
}

func writeMetadataExclusiveWithOps(path, kind string, value any, perm os.FileMode, ops *storageOps) error {
	return writeMetadataExclusiveWithOpsAtPoint(path, kind, value, perm, ops, "")
}

func writeMetadataExclusiveWithOpsAtPoint(path, kind string, value any, perm os.FileMode, ops *storageOps, faultPoint string) error {
	data, err := encodeMetadata(kind, value)
	if err != nil {
		return err
	}
	if ops == nil {
		ops = newStorageOps()
	}
	if err := ops.mkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	if err := ops.fail(faultPoint); err != nil {
		return err
	}
	file, err := ops.openFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return err
	}
	if err := writeAll(file, data); err != nil {
		_ = file.Close()
		return err
	}
	closeErr := func() error {
		if err := file.Sync(); err != nil {
			return err
		}
		return file.Close()
	}()
	if closeErr != nil {
		_ = file.Close()
		return closeErr
	}
	return ops.syncDir(filepath.Dir(path))
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) (err error) {
	return writeFileAtomicWithOps(path, data, perm, nil, "", "", "")
}

func writeFileAtomicWithOps(path string, data []byte, perm os.FileMode, ops *storageOps, tempSyncPoint, renamePoint, parentSyncPoint string) (err error) {
	if ops == nil {
		ops = newStorageOps()
	}
	dir := filepath.Dir(path)
	if err := ops.mkdirAll(dir, 0o750); err != nil {
		return err
	}
	temporary, temporaryName, err := ops.createTemp(dir, ".localmq-tmp-")
	if err != nil {
		return err
	}
	temporaryClosed := false
	defer func() {
		if !temporaryClosed {
			_ = temporary.Close()
		}
		if err != nil {
			_ = ops.remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(perm); err != nil {
		return err
	}
	if err := writeAll(temporary, data); err != nil {
		return err
	}
	if err := ops.fail(tempSyncPoint); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync temporary file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	temporaryClosed = true
	if err := ops.fail(renamePoint); err != nil {
		return err
	}
	if err := ops.rename(temporaryName, path); err != nil {
		return err
	}
	if err := ops.fail(parentSyncPoint); err != nil {
		return err
	}
	if err := ops.syncDir(dir); err != nil {
		return fmt.Errorf("sync metadata directory: %w", err)
	}
	return nil
}

func writeAll(file fileHandle, data []byte) error {
	for len(data) > 0 {
		written, err := file.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func syncDirectory(path string) error {
	return newStorageOps().syncDir(path)
}

type storageControl struct {
	FormatVersion         uint16 `json:"format_version"`
	StorageID             string `json:"storage_id"`
	ProducerStopFreeBytes int64  `json:"producer_stop_free_bytes"`
	RecoveryReserveBytes  int64  `json:"recovery_reserve_bytes"`
	MaxClockSkewNanos     int64  `json:"max_clock_skew_nanos"`
	UpdatedAtUnixNano     int64  `json:"updated_at_unix_nano"`
}

type topicManifest struct {
	FormatVersion     uint16 `json:"format_version"`
	Topic             string `json:"topic"`
	MaxRecordBytes    int64  `json:"max_record_bytes"`
	CreatedAtUnixNano int64  `json:"created_at_unix_nano"`
}

type topicControl struct {
	FormatVersion        uint16 `json:"format_version"`
	RetentionTimeSeconds int64  `json:"retention_time_seconds"`
	UpdatedAtUnixNano    int64  `json:"updated_at_unix_nano"`
}

type laneManifest struct {
	FormatVersion      uint16 `json:"format_version"`
	StorageID          string `json:"storage_id"`
	Topic              string `json:"topic"`
	LaneID             string `json:"lane_id"`
	ProducerInstanceID string `json:"producer_instance_id"`
	WriterLocation     string `json:"writer_location,omitempty"`
	ApplicationVersion string `json:"application_version,omitempty"`
	CreatedAtUnixNano  int64  `json:"created_at_unix_nano"`
}

type retiredMarker struct {
	FormatVersion       uint16 `json:"format_version"`
	Topic               string `json:"topic"`
	LaneID              string `json:"lane_id"`
	FinalNextSequence   uint64 `json:"final_next_sequence"`
	RetiredAtUnixNano   int64  `json:"retired_at_unix_nano"`
	RetireReason        string `json:"retire_reason"`
	RetireRequestID     string `json:"retire_request_id,omitempty"`
	FencingEvidenceHash string `json:"fencing_evidence_hash,omitempty"`
}

type durableEnd struct {
	FormatVersion     uint16 `json:"format_version"`
	StorageID         string `json:"storage_id"`
	Topic             string `json:"topic"`
	LaneID            string `json:"lane_id"`
	ActiveSegmentBase uint64 `json:"active_segment_base_sequence"`
	NextSequence      uint64 `json:"next_sequence"`
	DurableByteEnd    int64  `json:"durable_byte_end"`
	UpdatedAtUnixNano int64  `json:"updated_at_unix_nano"`
}

type retentionIndex struct {
	FormatVersion       uint16 `json:"format_version"`
	Topic               string `json:"topic"`
	LaneID              string `json:"lane_id"`
	EarliestRetainedSeq uint64 `json:"earliest_retained_sequence"`
	UpdatedAtUnixNano   int64  `json:"updated_at_unix_nano"`
}

type groupManifest struct {
	FormatVersion     uint16 `json:"format_version"`
	Topic             string `json:"topic"`
	Group             string `json:"group"`
	GroupType         string `json:"group_type"`
	InitialPosition   string `json:"initial_position"`
	CreatedAtUnixNano int64  `json:"created_at_unix_nano"`
}

type groupControl struct {
	FormatVersion     uint16 `json:"format_version"`
	State             string `json:"state"`
	UpdatedAtUnixNano int64  `json:"updated_at_unix_nano"`
	DeletedAtUnixNano int64  `json:"deleted_at_unix_nano,omitempty"`
}

type checkpointSnapshot struct {
	FormatVersion     uint16            `json:"format_version"`
	NextSequence      map[string]uint64 `json:"next_sequence"`
	UpdatedAtUnixNano int64             `json:"updated_at_unix_nano"`
}

type checkpointRecord struct {
	FormatVersion      uint16 `json:"format_version"`
	Topic              string `json:"topic"`
	Group              string `json:"group"`
	LaneID             string `json:"lane_id"`
	NextSequence       uint64 `json:"next_sequence"`
	ConsumerInstanceID string `json:"consumer_instance_id"`
	UpdatedAtUnixNano  int64  `json:"updated_at_unix_nano"`
}

type blockedRecord struct {
	FormatVersion      uint16 `json:"format_version"`
	Topic              string `json:"topic"`
	Group              string `json:"group"`
	LaneID             string `json:"lane_id"`
	Sequence           uint64 `json:"sequence"`
	ConsumerInstanceID string `json:"consumer_instance_id"`
	MessageID          string `json:"message_id"`
	ContentHash        string `json:"content_hash"`
	CreatedAtUnixNano  int64  `json:"created_at_unix_nano"`
	Error              string `json:"error"`
}

type auditRecord struct {
	FormatVersion     uint16 `json:"format_version"`
	RequestID         string `json:"request_id"`
	Operation         string `json:"operation"`
	Operator          string `json:"operator"`
	Reason            string `json:"reason"`
	Topic             string `json:"topic"`
	Group             string `json:"group,omitempty"`
	LaneID            string `json:"lane_id,omitempty"`
	Sequence          uint64 `json:"sequence,omitempty"`
	UpToSequence      uint64 `json:"up_to_sequence,omitempty"`
	MessageID         string `json:"message_id,omitempty"`
	ContentHash       string `json:"content_hash,omitempty"`
	CreatedAtUnixNano int64  `json:"created_at_unix_nano"`
}

type walRecord struct {
	Sequence      uint64
	AppendTime    time.Time
	MessageID     []byte
	EventType     string
	SchemaVersion uint32
	Payload       []byte
}

func encodeSegmentHeader(topic, laneID string, baseSequence uint64, createdAt time.Time) ([]byte, error) {
	if len(topic) > maxMetadataKindBytes*2 || len(laneID) > maxMetadataKindBytes*2 {
		return nil, errors.New("segment identity is too long")
	}
	length := segmentHeaderFixedBytes + len(topic) + len(laneID)
	result := make([]byte, length)
	copy(result[:4], segmentMagic)
	binary.BigEndian.PutUint16(result[4:6], formatVersion)
	binary.BigEndian.PutUint16(result[6:8], uint16(length))
	binary.BigEndian.PutUint64(result[8:16], baseSequence)
	binary.BigEndian.PutUint64(result[16:24], uint64(createdAt.UnixNano()))
	position := 24
	binary.BigEndian.PutUint16(result[position:position+2], uint16(len(topic)))
	position += 2
	copy(result[position:position+len(topic)], topic)
	position += len(topic)
	binary.BigEndian.PutUint16(result[position:position+2], uint16(len(laneID)))
	position += 2
	copy(result[position:position+len(laneID)], laneID)
	position += len(laneID)
	binary.BigEndian.PutUint32(result[position:], crc32.Checksum(result[:position], crc32cTable))
	return result, nil
}

type segmentHeader struct {
	Topic        string
	LaneID       string
	BaseSequence uint64
	CreatedAt    time.Time
	Length       int
}

func decodeSegmentHeader(data []byte) (segmentHeader, error) {
	if len(data) < segmentHeaderFixedBytes || string(data[:4]) != segmentMagic {
		return segmentHeader{}, errors.New("segment header is invalid")
	}
	if binary.BigEndian.Uint16(data[4:6]) != formatVersion {
		return segmentHeader{}, errors.New("segment header version is unsupported")
	}
	length := int(binary.BigEndian.Uint16(data[6:8]))
	if length < segmentHeaderFixedBytes || length > maxRecordHeaderBytes || len(data) < length {
		return segmentHeader{}, errors.New("segment header length is invalid")
	}
	if got, want := binary.BigEndian.Uint32(data[length-4:length]), crc32.Checksum(data[:length-4], crc32cTable); got != want {
		return segmentHeader{}, errors.New("segment header checksum mismatch")
	}
	position := 24
	topicLength := int(binary.BigEndian.Uint16(data[position : position+2]))
	position += 2
	if topicLength > length-position-2 {
		return segmentHeader{}, errors.New("segment topic length is invalid")
	}
	topic := string(data[position : position+topicLength])
	position += topicLength
	if position+2 > length-4 {
		return segmentHeader{}, errors.New("segment lane length is invalid")
	}
	laneLength := int(binary.BigEndian.Uint16(data[position : position+2]))
	position += 2
	if laneLength != length-position-4 {
		return segmentHeader{}, errors.New("segment identity length is invalid")
	}
	laneID := string(data[position : position+laneLength])
	return segmentHeader{
		Topic:        topic,
		LaneID:       laneID,
		BaseSequence: binary.BigEndian.Uint64(data[8:16]),
		CreatedAt:    time.Unix(0, int64(binary.BigEndian.Uint64(data[16:24]))).UTC(),
		Length:       length,
	}, nil
}

func encodeRecord(record walRecord, maxRecordBytes int64) ([]byte, error) {
	if len(record.MessageID) > 1<<16-1 || len(record.EventType) > 1<<16-1 {
		return nil, errors.New("record metadata field is too long")
	}
	bodyLength := 8 + 8 + 2 + len(record.MessageID) + 2 + len(record.EventType) + 4 + len(record.Payload) + 4
	if maxRecordBytes < 4 || int64(bodyLength+4) > maxRecordBytes || bodyLength > int(^uint32(0)) {
		return nil, fmt.Errorf("record exceeds %d bytes", maxRecordBytes)
	}
	result := make([]byte, 4+bodyLength)
	binary.BigEndian.PutUint32(result[:4], uint32(bodyLength))
	position := 4
	binary.BigEndian.PutUint64(result[position:position+8], record.Sequence)
	position += 8
	binary.BigEndian.PutUint64(result[position:position+8], uint64(record.AppendTime.UnixNano()))
	position += 8
	binary.BigEndian.PutUint16(result[position:position+2], uint16(len(record.MessageID)))
	position += 2
	copy(result[position:position+len(record.MessageID)], record.MessageID)
	position += len(record.MessageID)
	binary.BigEndian.PutUint16(result[position:position+2], uint16(len(record.EventType)))
	position += 2
	copy(result[position:position+len(record.EventType)], record.EventType)
	position += len(record.EventType)
	binary.BigEndian.PutUint32(result[position:position+4], record.SchemaVersion)
	position += 4
	copy(result[position:position+len(record.Payload)], record.Payload)
	position += len(record.Payload)
	binary.BigEndian.PutUint32(result[position:], crc32.Checksum(result[4:position], crc32cTable))
	return result, nil
}

func decodeRecord(data []byte, maxRecordBytes int64) (walRecord, int, error) {
	if len(data) < 4 {
		return walRecord{}, 0, io.ErrUnexpectedEOF
	}
	bodyLength := uint64(binary.BigEndian.Uint32(data[:4]))
	if maxRecordBytes < 4 || bodyLength < 8+8+2+2+4+4 || bodyLength+4 > uint64(maxRecordBytes) || bodyLength > uint64(len(data)-4) {
		return walRecord{}, 0, errors.New("record length is invalid")
	}
	recordBytes := data[4 : 4+int(bodyLength)]
	position := 0
	if len(recordBytes) < 8+8+2+2+4+4 {
		return walRecord{}, 0, errors.New("record header is truncated")
	}
	sequence := binary.BigEndian.Uint64(recordBytes[position : position+8])
	position += 8
	appendTime := time.Unix(0, int64(binary.BigEndian.Uint64(recordBytes[position:position+8]))).UTC()
	position += 8
	messageIDLength := int(binary.BigEndian.Uint16(recordBytes[position : position+2]))
	position += 2
	if messageIDLength > len(recordBytes)-position-2-4-4 {
		return walRecord{}, 0, errors.New("record message id length is invalid")
	}
	messageID := append([]byte(nil), recordBytes[position:position+messageIDLength]...)
	position += messageIDLength
	eventTypeLength := int(binary.BigEndian.Uint16(recordBytes[position : position+2]))
	position += 2
	if eventTypeLength > len(recordBytes)-position-4-4 {
		return walRecord{}, 0, errors.New("record event type length is invalid")
	}
	eventType := string(recordBytes[position : position+eventTypeLength])
	position += eventTypeLength
	if position+4+4 > len(recordBytes) {
		return walRecord{}, 0, errors.New("record payload header is truncated")
	}
	schemaVersion := binary.BigEndian.Uint32(recordBytes[position : position+4])
	position += 4
	payloadLength := len(recordBytes) - position - 4
	if payloadLength < 0 {
		return walRecord{}, 0, errors.New("record payload length is invalid")
	}
	payload := append([]byte(nil), recordBytes[position:position+payloadLength]...)
	position += payloadLength
	if got, want := binary.BigEndian.Uint32(recordBytes[position:]), crc32.Checksum(recordBytes[:position], crc32cTable); got != want {
		return walRecord{}, 0, errors.New("record checksum mismatch")
	}
	return walRecord{
		Sequence:      sequence,
		AppendTime:    appendTime,
		MessageID:     messageID,
		EventType:     eventType,
		SchemaVersion: schemaVersion,
		Payload:       payload,
	}, 4 + int(bodyLength), nil
}

type segmentFooter struct {
	FileLength     uint64
	NextSequence   uint64
	RecordCount    uint64
	MinAppendNanos int64
	MaxAppendNanos int64
	DataChecksum   uint32
}

func encodeSegmentFooter(footer segmentFooter) []byte {
	result := make([]byte, segmentFooterBytes)
	copy(result[:4], footerMagic)
	binary.BigEndian.PutUint32(result[4:8], uint32(segmentFooterBytes))
	binary.BigEndian.PutUint64(result[8:16], footer.FileLength)
	binary.BigEndian.PutUint64(result[16:24], footer.NextSequence)
	binary.BigEndian.PutUint64(result[24:32], footer.RecordCount)
	binary.BigEndian.PutUint64(result[32:40], uint64(footer.MinAppendNanos))
	binary.BigEndian.PutUint64(result[40:48], uint64(footer.MaxAppendNanos))
	binary.BigEndian.PutUint32(result[48:52], footer.DataChecksum)
	binary.BigEndian.PutUint32(result[52:56], crc32.Checksum(result[:52], crc32cTable))
	return result
}

func decodeSegmentFooter(data []byte) (segmentFooter, error) {
	if len(data) != segmentFooterBytes || string(data[:4]) != footerMagic {
		return segmentFooter{}, errors.New("segment footer is invalid")
	}
	if binary.BigEndian.Uint32(data[4:8]) != segmentFooterBytes || binary.BigEndian.Uint32(data[52:56]) != crc32.Checksum(data[:52], crc32cTable) {
		return segmentFooter{}, errors.New("segment footer checksum or length is invalid")
	}
	return segmentFooter{
		FileLength:     binary.BigEndian.Uint64(data[8:16]),
		NextSequence:   binary.BigEndian.Uint64(data[16:24]),
		RecordCount:    binary.BigEndian.Uint64(data[24:32]),
		MinAppendNanos: int64(binary.BigEndian.Uint64(data[32:40])),
		MaxAppendNanos: int64(binary.BigEndian.Uint64(data[40:48])),
		DataChecksum:   binary.BigEndian.Uint32(data[48:52]),
	}, nil
}

func contentHash(record walRecord) string {
	hash := sha256.New()
	_, _ = hash.Write(record.MessageID)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(record.EventType))
	_, _ = hash.Write([]byte{0})
	var schema [4]byte
	binary.BigEndian.PutUint32(schema[:], record.SchemaVersion)
	_, _ = hash.Write(schema[:])
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(record.Payload)
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func bytesHash(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:])
}

func cloneMessage(message Message) Message {
	message.MessageID = append([]byte(nil), message.MessageID...)
	message.Payload = append([]byte(nil), message.Payload...)
	return message
}

func validateMessage(message Message, topicMaxBytes int64) error {
	if err := validateTopicName(message.Topic); err != nil {
		return err
	}
	if len(message.MessageID) == 0 {
		return errors.New("message_id is required")
	}
	if len(message.EventType) == 0 || len(message.EventType) > 1<<16-1 {
		return errors.New("event_type is required and must be at most 65535 bytes")
	}
	if topicMaxBytes <= 0 {
		return errors.New("topic max_record_bytes is invalid")
	}
	// The exact encoded size is checked by encodeRecord. This early bound keeps
	// an oversized request out of the bounded writer queue.
	if int64(len(message.Payload)) > topicMaxBytes {
		return fmt.Errorf("payload exceeds topic max_record_bytes %d", topicMaxBytes)
	}
	return nil
}

func validateNonEmpty(value, label string) error {
	if value == "" || value != strings.TrimSpace(value) {
		return fmt.Errorf("%s is required and must not have surrounding whitespace", label)
	}
	return nil
}
