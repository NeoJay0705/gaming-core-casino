package localmq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	storageVersionDirectory = "v1"
	storageControlFile      = "storage-control"
	maintenanceLockFile     = "maintenance.lock"
	topicManifestFile       = "topic-manifest"
	topicControlFile        = "topic-control"
	laneManifestFile        = "manifest"
	retiredMarkerFile       = "retired"
	durableEndFile          = "durable-end"
	retentionIndexFile      = "retention-index"
	groupManifestFile       = "manifest"
	groupControlFile        = "control"
	checkpointBaseFile      = "checkpoint-base"
	segmentsDirectory       = "segments"
	lanesDirectory          = "lanes"
	groupsDirectory         = "groups"
	blockedDirectory        = "blocked"
	membersDirectory        = "members"
	gcStagingDirectory      = ".gc-staging"
	maxStorageEntries       = 100000
)

type capacityState uint8

const (
	capacityNormal capacityState = iota
	capacityProducerStop
	capacityRecoveryOnly
)

// InitStorageRequest 描述一次明確的 storage bootstrap。初始化只允許建立
// root control，不會建立 Topic、Group 或 lane。
type InitStorageRequest struct {
	RootPath              string
	StorageID             string
	ProducerStopFreeBytes int64
	RecoveryReserveBytes  int64
	MaxClockSkew          time.Duration
}

// storage 封裝 layout 與 metadata identity；它不持有 writer ownership。
type storage struct {
	cfg Config
	now func() time.Time
	ops *storageOps

	mu      sync.RWMutex
	started bool
}

func newStorage(cfg Config) *storage {
	return newStorageWithDeps(cfg, newStorageOps(), time.Now)
}

func newStorageWithDeps(cfg Config, ops *storageOps, now func() time.Time) *storage {
	if ops == nil {
		ops = newStorageOps()
	}
	if now == nil {
		now = time.Now
	}
	return &storage{cfg: cfg, now: now, ops: ops}
}

// InitStorage 建立或驗證 durable root control。若 control 已存在，所有
// identity 與 capacity policy 必須完全相同，避免不同 pod 靜默改寫 root policy。
func InitStorage(ctx context.Context, request InitStorageRequest) error {
	return initStorageWithOps(ctx, request, newStorageOps(), time.Now)
}

func initStorageWithOps(ctx context.Context, request InitStorageRequest, ops *storageOps, now func() time.Time) error {
	if ops == nil {
		ops = newStorageOps()
	}
	if now == nil {
		now = time.Now
	}
	if ctx == nil {
		return errors.New("localmq init storage context is nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("localmq init storage cancelled: %w", err)
	}
	cfg := DefaultConfig()
	cfg.RootPath = request.RootPath
	cfg.StorageID = request.StorageID
	if request.ProducerStopFreeBytes != 0 {
		cfg.ProducerStopFreeBytes = request.ProducerStopFreeBytes
	}
	if request.RecoveryReserveBytes != 0 {
		cfg.RecoveryReserveBytes = request.RecoveryReserveBytes
	}
	if request.MaxClockSkew != 0 {
		cfg.MaxClockSkew = request.MaxClockSkew
	}
	if err := validateConfig(&cfg); err != nil {
		return err
	}
	if _, err := parseStorageID(cfg.StorageID); err != nil {
		return err
	}
	root := filepath.Join(filepath.Clean(cfg.RootPath), storageVersionDirectory)
	if err := ops.mkdirAll(root, 0o750); err != nil {
		return fmt.Errorf("localmq create storage root: %w", err)
	}
	controlPath := filepath.Join(root, storageControlFile)
	var existing storageControl
	if err := readMetadataWithOps(controlPath, metadataKindStorageControl, &existing, ops); err == nil {
		if err := validateStorageControl(existing); err != nil {
			return err
		}
		if existing.StorageID != cfg.StorageID || existing.ProducerStopFreeBytes != cfg.ProducerStopFreeBytes || existing.RecoveryReserveBytes != cfg.RecoveryReserveBytes || existing.MaxClockSkewNanos != cfg.MaxClockSkew.Nanoseconds() {
			return errors.New("localmq storage-control conflicts with requested policy")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("localmq read storage-control: %w", err)
	}
	control := storageControl{
		FormatVersion:         formatVersion,
		StorageID:             cfg.StorageID,
		ProducerStopFreeBytes: cfg.ProducerStopFreeBytes,
		RecoveryReserveBytes:  cfg.RecoveryReserveBytes,
		MaxClockSkewNanos:     cfg.MaxClockSkew.Nanoseconds(),
		UpdatedAtUnixNano:     now().UnixNano(),
	}
	if err := writeMetadataExclusiveWithOps(controlPath, metadataKindStorageControl, control, 0o640, ops); err != nil {
		if errors.Is(err, os.ErrExist) {
			if readErr := readMetadataWithOps(controlPath, metadataKindStorageControl, &existing, ops); readErr != nil {
				return readErr
			}
			if existing.StorageID != cfg.StorageID || existing.ProducerStopFreeBytes != cfg.ProducerStopFreeBytes || existing.RecoveryReserveBytes != cfg.RecoveryReserveBytes || existing.MaxClockSkewNanos != cfg.MaxClockSkew.Nanoseconds() {
				return errors.New("localmq storage-control conflicts with requested policy")
			}
			return nil
		}
		return fmt.Errorf("localmq write storage-control: %w", err)
	}
	return nil
}

func (s *storage) start(ctx context.Context) error {
	if s == nil {
		return errors.New("localmq storage is nil")
	}
	if ctx == nil {
		return errors.New("localmq storage context is nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("localmq storage start cancelled: %w", err)
	}
	root := s.root()
	info, err := s.stat(root)
	if err != nil {
		return fmt.Errorf("localmq storage root: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("localmq storage root %q is not a directory", root)
	}
	var control storageControl
	if err := readMetadataWithOps(filepath.Join(root, storageControlFile), metadataKindStorageControl, &control, s.fsOps()); err != nil {
		return fmt.Errorf("localmq storage-control: %w", err)
	}
	if err := validateStorageControl(control); err != nil {
		return err
	}
	if control.StorageID != s.cfg.StorageID {
		return fmt.Errorf("localmq storage identity mismatch: control=%q config=%q", control.StorageID, s.cfg.StorageID)
	}
	if control.ProducerStopFreeBytes != s.cfg.ProducerStopFreeBytes || control.RecoveryReserveBytes != s.cfg.RecoveryReserveBytes || control.MaxClockSkewNanos != s.cfg.MaxClockSkew.Nanoseconds() {
		return errors.New("localmq storage-control policy conflicts with runtime config")
	}
	s.mu.Lock()
	s.started = true
	s.mu.Unlock()
	return nil
}

func (s *storage) stop() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.started = false
	s.mu.Unlock()
}

func (s *storage) isStarted() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.started
}

func (s *storage) root() string {
	return filepath.Join(s.cfg.RootPath, storageVersionDirectory)
}

func (s *storage) topicDir(topic string) string {
	return filepath.Join(s.root(), "topics", topic)
}

func (s *storage) lanesDir(topic string) string {
	return filepath.Join(s.topicDir(topic), lanesDirectory)
}

func (s *storage) laneDir(topic, laneID string) string {
	return filepath.Join(s.lanesDir(topic), laneID)
}

func (s *storage) segmentsDir(topic, laneID string) string {
	return filepath.Join(s.laneDir(topic, laneID), segmentsDirectory)
}

func (s *storage) groupDir(topic, group string) string {
	return filepath.Join(s.root(), groupsDirectory, topic, group)
}

func (s *storage) groupMembersDir(topic, group string) string {
	return filepath.Join(s.groupDir(topic, group), membersDirectory)
}

func (s *storage) topicManifest(topic string) (topicManifest, error) {
	var manifest topicManifest
	if err := readMetadataWithOps(filepath.Join(s.topicDir(topic), topicManifestFile), metadataKindTopicManifest, &manifest, s.fsOps()); err != nil {
		return topicManifest{}, err
	}
	if manifest.FormatVersion != formatVersion || manifest.Topic != topic || manifest.MaxRecordBytes <= 0 {
		return topicManifest{}, errors.New("topic manifest identity or format is invalid")
	}
	return manifest, nil
}

func (s *storage) topicControl(topic string) (topicControl, error) {
	var control topicControl
	if err := readMetadataWithOps(filepath.Join(s.topicDir(topic), topicControlFile), metadataKindTopicControl, &control, s.fsOps()); err != nil {
		return topicControl{}, err
	}
	if control.FormatVersion != formatVersion || control.RetentionTimeSeconds < 0 {
		return topicControl{}, errors.New("topic control identity or format is invalid")
	}
	return control, nil
}

func (s *storage) laneManifest(topic, laneID string) (laneManifest, error) {
	var manifest laneManifest
	if err := readMetadataWithOps(filepath.Join(s.laneDir(topic, laneID), laneManifestFile), metadataKindLaneManifest, &manifest, s.fsOps()); err != nil {
		return laneManifest{}, err
	}
	if manifest.FormatVersion != formatVersion || manifest.StorageID != s.cfg.StorageID || manifest.Topic != topic || manifest.LaneID != laneID {
		return laneManifest{}, errors.New("lane manifest identity or format is invalid")
	}
	if err := validateLaneID(manifest.LaneID); err != nil {
		return laneManifest{}, err
	}
	return manifest, nil
}

func (s *storage) durableEnd(topic, laneID string) (durableEnd, error) {
	var frontier durableEnd
	if err := readMetadataWithOps(filepath.Join(s.laneDir(topic, laneID), durableEndFile), metadataKindDurableEnd, &frontier, s.fsOps()); err != nil {
		return durableEnd{}, err
	}
	if frontier.FormatVersion != formatVersion || frontier.StorageID != s.cfg.StorageID || frontier.Topic != topic || frontier.LaneID != laneID || frontier.DurableByteEnd < 0 {
		return durableEnd{}, errors.New("durable-end identity or format is invalid")
	}
	return frontier, nil
}

func (s *storage) retention(topic, laneID string) (retentionIndex, error) {
	var index retentionIndex
	if err := readMetadataWithOps(filepath.Join(s.laneDir(topic, laneID), retentionIndexFile), metadataKindRetentionIndex, &index, s.fsOps()); err != nil {
		return retentionIndex{}, err
	}
	if index.FormatVersion != formatVersion || index.Topic != topic || index.LaneID != laneID {
		return retentionIndex{}, errors.New("retention index identity or format is invalid")
	}
	return index, nil
}

func (s *storage) retired(topic, laneID string) (retiredMarker, bool, error) {
	var marker retiredMarker
	err := readMetadataWithOps(filepath.Join(s.laneDir(topic, laneID), retiredMarkerFile), metadataKindRetired, &marker, s.fsOps())
	if errors.Is(err, os.ErrNotExist) {
		return retiredMarker{}, false, nil
	}
	if err != nil {
		return retiredMarker{}, false, err
	}
	if marker.FormatVersion != formatVersion || marker.Topic != topic || marker.LaneID != laneID {
		return retiredMarker{}, false, errors.New("retired marker identity or format is invalid")
	}
	if marker.RetireRequestID != "" {
		if err := validateRequestID(marker.RetireRequestID); err != nil {
			return retiredMarker{}, false, err
		}
		if len(marker.FencingEvidenceHash) != 64 {
			return retiredMarker{}, false, errors.New("retired marker fencing evidence hash is invalid")
		}
	} else if marker.FencingEvidenceHash != "" {
		return retiredMarker{}, false, errors.New("retired marker request identity is incomplete")
	}
	return marker, true, nil
}

func (s *storage) groupManifest(topic, group string) (groupManifest, error) {
	var manifest groupManifest
	if err := readMetadataWithOps(filepath.Join(s.groupDir(topic, group), groupManifestFile), metadataKindGroupManifest, &manifest, s.fsOps()); err != nil {
		return groupManifest{}, err
	}
	if manifest.FormatVersion != formatVersion || manifest.Topic != topic || manifest.Group != group || !validGroupType(manifest.GroupType) || !validInitialPosition(manifest.InitialPosition) {
		return groupManifest{}, errors.New("group manifest identity or format is invalid")
	}
	return manifest, nil
}

func (s *storage) groupControl(topic, group string) (groupControl, error) {
	var control groupControl
	if err := readMetadataWithOps(filepath.Join(s.groupDir(topic, group), groupControlFile), metadataKindGroupControl, &control, s.fsOps()); err != nil {
		return groupControl{}, err
	}
	if control.FormatVersion != formatVersion || !validGroupState(control.State) {
		return groupControl{}, errors.New("group control state or format is invalid")
	}
	return control, nil
}

func (s *storage) listLanes(topic string) ([]string, error) {
	entries, err := s.readDir(s.lanesDir(topic))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(entries) > maxStorageEntries {
		return nil, errors.New("lane directory exceeds configured bound")
	}
	lanes := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		if _, err := s.laneManifest(topic, entry.Name()); err != nil {
			return nil, fmt.Errorf("lane %q: %w", entry.Name(), err)
		}
		lanes = append(lanes, entry.Name())
	}
	sort.Strings(lanes)
	return lanes, nil
}

func (s *storage) listTopics() ([]string, error) {
	entries, err := s.readDir(filepath.Join(s.root(), "topics"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(entries) > maxStorageEntries {
		return nil, errors.New("topic directory exceeds configured bound")
	}
	topics := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") {
			topics = append(topics, entry.Name())
		}
	}
	sort.Strings(topics)
	return topics, nil
}

func (s *storage) listGroups(topic string) ([]string, error) {
	entries, err := s.readDir(filepath.Join(s.root(), groupsDirectory, topic))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(entries) > maxStorageEntries {
		return nil, errors.New("group directory exceeds configured bound")
	}
	groups := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") {
			groups = append(groups, entry.Name())
		}
	}
	sort.Strings(groups)
	return groups, nil
}

func (s *storage) freeBytes() (uint64, error) {
	return s.fsOps().statfs(s.root())
}

func (s *storage) capacity() (capacityState, uint64, error) {
	free, err := s.freeBytes()
	if err != nil {
		return capacityNormal, 0, err
	}
	if free < uint64(s.cfg.RecoveryReserveBytes) {
		return capacityRecoveryOnly, free, nil
	}
	if free < uint64(s.cfg.ProducerStopFreeBytes) {
		return capacityProducerStop, free, nil
	}
	return capacityNormal, free, nil
}

func (s *storage) maxSegmentFileBytes(topic string) int64 {
	maxRecordBytes := int64(maxMetadataBytes)
	if manifest, err := s.topicManifest(topic); err == nil && manifest.MaxRecordBytes > maxRecordBytes {
		maxRecordBytes = manifest.MaxRecordBytes
	}
	return s.cfg.SegmentMaxBytes + maxRecordBytes + segmentFooterBytes
}

func parseStorageID(value string) (uuid.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, fmt.Errorf("localmq storage_id must be a UUIDv7: %w", err)
	}
	if id.Version() != 7 {
		return uuid.Nil, fmt.Errorf("localmq storage_id must be UUIDv7, got version %d", id.Version())
	}
	return id, nil
}

func newUUIDv7() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generate UUIDv7: %w", err)
	}
	return id.String(), nil
}

func validateStorageControl(control storageControl) error {
	if control.FormatVersion != formatVersion {
		return errors.New("storage-control format is unsupported")
	}
	if _, err := parseStorageID(control.StorageID); err != nil {
		return err
	}
	if control.ProducerStopFreeBytes <= control.RecoveryReserveBytes || control.RecoveryReserveBytes <= 0 || control.MaxClockSkewNanos < 0 {
		return errors.New("storage-control capacity policy is invalid")
	}
	return nil
}
