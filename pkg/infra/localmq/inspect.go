package localmq

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const defaultInspectionLimit = 100

// Inspector 提供 bounded、read-only、payload-redacted 的運維查詢。
type Inspector struct {
	client *Client
}

func NewInspector(client *Client) (*Inspector, error) {
	if client == nil {
		return nil, errors.New("localmq inspector client is nil")
	}
	return &Inspector{client: client}, nil
}

type StorageInspection struct {
	RootPath              string
	StorageID             string
	ProducerStopFreeBytes int64
	RecoveryReserveBytes  int64
	FilesystemFreeBytes   uint64
	Topics                []string
}

type TopicInspection struct {
	Topic          string
	MaxRecordBytes int64
	Retention      time.Duration
	Lanes          []string
	Groups         []string
}

type LaneInspection struct {
	Topic              string
	LaneID             string
	ProducerInstanceID string
	CreatedAtUnixNano  int64
	RetentionFloor     uint64
	DurableEnd         uint64
	Retired            bool
	Segments           []SegmentInspection
}

type SegmentInspection struct {
	BaseSequence   uint64
	Sealed         bool
	SizeBytes      int64
	NextSequence   uint64
	RecordCount    uint64
	MinAppendNanos int64
	MaxAppendNanos int64
}

type GroupInspection struct {
	Topic           string
	Group           string
	GroupType       GroupType
	InitialPosition InitialPosition
	State           GroupState
	CheckpointBase  map[string]uint64
	MemberCount     int
}

type BlockedInspection struct {
	Topic              string
	Group              string
	LaneID             string
	Sequence           uint64
	ConsumerInstanceID string
	MessageID          string
	ContentHash        string
	CreatedAtUnixNano  int64
	Error              string
}

func (i *Inspector) InspectStorage(ctx context.Context) (StorageInspection, error) {
	if err := i.ready(ctx); err != nil {
		return StorageInspection{}, err
	}
	var control storageControl
	if err := readMetadataWithOps(filepath.Join(i.client.storage.root(), storageControlFile), metadataKindStorageControl, &control, i.client.storage.fsOps()); err != nil {
		return StorageInspection{}, err
	}
	topics, err := i.client.storage.listTopics()
	if err != nil {
		return StorageInspection{}, err
	}
	free, err := i.client.storage.freeBytes()
	if err != nil {
		return StorageInspection{}, err
	}
	return StorageInspection{
		RootPath:              i.client.cfg.RootPath,
		StorageID:             control.StorageID,
		ProducerStopFreeBytes: control.ProducerStopFreeBytes,
		RecoveryReserveBytes:  control.RecoveryReserveBytes,
		FilesystemFreeBytes:   free,
		Topics:                topics,
	}, nil
}

func (i *Inspector) InspectTopic(ctx context.Context, topic string, limit int) (TopicInspection, error) {
	if err := i.ready(ctx); err != nil {
		return TopicInspection{}, err
	}
	if err := validateTopicName(topic); err != nil {
		return TopicInspection{}, err
	}
	limit = normalizeInspectionLimit(limit)
	manifest, err := i.client.storage.topicManifest(topic)
	if err != nil {
		return TopicInspection{}, err
	}
	control, err := i.client.storage.topicControl(topic)
	if err != nil {
		return TopicInspection{}, err
	}
	lanes, err := i.client.storage.listLanes(topic)
	if err != nil {
		return TopicInspection{}, err
	}
	groups, err := i.client.storage.listGroups(topic)
	if err != nil {
		return TopicInspection{}, err
	}
	if len(lanes) > limit {
		lanes = lanes[:limit]
	}
	if len(groups) > limit {
		groups = groups[:limit]
	}
	return TopicInspection{Topic: topic, MaxRecordBytes: manifest.MaxRecordBytes, Retention: time.Duration(control.RetentionTimeSeconds) * time.Second, Lanes: lanes, Groups: groups}, nil
}

func (i *Inspector) InspectLane(ctx context.Context, topic, laneID string) (LaneInspection, error) {
	if err := i.ready(ctx); err != nil {
		return LaneInspection{}, err
	}
	if err := validateTopicAndLane(topic, laneID); err != nil {
		return LaneInspection{}, err
	}
	manifest, err := i.client.storage.laneManifest(topic, laneID)
	if err != nil {
		return LaneInspection{}, err
	}
	index, err := i.client.storage.retention(topic, laneID)
	if err != nil {
		return LaneInspection{}, err
	}
	frontier, frontierErr := i.client.storage.durableEnd(topic, laneID)
	marker, retired, retiredErr := i.client.storage.retired(topic, laneID)
	if retiredErr != nil {
		return LaneInspection{}, retiredErr
	}
	if frontierErr != nil && (!retired || !errors.Is(frontierErr, os.ErrNotExist)) {
		return LaneInspection{}, frontierErr
	}
	if frontierErr == nil && retired && frontier.NextSequence != marker.FinalNextSequence {
		return LaneInspection{}, errors.New("retired marker and durable frontier disagree")
	}
	durableEnd := uint64(0)
	if frontierErr == nil {
		durableEnd = frontier.NextSequence
	}
	if retired && marker.FinalNextSequence > durableEnd {
		durableEnd = marker.FinalNextSequence
	}
	segments, err := i.client.storage.listSegments(topic, laneID)
	if err != nil {
		return LaneInspection{}, err
	}
	result := LaneInspection{Topic: topic, LaneID: laneID, ProducerInstanceID: manifest.ProducerInstanceID, CreatedAtUnixNano: manifest.CreatedAtUnixNano, RetentionFloor: index.EarliestRetainedSeq, DurableEnd: durableEnd, Retired: retired, Segments: make([]SegmentInspection, 0, len(segments))}
	for _, segment := range segments {
		stat, err := i.client.storage.stat(segment.path)
		if err != nil {
			return LaneInspection{}, err
		}
		item := SegmentInspection{BaseSequence: segment.base, Sealed: !segment.open, SizeBytes: stat.Size()}
		if segment.open {
			item.NextSequence = durableEnd
		} else {
			footer, err := readSealedFooter(i.client.storage, topic, laneID, segment)
			if err != nil {
				return LaneInspection{}, err
			}
			item.NextSequence = footer.NextSequence
			item.RecordCount = footer.RecordCount
			item.MinAppendNanos = footer.MinAppendNanos
			item.MaxAppendNanos = footer.MaxAppendNanos
		}
		result.Segments = append(result.Segments, item)
	}
	return result, nil
}

func (i *Inspector) InspectGroup(ctx context.Context, topic, group string, limit int) (GroupInspection, error) {
	if err := i.ready(ctx); err != nil {
		return GroupInspection{}, err
	}
	if err := validateTopicAndGroup(topic, group); err != nil {
		return GroupInspection{}, err
	}
	limit = normalizeInspectionLimit(limit)
	manifest, err := i.client.storage.groupManifest(topic, group)
	if err != nil {
		return GroupInspection{}, err
	}
	control, err := i.client.storage.groupControl(topic, group)
	if err != nil {
		return GroupInspection{}, err
	}
	base, err := i.client.storage.readCheckpointBase(topic, group)
	if err != nil {
		return GroupInspection{}, err
	}
	if len(base.NextSequence) > limit {
		trimmed := make(map[string]uint64, limit)
		lanes := make([]string, 0, len(base.NextSequence))
		for laneID := range base.NextSequence {
			lanes = append(lanes, laneID)
		}
		sort.Strings(lanes)
		for _, laneID := range lanes[:limit] {
			trimmed[laneID] = base.NextSequence[laneID]
		}
		base.NextSequence = trimmed
	}
	members, err := i.client.storage.readDir(i.client.storage.groupMembersDir(topic, group))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return GroupInspection{}, err
	}
	return GroupInspection{Topic: topic, Group: group, GroupType: GroupType(manifest.GroupType), InitialPosition: InitialPosition(manifest.InitialPosition), State: GroupState(control.State), CheckpointBase: base.NextSequence, MemberCount: len(members)}, nil
}

func (i *Inspector) InspectBlocked(ctx context.Context, topic, group string, limit int) ([]BlockedInspection, error) {
	if err := i.ready(ctx); err != nil {
		return nil, err
	}
	if err := validateTopicAndGroup(topic, group); err != nil {
		return nil, err
	}
	limit = normalizeInspectionLimit(limit)
	lanes, err := i.client.storage.listLanes(topic)
	if err != nil {
		return nil, err
	}
	result := make([]BlockedInspection, 0, limit)
	for _, laneID := range lanes {
		markers, err := i.client.storage.listBlocked(topic, group, laneID)
		if err != nil {
			return nil, err
		}
		for _, marker := range markers {
			result = append(result, BlockedInspection{Topic: marker.Topic, Group: marker.Group, LaneID: marker.LaneID, Sequence: marker.Sequence, ConsumerInstanceID: marker.ConsumerInstanceID, MessageID: marker.MessageID, ContentHash: marker.ContentHash, CreatedAtUnixNano: marker.CreatedAtUnixNano, Error: marker.Error})
			if len(result) >= limit {
				return result, nil
			}
		}
	}
	sort.Slice(result, func(a, b int) bool {
		if result[a].LaneID != result[b].LaneID {
			return result[a].LaneID < result[b].LaneID
		}
		return result[a].Sequence < result[b].Sequence
	})
	return result, nil
}

func (i *Inspector) ready(ctx context.Context) error {
	if i == nil || i.client == nil {
		return errors.New("localmq inspector is nil")
	}
	if ctx == nil {
		return errors.New("localmq inspection context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if i.client.stateValue() != clientStateStarted {
		return errors.New("localmq inspector client is not started")
	}
	return nil
}

func normalizeInspectionLimit(limit int) int {
	if limit <= 0 {
		return defaultInspectionLimit
	}
	if limit > maxCheckpointFiles {
		return maxCheckpointFiles
	}
	return limit
}
