package localmq

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

const maxCheckpointFiles = 10000

func (s *storage) readCheckpointBase(topic, group string) (checkpointSnapshot, error) {
	var snapshot checkpointSnapshot
	if err := readMetadataWithOps(filepath.Join(s.groupDir(topic, group), checkpointBaseFile), metadataKindCheckpointBase, &snapshot, s.fsOps()); err != nil {
		return checkpointSnapshot{}, err
	}
	if snapshot.FormatVersion != formatVersion || snapshot.NextSequence == nil || len(snapshot.NextSequence) > maxCheckpointFiles {
		return checkpointSnapshot{}, errors.New("checkpoint-base format is invalid")
	}
	for laneID := range snapshot.NextSequence {
		if err := validateLaneID(laneID); err != nil {
			return checkpointSnapshot{}, err
		}
	}
	return snapshot, nil
}

func (s *storage) readCheckpoint(topic, group, instanceID, laneID string) (checkpointRecord, bool, error) {
	path := s.checkpointPath(topic, group, instanceID, laneID)
	var checkpoint checkpointRecord
	if err := readMetadataWithOps(path, metadataKindCheckpoint, &checkpoint, s.fsOps()); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return checkpointRecord{}, false, nil
		}
		return checkpointRecord{}, false, err
	}
	if checkpoint.FormatVersion != formatVersion || checkpoint.Topic != topic || checkpoint.Group != group || checkpoint.LaneID != laneID || checkpoint.ConsumerInstanceID != instanceID {
		return checkpointRecord{}, false, errors.New("checkpoint identity or format is invalid")
	}
	if err := validateLaneID(checkpoint.LaneID); err != nil {
		return checkpointRecord{}, false, err
	}
	return checkpoint, true, nil
}

func (s *storage) effectiveCheckpoint(topic, group, laneID string) (uint64, error) {
	snapshot, err := s.readCheckpointBase(topic, group)
	if err != nil {
		return 0, err
	}
	effective := snapshot.NextSequence[laneID]
	membersDir := s.groupMembersDir(topic, group)
	entries, err := s.readDir(membersDir)
	if errors.Is(err, os.ErrNotExist) {
		return effective, nil
	}
	if err != nil {
		return 0, err
	}
	if len(entries) > maxCheckpointFiles {
		return 0, errors.New("checkpoint member directory exceeds configured bound")
	}
	for _, member := range entries {
		if !member.IsDir() || strings.HasPrefix(member.Name(), ".") {
			continue
		}
		checkpoint, exists, err := s.readCheckpoint(topic, group, member.Name(), laneID)
		if err != nil {
			return 0, err
		}
		if exists && checkpoint.NextSequence > effective {
			effective = checkpoint.NextSequence
		}
	}
	durableEnd, err := laneDurableEnd(s, topic, laneID)
	if err != nil {
		return 0, err
	}
	if effective > durableEnd {
		return 0, errors.New("checkpoint exceeds lane durable end")
	}
	return effective, nil
}

func (s *storage) writeMemberCheckpoint(ctx context.Context, checkpoint checkpointRecord) error {
	if ctx == nil {
		return errors.New("checkpoint context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateTopicAndGroup(checkpoint.Topic, checkpoint.Group); err != nil {
		return err
	}
	if err := validateLaneID(checkpoint.LaneID); err != nil {
		return err
	}
	if checkpoint.ConsumerInstanceID == "" {
		return errors.New("checkpoint consumer_instance_id is required")
	}
	durableEnd, err := laneDurableEnd(s, checkpoint.Topic, checkpoint.LaneID)
	if err != nil {
		return err
	}
	if checkpoint.NextSequence > durableEnd {
		return errors.New("checkpoint exceeds lane durable end")
	}
	path := s.checkpointPath(checkpoint.Topic, checkpoint.Group, checkpoint.ConsumerInstanceID, checkpoint.LaneID)
	if current, exists, err := s.readCheckpoint(checkpoint.Topic, checkpoint.Group, checkpoint.ConsumerInstanceID, checkpoint.LaneID); err != nil {
		return err
	} else if exists && checkpoint.NextSequence < current.NextSequence {
		return nil
	}
	checkpoint.FormatVersion = formatVersion
	if checkpoint.UpdatedAtUnixNano == 0 {
		checkpoint.UpdatedAtUnixNano = s.now().UnixNano()
	}
	return writeMetadataAtomicWithOps(path, metadataKindCheckpoint, checkpoint, 0o640, s.fsOps(), "", faultCheckpointRename, "")
}

func (s *storage) writeCheckpointBase(ctx context.Context, topic, group string, next map[string]uint64) error {
	if ctx == nil {
		return errors.New("checkpoint base context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateTopicAndGroup(topic, group); err != nil {
		return err
	}
	if len(next) > maxCheckpointFiles {
		return errors.New("checkpoint-base exceeds configured lane bound")
	}
	clone := make(map[string]uint64, len(next))
	for laneID, sequence := range next {
		if err := validateLaneID(laneID); err != nil {
			return err
		}
		clone[laneID] = sequence
	}
	return writeMetadataAtomicWithOps(filepath.Join(s.groupDir(topic, group), checkpointBaseFile), metadataKindCheckpointBase, checkpointSnapshot{
		FormatVersion:     formatVersion,
		NextSequence:      clone,
		UpdatedAtUnixNano: s.now().UnixNano(),
	}, 0o640, s.fsOps(), "", faultCheckpointRename, "")
}

func (s *storage) checkpointPath(topic, group, instanceID, laneID string) string {
	return filepath.Join(s.groupMembersDir(topic, group), instanceID, laneID+".checkpoint")
}

func (s *storage) compactCheckpointBase(ctx context.Context, topic, group string, activeMembers map[string]struct{}) error {
	if ctx == nil {
		return errors.New("checkpoint compaction context is nil")
	}
	current, err := s.readCheckpointBase(topic, group)
	if err != nil {
		return err
	}
	next := make(map[string]uint64, len(current.NextSequence))
	for laneID, sequence := range current.NextSequence {
		next[laneID] = sequence
	}
	lanes, err := s.listLanes(topic)
	if err != nil {
		return err
	}
	existingLanes := make(map[string]struct{}, len(lanes))
	for _, laneID := range lanes {
		existingLanes[laneID] = struct{}{}
	}
	for laneID := range next {
		if _, exists := existingLanes[laneID]; !exists {
			delete(next, laneID)
		}
	}
	entries, err := s.readDir(s.groupMembersDir(topic, group))
	if errors.Is(err, os.ErrNotExist) {
		return s.writeCheckpointBase(ctx, topic, group, next)
	}
	if err != nil {
		return err
	}
	if len(entries) > maxCheckpointFiles {
		return errors.New("checkpoint member directory exceeds configured bound")
	}
	for _, member := range entries {
		if !member.IsDir() || strings.HasPrefix(member.Name(), ".") {
			continue
		}
		files, err := s.readDir(filepath.Join(s.groupMembersDir(topic, group), member.Name()))
		if err != nil {
			return err
		}
		if len(files) > maxCheckpointFiles {
			return errors.New("checkpoint member directory exceeds configured bound")
		}
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".checkpoint") {
				continue
			}
			laneID := strings.TrimSuffix(file.Name(), ".checkpoint")
			if _, exists := existingLanes[laneID]; !exists {
				continue
			}
			checkpoint, exists, err := s.readCheckpoint(topic, group, member.Name(), laneID)
			if err != nil {
				return err
			}
			if exists && checkpoint.NextSequence > next[laneID] {
				next[laneID] = checkpoint.NextSequence
			}
		}
	}
	if err := s.writeCheckpointBase(ctx, topic, group, next); err != nil {
		return err
	}
	// 只有 baseline durable 後才清除 inactive member files；active member
	// 目錄保留，避免與正在執行的 consumer 競爭。
	if activeMembers == nil {
		return s.syncDirectory(s.groupMembersDir(topic, group))
	}
	for _, member := range entries {
		if !member.IsDir() || strings.HasPrefix(member.Name(), ".") {
			continue
		}
		if _, active := activeMembers[member.Name()]; active {
			continue
		}
		if err := s.removeAll(filepath.Join(s.groupMembersDir(topic, group), member.Name())); err != nil {
			return err
		}
	}
	return s.syncDirectory(s.groupMembersDir(topic, group))
}

func (s *storage) initialCheckpoints(ctx context.Context, topic string, position InitialPosition) (map[string]uint64, error) {
	lanes, err := s.listLanes(topic)
	if err != nil {
		return nil, err
	}
	next := make(map[string]uint64, len(lanes))
	for _, laneID := range lanes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if position == InitialLatest {
			sequence, err := laneDurableEnd(s, topic, laneID)
			if err != nil {
				return nil, err
			}
			next[laneID] = sequence
			continue
		}
		index, err := s.retention(topic, laneID)
		if err != nil {
			return nil, err
		}
		next[laneID] = index.EarliestRetainedSeq
	}
	return next, nil
}

func (s *storage) clampCheckpointsToFloors(ctx context.Context, topic string, next map[string]uint64) error {
	lanes, err := s.listLanes(topic)
	if err != nil {
		return err
	}
	for _, laneID := range lanes {
		index, err := s.retention(topic, laneID)
		if err != nil {
			return err
		}
		if current, ok := next[laneID]; !ok || current < index.EarliestRetainedSeq {
			next[laneID] = index.EarliestRetainedSeq
		}
	}
	return ctx.Err()
}
