package localmq

import (
	"container/heap"
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"
)

type retentionBarrier struct {
	group   string
	blocked bool
}

type pressureCandidate struct {
	topic   string
	laneID  string
	segment segmentFile
	footer  segmentFooter
}

type pressureCandidateHeap []*pressureCandidate

func (h pressureCandidateHeap) Len() int { return len(h) }

func (h pressureCandidateHeap) Less(i, j int) bool {
	if h[i].footer.MaxAppendNanos != h[j].footer.MaxAppendNanos {
		return h[i].footer.MaxAppendNanos < h[j].footer.MaxAppendNanos
	}
	if h[i].topic != h[j].topic {
		return h[i].topic < h[j].topic
	}
	if h[i].laneID != h[j].laneID {
		return h[i].laneID < h[j].laneID
	}
	return h[i].segment.base < h[j].segment.base
}

func (h pressureCandidateHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *pressureCandidateHeap) Push(value any) {
	*h = append(*h, value.(*pressureCandidate))
}

func (h *pressureCandidateHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	old[last] = nil
	*h = old[:last]
	return value
}

func compactAllGroups(ctx context.Context, m *Maintainer) error {
	topics, err := m.client.storage.listTopics()
	if err != nil {
		return err
	}
	for _, topic := range topics {
		if err := ctx.Err(); err != nil {
			return err
		}
		groups, err := m.client.storage.listGroups(topic)
		if err != nil {
			return err
		}
		for _, group := range groups {
			if err := ctx.Err(); err != nil {
				return err
			}
			control, err := m.client.storage.groupControl(topic, group)
			if err != nil {
				return err
			}
			if control.State == string(GroupDeleted) {
				continue
			}
			var activeMembers map[string]struct{}
			if m.membershipFn != nil {
				membership, membershipErr := m.membershipFn(topic, group, "maintenance", m.client.cfg.Consumer.MembershipTimeout)
				if membershipErr != nil {
					return membershipErr
				}
				members, membershipErr := membership.activeMembers(ctx)
				if membershipErr != nil {
					return membershipErr
				}
				activeMembers = make(map[string]struct{}, len(members))
				for _, member := range members {
					activeMembers[member] = struct{}{}
				}
			}
			if err := m.client.storage.compactCheckpointBase(ctx, topic, group, activeMembers); err != nil {
				return err
			}
		}
	}
	return nil
}

func runRetention(ctx context.Context, m *Maintainer) error {
	free, err := m.client.storage.freeBytes()
	if err != nil {
		return err
	}
	topics, err := m.client.storage.listTopics()
	if err != nil {
		return err
	}
	if free < uint64(m.client.cfg.ProducerStopFreeBytes) {
		return runPressureRetention(ctx, m, topics)
	}
	for _, topic := range topics {
		if err := cleanupTopicStaging(ctx, m.client.storage, topic); err != nil {
			return err
		}
		if err := retainTopic(ctx, m, topic, false); err != nil {
			return err
		}
		if err := cleanupTopicStaging(ctx, m.client.storage, topic); err != nil {
			return err
		}
	}
	return nil
}

func runPressureRetention(ctx context.Context, m *Maintainer, topics []string) error {
	for _, topic := range topics {
		if err := cleanupTopicStaging(ctx, m.client.storage, topic); err != nil {
			return err
		}
		lanes, err := m.client.storage.listLanes(topic)
		if err != nil {
			return err
		}
		for _, laneID := range lanes {
			if err := cleanupLogicallyDeletedSegments(ctx, m, topic, laneID); err != nil {
				return err
			}
		}
	}
	if free, err := m.client.storage.freeBytes(); err != nil {
		return err
	} else if free >= uint64(m.client.cfg.ProducerStopFreeBytes) {
		return nil
	}
	candidates := make(pressureCandidateHeap, 0)
	for _, topic := range topics {
		lanes, err := m.client.storage.listLanes(topic)
		if err != nil {
			return err
		}
		for _, laneID := range lanes {
			candidate, err := pressureCandidateForLane(ctx, m, topic, laneID)
			if err != nil {
				return err
			}
			if candidate != nil {
				candidates = append(candidates, candidate)
			} else if err := maybeCollectRetiredLaneIfEmpty(ctx, m, topic, laneID); err != nil {
				return err
			}
		}
	}
	heap.Init(&candidates)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		free, err := m.client.storage.freeBytes()
		if err != nil {
			return err
		}
		if free >= uint64(m.client.cfg.ProducerStopFreeBytes) {
			return nil
		}
		if len(candidates) == 0 {
			return errors.New("localmq pressure GC is capacity-blocked: no eligible segment")
		}
		candidate := heap.Pop(&candidates).(*pressureCandidate)
		fresh, err := pressureCandidateForLane(ctx, m, candidate.topic, candidate.laneID)
		if err != nil {
			return err
		}
		if fresh == nil {
			continue
		}
		if !samePressureCandidate(candidate, fresh) {
			heap.Push(&candidates, fresh)
			continue
		}
		if err := deleteRetentionSegments(ctx, m, candidate.topic, candidate.laneID, []pressureCandidate{*candidate}, "pressure"); err != nil {
			return err
		}
		next, err := pressureCandidateForLane(ctx, m, candidate.topic, candidate.laneID)
		if err != nil {
			return err
		}
		if next != nil {
			heap.Push(&candidates, next)
		} else if err := maybeCollectRetiredLaneIfEmpty(ctx, m, candidate.topic, candidate.laneID); err != nil {
			return err
		}
	}
}

func samePressureCandidate(left, right *pressureCandidate) bool {
	return left.topic == right.topic && left.laneID == right.laneID && left.segment.base == right.segment.base && left.segment.path == right.segment.path && left.footer.FileLength == right.footer.FileLength && left.footer.NextSequence == right.footer.NextSequence && left.footer.MaxAppendNanos == right.footer.MaxAppendNanos
}

func cleanupTopicStaging(ctx context.Context, s *storage, topic string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	directory := filepath.Join(s.topicDir(topic), gcStagingDirectory)
	entries, err := s.readDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(entries) > maxCheckpointFiles {
		return errors.New("GC staging directory exceeds configured bound")
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.removeAll(filepath.Join(directory, entry.Name())); err != nil {
			return err
		}
	}
	return s.syncDirectory(directory)
}

func retainTopic(ctx context.Context, m *Maintainer, topic string, pressure bool) error {
	control, err := m.client.storage.topicControl(topic)
	if err != nil {
		return err
	}
	barriers, err := protectedBarriers(m.client.storage, topic)
	if err != nil {
		return err
	}
	lanes, err := m.client.storage.listLanes(topic)
	if err != nil {
		return err
	}
	for _, laneID := range lanes {
		if err := retainLane(ctx, m, topic, laneID, control, barriers, pressure); err != nil {
			return err
		}
	}
	return nil
}

func protectedBarriers(s *storage, topic string) ([]retentionBarrier, error) {
	groups, err := s.listGroups(topic)
	if err != nil {
		return nil, err
	}
	barriers := make([]retentionBarrier, 0, len(groups))
	for _, group := range groups {
		manifest, err := s.groupManifest(topic, group)
		if err != nil {
			return nil, err
		}
		control, err := s.groupControl(topic, group)
		if err != nil {
			return nil, err
		}
		if manifest.GroupType != string(GroupTypeProtected) || control.State == string(GroupDeleted) {
			continue
		}
		// INITIALIZING/DELETING 的 protected group 即使目前 checkpoint 足夠，
		// 仍須保守阻擋 floor 前進，直到 control 回到 ACTIVE 或 group 被刪除。
		barriers = append(barriers, retentionBarrier{group: group, blocked: control.State != string(GroupActive)})
	}
	return barriers, nil
}

func retainLane(ctx context.Context, m *Maintainer, topic, laneID string, topicControlValue topicControl, barriers []retentionBarrier, pressure bool) error {
	if err := cleanupLogicallyDeletedSegments(ctx, m, topic, laneID); err != nil {
		return err
	}
	segments, err := m.client.storage.listSegments(topic, laneID)
	if err != nil {
		return err
	}
	var sealed []segmentFile
	for _, segment := range segments {
		if !segment.open {
			sealed = append(sealed, segment)
		}
	}
	if len(sealed) == 0 {
		return maybeCollectRetiredLaneIfEmpty(ctx, m, topic, laneID)
	}
	eligible := make([]pressureCandidate, 0, len(sealed))
	for _, segment := range sealed {
		if err := ctx.Err(); err != nil {
			return err
		}
		footer, err := readSealedFooter(m.client.storage, topic, laneID, segment)
		if err != nil {
			return err
		}
		protected, err := segmentPassedBarriers(m.client.storage, topic, laneID, footer.NextSequence, barriers)
		if err != nil {
			return err
		}
		if !protected {
			break
		}
		oldEnough := false
		if topicControlValue.RetentionTimeSeconds > 0 {
			cutoff := time.Unix(0, footer.MaxAppendNanos).Add(time.Duration(topicControlValue.RetentionTimeSeconds) * time.Second).Add(2 * m.client.cfg.MaxClockSkew)
			oldEnough = !m.client.storage.now().Before(cutoff)
		}
		if oldEnough || pressure {
			eligible = append(eligible, pressureCandidate{topic: topic, laneID: laneID, segment: segment, footer: footer})
			if pressure {
				// retainLane 保留舊的 package-private 呼叫語意；真正的 pressure
				// GC 由全域 heap 控制，一次只提交一條 candidate。
				break
			}
			continue
		}
		break
	}
	if len(eligible) == 0 {
		return nil
	}
	valid, err := revalidateRetentionCandidates(ctx, m, topic, laneID, eligible, topicControlValue, pressure)
	if err != nil {
		return err
	}
	if len(valid) == 0 {
		return nil
	}
	return deleteRetentionSegments(ctx, m, topic, laneID, valid, retentionReason(pressure))
}

func retentionReason(pressure bool) string {
	if pressure {
		return "pressure"
	}
	return "time"
}

func segmentPassedBarriers(s *storage, topic, laneID string, nextSequence uint64, barriers []retentionBarrier) (bool, error) {
	for _, barrier := range barriers {
		if barrier.blocked {
			return false, nil
		}
		next, err := s.effectiveCheckpoint(topic, barrier.group, laneID)
		if err != nil {
			return false, err
		}
		if next < nextSequence {
			return false, nil
		}
	}
	return true, nil
}

func revalidateRetentionCandidates(ctx context.Context, m *Maintainer, topic, laneID string, candidates []pressureCandidate, topicControlValue topicControl, pressure bool) ([]pressureCandidate, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	currentControl, err := m.client.storage.topicControl(topic)
	if err != nil {
		return nil, err
	}
	barriers, err := protectedBarriers(m.client.storage, topic)
	if err != nil {
		return nil, err
	}
	index, err := m.client.storage.retention(topic, laneID)
	if err != nil {
		return nil, err
	}
	if currentControl.RetentionTimeSeconds != topicControlValue.RetentionTimeSeconds && !pressure {
		topicControlValue = currentControl
	}
	segments, err := m.client.storage.listSegments(topic, laneID)
	if err != nil {
		return nil, err
	}
	valid := make([]pressureCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(valid) == 0 && candidate.segment.base != index.EarliestRetainedSeq {
			return nil, nil
		}
		if len(valid) > 0 && candidate.segment.base != valid[len(valid)-1].footer.NextSequence {
			break
		}
		found := false
		for _, segment := range segments {
			if segment.base == candidate.segment.base && segment.path == candidate.segment.path && !segment.open {
				found = true
				candidate.segment = segment
				break
			}
		}
		if !found {
			break
		}
		footer, err := readSealedFooter(m.client.storage, topic, laneID, candidate.segment)
		if err != nil {
			return nil, err
		}
		if footer.FileLength != candidate.footer.FileLength || footer.NextSequence != candidate.footer.NextSequence || footer.MaxAppendNanos != candidate.footer.MaxAppendNanos {
			break
		}
		passed, err := segmentPassedBarriers(m.client.storage, topic, laneID, footer.NextSequence, barriers)
		if err != nil {
			return nil, err
		}
		if !passed {
			break
		}
		if !pressure {
			cutoff := time.Unix(0, footer.MaxAppendNanos).Add(time.Duration(currentControl.RetentionTimeSeconds) * time.Second).Add(2 * m.client.cfg.MaxClockSkew)
			if currentControl.RetentionTimeSeconds <= 0 || m.client.storage.now().Before(cutoff) {
				break
			}
		}
		candidate.footer = footer
		valid = append(valid, candidate)
	}
	return valid, nil
}

func deleteRetentionSegments(ctx context.Context, m *Maintainer, topic, laneID string, candidates []pressureCandidate, reason string) error {
	if len(candidates) == 0 {
		return nil
	}
	currentControl, err := m.client.storage.topicControl(topic)
	if err != nil {
		return err
	}
	valid, err := revalidateRetentionCandidates(ctx, m, topic, laneID, candidates, currentControl, reason == "pressure")
	if err != nil {
		return err
	}
	if len(valid) != len(candidates) {
		// Controls/checkpoints changed after planning. The caller will rescan
		// this lane on the next pressure iteration; no floor is published.
		return nil
	}
	candidates = valid
	index, err := m.client.storage.retention(topic, laneID)
	if err != nil {
		return err
	}
	if candidates[0].segment.base != index.EarliestRetainedSeq {
		return nil
	}
	last := candidates[len(candidates)-1].footer.NextSequence
	if last <= index.EarliestRetainedSeq {
		return nil
	}
	index.EarliestRetainedSeq = last
	index.UpdatedAtUnixNano = m.client.storage.now().UnixNano()
	if err := writeMetadataAtomicWithOps(filepath.Join(m.client.storage.laneDir(topic, laneID), retentionIndexFile), metadataKindRetentionIndex, index, 0o640, m.client.storage.fsOps(), "", faultRetentionFloorRename, ""); err != nil {
		return err
	}
	return stageAndRemoveRetentionSegments(ctx, m, topic, laneID, candidates, reason)
}

func cleanupLogicallyDeletedSegments(ctx context.Context, m *Maintainer, topic, laneID string) error {
	index, err := m.client.storage.retention(topic, laneID)
	if err != nil {
		return err
	}
	segments, err := m.client.storage.listSegments(topic, laneID)
	if err != nil {
		return err
	}
	stale := make([]pressureCandidate, 0)
	for _, segment := range segments {
		if segment.open {
			break
		}
		footer, err := readSealedFooter(m.client.storage, topic, laneID, segment)
		if err != nil {
			return err
		}
		if footer.NextSequence > index.EarliestRetainedSeq {
			break
		}
		stale = append(stale, pressureCandidate{topic: topic, laneID: laneID, segment: segment, footer: footer})
	}
	return stageAndRemoveRetentionSegments(ctx, m, topic, laneID, stale, "recovery")
}

func stageAndRemoveRetentionSegments(ctx context.Context, m *Maintainer, topic, laneID string, candidates []pressureCandidate, reason string) error {
	if len(candidates) == 0 {
		return nil
	}
	staging := filepath.Join(m.client.storage.topicDir(topic), gcStagingDirectory, laneID)
	if err := m.client.storage.mkdirAll(staging, 0o750); err != nil {
		return err
	}
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		stat, statErr := m.client.storage.stat(candidate.segment.path)
		if errors.Is(statErr, os.ErrNotExist) {
			continue
		}
		if statErr != nil {
			return statErr
		}
		target := filepath.Join(staging, filepath.Base(candidate.segment.path))
		if err := m.client.storage.fault(faultStagingRename); err != nil {
			return err
		}
		if err := m.client.storage.rename(candidate.segment.path, target); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		m.client.metricGC(topic, reason, stat.Size())
	}
	if err := m.client.storage.syncDirectory(m.client.storage.segmentsDir(topic, laneID)); err != nil {
		return err
	}
	if err := m.client.storage.syncDirectory(staging); err != nil {
		return err
	}
	for _, candidate := range candidates {
		target := filepath.Join(staging, filepath.Base(candidate.segment.path))
		if err := m.client.storage.remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return m.client.storage.syncDirectory(staging)
}

func pressureCandidateForLane(ctx context.Context, m *Maintainer, topic, laneID string) (*pressureCandidate, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := m.client.storage.topicControl(topic); err != nil {
		return nil, err
	}
	barriers, err := protectedBarriers(m.client.storage, topic)
	if err != nil {
		return nil, err
	}
	index, err := m.client.storage.retention(topic, laneID)
	if err != nil {
		return nil, err
	}
	segments, err := m.client.storage.listSegments(topic, laneID)
	if err != nil {
		return nil, err
	}
	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if segment.open {
			continue
		}
		footer, err := readSealedFooter(m.client.storage, topic, laneID, segment)
		if err != nil {
			return nil, err
		}
		if footer.NextSequence <= index.EarliestRetainedSeq {
			continue
		}
		if segment.base != index.EarliestRetainedSeq {
			return nil, errors.New("pressure GC found a non-contiguous segment")
		}
		passed, err := segmentPassedBarriers(m.client.storage, topic, laneID, footer.NextSequence, barriers)
		if err != nil {
			return nil, err
		}
		if !passed {
			return nil, nil
		}
		return &pressureCandidate{topic: topic, laneID: laneID, segment: segment, footer: footer}, nil
	}
	return nil, nil
}

func maybeCollectRetiredLaneIfEmpty(ctx context.Context, m *Maintainer, topic, laneID string) error {
	segments, err := m.client.storage.listSegments(topic, laneID)
	if err != nil {
		return err
	}
	for _, segment := range segments {
		if !segment.open {
			return nil
		}
	}
	barriers, err := protectedBarriers(m.client.storage, topic)
	if err != nil {
		return err
	}
	return maybeCollectRetiredLane(ctx, m, topic, laneID, barriers)
}

func maybeCollectRetiredLane(ctx context.Context, m *Maintainer, topic, laneID string, barriers []retentionBarrier) error {
	marker, retired, err := m.client.storage.retired(topic, laneID)
	if err != nil || !retired {
		return err
	}
	for _, barrier := range barriers {
		if barrier.blocked {
			return nil
		}
		next, err := m.client.storage.effectiveCheckpoint(topic, barrier.group, laneID)
		if err != nil {
			return err
		}
		if next < marker.FinalNextSequence {
			return nil
		}
	}
	groups, err := m.client.storage.listGroups(topic)
	if err != nil {
		return err
	}
	for _, group := range groups {
		control, err := m.client.storage.groupControl(topic, group)
		if err != nil {
			return err
		}
		if control.State == string(GroupDeleted) {
			continue
		}
		blocked, err := m.client.storage.listBlocked(topic, group, laneID)
		if err != nil {
			return err
		}
		if len(blocked) != 0 {
			return nil
		}
	}
	index, err := m.client.storage.retention(topic, laneID)
	if err != nil {
		return err
	}
	if index.EarliestRetainedSeq < marker.FinalNextSequence {
		index.EarliestRetainedSeq = marker.FinalNextSequence
		index.UpdatedAtUnixNano = m.client.storage.now().UnixNano()
		if err := writeMetadataAtomicWithOps(filepath.Join(m.client.storage.laneDir(topic, laneID), retentionIndexFile), metadataKindRetentionIndex, index, 0o640, m.client.storage.fsOps(), "", faultRetentionFloorRename, ""); err != nil {
			return err
		}
	}
	stagingParent := filepath.Join(m.client.storage.topicDir(topic), gcStagingDirectory)
	if err := m.client.storage.mkdirAll(stagingParent, 0o750); err != nil {
		return err
	}
	target := filepath.Join(stagingParent, laneID+"-retired")
	if err := m.client.storage.fault(faultStagingRename); err != nil {
		return err
	}
	if err := m.client.storage.rename(m.client.storage.laneDir(topic, laneID), target); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := m.client.storage.syncDirectory(stagingParent); err != nil {
		return err
	}
	if err := m.client.storage.syncDirectory(m.client.storage.lanesDir(topic)); err != nil {
		return err
	}
	if err := m.client.storage.removeAll(target); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return m.client.storage.syncDirectory(stagingParent)
}

func readSealedFooter(s *storage, topic, laneID string, segment segmentFile) (segmentFooter, error) {
	if segment.open {
		return segmentFooter{}, errors.New("retention footer requires a sealed segment")
	}
	file, err := s.open(segment.path)
	if err != nil {
		return segmentFooter{}, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return segmentFooter{}, err
	}
	if stat.Size() < segmentFooterBytes || stat.Size() > s.maxSegmentFileBytes(topic) {
		return segmentFooter{}, errors.New("sealed segment size is invalid")
	}
	header, err := readSegmentHeader(file, stat)
	if err != nil {
		return segmentFooter{}, err
	}
	if header.Topic != topic || header.LaneID != laneID || header.BaseSequence != segment.base {
		return segmentFooter{}, errors.New("sealed segment identity mismatch")
	}
	footerBytes := make([]byte, segmentFooterBytes)
	if err := readAtFull(file, stat.Size()-segmentFooterBytes, footerBytes); err != nil {
		return segmentFooter{}, err
	}
	footer, err := decodeSegmentFooter(footerBytes)
	if err != nil {
		return segmentFooter{}, err
	}
	if footer.FileLength != uint64(stat.Size()) || footer.FileLength < uint64(header.Length+segmentFooterBytes) || footer.NextSequence < segment.base || footer.RecordCount == 0 || footer.NextSequence-segment.base != footer.RecordCount || footer.MaxAppendNanos < footer.MinAppendNanos {
		return segmentFooter{}, errors.New("sealed segment footer summary is invalid")
	}
	return footer, nil
}
