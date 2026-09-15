package localmq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

func (m *Maintainer) acknowledgeRecords(ctx context.Context, request AcknowledgeRequest) error {
	if err := validateTopicAndGroup(request.Topic, request.Group); err != nil {
		return err
	}
	if err := validateLaneID(request.LaneID); err != nil {
		return err
	}
	if err := validateNonEmpty(request.Operator, "operator"); err != nil {
		return err
	}
	if err := validateNonEmpty(request.Reason, "reason"); err != nil {
		return err
	}
	if err := validateRequestID(request.RequestID); err != nil {
		return err
	}
	if audit, exists, err := m.readAudit(request.Topic, request.Group, request.RequestID); err != nil {
		return err
	} else if exists {
		if audit.Operation != "acknowledge_records" || audit.Topic != request.Topic || audit.Group != request.Group || audit.LaneID != request.LaneID || audit.UpToSequence != request.UpToSequence {
			return errors.New("request_id was already used for a different acknowledge operation")
		}
		return nil
	}
	control, err := m.client.storage.groupControl(request.Topic, request.Group)
	if err != nil {
		return err
	}
	if control.State != string(GroupActive) {
		return fmt.Errorf("group is %s, acknowledge requires ACTIVE", control.State)
	}
	if _, err := m.client.storage.laneManifest(request.Topic, request.LaneID); err != nil {
		return err
	}
	frontier, err := laneDurableEnd(m.client.storage, request.Topic, request.LaneID)
	if err != nil {
		return err
	}
	if request.UpToSequence > frontier {
		return fmt.Errorf("acknowledge sequence %d exceeds durable end %d", request.UpToSequence, frontier)
	}
	base, err := m.client.storage.readCheckpointBase(request.Topic, request.Group)
	if err != nil {
		return err
	}
	effective, err := m.client.storage.effectiveCheckpoint(request.Topic, request.Group, request.LaneID)
	if err != nil {
		return err
	}
	baseline := base.NextSequence[request.LaneID]
	newBaseline := baseline
	if effective > newBaseline {
		newBaseline = effective
	}
	if request.UpToSequence > newBaseline {
		newBaseline = request.UpToSequence
	}
	if request.UpToSequence > effective {
		marker, exists, err := m.client.storage.earliestBlocked(request.Topic, request.Group, request.LaneID, effective)
		if err != nil {
			return err
		}
		if !exists || marker.Sequence >= request.UpToSequence {
			return errors.New("acknowledge target is not backed by a blocked record")
		}
		base.NextSequence[request.LaneID] = newBaseline
		if err := m.client.storage.writeCheckpointBase(ctx, request.Topic, request.Group, base.NextSequence); err != nil {
			return err
		}
	} else {
		// baseline 已發布但 marker cleanup 可能尚未完成；只有仍有
		// target 以下 marker 才允許這次 request 做安全重試。
		markers, err := m.client.storage.listBlocked(request.Topic, request.Group, request.LaneID)
		if err != nil {
			return err
		}
		found := false
		for _, marker := range markers {
			if marker.Sequence < request.UpToSequence {
				found = true
				break
			}
		}
		if baseline < request.UpToSequence || !found {
			return errors.New("acknowledge target is not backed by a blocked record")
		}
	}
	if err := m.client.storage.removeBlockedBelow(ctx, request.Topic, request.Group, request.LaneID, newBaseline); err != nil {
		return err
	}
	return m.writeAuditRecord(auditRecord{
		RequestID:         request.RequestID,
		Operation:         "acknowledge_records",
		Operator:          request.Operator,
		Reason:            request.Reason,
		Topic:             request.Topic,
		Group:             request.Group,
		LaneID:            request.LaneID,
		UpToSequence:      request.UpToSequence,
		CreatedAtUnixNano: m.client.storage.now().UnixNano(),
	})
}

func (m *Maintainer) forceRetireLane(ctx context.Context, request ForceRetireLaneRequest) error {
	if err := validateTopicAndLane(request.Topic, request.LaneID); err != nil {
		return err
	}
	if err := validateNonEmpty(request.FencingEvidence, "fencing_evidence"); err != nil {
		return err
	}
	if err := validateNonEmpty(request.Operator, "operator"); err != nil {
		return err
	}
	if err := validateNonEmpty(request.Reason, "reason"); err != nil {
		return err
	}
	if err := validateRequestID(request.RequestID); err != nil {
		return err
	}
	evidenceHash := bytesHash([]byte(request.FencingEvidence))
	if audit, exists, err := m.readAudit(request.Topic, "", request.RequestID); err != nil {
		return err
	} else if exists {
		if audit.Operation != "force_retire_lane" || audit.Topic != request.Topic || audit.Group != "" || audit.LaneID != request.LaneID || audit.ContentHash != evidenceHash {
			return errors.New("request_id was already used for a different retire operation")
		}
		return nil
	}
	if _, err := m.client.storage.laneManifest(request.Topic, request.LaneID); err != nil {
		return err
	}
	marker, retired, err := m.client.storage.retired(request.Topic, request.LaneID)
	if err != nil {
		return err
	}
	if retired {
		if marker.RetireRequestID == "" {
			// 同 request 的完整 audit 已在函式開頭處理；無 audit 的 legacy
			// marker 無法證明 fencing identity，必須 fail closed。
			return errors.New("retired lane has no matching force-retire request audit")
		}
		if marker.RetireRequestID != request.RequestID || marker.FencingEvidenceHash != evidenceHash {
			return errors.New("lane was already retired by a different request")
		}
	}
	if !retired {
		if err := forceRetireActiveLaneWithRequest(ctx, m.client.storage, request.Topic, request.LaneID, request.RequestID, evidenceHash); err != nil {
			return err
		}
		marker, retired, err = m.client.storage.retired(request.Topic, request.LaneID)
		if err != nil || !retired {
			if err == nil {
				err = errors.New("force retire did not publish retired marker")
			}
			return err
		}
	}
	if err := removeRetiredDurableEnd(m.client.storage, request.Topic, request.LaneID, marker); err != nil {
		return err
	}
	return m.writeAuditRecord(auditRecord{
		RequestID:         request.RequestID,
		Operation:         "force_retire_lane",
		Operator:          request.Operator,
		Reason:            request.Reason,
		Topic:             request.Topic,
		LaneID:            request.LaneID,
		Sequence:          marker.FinalNextSequence,
		ContentHash:       evidenceHash,
		CreatedAtUnixNano: m.client.storage.now().UnixNano(),
	})
}

func removeRetiredDurableEnd(s *storage, topic, laneID string, marker retiredMarker) error {
	frontier, err := s.durableEnd(topic, laneID)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if frontier.Topic != topic || frontier.LaneID != laneID || frontier.NextSequence != marker.FinalNextSequence {
		return errors.New("retired marker and durable frontier disagree")
	}
	if frontier.NextSequence == frontier.ActiveSegmentBase {
		segmentsDir := s.segmentsDir(topic, laneID)
		sealedPath := filepath.Join(segmentsDir, fmt.Sprintf("%d.wal", frontier.ActiveSegmentBase))
		if _, statErr := s.stat(sealedPath); statErr == nil {
			return errors.New("empty retired frontier unexpectedly has a sealed segment")
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return statErr
		}
		openSegment := segmentFile{
			base: frontier.ActiveSegmentBase,
			path: filepath.Join(segmentsDir, fmt.Sprintf("%d.open", frontier.ActiveSegmentBase)),
			open: true,
		}
		if _, statErr := s.stat(openSegment.path); statErr == nil {
			if err := validateOrphanOpenSegment(s, topic, laneID, openSegment); err != nil {
				return err
			}
			if err := s.remove(openSegment.path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if err := s.syncDirectory(segmentsDir); err != nil {
				return err
			}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return statErr
		}
	}
	if err := s.remove(filepath.Join(s.laneDir(topic, laneID), durableEndFile)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return s.syncDirectory(s.laneDir(topic, laneID))
}

func forceRetireActiveLane(ctx context.Context, s *storage, topic, laneID string) error {
	return forceRetireActiveLaneWithRequest(ctx, s, topic, laneID, "", "")
}

func forceRetireActiveLaneWithRequest(ctx context.Context, s *storage, topic, laneID, requestID, evidenceHash string) error {
	if ctx == nil {
		return errors.New("force retire context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	frontier, err := s.durableEnd(topic, laneID)
	if err != nil {
		return err
	}
	segments, err := s.listSegments(topic, laneID)
	if err != nil {
		return err
	}
	if len(segments) == 0 {
		return errors.New("lane has no segments")
	}
	var active *segmentFile
	var orphanOpen []segmentFile
	for i := range segments {
		segment := segments[i]
		if segment.base == frontier.ActiveSegmentBase {
			if active != nil {
				return errors.New("lane has more than one segment at the frontier active base")
			}
			active = &segments[i]
			continue
		}
		if !segment.open {
			continue
		}
		if segment.base != frontier.NextSequence {
			return errors.New("lane has a mismatched active segment")
		}
		if err := validateOrphanOpenSegment(s, topic, laneID, segment); err != nil {
			return err
		}
		orphanOpen = append(orphanOpen, segment)
	}
	if active == nil {
		return errors.New("lane segment at frontier active base is missing")
	}

	var expectedNext uint64
	hasExpected := false
	authoritativeValidated := false
	for index, segment := range segments {
		if segment.open {
			if &segments[index] == active {
				if hasExpected && segment.base != expectedNext {
					return errors.New("lane segment coverage has a sequence gap")
				}
				if err := validateOpenSegmentIdentity(s, topic, laneID, segment, frontier); err != nil {
					return err
				}
				if segment.base != frontier.ActiveSegmentBase {
					return errors.New("active segment base does not match durable frontier")
				}
				summary, err := validateActivePrefix(s, topic, laneID, segment.path, frontier)
				if err != nil {
					return err
				}
				expectedNext = frontier.NextSequence
				hasExpected = true
				authoritativeValidated = true
				_ = summary
				continue
			}
			if index != len(segments)-1 {
				return errors.New("orphan active segment is not the final segment")
			}
			continue
		}
		if authoritativeValidated {
			return errors.New("sealed segment exists after the authoritative frontier segment")
		}
		if hasExpected && segment.base != expectedNext {
			return errors.New("lane segment coverage has a sequence gap")
		}
		validated, err := validateSealedSegment(s, topic, laneID, segment)
		if err != nil {
			return err
		}
		expectedNext = validated.nextSequence
		hasExpected = true
		if segment.base == frontier.ActiveSegmentBase {
			if validated.nextSequence != frontier.NextSequence {
				return errors.New("sealed active segment end does not match durable frontier")
			}
			authoritativeValidated = true
		}
	}
	if !authoritativeValidated {
		return errors.New("authoritative frontier segment was not validated")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if active.open {
		summary, err := validateActivePrefix(s, topic, laneID, active.path, frontier)
		if err != nil {
			return err
		}
		file, err := s.openFile(active.path, os.O_RDWR, 0o640)
		if err != nil {
			return err
		}
		if err := file.Truncate(frontier.DurableByteEnd); err != nil {
			_ = file.Close()
			return err
		}
		if summary.count == 0 {
			if err := s.fault(faultFooterSync); err != nil {
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
		} else {
			if _, err := file.Seek(frontier.DurableByteEnd, io.SeekStart); err != nil {
				_ = file.Close()
				return err
			}
			footer := encodeSegmentFooter(segmentFooter{
				FileLength:     uint64(frontier.DurableByteEnd + segmentFooterBytes),
				NextSequence:   frontier.NextSequence,
				RecordCount:    summary.count,
				MinAppendNanos: summary.minAppend,
				MaxAppendNanos: summary.maxAppend,
				DataChecksum:   summary.dataChecksum,
			})
			if _, err := file.Write(footer); err != nil {
				_ = file.Close()
				return err
			}
			if err := s.fault(faultFooterSync); err != nil {
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
			sealedPath := filepath.Join(s.segmentsDir(topic, laneID), fmt.Sprintf("%d.wal", active.base))
			if err := s.fault(faultSegmentRename); err != nil {
				return err
			}
			if err := s.rename(active.path, sealedPath); err != nil {
				return err
			}
		}
	}
	for _, orphan := range orphanOpen {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.remove(orphan.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := s.syncDirectory(s.segmentsDir(topic, laneID)); err != nil {
		return err
	}
	retired := retiredMarker{
		FormatVersion:       formatVersion,
		Topic:               topic,
		LaneID:              laneID,
		FinalNextSequence:   frontier.NextSequence,
		RetiredAtUnixNano:   s.now().UnixNano(),
		RetireReason:        "externally_fenced_recovery",
		RetireRequestID:     requestID,
		FencingEvidenceHash: evidenceHash,
	}
	retiredPath := filepath.Join(s.laneDir(topic, laneID), retiredMarkerFile)
	if err := writeMetadataExclusiveWithOps(retiredPath, metadataKindRetired, retired, 0o640, s.fsOps()); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		var existing retiredMarker
		if readErr := readMetadataWithOps(retiredPath, metadataKindRetired, &existing, s.fsOps()); readErr != nil {
			return readErr
		}
		if existing.Topic != topic || existing.LaneID != laneID || existing.FinalNextSequence != retired.FinalNextSequence || existing.RetireRequestID != requestID || existing.FencingEvidenceHash != evidenceHash {
			return errors.New("existing retired marker conflicts with force retire")
		}
	}
	return removeRetiredDurableEnd(s, topic, laneID, retired)
}

type activePrefixSummary struct {
	count        uint64
	minAppend    int64
	maxAppend    int64
	dataChecksum uint32
}

func validateOpenSegmentIdentity(s *storage, topic, laneID string, segment segmentFile, frontier durableEnd) error {
	stat, err := s.stat(segment.path)
	if err != nil {
		return err
	}
	if stat.Size() > s.maxSegmentFileBytes(topic) {
		return errors.New("active segment exceeds configured size bound")
	}
	file, err := s.open(segment.path)
	if err != nil {
		return err
	}
	defer file.Close()
	header, err := readSegmentHeader(file, stat)
	if err != nil {
		return err
	}
	if header.Topic != topic || header.LaneID != laneID || header.BaseSequence != frontier.ActiveSegmentBase {
		return errors.New("active segment identity does not match durable frontier")
	}
	if frontier.DurableByteEnd < int64(header.Length) || frontier.DurableByteEnd > stat.Size() {
		return errors.New("active segment and durable frontier are inconsistent")
	}
	return nil
}

func validateActivePrefix(s *storage, topic, laneID, path string, frontier durableEnd) (activePrefixSummary, error) {
	stat, err := s.stat(path)
	if err != nil {
		return activePrefixSummary{}, err
	}
	if stat.Size() > s.maxSegmentFileBytes(topic) {
		return activePrefixSummary{}, errors.New("active segment exceeds configured size bound")
	}
	data, err := s.readFile(path)
	if err != nil {
		return activePrefixSummary{}, err
	}
	header, err := decodeSegmentHeader(data)
	if err != nil {
		return activePrefixSummary{}, err
	}
	if header.Topic != topic || header.LaneID != laneID || header.BaseSequence != frontier.ActiveSegmentBase || frontier.DurableByteEnd < int64(header.Length) || frontier.DurableByteEnd > int64(len(data)) {
		return activePrefixSummary{}, errors.New("active segment and durable frontier are inconsistent")
	}
	dataEnd := int(frontier.DurableByteEnd)
	position := header.Length
	expected := header.BaseSequence
	var previousAppend time.Time
	summary := activePrefixSummary{}
	for position < dataEnd {
		record, consumed, err := decodeRecord(data[position:dataEnd], s.topicMaxRecordBytes(topic))
		if err != nil {
			return activePrefixSummary{}, err
		}
		if record.Sequence != expected {
			return activePrefixSummary{}, errors.New("active segment sequence is not continuous")
		}
		if record.Sequence == ^uint64(0) {
			return activePrefixSummary{}, errors.New("active segment sequence is exhausted")
		}
		if !previousAppend.IsZero() && record.AppendTime.Before(previousAppend) {
			return activePrefixSummary{}, errors.New("active segment append time is not monotonic")
		}
		if summary.count == 0 {
			summary.minAppend = record.AppendTime.UnixNano()
		}
		summary.maxAppend = record.AppendTime.UnixNano()
		summary.count++
		previousAppend = record.AppendTime
		if expected == ^uint64(0) {
			if position+consumed < dataEnd {
				return activePrefixSummary{}, errors.New("active segment sequence is exhausted")
			}
		} else {
			expected++
		}
		position += consumed
	}
	if expected != frontier.NextSequence {
		return activePrefixSummary{}, errors.New("active segment sequence end does not match durable frontier")
	}
	summary.dataChecksum = crc32c(data[:dataEnd])
	return summary, nil
}

func validateOrphanOpenSegment(s *storage, topic, laneID string, segment segmentFile) error {
	stat, err := s.stat(segment.path)
	if err != nil {
		return err
	}
	if stat.Size() > maxRecordHeaderBytes {
		return errors.New("lane has an orphan active segment exceeding header bound")
	}
	file, err := s.open(segment.path)
	if err != nil {
		return err
	}
	defer file.Close()
	header, err := readSegmentHeader(file, stat)
	if err != nil {
		return err
	}
	if header.Topic != topic || header.LaneID != laneID || header.BaseSequence != segment.base || int64(header.Length) != stat.Size() {
		return errors.New("lane has an orphan active segment with data or wrong identity")
	}
	return nil
}

func validateSealedSegment(s *storage, topic, laneID string, segment segmentFile) (segmentReadResult, error) {
	result, err := readSegment(s, topic, laneID, segmentFile{base: segment.base, path: segment.path}, durableEnd{}, 0, 0, 0)
	if err != nil {
		return segmentReadResult{}, fmt.Errorf("sealed segment %d validation: %w", segment.base, err)
	}
	return result, nil
}

func laneDurableEnd(s *storage, topic, laneID string) (uint64, error) {
	frontier, err := s.durableEnd(topic, laneID)
	marker, retired, markerErr := s.retired(topic, laneID)
	if markerErr != nil {
		return 0, markerErr
	}
	if err == nil {
		if retired && frontier.NextSequence != marker.FinalNextSequence {
			return 0, errors.New("retired marker and durable frontier disagree")
		}
		return frontier.NextSequence, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	if !retired {
		return 0, err
	}
	return marker.FinalNextSequence, nil
}

func (m *Maintainer) writeAuditRecord(record auditRecord) error {
	if err := validateRequestID(record.RequestID); err != nil {
		return err
	}
	directory := filepath.Join(m.client.storage.root(), "audit")
	if record.Group != "" {
		directory = filepath.Join(m.client.storage.groupDir(record.Topic, record.Group), "audit")
	}
	record.FormatVersion = formatVersion
	path := filepath.Join(directory, record.RequestID+".audit")
	if err := writeMetadataExclusiveWithOps(path, metadataKindAudit, record, 0o640, m.client.storage.fsOps()); err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil
		}
		return err
	}
	return nil
}

func (m *Maintainer) readAudit(topic, group, requestID string) (auditRecord, bool, error) {
	if err := validateRequestID(requestID); err != nil {
		return auditRecord{}, false, err
	}
	directory := filepath.Join(m.client.storage.root(), "audit")
	if group != "" {
		directory = filepath.Join(m.client.storage.groupDir(topic, group), "audit")
	}
	var record auditRecord
	err := readMetadataWithOps(filepath.Join(directory, requestID+".audit"), metadataKindAudit, &record, m.client.storage.fsOps())
	if errors.Is(err, os.ErrNotExist) {
		return auditRecord{}, false, nil
	}
	if err != nil {
		return auditRecord{}, false, err
	}
	if record.FormatVersion != formatVersion || record.RequestID != requestID {
		return auditRecord{}, false, errors.New("audit record identity or format is invalid")
	}
	return record, true, nil
}
