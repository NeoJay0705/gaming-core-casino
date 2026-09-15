package localmq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const maxBlockedMarkers = 10000

func (s *storage) blockedPath(topic, group, laneID, instanceID string, sequence uint64) string {
	return filepath.Join(s.groupDir(topic, group), blockedDirectory, laneID, fmt.Sprintf("%d-%s.blocked", sequence, instanceID))
}

func (s *storage) writeBlocked(ctx context.Context, marker blockedRecord) error {
	if ctx == nil {
		return errors.New("blocked marker context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateTopicAndGroup(marker.Topic, marker.Group); err != nil {
		return err
	}
	if err := validateLaneID(marker.LaneID); err != nil {
		return err
	}
	if marker.ConsumerInstanceID == "" {
		return errors.New("blocked marker consumer_instance_id is required")
	}
	path := s.blockedPath(marker.Topic, marker.Group, marker.LaneID, marker.ConsumerInstanceID, marker.Sequence)
	marker.FormatVersion = formatVersion
	if err := writeMetadataExclusiveWithOps(path, metadataKindBlocked, marker, 0o640, s.fsOps()); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		var existing blockedRecord
		if readErr := readMetadataWithOps(path, metadataKindBlocked, &existing, s.fsOps()); readErr != nil {
			return readErr
		}
		if existing.Topic != marker.Topic || existing.Group != marker.Group || existing.LaneID != marker.LaneID || existing.Sequence != marker.Sequence {
			return errors.New("existing blocked marker conflicts with request")
		}
	}
	return nil
}

func (s *storage) listBlocked(topic, group, laneID string) ([]blockedRecord, error) {
	directory := filepath.Join(s.groupDir(topic, group), blockedDirectory, laneID)
	entries, err := s.readDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(entries) > maxBlockedMarkers {
		return nil, errors.New("blocked marker directory exceeds configured bound")
	}
	markers := make([]blockedRecord, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".blocked") {
			continue
		}
		sequence, instanceID, err := parseBlockedName(entry.Name())
		if err != nil {
			return nil, err
		}
		var marker blockedRecord
		if err := readMetadataWithOps(filepath.Join(directory, entry.Name()), metadataKindBlocked, &marker, s.fsOps()); err != nil {
			return nil, err
		}
		if marker.FormatVersion != formatVersion || marker.Topic != topic || marker.Group != group || marker.LaneID != laneID || marker.Sequence != sequence || marker.ConsumerInstanceID != instanceID {
			return nil, errors.New("blocked marker identity or format is invalid")
		}
		markers = append(markers, marker)
	}
	sort.Slice(markers, func(i, j int) bool {
		if markers[i].Sequence != markers[j].Sequence {
			return markers[i].Sequence < markers[j].Sequence
		}
		return markers[i].ConsumerInstanceID < markers[j].ConsumerInstanceID
	})
	return markers, nil
}

func (s *storage) earliestBlocked(topic, group, laneID string, checkpoint uint64) (blockedRecord, bool, error) {
	markers, err := s.listBlocked(topic, group, laneID)
	if err != nil {
		return blockedRecord{}, false, err
	}
	for _, marker := range markers {
		if marker.Sequence >= checkpoint {
			return marker, true, nil
		}
	}
	return blockedRecord{}, false, nil
}

func (s *storage) removeBlockedBelow(ctx context.Context, topic, group, laneID string, nextSequence uint64) error {
	if ctx == nil {
		return errors.New("blocked cleanup context is nil")
	}
	markers, err := s.listBlocked(topic, group, laneID)
	if err != nil {
		return err
	}
	for _, marker := range markers {
		if marker.Sequence >= nextSequence {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		path := s.blockedPath(topic, group, laneID, marker.ConsumerInstanceID, marker.Sequence)
		if err := s.remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return s.syncDirectory(filepath.Join(s.groupDir(topic, group), blockedDirectory, laneID))
}

func parseBlockedName(name string) (uint64, string, error) {
	if !strings.HasSuffix(name, ".blocked") {
		return 0, "", errors.New("blocked marker suffix is invalid")
	}
	base := strings.TrimSuffix(name, ".blocked")
	separator := strings.IndexByte(base, '-')
	if separator <= 0 || separator == len(base)-1 {
		return 0, "", errors.New("blocked marker name is invalid")
	}
	sequence, err := strconv.ParseUint(base[:separator], 10, 64)
	if err != nil {
		return 0, "", err
	}
	return sequence, base[separator+1:], nil
}
