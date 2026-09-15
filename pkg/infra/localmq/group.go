package localmq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// CreateTopicRequest 建立 immutable Topic manifest 與可更新的 retention control。
type CreateTopicRequest struct {
	Topic          string
	MaxRecordBytes int64
	Retention      time.Duration
}

// CreateGroupRequest 建立 immutable consumer group manifest。
type CreateGroupRequest struct {
	Topic           string
	Group           string
	GroupType       GroupType
	InitialPosition InitialPosition
}

// DeleteGroupRequest 要求 group 進入 DELETING，並在 Redis membership 確認
// 經過完整 timeout 後發布 durable DELETED tombstone。
type DeleteGroupRequest struct {
	Topic     string
	Group     string
	Operator  string
	Reason    string
	RequestID string
}

func (m *Maintainer) createTopic(ctx context.Context, request CreateTopicRequest) error {
	if err := validateTopicName(request.Topic); err != nil {
		return err
	}
	if request.MaxRecordBytes == 0 {
		request.MaxRecordBytes = defaultMaxRecordBytes
	}
	if request.MaxRecordBytes <= 0 || request.MaxRecordBytes > maxMetadataBytes {
		return errors.New("max_record_bytes is outside supported bounds")
	}
	if request.Retention < 0 {
		return errors.New("retention cannot be negative")
	}
	if request.Retention%time.Second != 0 {
		return errors.New("retention must use whole seconds")
	}
	topicDir := m.client.storage.topicDir(request.Topic)
	if err := m.client.storage.mkdirAll(filepath.Join(topicDir, lanesDirectory), 0o750); err != nil {
		return err
	}
	if err := m.client.storage.mkdirAll(filepath.Join(topicDir, gcStagingDirectory), 0o750); err != nil {
		return err
	}
	now := m.client.storage.now()
	manifest := topicManifest{
		FormatVersion:     formatVersion,
		Topic:             request.Topic,
		MaxRecordBytes:    request.MaxRecordBytes,
		CreatedAtUnixNano: now.UnixNano(),
	}
	manifestPath := filepath.Join(topicDir, topicManifestFile)
	if err := writeMetadataExclusiveWithOps(manifestPath, metadataKindTopicManifest, manifest, 0o640, m.client.storage.fsOps()); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		existing, readErr := m.client.storage.topicManifest(request.Topic)
		if readErr != nil {
			return readErr
		}
		if existing.MaxRecordBytes != request.MaxRecordBytes {
			return errors.New("topic already exists with different immutable settings")
		}
	}
	control := topicControl{
		FormatVersion:        formatVersion,
		RetentionTimeSeconds: int64(request.Retention / time.Second),
		UpdatedAtUnixNano:    now.UnixNano(),
	}
	controlPath := filepath.Join(topicDir, topicControlFile)
	if err := writeMetadataExclusiveWithOps(controlPath, metadataKindTopicControl, control, 0o640, m.client.storage.fsOps()); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		existing, readErr := m.client.storage.topicControl(request.Topic)
		if readErr != nil {
			return readErr
		}
		if existing.RetentionTimeSeconds != control.RetentionTimeSeconds {
			return errors.New("topic already exists with different retention policy")
		}
	}
	return m.client.storage.syncDirectory(filepath.Dir(topicDir))
}

func (m *Maintainer) createGroup(ctx context.Context, request CreateGroupRequest) error {
	if err := validateTopicAndGroup(request.Topic, request.Group); err != nil {
		return err
	}
	if !validGroupType(string(request.GroupType)) {
		return fmt.Errorf("group_type %q is invalid", request.GroupType)
	}
	if !validInitialPosition(string(request.InitialPosition)) {
		return fmt.Errorf("initial_position %q is invalid", request.InitialPosition)
	}
	if _, err := m.client.storage.topicManifest(request.Topic); err != nil {
		return err
	}
	groupDir := m.client.storage.groupDir(request.Topic, request.Group)
	if err := m.client.storage.mkdirAll(groupDir, 0o750); err != nil {
		return err
	}
	if err := m.client.storage.mkdirAll(filepath.Join(groupDir, blockedDirectory), 0o750); err != nil {
		return err
	}
	if err := m.client.storage.mkdirAll(filepath.Join(groupDir, membersDirectory), 0o750); err != nil {
		return err
	}
	now := m.client.storage.now()
	manifest := groupManifest{
		FormatVersion:     formatVersion,
		Topic:             request.Topic,
		Group:             request.Group,
		GroupType:         string(request.GroupType),
		InitialPosition:   string(request.InitialPosition),
		CreatedAtUnixNano: now.UnixNano(),
	}
	manifestPath := filepath.Join(groupDir, groupManifestFile)
	if err := writeMetadataExclusiveWithOps(manifestPath, metadataKindGroupManifest, manifest, 0o640, m.client.storage.fsOps()); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		existing, readErr := m.client.storage.groupManifest(request.Topic, request.Group)
		if readErr != nil {
			return readErr
		}
		if existing.GroupType != manifest.GroupType || existing.InitialPosition != manifest.InitialPosition {
			return errors.New("group already exists with different immutable settings")
		}
		control, controlErr := m.client.storage.groupControl(request.Topic, request.Group)
		if controlErr != nil && !errors.Is(controlErr, os.ErrNotExist) {
			return controlErr
		}
		if controlErr == nil && control.State == string(GroupDeleted) {
			return errors.New("deleted group cannot be recreated")
		}
		if controlErr == nil && control.State != string(GroupInitializing) {
			return errors.New("group already exists")
		}
	}
	controlPath := filepath.Join(groupDir, groupControlFile)
	control := groupControl{FormatVersion: formatVersion, State: string(GroupInitializing), UpdatedAtUnixNano: now.UnixNano()}
	if existingControl, controlErr := m.client.storage.groupControl(request.Topic, request.Group); controlErr == nil {
		if existingControl.State != string(GroupInitializing) {
			return errors.New("group already exists")
		}
		control = existingControl
	} else if errors.Is(controlErr, os.ErrNotExist) {
		if err := writeMetadataExclusiveWithOpsAtPoint(controlPath, metadataKindGroupControl, control, 0o640, m.client.storage.fsOps(), faultGroupControlRename); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
	} else {
		return controlErr
	}
	checkpoints, err := m.client.storage.initialCheckpoints(ctx, request.Topic, request.InitialPosition)
	if err != nil {
		return err
	}
	if err := m.client.storage.clampCheckpointsToFloors(ctx, request.Topic, checkpoints); err != nil {
		return err
	}
	if err := m.client.storage.writeCheckpointBase(ctx, request.Topic, request.Group, checkpoints); err != nil {
		return err
	}
	control.State = string(GroupActive)
	control.UpdatedAtUnixNano = m.client.storage.now().UnixNano()
	return writeMetadataAtomicWithOps(controlPath, metadataKindGroupControl, control, 0o640, m.client.storage.fsOps(), "", faultGroupControlRename, "")
}

func (m *Maintainer) deleteGroup(ctx context.Context, request DeleteGroupRequest) error {
	if err := validateTopicAndGroup(request.Topic, request.Group); err != nil {
		return err
	}
	if err := validateNonEmpty(request.Operator, "operator"); err != nil {
		return err
	}
	if err := validateNonEmpty(request.Reason, "reason"); err != nil {
		return err
	}
	if err := validateNonEmpty(request.RequestID, "request_id"); err != nil {
		return err
	}
	if m.membershipFn == nil {
		return errors.New("localmq DeleteGroup requires Redis membership client")
	}
	_, err := m.client.storage.groupManifest(request.Topic, request.Group)
	if err != nil {
		return err
	}
	control, err := m.client.storage.groupControl(request.Topic, request.Group)
	if err != nil {
		return err
	}
	if control.State == string(GroupDeleted) {
		return nil
	}
	control.State = string(GroupDeleting)
	control.UpdatedAtUnixNano = m.client.storage.now().UnixNano()
	if err := writeMetadataAtomicWithOps(filepath.Join(m.client.storage.groupDir(request.Topic, request.Group), groupControlFile), metadataKindGroupControl, control, 0o640, m.client.storage.fsOps(), "", faultGroupControlRename, ""); err != nil {
		return err
	}
	membership, err := m.membershipFn(request.Topic, request.Group, "maintenance", m.client.cfg.Consumer.MembershipTimeout)
	if err != nil {
		return err
	}
	deadline := m.client.storage.now().Add(m.client.cfg.Consumer.MembershipTimeout)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		members, err := membership.activeMembers(ctx)
		if err != nil {
			return err
		}
		if len(members) == 0 {
			break
		}
		if !waitWithContext(ctx, m.client.cfg.Consumer.HeartbeatInterval) {
			return ctx.Err()
		}
	}
	if now := m.client.storage.now(); now.Before(deadline) {
		if !waitWithContext(ctx, deadline.Sub(now)) {
			return ctx.Err()
		}
	}
	control.State = string(GroupDeleted)
	control.DeletedAtUnixNano = m.client.storage.now().UnixNano()
	control.UpdatedAtUnixNano = control.DeletedAtUnixNano
	if err := writeMetadataAtomicWithOps(filepath.Join(m.client.storage.groupDir(request.Topic, request.Group), groupControlFile), metadataKindGroupControl, control, 0o640, m.client.storage.fsOps(), "", faultGroupControlRename, ""); err != nil {
		return err
	}
	return m.writeAuditRecord(auditRecord{
		RequestID:         request.RequestID,
		Operation:         "delete_group",
		Operator:          request.Operator,
		Reason:            request.Reason,
		Topic:             request.Topic,
		Group:             request.Group,
		CreatedAtUnixNano: m.client.storage.now().UnixNano(),
	})
}
