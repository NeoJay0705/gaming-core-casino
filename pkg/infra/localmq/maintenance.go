package localmq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	infraredis "github.com/NeoJay0705/gaming-core-casino/pkg/infra/redis"
)

// AcknowledgeRequest 描述人工處置單一 blocked record 的 durable skip。
type AcknowledgeRequest struct {
	Topic        string
	Group        string
	LaneID       string
	UpToSequence uint64
	Operator     string
	Reason       string
	RequestID    string
}

// ForceRetireLaneRequest 只接受 deployment 提供的外部 fencing 證據；
// localmq 不會自行推論 stale writer 已經消失。
type ForceRetireLaneRequest struct {
	Topic           string
	LaneID          string
	FencingEvidence string
	Operator        string
	Reason          string
	RequestID       string
}

type maintainerState uint8

const (
	maintainerStateNew maintainerState = iota
	maintainerStateStarted
	maintainerStateStopping
	maintainerStateStopped
	maintainerStateFailed
)

// Maintainer 是同一 storage root 的 single maintenance writer。Filesystem
// lock 只避免兩個健康 process 誤啟動；真正 replacement fencing 由部署負責。
type Maintainer struct {
	client       *Client
	membershipFn membershipFactory
	cfg          MaintainerConfig

	mu          sync.Mutex
	operationMu sync.Mutex
	state       maintainerState
	instanceID  string
	stopCh      chan struct{}
	doneCh      chan struct{}
	lockPath    string
	lastErr     error
}

// NewMaintainer 建立尚未啟動的 maintenance controller。Redis 只有在
// DeleteGroup 時必要，GC/checkpoint compaction 不依賴 Redis state。
func NewMaintainer(client *Client, redisClient *infraredis.Client, cfg MaintainerConfig) (*Maintainer, error) {
	var membershipFn membershipFactory
	if redisClient != nil {
		membershipFn = redisMembershipFactory(redisClient)
	}
	return newMaintainerWithMembershipFactory(client, cfg, membershipFn)
}

func newMaintainerWithMembershipFactory(client *Client, cfg MaintainerConfig, membershipFn membershipFactory) (*Maintainer, error) {
	if client == nil {
		return nil, errors.New("localmq maintainer client is nil")
	}
	if cfg.Interval == 0 {
		cfg.Interval = time.Minute
	}
	if cfg.Interval <= 0 {
		return nil, errors.New("localmq maintainer intervals are invalid")
	}
	instanceID, err := newUUIDv7()
	if err != nil {
		return nil, err
	}
	return &Maintainer{
		client:       client,
		membershipFn: membershipFn,
		cfg:          cfg,
		state:        maintainerStateNew,
		instanceID:   instanceID,
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
		lockPath:     filepath.Join(client.storage.root(), maintenanceLockFile),
	}, nil
}

// InitStorage 是明確的 bootstrap operation，不需要先啟動 Client。
func (m *Maintainer) InitStorage(ctx context.Context, request InitStorageRequest) error {
	return InitStorage(ctx, request)
}

func (m *Maintainer) Start(ctx context.Context) error {
	if m == nil {
		return errors.New("localmq maintainer is nil")
	}
	if ctx == nil {
		return errors.New("localmq maintainer start context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	if m.state != maintainerStateNew {
		state := m.state
		m.mu.Unlock()
		return fmt.Errorf("localmq maintainer start is not allowed in state %d", state)
	}
	m.mu.Unlock()
	if m.client.stateValue() != clientStateStarted {
		return errors.New("localmq maintainer client is not started")
	}
	if err := m.acquireLock(); err != nil {
		return err
	}
	m.mu.Lock()
	m.state = maintainerStateStarted
	m.mu.Unlock()
	go m.loop()
	return nil
}

func (m *Maintainer) Stop(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("localmq maintainer stop context is nil")
	}
	m.mu.Lock()
	if m.state == maintainerStateNew || m.state == maintainerStateStopped {
		m.mu.Unlock()
		return nil
	}
	if m.state == maintainerStateStopping {
		done := m.doneCh
		m.mu.Unlock()
		select {
		case <-done:
			return m.releaseLock()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	m.state = maintainerStateStopping
	close(m.stopCh)
	m.mu.Unlock()
	select {
	case <-m.doneCh:
		m.mu.Lock()
		m.state = maintainerStateStopped
		m.mu.Unlock()
		return m.releaseLock()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Maintainer) loop() {
	ticker := time.NewTicker(m.cfg.Interval)
	defer ticker.Stop()
	defer close(m.doneCh)
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			if m.cfg.RunGC {
				if err := m.RunOnce(context.Background()); err != nil {
					// Core 沒有 logging dependency；caller 可透過 RunOnce
					// 或 deployment health adapter 取得同一錯誤。
					m.setLastError(err)
				}
			}
		}
	}
}

func (m *Maintainer) acquireLock() error {
	file, err := m.client.storage.openFile(m.lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		return fmt.Errorf("localmq maintenance lock: %w", err)
	}
	if _, err := file.Write([]byte(m.instanceID + "\n")); err != nil {
		_ = file.Close()
		_ = m.client.storage.remove(m.lockPath)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = m.client.storage.remove(m.lockPath)
		return err
	}
	if err := file.Close(); err != nil {
		_ = m.client.storage.remove(m.lockPath)
		return err
	}
	if err := m.client.storage.syncDirectory(filepath.Dir(m.lockPath)); err != nil {
		_ = m.client.storage.remove(m.lockPath)
		return err
	}
	return nil
}

func (m *Maintainer) releaseLock() error {
	data, err := m.client.storage.readFile(m.lockPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if string(data) != m.instanceID+"\n" {
		return errors.New("localmq maintenance lock owner changed")
	}
	if err := m.client.storage.remove(m.lockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return m.client.storage.syncDirectory(filepath.Dir(m.lockPath))
}

func (m *Maintainer) withOperation(ctx context.Context, operation func() error) error {
	if m == nil {
		return errors.New("localmq maintainer is nil")
	}
	if ctx == nil {
		return errors.New("localmq maintenance context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	started := m.state == maintainerStateStarted
	m.mu.Unlock()
	if !started {
		return errors.New("localmq maintainer is not started")
	}
	m.operationMu.Lock()
	defer m.operationMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return operation()
}

func (m *Maintainer) CreateTopic(ctx context.Context, request CreateTopicRequest) error {
	return m.withOperation(ctx, func() error { return m.createTopic(ctx, request) })
}

func (m *Maintainer) CreateGroup(ctx context.Context, request CreateGroupRequest) error {
	return m.withOperation(ctx, func() error { return m.createGroup(ctx, request) })
}

func (m *Maintainer) DeleteGroup(ctx context.Context, request DeleteGroupRequest) error {
	return m.withOperation(ctx, func() error { return m.deleteGroup(ctx, request) })
}

func (m *Maintainer) AcknowledgeRecords(ctx context.Context, request AcknowledgeRequest) error {
	return m.withOperation(ctx, func() error { return m.acknowledgeRecords(ctx, request) })
}

func (m *Maintainer) ForceRetireLane(ctx context.Context, request ForceRetireLaneRequest) error {
	return m.withOperation(ctx, func() error { return m.forceRetireLane(ctx, request) })
}

// RunOnce 執行一次 checkpoint compaction 與 retention/pressure GC。
func (m *Maintainer) RunOnce(ctx context.Context) error {
	if m == nil {
		return errors.New("localmq maintainer is nil")
	}
	if ctx == nil {
		return errors.New("localmq maintenance context is nil")
	}
	m.mu.Lock()
	started := m.state == maintainerStateStarted
	m.mu.Unlock()
	if !started {
		return errors.New("localmq maintainer is not started")
	}
	m.operationMu.Lock()
	defer m.operationMu.Unlock()
	if err := compactAllGroups(ctx, m); err != nil {
		return err
	}
	return runRetention(ctx, m)
}

func (m *Maintainer) setLastError(err error) {
	if m == nil || err == nil {
		return
	}
	// 只保留最後一個錯誤，避免 maintenance controller 自己建立 unbounded
	// error queue；inspection/health adapter 會在後續版本接入這個欄位。
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastErr = err
}

// LastError 回傳 background maintenance 最近一次錯誤；不保留無界錯誤歷史。
func (m *Maintainer) LastError() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastErr
}

var _ interface {
	Start(context.Context) error
	Stop(context.Context) error
} = (*Maintainer)(nil)
