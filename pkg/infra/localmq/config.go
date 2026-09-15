package localmq

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
)

const (
	defaultPublishQueueCapacity = 4096
	defaultGroupCommitMessages  = 256
	defaultGroupCommitBytes     = 1 << 20
	defaultGroupCommitWait      = 2 * time.Millisecond
	defaultSegmentMaxBytes      = 128 << 20
	defaultSegmentMaxAge        = time.Hour
	defaultProducerStopFree     = 1 << 30
	defaultRecoveryReserve      = 512 << 20
	defaultHeartbeatInterval    = 3 * time.Second
	defaultMembershipTimeout    = 15 * time.Second
	defaultReconcileInterval    = 5 * time.Second
	defaultWorkerConcurrency    = 8
	defaultBatchMaxMessages     = 500
	defaultBatchMaxBytes        = 4 << 20
	defaultBatchMaxWait         = 10 * time.Millisecond
	defaultHandlerTimeout       = 30 * time.Second
	defaultCheckpointRecords    = 5000
	defaultCheckpointDelay      = 100 * time.Millisecond
	defaultMaxRecordBytes       = 1 << 20
)

// Config 是 process runtime config。Topic/Group/root control 會以 durable
// metadata 為準，不由各 pod 的 local config 靜默覆寫。
type Config struct {
	RootPath               string         `config:"root_path"`
	StorageID              string         `config:"storage_id"`
	PublishQueueCapacity   int            `config:"publish_queue_capacity"`
	GroupCommitMaxMessages int            `config:"group_commit_max_messages"`
	GroupCommitMaxBytes    int64          `config:"group_commit_max_bytes"`
	GroupCommitMaxWait     time.Duration  `config:"group_commit_max_wait"`
	SegmentMaxBytes        int64          `config:"segment_max_bytes"`
	SegmentMaxAge          time.Duration  `config:"segment_max_age"`
	ProducerStopFreeBytes  int64          `config:"producer_stop_free_bytes"`
	RecoveryReserveBytes   int64          `config:"recovery_reserve_bytes"`
	MaxClockSkew           time.Duration  `config:"max_clock_skew"`
	Consumer               ConsumerConfig `config:"consumer"`
}

// ConsumerConfig 控制 consumer runtime；Topic/Group 由 consumer options 提供。
type ConsumerConfig struct {
	HeartbeatInterval    time.Duration `config:"heartbeat_interval"`
	MembershipTimeout    time.Duration `config:"membership_timeout"`
	ReconcileInterval    time.Duration `config:"reconcile_interval"`
	WorkerConcurrency    int           `config:"worker_concurrency"`
	BatchMaxMessages     int           `config:"batch_max_messages"`
	BatchMaxBytes        int64         `config:"batch_max_bytes"`
	BatchMaxWait         time.Duration `config:"batch_max_wait"`
	HandlerTimeout       time.Duration `config:"handler_timeout"`
	CheckpointMaxRecords int           `config:"checkpoint_max_records"`
	CheckpointMaxDelay   time.Duration `config:"checkpoint_max_delay"`
}

// MaintainerConfig 控制單一 maintenance controller。
type MaintainerConfig struct {
	Interval time.Duration `config:"interval"`
	RunGC    bool          `config:"run_gc"`
}

// DefaultConfig 回傳適合 unit/contract test 與 local development 的 defaults。
func DefaultConfig() Config {
	return Config{
		PublishQueueCapacity:   defaultPublishQueueCapacity,
		GroupCommitMaxMessages: defaultGroupCommitMessages,
		GroupCommitMaxBytes:    defaultGroupCommitBytes,
		GroupCommitMaxWait:     defaultGroupCommitWait,
		SegmentMaxBytes:        defaultSegmentMaxBytes,
		SegmentMaxAge:          defaultSegmentMaxAge,
		ProducerStopFreeBytes:  defaultProducerStopFree,
		RecoveryReserveBytes:   defaultRecoveryReserve,
		MaxClockSkew:           time.Second,
		Consumer: ConsumerConfig{
			HeartbeatInterval:    defaultHeartbeatInterval,
			MembershipTimeout:    defaultMembershipTimeout,
			ReconcileInterval:    defaultReconcileInterval,
			WorkerConcurrency:    defaultWorkerConcurrency,
			BatchMaxMessages:     defaultBatchMaxMessages,
			BatchMaxBytes:        defaultBatchMaxBytes,
			BatchMaxWait:         defaultBatchMaxWait,
			HandlerTimeout:       defaultHandlerTimeout,
			CheckpointMaxRecords: defaultCheckpointRecords,
			CheckpointMaxDelay:   defaultCheckpointDelay,
		},
	}
}

// New 讀取並驗證 local_mq 設定，不執行 filesystem I/O。
func New(snapshot config.SourceSnapshot) (*Client, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("localmq config snapshot is nil")
	}
	cfg := DefaultConfig()
	if err := snapshot.Bind("local_mq", &cfg, config.Strict()); err != nil {
		return nil, fmt.Errorf("localmq config: %w", err)
	}
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	client, err := newClient(cfg)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func validateConfig(cfg *Config) error {
	cfg.RootPath = filepath.Clean(strings.TrimSpace(cfg.RootPath))
	cfg.StorageID = strings.TrimSpace(cfg.StorageID)
	if cfg.RootPath == "." || cfg.RootPath == "" {
		return fmt.Errorf("localmq config: root_path is required")
	}
	if cfg.StorageID == "" {
		return fmt.Errorf("localmq config: storage_id is required")
	}
	if _, err := parseStorageID(cfg.StorageID); err != nil {
		return err
	}
	if cfg.PublishQueueCapacity <= 0 || cfg.GroupCommitMaxMessages <= 0 || cfg.GroupCommitMaxBytes <= 0 || cfg.GroupCommitMaxWait <= 0 {
		return fmt.Errorf("localmq config: publish and group commit limits must be greater than zero")
	}
	if cfg.PublishQueueCapacity > 1<<20 || cfg.GroupCommitMaxMessages > 1<<20 || cfg.GroupCommitMaxBytes > 256<<20 {
		return fmt.Errorf("localmq config: publish and group commit limits exceed bounded safety limits")
	}
	if cfg.SegmentMaxBytes <= 0 || cfg.SegmentMaxAge <= 0 {
		return fmt.Errorf("localmq config: segment limits must be greater than zero")
	}
	if cfg.SegmentMaxBytes > 1<<40 {
		return fmt.Errorf("localmq config: segment_max_bytes exceeds bounded safety limit")
	}
	if cfg.RecoveryReserveBytes <= 0 || cfg.ProducerStopFreeBytes <= cfg.RecoveryReserveBytes {
		return fmt.Errorf("localmq config: producer_stop_free_bytes must be greater than recovery_reserve_bytes > 0")
	}
	if cfg.MaxClockSkew < 0 {
		return fmt.Errorf("localmq config: max_clock_skew cannot be negative")
	}
	cc := &cfg.Consumer
	if cc.HeartbeatInterval <= 0 || cc.MembershipTimeout < 3*cc.HeartbeatInterval || cc.ReconcileInterval <= 0 || cc.WorkerConcurrency <= 0 || cc.BatchMaxMessages <= 0 || cc.BatchMaxBytes <= 0 || cc.BatchMaxWait <= 0 || cc.HandlerTimeout <= 0 || cc.CheckpointMaxRecords <= 0 || cc.CheckpointMaxDelay <= 0 {
		return fmt.Errorf("localmq config: consumer limits are invalid")
	}
	if cc.BatchMaxBytes > 256<<20 || cc.BatchMaxMessages > 1<<20 || cc.WorkerConcurrency > 1<<20 {
		return fmt.Errorf("localmq config: consumer limits exceed bounded safety limits")
	}
	return nil
}
