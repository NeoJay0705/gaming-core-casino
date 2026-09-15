package localmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type clientState uint8

const (
	clientStateNew clientState = iota
	clientStateStarting
	clientStateStarted
	clientStateStopping
	clientStateStopped
	clientStateFailed
)

// Client 是 framework-managed 的 local durable MQ facade。它只在實際
// Publish 某個 Topic 時建立該 process/topic 的 writer lane。
type Client struct {
	cfg     Config
	storage *storage

	mu         sync.Mutex
	state      clientState
	instanceID string
	lanes      map[string]*writerLane
	blocked    map[string]bool
	stopDone   chan struct{}
	stopErr    error
	clock      func() time.Time
	metrics    *clientMetrics
}

func newClient(cfg Config) (*Client, error) {
	return newClientWithDeps(cfg, newStorageOps(), time.Now)
}

func newClientWithDeps(cfg Config, ops *storageOps, clock func() time.Time) (*Client, error) {
	instanceID, err := newUUIDv7()
	if err != nil {
		return nil, err
	}
	if clock == nil {
		clock = time.Now
	}
	storage := newStorageWithDeps(cfg, ops, clock)
	return &Client{
		cfg:        cfg,
		storage:    storage,
		state:      clientStateNew,
		instanceID: instanceID,
		lanes:      make(map[string]*writerLane),
		blocked:    make(map[string]bool),
		clock:      clock,
	}, nil
}

// Config 回傳 immutable runtime config 的 value copy。
func (c *Client) Config() Config {
	if c == nil {
		return Config{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	result := c.cfg
	result.Consumer = c.cfg.Consumer
	return result
}

// Start 驗證既有 storage-control 並啟動 client。Storage bootstrap 必須由
// 明確的 InitStorage 操作完成，Client 不會偷偷建立 root policy。
func (c *Client) Start(ctx context.Context) error {
	if c == nil {
		return errors.New("localmq client is nil")
	}
	if ctx == nil {
		return errors.New("localmq start context is nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("localmq start cancelled: %w", err)
	}
	c.mu.Lock()
	if c.state != clientStateNew {
		state := c.state
		c.mu.Unlock()
		return fmt.Errorf("localmq start is not allowed in state %d", state)
	}
	c.state = clientStateStarting
	c.mu.Unlock()
	if err := c.storage.start(ctx); err != nil {
		c.mu.Lock()
		c.state = clientStateFailed
		c.mu.Unlock()
		return err
	}
	c.mu.Lock()
	if c.state != clientStateStarting {
		c.mu.Unlock()
		c.storage.stop()
		return errors.New("localmq start was interrupted")
	}
	c.state = clientStateStarted
	c.mu.Unlock()
	return nil
}

// Stop 停止 admission，排空已入 queue 的 requests，seal non-empty WAL
// segment，最後以 exclusive retired marker 結束每條 process-owned lane。
func (c *Client) Stop(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("localmq stop context is nil")
	}
	c.mu.Lock()
	if c.state == clientStateNew || c.state == clientStateStopped || c.state == clientStateFailed {
		c.mu.Unlock()
		return nil
	}
	if c.state == clientStateStopping {
		done := c.stopDone
		c.mu.Unlock()
		select {
		case <-done:
			c.mu.Lock()
			err := c.stopErr
			c.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c.state = clientStateStopping
	c.stopDone = make(chan struct{})
	stopDone := c.stopDone
	lanes := make([]*writerLane, 0, len(c.lanes))
	for _, lane := range c.lanes {
		lanes = append(lanes, lane)
	}
	c.mu.Unlock()

	var stopErr error
	for _, lane := range lanes {
		if err := lane.stop(ctx); err != nil && stopErr == nil {
			stopErr = err
		}
	}
	if stopErr != nil {
		c.mu.Lock()
		c.stopErr = stopErr
		c.state = clientStateFailed
		close(stopDone)
		c.mu.Unlock()
		return stopErr
	}
	c.storage.stop()
	c.mu.Lock()
	c.stopErr = nil
	c.state = clientStateStopped
	close(stopDone)
	c.mu.Unlock()
	return stopErr
}

// Publish 將 opaque event 放入 process/topic bounded queue。成功回覆前，
// record 與 durable-end 都已完成 sync；Topic 不存在時不會自動建立。
func (c *Client) Publish(ctx context.Context, message Message) (Receipt, error) {
	if ctx == nil {
		return Receipt{}, &PublishError{Kind: PublishRejectedBeforeWrite, Cause: errors.New("publish context is nil")}
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, &PublishError{Kind: PublishRejectedBeforeWrite, Cause: err}
	}
	if err := validateTopicName(message.Topic); err != nil {
		return Receipt{}, &PublishError{Kind: PublishRejectedBeforeWrite, Cause: err}
	}
	c.mu.Lock()
	if c.state != clientStateStarted {
		state := c.state
		c.mu.Unlock()
		return Receipt{}, &PublishError{Kind: PublishRejectedBeforeWrite, Cause: fmt.Errorf("localmq client is not started (state %d)", state)}
	}
	manifest, err := c.storage.topicManifest(message.Topic)
	if err != nil {
		c.mu.Unlock()
		return Receipt{}, &PublishError{Kind: PublishRejectedBeforeWrite, Cause: fmt.Errorf("topic %q: %w", message.Topic, err)}
	}
	if err := validateMessage(message, manifest.MaxRecordBytes); err != nil {
		c.mu.Unlock()
		return Receipt{}, &PublishError{Kind: PublishRejectedBeforeWrite, Cause: err}
	}
	lane := c.lanes[message.Topic]
	if lane != nil && !lane.isAvailable() {
		delete(c.lanes, message.Topic)
		lane = nil
	}
	if lane == nil {
		lane, err = newWriterLane(c, c.storage, c.cfg, c.instanceID, message.Topic, manifest.MaxRecordBytes, c.clock, c.metrics)
		if err != nil {
			c.mu.Unlock()
			return Receipt{}, &PublishError{Kind: PublishRejectedBeforeWrite, Cause: err}
		}
		c.lanes[message.Topic] = lane
		go lane.run()
	}
	c.mu.Unlock()
	startedAt := c.clock()
	receipt, publishErr := lane.publish(ctx, cloneMessage(message))
	result := "success"
	if publishErr != nil {
		if IsDurabilityUnknown(publishErr) {
			result = "durability_unknown"
		} else {
			result = "rejected"
		}
	}
	c.metricPublish(message.Topic, result, c.clock().Sub(startedAt))
	return receipt, publishErr
}

// RegisterMetrics 將固定名稱、低 cardinality 的 client metrics 註冊至
// repository 共用的 Prometheus Registerer。Topic/group 是 bounded labels；
// 不使用 message_id、lane_id 或 error text 作 label。
func (c *Client) RegisterMetrics(registerer prometheus.Registerer) error {
	if c == nil {
		return errors.New("localmq metrics: client is nil")
	}
	if registerer == nil {
		return errors.New("localmq metrics: registerer is nil")
	}
	c.mu.Lock()
	if c.metrics != nil {
		c.mu.Unlock()
		return errors.New("localmq metrics are already registered")
	}
	metrics := newClientMetrics()
	c.metrics = metrics
	c.mu.Unlock()
	if err := registerer.Register(metrics); err != nil {
		c.mu.Lock()
		c.metrics = nil
		c.mu.Unlock()
		return err
	}
	return nil
}

func (c *Client) metricPublish(topic, result string, elapsed time.Duration) {
	c.mu.Lock()
	metrics := c.metrics
	c.mu.Unlock()
	if metrics == nil {
		return
	}
	metrics.publishTotal.WithLabelValues(topic, result).Inc()
	metrics.publishLatency.WithLabelValues(topic).Observe(elapsed.Seconds())
}

func (c *Client) metricQueue(topic string, depth int) {
	c.mu.Lock()
	metrics := c.metrics
	c.mu.Unlock()
	if metrics != nil {
		metrics.queueDepth.WithLabelValues(topic).Set(float64(depth))
	}
}

func (c *Client) metricRebalance(topic, group string) {
	c.mu.Lock()
	metrics := c.metrics
	c.mu.Unlock()
	if metrics != nil {
		metrics.rebalanceTotal.WithLabelValues(topic, group).Inc()
	}
}

func (c *Client) metricCheckpointFailure(topic, group string) {
	c.mu.Lock()
	metrics := c.metrics
	c.mu.Unlock()
	if metrics != nil {
		metrics.checkpointFailures.WithLabelValues(topic, group).Inc()
	}
}

func (c *Client) metricConsumerState(topic, group string, lag uint64, oldestAge time.Duration) {
	c.mu.Lock()
	metrics := c.metrics
	c.mu.Unlock()
	if metrics != nil {
		metrics.consumerLag.WithLabelValues(topic, group).Set(float64(lag))
		metrics.oldestAge.WithLabelValues(topic, group).Set(oldestAge.Seconds())
	}
}

func (c *Client) metricBlocked(topic, group, laneID string, blocked bool) {
	c.mu.Lock()
	metrics := c.metrics
	key := topic + "\x00" + group + "\x00" + laneID
	if blocked {
		c.blocked[key] = true
	} else {
		delete(c.blocked, key)
	}
	count := 0
	groupPrefix := topic + "\x00" + group + "\x00"
	for item := range c.blocked {
		if len(item) >= len(groupPrefix) && item[:len(groupPrefix)] == groupPrefix {
			count++
		}
	}
	c.mu.Unlock()
	if metrics != nil {
		metrics.blockedLanes.WithLabelValues(topic, group).Set(float64(count))
	}
}

func (c *Client) metricGC(topic, reason string, bytes int64) {
	c.mu.Lock()
	metrics := c.metrics
	c.mu.Unlock()
	if metrics != nil && bytes > 0 {
		metrics.gcBytes.WithLabelValues(topic, reason).Add(float64(bytes))
	}
}

func (c *Client) metricBestEffortSkipped(topic, group string, messages, bytes uint64) {
	c.mu.Lock()
	metrics := c.metrics
	c.mu.Unlock()
	if metrics != nil {
		metrics.skippedMessages.WithLabelValues(topic, group).Add(float64(messages))
		metrics.skippedBytes.WithLabelValues(topic, group).Add(float64(bytes))
	}
}

func (c *Client) stateValue() clientState {
	if c == nil {
		return clientStateFailed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

var _ interface {
	Start(context.Context) error
	Stop(context.Context) error
} = (*Client)(nil)
