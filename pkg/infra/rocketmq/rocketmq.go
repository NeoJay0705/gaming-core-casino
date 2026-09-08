// Package rocketmq provides a framework-managed RocketMQ producer connection.
package rocketmq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	rocketmq4 "github.com/apache/rocketmq-client-go/v2"
	primitive4 "github.com/apache/rocketmq-client-go/v2/primitive"
	producer4 "github.com/apache/rocketmq-client-go/v2/producer"
)

// Config configures a RocketMQ producer. Consumers, subscriptions and message
// handlers deliberately belong to product business modules and are not created
// by this infrastructure resource.
type Config struct {
	Endpoint           string `config:"endpoint" yaml:"endpoint"`
	Namespace          string `config:"namespace" yaml:"namespace"`
	AccessKey          string `config:"access_key" yaml:"access_key"`
	SecretKey          string `config:"secret_key" yaml:"secret_key"`
	ProducerRetryTimes int    `config:"producer_retry_times" yaml:"producer_retry_times"`
}

// Message is the driver-neutral producer input. Event schemas and routing
// policy remain product concerns.
type Message struct {
	Topic        string
	Body         []byte
	Tag          string
	Keys         []string
	Properties   map[string]string
	MessageGroup string
}

// SendReceipt is the broker acknowledgement returned by Send.
type SendReceipt struct {
	MessageID     string
	TransactionID string
	Offset        int64
}

// Client owns the SDK producer. Product modules can depend on *Client and send
// driver-neutral messages from their own lifecycle-managed services.
type Client struct {
	mu          sync.RWMutex
	cfg         Config
	producer    producer
	state       state
	sends       sync.WaitGroup
	stopDone    chan struct{}
	newProducer func(Config) (producer, error)
}

type producer interface {
	Send(context.Context, *Message) ([]SendReceipt, error)
	Stop() error
}

type state uint8

const (
	stateNew state = iota
	stateStarted
	stateStopping
	stateStopped
	stateFailed
)

// New reads and validates RocketMQ configuration. It performs no I/O.
func New(snapshot config.SourceSnapshot) (*Client, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("rocketmq config snapshot is nil")
	}
	var cfg Config
	if err := snapshot.Bind("rocketmq", &cfg, config.Strict()); err != nil {
		return nil, fmt.Errorf("rocketmq config: %w", err)
	}
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	return &Client{cfg: cfg, newProducer: newProducer}, nil
}

// Start creates and starts only a producer. It never creates a consumer or a
// subscription as doing so without a product handler creates orphan backlog.
func (c *Client) Start(ctx context.Context) error {
	if c == nil {
		return fmt.Errorf("rocketmq client is nil")
	}
	if ctx == nil {
		return fmt.Errorf("rocketmq start context is nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("rocketmq start cancelled: %w", err)
	}
	c.mu.Lock()
	if c.state != stateNew {
		state := c.state
		c.mu.Unlock()
		return fmt.Errorf("rocketmq start is not allowed in state %d", state)
	}
	c.mu.Unlock()

	p, err := c.newProducer(c.cfg)
	if err != nil {
		c.fail()
		return fmt.Errorf("rocketmq create producer: %w", err)
	}
	if err := ctx.Err(); err != nil {
		_ = p.Stop()
		c.fail()
		return fmt.Errorf("rocketmq start cancelled: %w", err)
	}
	c.mu.Lock()
	if c.state != stateNew {
		c.mu.Unlock()
		_ = p.Stop()
		return fmt.Errorf("rocketmq start was interrupted")
	}
	c.producer, c.state = p, stateStarted
	c.mu.Unlock()
	return nil
}

// Stop rejects new sends, waits for in-flight sends, then shuts down the SDK
// producer. If its context expires, it forces shutdown to unblock callers.
func (c *Client) Stop(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("rocketmq stop context is nil")
	}
	c.mu.Lock()
	if c.state == stateStopping {
		done := c.stopDone
		c.mu.Unlock()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return fmt.Errorf("rocketmq stop cancelled: %w", ctx.Err())
		}
	}
	if c.state != stateStarted {
		c.mu.Unlock()
		return nil
	}
	p := c.producer
	c.producer, c.state = nil, stateStopping
	done := make(chan struct{})
	c.stopDone = done
	c.mu.Unlock()

	wait := make(chan struct{})
	go func() {
		c.sends.Wait()
		close(wait)
	}()
	var err error
	select {
	case <-wait:
		err = p.Stop()
	case <-ctx.Done():
		err = errors.Join(fmt.Errorf("rocketmq stop cancelled: %w", ctx.Err()), p.Stop())
	}
	c.mu.Lock()
	c.state = stateStopped
	close(done)
	c.mu.Unlock()
	return err
}

// Send publishes one message through the started producer.
func (c *Client) Send(ctx context.Context, message *Message) ([]SendReceipt, error) {
	if c == nil {
		return nil, fmt.Errorf("rocketmq client is nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("rocketmq send context is nil")
	}
	if message == nil {
		return nil, fmt.Errorf("rocketmq message is nil")
	}
	if strings.TrimSpace(message.Topic) == "" {
		return nil, fmt.Errorf("rocketmq message topic is required")
	}
	c.mu.Lock()
	p := c.producer
	started := c.state == stateStarted
	if started && p != nil {
		c.sends.Add(1)
	}
	c.mu.Unlock()
	if !started || p == nil {
		return nil, fmt.Errorf("rocketmq producer is not started")
	}
	defer c.sends.Done()
	return p.Send(ctx, message)
}

// Config returns the normalized immutable configuration.
func (c *Client) Config() Config {
	if c == nil {
		return Config{}
	}
	return c.cfg
}

func (c *Client) fail() {
	c.mu.Lock()
	if c.state == stateNew {
		c.state = stateFailed
	}
	c.mu.Unlock()
}

func validateConfig(cfg *Config) error {
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)
	if cfg.Endpoint == "" {
		return fmt.Errorf("rocketmq config: endpoint is required")
	}
	if _, err := nameServers(cfg.Endpoint); err != nil {
		return fmt.Errorf("rocketmq config: %w", err)
	}
	if (cfg.AccessKey == "") != (cfg.SecretKey == "") {
		return fmt.Errorf("rocketmq config: access_key and secret_key must be set together")
	}
	if cfg.ProducerRetryTimes < 0 {
		return fmt.Errorf("rocketmq config: producer_retry_times cannot be negative")
	}
	return nil
}

func newProducer(cfg Config) (producer, error) {
	return newRocketMQ4Producer(cfg)
}

func newRocketMQ4Producer(cfg Config) (producer, error) {
	return newRocketMQ4ProducerWithFactory(cfg, createRocketMQ4Producer)
}

type rocketMQ4SDKProducer interface {
	Start() error
	Shutdown() error
	SendSync(context.Context, ...*primitive4.Message) (*primitive4.SendResult, error)
}

func createRocketMQ4Producer(options ...producer4.Option) (rocketMQ4SDKProducer, error) {
	return rocketmq4.NewProducer(options...)
}

func newRocketMQ4ProducerWithFactory(cfg Config, create func(...producer4.Option) (rocketMQ4SDKProducer, error)) (producer, error) {
	nameServers, err := nameServers(cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	options := []producer4.Option{
		producer4.WithNameServer(nameServers),
		producer4.WithQueueSelector(producer4.NewHashQueueSelector()),
	}
	if cfg.Namespace != "" {
		options = append(options, producer4.WithNamespace(cfg.Namespace))
	}
	if cfg.ProducerRetryTimes > 0 {
		options = append(options, producer4.WithRetry(cfg.ProducerRetryTimes))
	}
	if cfg.AccessKey != "" {
		options = append(options, producer4.WithCredentials(primitive4.Credentials{AccessKey: cfg.AccessKey, SecretKey: cfg.SecretKey}))
	}
	p, err := create(options...)
	if err != nil {
		return nil, err
	}
	if err := p.Start(); err != nil {
		_ = p.Shutdown()
		return nil, err
	}
	return rocketMQ4Producer{producer: p}, nil
}

type rocketMQ4Producer struct{ producer rocketMQ4SDKProducer }

func (p rocketMQ4Producer) Send(ctx context.Context, message *Message) ([]SendReceipt, error) {
	rmqMessage := primitive4.NewMessage(message.Topic, append([]byte(nil), message.Body...))
	if message.Tag != "" {
		rmqMessage.WithTag(message.Tag)
	}
	if len(message.Keys) > 0 {
		rmqMessage.WithKeys(append([]string(nil), message.Keys...))
	}
	if message.MessageGroup != "" {
		rmqMessage.WithShardingKey(message.MessageGroup)
	}
	for key, value := range message.Properties {
		rmqMessage.WithProperty(key, value)
	}
	result, err := p.producer.SendSync(ctx, rmqMessage)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	return []SendReceipt{{MessageID: result.MsgID, TransactionID: result.TransactionID, Offset: result.QueueOffset}}, nil
}

func (p rocketMQ4Producer) Stop() error { return p.producer.Shutdown() }

func nameServers(endpoint string) ([]string, error) {
	parts := strings.FieldsFunc(endpoint, func(r rune) bool { return r == ',' || r == ';' })
	if len(parts) == 0 {
		return nil, fmt.Errorf("rocketmq4 endpoint is required")
	}
	result := make([]string, 0, len(parts))
	for _, item := range parts {
		item = strings.TrimSpace(item)
		if item == "" {
			return nil, fmt.Errorf("rocketmq4 endpoint contains an empty nameserver")
		}
		if _, _, err := net.SplitHostPort(item); err != nil {
			return nil, fmt.Errorf("rocketmq4 endpoint %q must be host:port: %w", item, err)
		}
		result = append(result, item)
	}
	return result, nil
}
