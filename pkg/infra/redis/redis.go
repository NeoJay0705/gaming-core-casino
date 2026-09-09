// Package redis provides a framework-managed Redis connection pool.
package redis

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/redis/go-redis/v9"
)

const (
	defaultPoolSize     = 100
	defaultMinIdleConns = 10
	defaultDialTimeout  = 5 * time.Second
	defaultReadTimeout  = 5 * time.Second
	defaultWriteTimeout = 5 * time.Second
)

// KeyPrefix is the application-wide Redis namespace root. It is kept as a
// distinct type so a Server Send keyspace cannot accidentally be built from an
// unrelated string dependency.
type KeyPrefix string

// NewKeyPrefix validates the single Redis namespace owned by infrastructure.
// Subsystems must append their own fixed segments instead of defining another
// root prefix.
func NewKeyPrefix(value string) (KeyPrefix, error) {
	if value == "" || value != strings.TrimSpace(value) {
		return "", fmt.Errorf("redis config: key_prefix is required and must not have surrounding whitespace")
	}
	if strings.ContainsAny(value, "*?[]\\") {
		return "", fmt.Errorf("redis config: key_prefix contains a Redis pattern character")
	}
	return KeyPrefix(value), nil
}

// Config configures either a single Redis instance or a Redis Cluster.
// Timeouts use Go duration syntax in YAML, for example "5s".
type Config struct {
	Addr            string        `config:"addr" yaml:"addr"`
	Addrs           []string      `config:"addrs" yaml:"addrs"`
	KeyPrefix       string        `config:"key_prefix" yaml:"key_prefix"`
	Username        string        `config:"username" yaml:"username"`
	Password        string        `config:"password" yaml:"password"`
	DB              int           `config:"db" yaml:"db"`
	PoolSize        int           `config:"pool_size" yaml:"pool_size"`
	MinIdleConns    int           `config:"min_idle_conns" yaml:"min_idle_conns"`
	MaxIdleConns    int           `config:"max_idle_conns" yaml:"max_idle_conns"`
	PoolTimeout     time.Duration `config:"pool_timeout" yaml:"pool_timeout"`
	DialTimeout     time.Duration `config:"dial_timeout" yaml:"dial_timeout"`
	ReadTimeout     time.Duration `config:"read_timeout" yaml:"read_timeout"`
	WriteTimeout    time.Duration `config:"write_timeout" yaml:"write_timeout"`
	ConnMaxIdleTime time.Duration `config:"conn_max_idle_time" yaml:"conn_max_idle_time"`
	ConnMaxLifetime time.Duration `config:"conn_max_lifetime" yaml:"conn_max_lifetime"`
}

// Client owns one go-redis client and its connection pool.
type Client struct {
	mu     sync.RWMutex
	cfg    Config
	client redis.UniversalClient
	state  state
}

type state uint8

const (
	stateNew state = iota
	stateStarted
	stateStopped
	stateFailed
)

// New reads and validates the Redis configuration. It performs no I/O.
func New(snapshot config.SourceSnapshot) (*Client, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("redis config snapshot is nil")
	}
	var cfg Config
	if err := snapshot.Bind("redis", &cfg, config.Strict()); err != nil {
		return nil, fmt.Errorf("redis config: %w", err)
	}
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	return &Client{cfg: cfg}, nil
}

// Start creates the pool and verifies the connection with PING.
func (c *Client) Start(ctx context.Context) error {
	if c == nil {
		return fmt.Errorf("redis client is nil")
	}
	if ctx == nil {
		return fmt.Errorf("redis start context is nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("redis start cancelled: %w", err)
	}

	c.mu.Lock()
	if c.state != stateNew {
		state := c.state
		c.mu.Unlock()
		return fmt.Errorf("redis start is not allowed in state %d", state)
	}
	c.mu.Unlock()

	client := newUniversalClient(c.cfg)
	pingCtx, cancel := context.WithTimeout(ctx, c.cfg.DialTimeout)
	err := client.Ping(pingCtx).Err()
	cancel()
	if err != nil {
		_ = client.Close()
		c.fail()
		return fmt.Errorf("redis ping: %w", err)
	}
	if err := ctx.Err(); err != nil {
		_ = client.Close()
		c.fail()
		return fmt.Errorf("redis start cancelled: %w", err)
	}

	c.mu.Lock()
	if c.state != stateNew {
		c.mu.Unlock()
		_ = client.Close()
		return fmt.Errorf("redis start was interrupted")
	}
	c.client = client
	c.state = stateStarted
	c.mu.Unlock()
	return nil
}

// Stop closes all pooled connections. Close is not context-aware in go-redis.
func (c *Client) Stop(context.Context) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	if c.state != stateStarted {
		c.mu.Unlock()
		return nil
	}
	client := c.client
	c.client = nil
	c.state = stateStopped
	c.mu.Unlock()
	return client.Close()
}

// Client returns the started universal client, suitable for both single and
// cluster operations.
func (c *Client) Client() (redis.UniversalClient, error) {
	if c == nil {
		return nil, fmt.Errorf("redis client is nil")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.state != stateStarted || c.client == nil {
		return nil, fmt.Errorf("redis client is not started")
	}
	return c.client, nil
}

// KeyPrefix returns the validated application-wide Redis namespace.
func (c *Client) KeyPrefix() KeyPrefix {
	if c == nil {
		return ""
	}
	return KeyPrefix(c.cfg.KeyPrefix)
}

// KeyPrefixFromClient exposes the validated namespace as a strongly typed DI
// value. Requiring the Client keeps the prefix source in the infrastructure
// configuration and avoids a second independent config binding.
func KeyPrefixFromClient(c *Client) (KeyPrefix, error) {
	if c == nil {
		return "", fmt.Errorf("redis config: client is nil")
	}
	return c.KeyPrefix(), nil
}

// Config returns the normalized immutable configuration.
func (c *Client) Config() Config {
	if c == nil {
		return Config{}
	}
	return cloneConfig(c.cfg)
}

func cloneConfig(cfg Config) Config {
	result := cfg
	result.Addrs = append([]string(nil), cfg.Addrs...)
	return result
}

func (c *Client) fail() {
	c.mu.Lock()
	if c.state == stateNew {
		c.state = stateFailed
	}
	c.mu.Unlock()
}

func validateConfig(cfg *Config) error {
	cfg.Addr = strings.TrimSpace(cfg.Addr)
	if _, err := NewKeyPrefix(cfg.KeyPrefix); err != nil {
		return err
	}
	for i, addr := range cfg.Addrs {
		cfg.Addrs[i] = strings.TrimSpace(addr)
		if cfg.Addrs[i] == "" {
			return fmt.Errorf("redis config: addrs[%d] is empty", i)
		}
	}
	if cfg.Addr != "" && len(cfg.Addrs) != 0 {
		return fmt.Errorf("redis config: addr and addrs are mutually exclusive")
	}
	if cfg.Addr == "" && len(cfg.Addrs) == 0 {
		return fmt.Errorf("redis config: one of addr or addrs is required")
	}
	if len(cfg.Addrs) != 0 {
		if cfg.DB != 0 {
			return fmt.Errorf("redis config: cluster does not support db=%d", cfg.DB)
		}
	}
	if cfg.PoolSize < 0 || cfg.MinIdleConns < 0 || cfg.MaxIdleConns < 0 {
		return fmt.Errorf("redis config: pool sizes cannot be negative")
	}
	if cfg.MaxIdleConns > 0 && cfg.MinIdleConns > cfg.MaxIdleConns {
		return fmt.Errorf("redis config: min_idle_conns cannot exceed max_idle_conns")
	}
	for _, item := range []struct {
		name  string
		value time.Duration
	}{
		{"pool_timeout", cfg.PoolTimeout},
		{"dial_timeout", cfg.DialTimeout},
		{"read_timeout", cfg.ReadTimeout},
		{"write_timeout", cfg.WriteTimeout},
		{"conn_max_idle_time", cfg.ConnMaxIdleTime},
		{"conn_max_lifetime", cfg.ConnMaxLifetime},
	} {
		if item.value < 0 {
			return fmt.Errorf("redis config: %s cannot be negative", item.name)
		}
	}
	if cfg.PoolSize == 0 {
		cfg.PoolSize = defaultPoolSize
	}
	if cfg.MinIdleConns == 0 {
		cfg.MinIdleConns = defaultMinIdleConns
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = defaultDialTimeout
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = defaultReadTimeout
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = defaultWriteTimeout
	}
	return nil
}

func newUniversalClient(cfg Config) redis.UniversalClient {
	if len(cfg.Addrs) != 0 {
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:           cfg.Addrs,
			Username:        cfg.Username,
			Password:        cfg.Password,
			PoolSize:        cfg.PoolSize,
			MinIdleConns:    cfg.MinIdleConns,
			MaxIdleConns:    cfg.MaxIdleConns,
			PoolTimeout:     cfg.PoolTimeout,
			DialTimeout:     cfg.DialTimeout,
			ReadTimeout:     cfg.ReadTimeout,
			WriteTimeout:    cfg.WriteTimeout,
			ConnMaxIdleTime: cfg.ConnMaxIdleTime,
			ConnMaxLifetime: cfg.ConnMaxLifetime,
		})
	}
	return redis.NewClient(&redis.Options{
		Addr:            cfg.Addr,
		Username:        cfg.Username,
		Password:        cfg.Password,
		DB:              cfg.DB,
		PoolSize:        cfg.PoolSize,
		MinIdleConns:    cfg.MinIdleConns,
		MaxIdleConns:    cfg.MaxIdleConns,
		PoolTimeout:     cfg.PoolTimeout,
		DialTimeout:     cfg.DialTimeout,
		ReadTimeout:     cfg.ReadTimeout,
		WriteTimeout:    cfg.WriteTimeout,
		ConnMaxIdleTime: cfg.ConnMaxIdleTime,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
	})
}
