// Package database provides a framework-managed MySQL/TiDB connection pool.
package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const defaultConnMaxLifetime = 30 * time.Minute

// Config configures the database/sql pool used by GORM. Durations use Go
// duration syntax in YAML, for example "30m".
type Config struct {
	DSN             string        `config:"dsn" yaml:"dsn"`
	MaxOpenConns    int           `config:"max_open_conns" yaml:"max_open_conns"`
	MaxIdleConns    int           `config:"max_idle_conns" yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `config:"conn_max_lifetime" yaml:"conn_max_lifetime"`
	ConnMaxIdleTime time.Duration `config:"conn_max_idle_time" yaml:"conn_max_idle_time"`
}

// Client owns the GORM handle and its underlying database/sql pool.
type Client struct {
	mu    sync.RWMutex
	cfg   Config
	db    *gorm.DB
	sqlDB *sql.DB
	state state
	open  func(Config) (*gorm.DB, *sql.DB, error)
}

type state uint8

const (
	stateNew state = iota
	stateStarted
	stateStopped
	stateFailed
)

// New reads and validates database configuration. It performs no I/O.
func New(snapshot config.SourceSnapshot) (*Client, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("database config snapshot is nil")
	}
	var cfg Config
	if err := snapshot.Bind("database", &cfg, config.Strict()); err != nil {
		return nil, fmt.Errorf("database config: %w", err)
	}
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	return &Client{cfg: cfg, open: open}, nil
}

// Start opens, configures, and pings the pool before making it available.
func (c *Client) Start(ctx context.Context) error {
	if c == nil {
		return fmt.Errorf("database client is nil")
	}
	if ctx == nil {
		return fmt.Errorf("database start context is nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("database start cancelled: %w", err)
	}
	c.mu.Lock()
	if c.state != stateNew {
		state := c.state
		c.mu.Unlock()
		return fmt.Errorf("database start is not allowed in state %d", state)
	}
	c.mu.Unlock()

	db, sqlDB, err := c.open(c.cfg)
	if err != nil {
		c.fail()
		return fmt.Errorf("database open: %w", err)
	}
	closePool := func() { _ = sqlDB.Close() }
	if c.cfg.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(c.cfg.MaxOpenConns)
	}
	if c.cfg.MaxIdleConns > 0 {
		sqlDB.SetMaxIdleConns(c.cfg.MaxIdleConns)
	}
	sqlDB.SetConnMaxLifetime(c.cfg.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(c.cfg.ConnMaxIdleTime)
	if err := sqlDB.PingContext(ctx); err != nil {
		closePool()
		c.fail()
		return fmt.Errorf("database ping: %w", err)
	}
	if err := ctx.Err(); err != nil {
		closePool()
		c.fail()
		return fmt.Errorf("database start cancelled: %w", err)
	}

	c.mu.Lock()
	if c.state != stateNew {
		c.mu.Unlock()
		closePool()
		return fmt.Errorf("database start was interrupted")
	}
	c.db, c.sqlDB, c.state = db, sqlDB, stateStarted
	c.mu.Unlock()
	return nil
}

// Stop closes the underlying connection pool.
func (c *Client) Stop(context.Context) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	if c.state != stateStarted {
		c.mu.Unlock()
		return nil
	}
	sqlDB := c.sqlDB
	c.db, c.sqlDB, c.state = nil, nil, stateStopped
	c.mu.Unlock()
	return sqlDB.Close()
}

// DB returns the started GORM handle.
func (c *Client) DB() (*gorm.DB, error) {
	if c == nil {
		return nil, fmt.Errorf("database client is nil")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.state != stateStarted || c.db == nil {
		return nil, fmt.Errorf("database client is not started")
	}
	return c.db, nil
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
	cfg.DSN = strings.TrimSpace(cfg.DSN)
	if cfg.DSN == "" {
		return fmt.Errorf("database config: dsn is required")
	}
	if cfg.MaxOpenConns < 0 || cfg.MaxIdleConns < 0 {
		return fmt.Errorf("database config: pool sizes cannot be negative")
	}
	if cfg.MaxOpenConns > 0 && cfg.MaxIdleConns > cfg.MaxOpenConns {
		return fmt.Errorf("database config: max_idle_conns cannot exceed max_open_conns")
	}
	if cfg.ConnMaxLifetime < 0 || cfg.ConnMaxIdleTime < 0 {
		return fmt.Errorf("database config: connection lifetimes cannot be negative")
	}
	if cfg.ConnMaxLifetime == 0 {
		cfg.ConnMaxLifetime = defaultConnMaxLifetime
	}
	return nil
}

func open(cfg Config) (*gorm.DB, *sql.DB, error) {
	db, err := gorm.Open(mysql.New(mysql.Config{
		DSN:                       cfg.DSN,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return nil, nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}
	return db, sqlDB, nil
}
