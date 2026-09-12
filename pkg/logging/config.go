// Package logging 提供 framework product 共用的 structured logging 與
// W3C trace correlation 基礎。
package logging

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
)

const defaultLevel = "info"

// Config 是 App-local logging 設定。輸出格式與 destination 固定由
// logging package 管理，不透過 config 暴露 file 或 exporter 選項。
type Config struct {
	Level string `config:"level" yaml:"level"`
}

// Module 將一個固定 service identity 的 logger factory 注入 framework。
// service 由 product base module 提供，不從 deployment config 讀取，避免
// 設定誤把不同 product 的 log 混在一起。
func Module(r framework.Registry, service string) error {
	if r == nil || framework.IsNilDependency(r) {
		return fmt.Errorf("logging registry is nil")
	}
	service = strings.TrimSpace(service)
	if service == "" {
		return fmt.Errorf("logging service is required")
	}
	if err := r.Provide(newConfig); err != nil {
		return err
	}
	return r.Provide(func(cfg Config) (*Factory, error) {
		return newFactory(service, cfg, nil)
	})
}

func newConfig(snapshot config.SourceSnapshot) (Config, error) {
	if snapshot == nil || framework.IsNilDependency(snapshot) {
		return Config{}, fmt.Errorf("logging config snapshot is nil")
	}
	cfg := Config{Level: defaultLevel}
	if snapshot.Has("logging") {
		var input Config
		if err := snapshot.Bind("logging", &input, config.Strict()); err != nil {
			return Config{}, fmt.Errorf("logging config: bind logging: %w", err)
		}
		if strings.TrimSpace(input.Level) != "" {
			cfg.Level = input.Level
		}
	}
	if err := validateConfig(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func validateConfig(cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("logging config is nil")
	}
	cfg.Level = strings.ToLower(strings.TrimSpace(cfg.Level))
	if cfg.Level == "" {
		cfg.Level = defaultLevel
	}
	switch cfg.Level {
	case "debug", "info", "warn", "error":
		return nil
	default:
		return fmt.Errorf("logging config: level %q is invalid; want debug, info, warn, or error", cfg.Level)
	}
}

func levelFor(value string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("logging level %q is invalid", value)
	}
}
