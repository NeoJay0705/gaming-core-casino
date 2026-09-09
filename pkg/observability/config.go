package observability

import (
	"fmt"
	"net"
	"strings"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
)

const configPath = "observability"

// Config 是共用 Observability HTTP Server 的設定。
type Config struct {
	ListenAddr string `config:"listen_addr" yaml:"listen_addr"`
}

func newConfig(snapshot config.SourceSnapshot) (Config, error) {
	if snapshot == nil || framework.IsNilDependency(snapshot) {
		return Config{}, fmt.Errorf("observability config snapshot is nil")
	}
	if !snapshot.Has(configPath) {
		return Config{}, fmt.Errorf("observability config: %s is required", configPath)
	}
	var cfg Config
	if err := snapshot.Bind(configPath, &cfg, config.Strict()); err != nil {
		return Config{}, fmt.Errorf("observability config: bind %q: %w", configPath, err)
	}
	if err := validateConfig(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func validateConfig(cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("observability config is nil")
	}
	cfg.ListenAddr = strings.TrimSpace(cfg.ListenAddr)
	if cfg.ListenAddr == "" {
		return fmt.Errorf("observability config: listen_addr is required")
	}
	_, port, err := net.SplitHostPort(cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("observability config: invalid listen_addr %q: %w", cfg.ListenAddr, err)
	}
	if port == "" {
		return fmt.Errorf("observability config: invalid listen_addr %q: port is required", cfg.ListenAddr)
	}
	return nil
}
