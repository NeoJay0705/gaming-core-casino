package gameproduct

import (
	"fmt"
	"strings"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/redis"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend/redisstore"
	"go.uber.org/dig"
)

type gameGRPCGateClientConfig struct {
	Timeout time.Duration    `config:"timeout" yaml:"timeout"`
	Fanout  gameFanoutConfig `config:"fanout" yaml:"fanout"`
}

type gameFanoutConfig struct {
	Target       string `config:"target" yaml:"target"`
	MaxEndpoints int    `config:"max_endpoints" yaml:"max_endpoints"`
}

type gameServerSendBroadcastConfig struct {
	Primary string `config:"primary" yaml:"primary"`
}

type gameServerSendConfig struct {
	Broadcast gameServerSendBroadcastConfig `config:"broadcast" yaml:"broadcast"`
}

func gameGRPCGateClient(snapshot config.SourceSnapshot) (serversend.TransportConfig, gameFanoutConfig, bool, error) {
	if snapshot == nil {
		return serversend.TransportConfig{}, gameFanoutConfig{}, false, fmt.Errorf("game gRPC: config snapshot is nil")
	}
	var cfg gameGRPCGateClientConfig
	if !snapshot.Has("grpc.clients.gate") {
		return serversend.TransportConfig{}, gameFanoutConfig{}, false, nil
	}
	if err := snapshot.Bind("grpc.clients.gate", &cfg, config.Strict()); err != nil {
		return serversend.TransportConfig{}, gameFanoutConfig{}, false, fmt.Errorf("game gRPC: bind grpc.clients.gate: %w", err)
	}
	if cfg.Timeout < 0 {
		return serversend.TransportConfig{}, gameFanoutConfig{}, false, fmt.Errorf("game gRPC: gate client timeout must not be negative")
	}
	cfg.Fanout.Target = strings.TrimSpace(cfg.Fanout.Target)
	if cfg.Fanout.Target == "" {
		return serversend.TransportConfig{}, gameFanoutConfig{}, false, fmt.Errorf("game gRPC: grpc.clients.gate.fanout.target is required")
	}
	if cfg.Fanout.MaxEndpoints < 0 {
		return serversend.TransportConfig{}, gameFanoutConfig{}, false, fmt.Errorf("game gRPC: grpc.clients.gate.fanout.max_endpoints cannot be negative")
	}
	if _, err := serversend.NewDNSGateDirectory(cfg.Fanout.Target, nil); err != nil {
		return serversend.TransportConfig{}, gameFanoutConfig{}, false, fmt.Errorf("game gRPC: validate grpc.clients.gate.fanout.target: %w", err)
	}
	return serversend.TransportConfig{RequestTimeout: cfg.Timeout}, cfg.Fanout, true, nil
}

func gameServerSendBroadcast(snapshot config.SourceSnapshot) (gameServerSendBroadcastConfig, bool, error) {
	if snapshot == nil {
		return gameServerSendBroadcastConfig{}, false, fmt.Errorf("game broadcast: config snapshot is nil")
	}
	defaultConfig := gameServerSendBroadcastConfig{Primary: "redis"}
	if snapshot.Has("room_broadcast") {
		return gameServerSendBroadcastConfig{}, false, fmt.Errorf("game broadcast: legacy room_broadcast key is unsupported; use server_send.broadcast")
	}
	if !snapshot.Has("server_send") {
		return defaultConfig, false, nil
	}
	if !snapshot.Has("server_send.broadcast") {
		return gameServerSendBroadcastConfig{}, false, fmt.Errorf("game broadcast: server_send.broadcast is required")
	}
	var root gameServerSendConfig
	if err := snapshot.Bind("server_send", &root, config.Strict()); err != nil {
		return gameServerSendBroadcastConfig{}, false, fmt.Errorf("game broadcast: bind server_send: %w", err)
	}
	cfg := root.Broadcast
	cfg.Primary = strings.ToLower(strings.TrimSpace(cfg.Primary))
	if cfg.Primary == "" {
		cfg.Primary = "redis"
	}
	if cfg.Primary != "redis" && cfg.Primary != "grpc" {
		return gameServerSendBroadcastConfig{}, false, fmt.Errorf("game broadcast: primary %q is invalid", cfg.Primary)
	}
	return cfg, true, nil
}

func newGameServerSendTransport(cfg serversend.TransportConfig) (*serversend.GRPCTransport, error) {
	return serversend.NewGRPCTransport(cfg)
}

func newGameServerSendKeyspace(prefix redis.KeyPrefix) (serversend.Keyspace, error) {
	return serversend.NewKeyspace(prefix)
}

func newGamePresenceResolver(redisClient *redis.Client, keys serversend.Keyspace) (*serversend.RedisPresenceResolver, error) {
	return serversend.NewRedisPresenceResolver(redisstore.New(redisClient), keys)
}

func newGameGateDirectory(redisClient *redis.Client, keys serversend.Keyspace) (*serversend.RedisGateDirectory, error) {
	return serversend.NewRedisGateDirectory(redisstore.New(redisClient), keys)
}

func newGameGateFanoutDirectory(cfg gameFanoutConfig) (*serversend.DNSGateDirectory, error) {
	return serversend.NewDNSGateDirectory(cfg.Target, nil)
}

func newGameFanoutSender(cfg gameFanoutConfig, directory *serversend.DNSGateDirectory, transport *serversend.GRPCTransport) (*serversend.FanoutSender, error) {
	return serversend.NewFanoutSender(directory, transport, serversend.FanoutConfig{MaxEndpoints: cfg.MaxEndpoints})
}

func newGameRequestPlayerSender(metrics *gameMetrics) (serversend.RequestPlayerSender, error) {
	sender, err := serversend.NewDirectRequestPlayerSender()
	if err != nil {
		return nil, err
	}
	return &measuredRequestPlayerSender{delegate: sender, metrics: metrics}, nil
}

type gamePlayerSenderInputs struct {
	dig.In

	Presence  *serversend.RedisPresenceResolver
	Directory *serversend.RedisGateDirectory
	Fallback  *serversend.DNSGateDirectory
	Config    gameFanoutConfig
	Transport *serversend.GRPCTransport
	Metrics   *gameMetrics
}

func newGamePlayerSender(inputs gamePlayerSenderInputs) (serversend.PlayerSender, error) {
	sender, err := serversend.NewBatchPlayerSender(
		inputs.Presence,
		inputs.Directory,
		inputs.Fallback,
		inputs.Transport,
		serversend.FanoutConfig{MaxEndpoints: inputs.Config.MaxEndpoints},
	)
	if err != nil {
		return nil, err
	}
	return &measuredPlayerSender{delegate: sender, metrics: inputs.Metrics}, nil
}

func newGameRedisBroadcastSender(redisClient *redis.Client, keys serversend.Keyspace) (*serversend.RedisBroadcastSender, error) {
	return serversend.NewRedisBroadcastSender(redisstore.New(redisClient), keys)
}

type gameBroadcastSenderInputs struct {
	dig.In

	Config  gameServerSendBroadcastConfig
	Redis   *serversend.RedisBroadcastSender `optional:"true"`
	Fanout  *serversend.FanoutSender         `optional:"true"`
	Metrics *gameMetrics
}

func newGameBroadcastSender(inputs gameBroadcastSenderInputs) (serversend.BroadcastSender, error) {
	switch inputs.Config.Primary {
	case "redis":
		if inputs.Redis == nil {
			return nil, fmt.Errorf("game broadcast: Redis sender is not configured")
		}
		if inputs.Fanout == nil {
			return nil, fmt.Errorf("game broadcast: gRPC fan-out sender is not configured")
		}
		sender, err := serversend.NewFallbackBroadcastSender(inputs.Redis, inputs.Fanout)
		if err != nil {
			return nil, err
		}
		return &measuredBroadcastSender{delegate: sender, metrics: inputs.Metrics}, nil
	case "grpc":
		if inputs.Fanout == nil {
			return nil, fmt.Errorf("game broadcast: gRPC fan-out sender is not configured")
		}
		return &measuredBroadcastSender{delegate: inputs.Fanout, metrics: inputs.Metrics}, nil
	default:
		return nil, fmt.Errorf("game broadcast: unsupported primary %q", inputs.Config.Primary)
	}
}
