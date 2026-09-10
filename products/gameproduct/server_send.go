package gameproduct

import (
	"fmt"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/redis"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend/redisstore"
	"go.uber.org/dig"
)

func gameServerSendConfig(snapshot config.SourceSnapshot) (serversend.Config, bool, error) {
	if snapshot == nil {
		return serversend.Config{}, false, fmt.Errorf("game server send: config snapshot is nil")
	}
	if !snapshot.Has("server_send") {
		return serversend.Config{}, false, nil
	}
	var cfg serversend.Config
	if err := snapshot.Bind("server_send", &cfg, config.Strict()); err != nil {
		return serversend.Config{}, false, fmt.Errorf("game server send: bind config: %w", err)
	}
	var err error
	if cfg, err = cfg.NormalizeForGame(); err != nil {
		return serversend.Config{}, false, fmt.Errorf("game server send: validate config: %w", err)
	}
	return cfg, true, nil
}

func newGameServerSendTransport(cfg serversend.Config) (*serversend.GRPCTransport, error) {
	return serversend.NewGRPCTransport(serversend.TransportConfig{RequestTimeout: cfg.RequestTimeout, MaxPayloadBytes: cfg.MaxPayloadBytes})
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

func newGameFanoutSender(cfg serversend.Config, transport *serversend.GRPCTransport) (*serversend.FanoutSender, error) {
	directory, err := serversend.NewDNSGateDirectory(cfg.Fanout.GRPCTarget, nil)
	if err != nil {
		return nil, err
	}
	return serversend.NewFanoutSender(directory, transport, serversend.FanoutConfig{MaxEndpoints: cfg.Fanout.MaxEndpoints})
}

func newGameRequestPlayerSender(transport *serversend.GRPCTransport, metrics *gameMetrics) (serversend.RequestPlayerSender, error) {
	sender, err := serversend.NewDirectRequestPlayerSender(transport)
	if err != nil {
		return nil, err
	}
	return &measuredRequestPlayerSender{delegate: sender, metrics: metrics}, nil
}

type gamePlayerSenderInputs struct {
	dig.In

	Config    serversend.Config
	Presence  *serversend.RedisPresenceResolver
	Directory *serversend.RedisGateDirectory
	Transport *serversend.GRPCTransport
	Fanout    *serversend.FanoutSender `optional:"true"`
}

func newGamePlayerSender(inputs gamePlayerSenderInputs) (serversend.PlayerSender, error) {
	var fallback *serversend.FanoutSender
	if inputs.Config.Player.Fallback == "grpc" {
		if inputs.Fanout == nil {
			return nil, fmt.Errorf("game server send: gRPC player fallback is not configured")
		}
		fallback = inputs.Fanout
	}
	return serversend.NewRoutedPlayerSender(inputs.Presence, inputs.Directory, inputs.Transport, fallback)
}

func newGameRedisBroadcastSender(cfg serversend.Config, redisClient *redis.Client, keys serversend.Keyspace) (*serversend.RedisBroadcastSender, error) {
	return serversend.NewRedisBroadcastSender(redisstore.New(redisClient), keys, serversend.RedisBroadcastConfig{MaxPayloadBytes: cfg.MaxPayloadBytes})
}

type gameBroadcastSenderInputs struct {
	dig.In

	Config serversend.Config
	Redis  *serversend.RedisBroadcastSender `optional:"true"`
	Fanout *serversend.FanoutSender         `optional:"true"`
}

func newGameBroadcastSender(inputs gameBroadcastSenderInputs) (serversend.BroadcastSender, error) {
	switch inputs.Config.Broadcast.Primary {
	case "redis":
		if inputs.Redis == nil {
			return nil, fmt.Errorf("game server send: Redis broadcast sender is not configured")
		}
		if inputs.Config.Broadcast.Fallback == "grpc" {
			if inputs.Fanout == nil {
				return nil, fmt.Errorf("game server send: gRPC broadcast fallback is not configured")
			}
			return serversend.NewFallbackBroadcastSender(inputs.Redis, inputs.Fanout, serversend.BroadcastFallbackConfig{MaxPayloadBytes: inputs.Config.MaxPayloadBytes})
		}
		return inputs.Redis, nil
	case "grpc":
		if inputs.Fanout == nil {
			return nil, fmt.Errorf("game server send: gRPC broadcast sender is not configured")
		}
		return inputs.Fanout, nil
	default:
		return nil, fmt.Errorf("game server send: unsupported broadcast primary %q", inputs.Config.Broadcast.Primary)
	}
}
