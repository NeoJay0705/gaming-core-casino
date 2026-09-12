package gateproduct

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/grpcserver"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/redis"
	"github.com/NeoJay0705/gaming-core-casino/pkg/logging"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend/redisstore"
	"go.uber.org/dig"
)

type gateServerSendBroadcastConfig struct {
	Primary string `config:"primary" yaml:"primary"`
}

type gateServerSendConfig struct {
	Broadcast gateServerSendBroadcastConfig `config:"broadcast" yaml:"broadcast"`
}

// gateIdentity is process-scoped Gate identity shared by session ownership
// and endpoint registration. It is deliberately independent from delivery
// service configuration.
type gateIdentity = serversend.RuntimeGateIdentity

func newGateIdentity() (gateIdentity, error) {
	return serversend.NewRuntimeGateIdentity()
}

func gateSessionOwnershipConfig(snapshot config.SourceSnapshot) (serversend.PresenceConfig, error) {
	if snapshot == nil {
		return serversend.PresenceConfig{}, fmt.Errorf("gate ownership: config snapshot is nil")
	}
	if !snapshot.Has("session_ownership") {
		return serversend.PresenceConfig{}, fmt.Errorf("gate ownership: session_ownership is required")
	}
	var cfg serversend.PresenceConfig
	if err := snapshot.Bind("session_ownership", &cfg, config.Strict()); err != nil {
		return serversend.PresenceConfig{}, fmt.Errorf("gate ownership: bind session_ownership: %w", err)
	}
	if cfg.LeaseTTL <= 0 {
		return serversend.PresenceConfig{}, fmt.Errorf("gate ownership: lease_ttl must be positive")
	}
	return cfg, nil
}

func gateServerSendBroadcast(snapshot config.SourceSnapshot) (gateServerSendBroadcastConfig, bool, error) {
	if snapshot == nil {
		return gateServerSendBroadcastConfig{}, false, fmt.Errorf("gate broadcast: config snapshot is nil")
	}
	defaultConfig := gateServerSendBroadcastConfig{Primary: "redis"}
	if snapshot.Has("room_broadcast") {
		return gateServerSendBroadcastConfig{}, false, fmt.Errorf("gate broadcast: legacy room_broadcast key is unsupported; use server_send.broadcast")
	}
	if !snapshot.Has("server_send") {
		return defaultConfig, false, nil
	}
	if !snapshot.Has("server_send.broadcast") {
		return gateServerSendBroadcastConfig{}, false, fmt.Errorf("gate broadcast: server_send.broadcast is required")
	}
	var root gateServerSendConfig
	if err := snapshot.Bind("server_send", &root, config.Strict()); err != nil {
		return gateServerSendBroadcastConfig{}, false, fmt.Errorf("gate broadcast: bind server_send: %w", err)
	}
	cfg := root.Broadcast
	cfg.Primary = strings.ToLower(strings.TrimSpace(cfg.Primary))
	if cfg.Primary == "" {
		cfg.Primary = "redis"
	}
	if cfg.Primary != "redis" && cfg.Primary != "grpc" {
		return gateServerSendBroadcastConfig{}, false, fmt.Errorf("gate broadcast: primary %q is invalid", cfg.Primary)
	}
	return cfg, true, nil
}

func newGatePresenceRegistry(identity gateIdentity, cfg serversend.PresenceConfig, redisClient *redis.Client, keys serversend.Keyspace) (*serversend.GatePresenceRegistry, error) {
	if identity.GateID == "" {
		return nil, fmt.Errorf("gate ownership: runtime Gate identity is required")
	}
	presence, err := serversend.NewPresenceRegistry(redisstore.New(redisClient), keys, cfg)
	if err != nil {
		return nil, err
	}
	return serversend.NewGatePresenceRegistry(presence, identity.GateID)
}

func newGateSessionRegistry(presence *serversend.GatePresenceRegistry, cfg serversend.PresenceConfig, factory *logging.Factory) (*SessionRegistry, error) {
	logger, err := factory.Component("session")
	if err != nil {
		return nil, err
	}
	return newSessionRegistryWithLogger(presence, cfg.LeaseTTL, logger)
}

func newServerSendKeyspace(prefix redis.KeyPrefix) (serversend.Keyspace, error) {
	return serversend.NewKeyspace(prefix)
}

func newGateServerSendTransport(cfg serversend.TransportConfig) (*serversend.GRPCTransport, error) {
	return serversend.NewGRPCTransport(cfg)
}

func newGateFanoutSender(cfg gateFanoutConfig, transport *serversend.GRPCTransport) (*serversend.FanoutSender, error) {
	directory, err := serversend.NewDNSGateDirectory(cfg.Target, nil)
	if err != nil {
		return nil, err
	}
	return serversend.NewFanoutSender(directory, transport, serversend.FanoutConfig{MaxEndpoints: cfg.MaxEndpoints})
}

func newGateRedisBroadcastSender(redisClient *redis.Client, keys serversend.Keyspace) (*serversend.RedisBroadcastSender, error) {
	return serversend.NewRedisBroadcastSender(redisstore.New(redisClient), keys)
}

type gateBroadcastSenderInputs struct {
	dig.In

	Config gateServerSendBroadcastConfig
	Redis  *serversend.RedisBroadcastSender `optional:"true"`
	Fanout *serversend.FanoutSender         `optional:"true"`
}

func newGateBroadcastSender(inputs gateBroadcastSenderInputs) (serversend.BroadcastSender, error) {
	if inputs.Fanout == nil {
		return nil, fmt.Errorf("gate broadcast: gRPC fan-out sender is not configured")
	}
	switch inputs.Config.Primary {
	case "redis":
		if inputs.Redis == nil {
			return nil, fmt.Errorf("gate broadcast: Redis sender is not configured")
		}
		return serversend.NewFallbackBroadcastSender(inputs.Redis, inputs.Fanout)
	case "grpc":
		return inputs.Fanout, nil
	default:
		return nil, fmt.Errorf("gate broadcast: unsupported primary %q", inputs.Config.Primary)
	}
}

// gateGRPCEndpointRegistration publishes the address of the product-level
// Gate gRPC server. It does not own or register a particular business service.
type gateGRPCEndpointRegistration struct {
	server *grpcserver.Server
	store  *redisstore.Store
	keys   serversend.Keyspace
	gateID serversend.GateID
	config serversend.EndpointRegistrarConfig
	logger *logging.Logger

	mu        sync.Mutex
	registrar *serversend.EndpointRegistrar
}

func newGateGRPCEndpointRegistration(server *grpcserver.Server, identity gateIdentity, cfg serversend.EndpointRegistrarConfig, redisClient *redis.Client, keys serversend.Keyspace, factory *logging.Factory) (*gateGRPCEndpointRegistration, error) {
	if server == nil {
		return nil, fmt.Errorf("gate gRPC endpoint: server is required")
	}
	if identity.GateID == "" {
		return nil, fmt.Errorf("gate gRPC endpoint: runtime Gate identity is required")
	}
	if redisClient == nil {
		return nil, fmt.Errorf("gate gRPC endpoint: Redis client is required")
	}
	if keys.Prefix() == "" {
		return nil, fmt.Errorf("gate gRPC endpoint: keyspace is required")
	}
	logger, err := factory.Component("grpc.endpoint")
	if err != nil {
		return nil, err
	}
	return &gateGRPCEndpointRegistration{server: server, store: redisstore.New(redisClient), keys: keys, gateID: identity.GateID, config: cfg, logger: logger}, nil
}

func (r *gateGRPCEndpointRegistration) Start(ctx context.Context) error {
	if r == nil {
		return errors.New("gate gRPC endpoint: registration is nil")
	}
	address := r.server.Addr()
	if address == "" {
		return errors.New("gate gRPC endpoint: gRPC server is not started")
	}
	listenerAddress, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return fmt.Errorf("gate gRPC endpoint: resolve listener address: %w", err)
	}
	advertiseAddress, err := serversend.ResolveAdvertiseEndpoint(listenerAddress)
	if err != nil {
		return err
	}
	registrar, err := serversend.NewEndpointRegistrar(r.store, r.keys, serversend.GateEndpoint{GateID: r.gateID, Address: advertiseAddress}, r.config, r.logger)
	if err != nil {
		return err
	}
	if err := registrar.Start(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	r.registrar = registrar
	r.mu.Unlock()
	return nil
}

func (r *gateGRPCEndpointRegistration) Stop(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	registrar := r.registrar
	r.registrar = nil
	r.mu.Unlock()
	if registrar == nil {
		return nil
	}
	return registrar.Stop(ctx)
}

// gateServerSendBroadcastRuntime 只擁有 optional Redis broadcast subscriber；
// GateDelivery gRPC service 與 endpoint registration 各自維持獨立 lifecycle。
type gateServerSendBroadcastRuntime struct {
	dispatcher *dispatcher.Dispatcher
	store      *redisstore.Store
	keys       serversend.Keyspace
	logger     *logging.Logger

	mu         sync.Mutex
	subscriber *serversend.RedisBroadcastSubscriber
}

func newGateServerSendBroadcastRuntime(cfg gateServerSendBroadcastConfig, commandDispatcher *dispatcher.Dispatcher, redisClient *redis.Client, keys serversend.Keyspace) (*gateServerSendBroadcastRuntime, error) {
	return newGateServerSendBroadcastRuntimeWithLogger(cfg, commandDispatcher, redisClient, keys, nil)
}

func newGateServerSendBroadcastRuntimeWithLogger(cfg gateServerSendBroadcastConfig, commandDispatcher *dispatcher.Dispatcher, redisClient *redis.Client, keys serversend.Keyspace, factory *logging.Factory) (*gateServerSendBroadcastRuntime, error) {
	if cfg.Primary != "redis" {
		return nil, fmt.Errorf("gate broadcast: runtime requires redis primary")
	}
	if commandDispatcher == nil || redisClient == nil || keys.Prefix() == "" {
		return nil, fmt.Errorf("gate broadcast: dependencies are required")
	}
	var logger *logging.Logger
	if factory != nil {
		var err error
		logger, err = factory.Component("redis.broadcast")
		if err != nil {
			return nil, err
		}
	}
	return &gateServerSendBroadcastRuntime{dispatcher: commandDispatcher, store: redisstore.New(redisClient), keys: keys, logger: logger}, nil
}

func (r *gateServerSendBroadcastRuntime) Start(ctx context.Context) error {
	if r == nil {
		return errors.New("gate broadcast: runtime is nil")
	}
	rawRedis, err := r.store.Client()
	if err != nil {
		return fmt.Errorf("gate broadcast: get Redis client: %w", err)
	}
	subscriber, err := serversend.NewRedisBroadcastSubscriber(rawRedis, r.keys, r.dispatcher, r.logger)
	if err != nil {
		return err
	}
	if err := subscriber.Start(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	r.subscriber = subscriber
	r.mu.Unlock()
	return nil
}

func (r *gateServerSendBroadcastRuntime) Stop(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	subscriber := r.subscriber
	r.subscriber = nil
	r.mu.Unlock()
	if subscriber == nil {
		return nil
	}
	return subscriber.Stop(ctx)
}
