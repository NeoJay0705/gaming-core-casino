package gateproduct

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/redis"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend/redisstore"
)

// gateServerSendRuntime owns the Gate-side Game-to-Gate listener, endpoint
// registration, and Redis room broadcast subscription as one lifecycle unit.
type gateServerSendRuntime struct {
	receiver *serversend.ReceiverServer
	local    *gateServerSendReceiver
	store    *redisstore.Store
	keys     serversend.Keyspace
	gateID   serversend.GateID
	config   serversend.Config

	mu         sync.RWMutex
	registrar  *serversend.EndpointRegistrar
	subscriber *serversend.RedisBroadcastSubscriber
	started    bool
}

func gateServerSendConfig(snapshot config.SourceSnapshot) (serversend.Config, bool, error) {
	if snapshot == nil {
		return serversend.Config{}, false, fmt.Errorf("gate server send: config snapshot is nil")
	}
	if !snapshot.Has("server_send") {
		return serversend.Config{}, false, nil
	}
	var cfg serversend.Config
	if err := snapshot.Bind("server_send", &cfg, config.Strict()); err != nil {
		return serversend.Config{}, false, fmt.Errorf("gate server send: bind config: %w", err)
	}
	normalized, err := cfg.NormalizeForGate()
	if err != nil {
		return serversend.Config{}, false, fmt.Errorf("gate server send: validate config: %w", err)
	}
	return normalized, true, nil
}

func newGateServerSendRuntime(identity serversend.RuntimeGateIdentity, cfg serversend.Config, sessions *SessionRegistry, redisClient *redis.Client, keys serversend.Keyspace) (*gateServerSendRuntime, error) {
	if identity.GateID == "" {
		return nil, fmt.Errorf("gate server send: runtime Gate identity is required")
	}
	local, err := newGateServerSendReceiver(sessions)
	if err != nil {
		return nil, err
	}
	receiver, err := serversend.NewReceiverServer(serversend.ReceiverConfig{ListenAddr: cfg.Gate.ListenAddr, MaxPayloadBytes: cfg.MaxPayloadBytes}, local)
	if err != nil {
		return nil, err
	}
	return &gateServerSendRuntime{receiver: receiver, local: local, store: redisstore.New(redisClient), keys: keys, gateID: identity.GateID, config: cfg}, nil
}

func (r *gateServerSendRuntime) Start(ctx context.Context) error {
	if r == nil {
		return errors.New("gate server send: runtime is nil")
	}
	if err := r.receiver.Start(ctx); err != nil {
		return err
	}
	address, err := net.ResolveTCPAddr("tcp", r.receiver.Addr())
	if err != nil {
		_ = r.receiver.Stop(context.Background())
		return fmt.Errorf("gate server send: resolve listener address: %w", err)
	}
	advertiseAddress, err := serversend.ResolveAdvertiseEndpoint(address)
	if err != nil {
		_ = r.receiver.Stop(context.Background())
		return err
	}
	registrar, err := serversend.NewEndpointRegistrar(r.store, r.keys, serversend.GateEndpoint{GateID: r.gateID, Address: advertiseAddress}, serversend.EndpointRegistrarConfig{TTL: r.config.Gate.EndpointTTL, Refresh: r.config.Gate.EndpointRefresh})
	if err != nil {
		_ = r.receiver.Stop(context.Background())
		return err
	}
	if err := registrar.Start(ctx); err != nil {
		_ = r.receiver.Stop(context.Background())
		return err
	}
	var subscriber *serversend.RedisBroadcastSubscriber
	if r.config.Broadcast.Primary == "redis" {
		rawRedis, err := r.store.Client()
		if err != nil {
			_ = registrar.Stop(context.Background())
			_ = r.receiver.Stop(context.Background())
			return fmt.Errorf("gate server send: get Redis client for room subscription: %w", err)
		}
		subscriber, err = serversend.NewRedisBroadcastSubscriber(rawRedis, r.keys, r.local, serversend.RedisSubscriberConfig{MaxPayloadBytes: r.config.MaxPayloadBytes})
		if err != nil {
			_ = registrar.Stop(context.Background())
			_ = r.receiver.Stop(context.Background())
			return err
		}
		if err := subscriber.Start(ctx); err != nil {
			_ = registrar.Stop(context.Background())
			_ = r.receiver.Stop(context.Background())
			return err
		}
	}
	r.mu.Lock()
	r.registrar, r.subscriber, r.started = registrar, subscriber, true
	r.mu.Unlock()
	return nil
}

func (r *gateServerSendRuntime) Stop(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	registrar, subscriber := r.registrar, r.subscriber
	r.registrar, r.subscriber, r.started = nil, nil, false
	r.mu.Unlock()
	var errs []error
	if subscriber != nil {
		if err := subscriber.Stop(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if registrar != nil {
		if err := registrar.Stop(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if err := r.receiver.Stop(ctx); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// Route returns the current Gate identity injected into every Gate-to-Game
// request. The network endpoint is deliberately not placed in request
// metadata; Game resolves it through its trusted Gate directory.
func (r *gateServerSendRuntime) Route() string {
	if r == nil {
		return ""
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.started {
		return ""
	}
	return string(r.gateID)
}

func newGatePresenceRegistry(identity serversend.RuntimeGateIdentity, cfg serversend.Config, redisClient *redis.Client, keys serversend.Keyspace) (*serversend.GatePresenceRegistry, error) {
	if identity.GateID == "" {
		return nil, fmt.Errorf("gate server send: runtime Gate identity is required")
	}
	presence, err := serversend.NewPresenceRegistry(redisstore.New(redisClient), keys, cfg.Presence)
	if err != nil {
		return nil, err
	}
	return serversend.NewGatePresenceRegistry(presence, identity.GateID)
}

func newGateSessionRegistry(presence *serversend.GatePresenceRegistry, cfg serversend.Config) (*SessionRegistry, error) {
	return newSessionRegistry(presence, cfg.Presence.LeaseTTL)
}

func newServerSendKeyspace(prefix redis.KeyPrefix) (serversend.Keyspace, error) {
	return serversend.NewKeyspace(prefix)
}
