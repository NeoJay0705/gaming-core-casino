package serversend

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrGateEndpointNotFound = errors.New("server send: Gate endpoint is not found")

// GateEndpointStore is the narrow Redis dependency used for Gate endpoint
// discovery and registration.
type GateEndpointStore interface {
	Set(context.Context, string, any, time.Duration) *redis.StatusCmd
	Get(context.Context, string) *redis.StringCmd
	Del(context.Context, ...string) *redis.IntCmd
}

// RedisGateDirectory resolves a Gate ID through its ephemeral Redis endpoint
// registration. It intentionally does not enumerate all Gates: fan-out must
// use a directory independent from a failed Redis route store.
type RedisGateDirectory struct {
	store GateEndpointStore
	keys  Keyspace
}

func NewRedisGateDirectory(store GateEndpointStore, keys Keyspace) (*RedisGateDirectory, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: Gate endpoint store is required", ErrRouteStoreUnavailable)
	}
	if keys.Prefix() == "" {
		return nil, fmt.Errorf("%w: redis key prefix is required", ErrDestinationInvalid)
	}
	return &RedisGateDirectory{store: store, keys: keys}, nil
}

func (d *RedisGateDirectory) Resolve(ctx context.Context, gateID GateID) (GateEndpoint, error) {
	if d == nil || d.store == nil {
		return GateEndpoint{}, fmt.Errorf("%w: Gate directory is not configured", ErrRouteStoreUnavailable)
	}
	if gateID == "" {
		return GateEndpoint{}, fmt.Errorf("%w: gate id is required", ErrDestinationInvalid)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	address, err := d.store.Get(ctx, d.keys.gateEndpoint(gateID)).Result()
	if errors.Is(err, redis.Nil) {
		return GateEndpoint{}, fmt.Errorf("%w: %q", ErrGateEndpointNotFound, gateID)
	}
	if err != nil {
		return GateEndpoint{}, routeStoreError(fmt.Sprintf("resolve Gate endpoint %q", gateID), err)
	}
	endpoint, err := (GateEndpoint{GateID: gateID, Address: address}).validated()
	if err != nil {
		return GateEndpoint{}, routeStoreError(fmt.Sprintf("validate Gate endpoint %q", gateID), err)
	}
	return endpoint, nil
}

// EndpointRegistrarConfig controls the TTL heartbeat for one Gate endpoint.
type EndpointRegistrarConfig struct {
	TTL     time.Duration `config:"ttl" yaml:"ttl"`
	Refresh time.Duration `config:"refresh" yaml:"refresh"`
}

func (c EndpointRegistrarConfig) normalized() (EndpointRegistrarConfig, error) {
	if c.TTL <= 0 {
		return EndpointRegistrarConfig{}, fmt.Errorf("%w: endpoint ttl must be positive", ErrDestinationInvalid)
	}
	if c.Refresh < 0 {
		return EndpointRegistrarConfig{}, fmt.Errorf("%w: endpoint refresh must not be negative", ErrDestinationInvalid)
	}
	if c.Refresh == 0 {
		c.Refresh = c.TTL / 3
	}
	if c.Refresh <= 0 || c.Refresh >= c.TTL {
		return EndpointRegistrarConfig{}, fmt.Errorf("%w: endpoint refresh must be positive and smaller than ttl", ErrDestinationInvalid)
	}
	return c, nil
}

// EndpointRegistrar keeps a Gate delivery endpoint visible in Redis until its
// lifecycle stops. The GateID is process-unique, so this process owns a unique
// Redis key and does not need a second endpoint owner token or fencing script.
type EndpointRegistrar struct {
	store    GateEndpointStore
	keys     Keyspace
	endpoint GateEndpoint
	cfg      EndpointRegistrarConfig

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	started bool
}

func NewEndpointRegistrar(store GateEndpointStore, keys Keyspace, endpoint GateEndpoint, cfg EndpointRegistrarConfig) (*EndpointRegistrar, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: Gate endpoint store is required", ErrRouteStoreUnavailable)
	}
	if keys.Prefix() == "" {
		return nil, fmt.Errorf("%w: redis key prefix is required", ErrDestinationInvalid)
	}
	var err error
	if endpoint, err = endpoint.validated(); err != nil {
		return nil, err
	}
	if cfg, err = cfg.normalized(); err != nil {
		return nil, err
	}
	return &EndpointRegistrar{store: store, keys: keys, endpoint: endpoint, cfg: cfg}, nil
}

func (r *EndpointRegistrar) Start(ctx context.Context) error {
	if r == nil {
		return errors.New("server send: endpoint registrar is nil")
	}
	if ctx == nil {
		return errors.New("server send: endpoint registrar context is nil")
	}
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return errors.New("server send: endpoint registrar is already started")
	}
	if err := r.register(ctx); err != nil {
		r.mu.Unlock()
		return err
	}
	loopCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.done = make(chan struct{})
	r.started = true
	done := r.done
	r.mu.Unlock()
	go r.refreshLoop(loopCtx, done)
	return nil
}

func (r *EndpointRegistrar) Stop(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	if !r.started {
		r.mu.Unlock()
		return nil
	}
	cancel, done := r.cancel, r.done
	r.cancel, r.done, r.started = nil, nil, false
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	var waitErr error
	select {
	case <-done:
	case <-ctx.Done():
		waitErr = ctx.Err()
	}

	// Stop should still make a bounded best effort to delete this process's
	// unique key even when the lifecycle context has already expired.
	releaseParent := context.WithoutCancel(ctx)
	releaseCtx, cancelRelease := context.WithTimeout(releaseParent, min(r.cfg.Refresh, time.Second))
	defer cancelRelease()
	if err := r.store.Del(releaseCtx, r.keys.gateEndpoint(r.endpoint.GateID)).Err(); err != nil {
		return errors.Join(waitErr, routeStoreError(fmt.Sprintf("release Gate endpoint %q", r.endpoint.GateID), err))
	}
	return waitErr
}

func (r *EndpointRegistrar) register(ctx context.Context) error {
	if err := r.store.Set(ctx, r.keys.gateEndpoint(r.endpoint.GateID), r.endpoint.Address, r.cfg.TTL).Err(); err != nil {
		return routeStoreError(fmt.Sprintf("register Gate endpoint %q", r.endpoint.GateID), err)
	}
	return nil
}

func (r *EndpointRegistrar) renew(ctx context.Context) error {
	if err := r.store.Set(ctx, r.keys.gateEndpoint(r.endpoint.GateID), r.endpoint.Address, r.cfg.TTL).Err(); err != nil {
		return routeStoreError(fmt.Sprintf("renew Gate endpoint %q", r.endpoint.GateID), err)
	}
	return nil
}

func (r *EndpointRegistrar) refreshLoop(ctx context.Context, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(r.cfg.Refresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.renew(ctx); err != nil {
				// A transient store failure is retried on the next tick. Since this
				// registrar's GateID is process-unique, renewal cannot overwrite a
				// different process's endpoint key.
				log.Printf("[server send] Gate endpoint renewal failed: gate=%s err=%v", r.endpoint.GateID, err)
			}
		}
	}
}

var _ GateResolver = (*RedisGateDirectory)(nil)
var _ interface {
	Start(context.Context) error
	Stop(context.Context) error
} = (*EndpointRegistrar)(nil)
