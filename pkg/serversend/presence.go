package serversend

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	presenceGateIDField       = "gate_id"
	presenceConnectionIDField = "connection_id"
	presenceEpochField        = "epoch"
)

const claimPresenceScript = `
local epoch = redis.call("HINCRBY", KEYS[1], "epoch", 1)
redis.call("HSET", KEYS[1], "gate_id", ARGV[1], "connection_id", ARGV[2], "epoch", epoch)
redis.call("PEXPIRE", KEYS[1], ARGV[3])
return epoch
`

const renewPresenceScript = `
if redis.call("HGET", KEYS[1], "gate_id") ~= ARGV[1] then return 0 end
if redis.call("HGET", KEYS[1], "connection_id") ~= ARGV[2] then return 0 end
if redis.call("HGET", KEYS[1], "epoch") ~= ARGV[3] then return 0 end
redis.call("PEXPIRE", KEYS[1], ARGV[4])
return 1
`

const releasePresenceScript = `
if redis.call("EXISTS", KEYS[1]) == 0 then return 2 end
if redis.call("HGET", KEYS[1], "gate_id") ~= ARGV[1] then return 0 end
if redis.call("HGET", KEYS[1], "connection_id") ~= ARGV[2] then return 0 end
if redis.call("HGET", KEYS[1], "epoch") ~= ARGV[3] then return 0 end
return redis.call("DEL", KEYS[1])
`

// PresenceStore is the narrow Redis dependency required by PresenceRegistry.
// redis.UniversalClient satisfies it for both standalone and cluster Redis.
type PresenceStore interface {
	Eval(context.Context, string, []string, ...any) *redis.Cmd
	HGetAll(context.Context, string) *redis.MapStringStringCmd
}

// Presence records the Gate connection currently authoritative for a player.
type Presence struct {
	LoginName    LoginName
	GateID       GateID
	ConnectionID ConnectionID
	Epoch        uint64
}

// PresenceConfig controls the Redis lease duration for one online player.
// It is a Gate-owned setting; Game only needs PresenceResolver.
type PresenceConfig struct {
	LeaseTTL time.Duration `config:"lease_ttl" yaml:"lease_ttl"`
}

func (c PresenceConfig) normalized() (PresenceConfig, error) {
	if c.LeaseTTL <= 0 {
		return PresenceConfig{}, fmt.Errorf("%w: presence lease ttl must be positive", ErrDestinationInvalid)
	}
	return c, nil
}

// PresenceResolver is the read-only presence dependency needed by Game.
type PresenceResolver interface {
	Resolve(context.Context, LoginName) (Presence, error)
}

// RedisPresenceResolver reads the current authoritative Gate owner. It has no
// lease configuration because resolving a player is independent of owning a
// session lease.
type RedisPresenceResolver struct {
	store PresenceStore
	keys  Keyspace
}

// NewRedisPresenceResolver builds the Game-side read path over Redis.
func NewRedisPresenceResolver(store PresenceStore, keys Keyspace) (*RedisPresenceResolver, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: presence store is required", ErrRouteStoreUnavailable)
	}
	if keys.Prefix() == "" {
		return nil, fmt.Errorf("%w: redis key prefix is required", ErrDestinationInvalid)
	}
	return &RedisPresenceResolver{store: store, keys: keys}, nil
}

// PresenceRegistry owns atomic Redis claim, resolve, renew, and release
// operations for Gate session lifecycle. It does not authenticate identities
// or own its Redis pool.
type PresenceRegistry struct {
	resolver *RedisPresenceResolver
	store    PresenceStore
	keys     Keyspace
	leaseTTL time.Duration
}

// NewPresenceRegistry builds a distributed presence registry over Redis.
func NewPresenceRegistry(store PresenceStore, keys Keyspace, cfg PresenceConfig) (*PresenceRegistry, error) {
	var err error
	if cfg, err = cfg.normalized(); err != nil {
		return nil, err
	}
	resolver, err := NewRedisPresenceResolver(store, keys)
	if err != nil {
		return nil, err
	}
	return &PresenceRegistry{resolver: resolver, store: store, keys: keys, leaseTTL: cfg.LeaseTTL}, nil
}

// Claim makes presence the latest authoritative owner for its LoginName.
// Each successful claim gets a higher epoch so an old connection cannot renew
// or release a replacement's lease.
func (r *PresenceRegistry) Claim(ctx context.Context, presence Presence) (Presence, error) {
	if err := validatePresenceIdentity(presence); err != nil {
		return Presence{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if r == nil || r.store == nil {
		return Presence{}, fmt.Errorf("%w: presence registry is not configured", ErrRouteStoreUnavailable)
	}
	epoch, err := r.store.Eval(ctx, claimPresenceScript, []string{r.keys.presence(presence.LoginName)}, string(presence.GateID), string(presence.ConnectionID), r.leaseTTL.Milliseconds()).Int64()
	if err != nil {
		return Presence{}, routeStoreError("claim player presence", err)
	}
	if epoch <= 0 {
		return Presence{}, fmt.Errorf("%w: claim returned invalid epoch %d", ErrRouteStoreUnavailable, epoch)
	}
	presence.Epoch = uint64(epoch)
	return presence, nil
}

// Resolve returns the latest unexpired authoritative player owner.
func (r *PresenceRegistry) Resolve(ctx context.Context, loginName LoginName) (Presence, error) {
	if r == nil || r.resolver == nil {
		return Presence{}, fmt.Errorf("%w: presence resolver is not configured", ErrRouteStoreUnavailable)
	}
	return r.resolver.Resolve(ctx, loginName)
}

// Resolve returns the latest unexpired authoritative player owner.
func (r *RedisPresenceResolver) Resolve(ctx context.Context, loginName LoginName) (Presence, error) {
	if r == nil || r.store == nil {
		return Presence{}, fmt.Errorf("%w: presence resolver is not configured", ErrRouteStoreUnavailable)
	}
	if loginName == "" {
		return Presence{}, fmt.Errorf("%w: login name is required", ErrDestinationInvalid)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	values, err := r.store.HGetAll(ctx, r.keys.presence(loginName)).Result()
	if errors.Is(err, redis.Nil) {
		return Presence{}, fmt.Errorf("%w: %q", ErrPresenceNotFound, loginName)
	}
	if err != nil {
		return Presence{}, routeStoreError("resolve player presence", err)
	}
	if len(values) == 0 {
		return Presence{}, fmt.Errorf("%w: %q", ErrPresenceNotFound, loginName)
	}
	epoch, err := parsePresenceEpoch(values[presenceEpochField])
	if err != nil || epoch == 0 || values[presenceGateIDField] == "" || values[presenceConnectionIDField] == "" {
		return Presence{}, fmt.Errorf("%w: malformed presence for %q", ErrDestinationInvalid, loginName)
	}
	return Presence{LoginName: loginName, GateID: GateID(values[presenceGateIDField]), ConnectionID: ConnectionID(values[presenceConnectionIDField]), Epoch: epoch}, nil
}

func parsePresenceEpoch(value string) (uint64, error) {
	return strconv.ParseUint(value, 10, 64)
}

// Renew extends an unchanged presence lease. A stale lease returns
// ErrPresenceNotOwner and must not recreate the key.
func (r *PresenceRegistry) Renew(ctx context.Context, presence Presence) error {
	if err := validatePresenceLease(presence); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if r == nil || r.store == nil {
		return fmt.Errorf("%w: presence registry is not configured", ErrRouteStoreUnavailable)
	}
	updated, err := r.store.Eval(ctx, renewPresenceScript, []string{r.keys.presence(presence.LoginName)}, string(presence.GateID), string(presence.ConnectionID), strconv.FormatUint(presence.Epoch, 10), r.leaseTTL.Milliseconds()).Int64()
	if err != nil {
		return routeStoreError("renew player presence", err)
	}
	if updated != 1 {
		return fmt.Errorf("%w: %q", ErrPresenceNotOwner, presence.LoginName)
	}
	return nil
}

// Release removes a lease only when the exact claimed owner and epoch still
// match. Releasing an already-absent lease is idempotent; a replacement lease
// remains an ErrPresenceNotOwner.
func (r *PresenceRegistry) Release(ctx context.Context, presence Presence) error {
	if err := validatePresenceLease(presence); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if r == nil || r.store == nil {
		return fmt.Errorf("%w: presence registry is not configured", ErrRouteStoreUnavailable)
	}
	removed, err := r.store.Eval(ctx, releasePresenceScript, []string{r.keys.presence(presence.LoginName)}, string(presence.GateID), string(presence.ConnectionID), strconv.FormatUint(presence.Epoch, 10)).Int64()
	if err != nil {
		return routeStoreError("release player presence", err)
	}
	switch removed {
	case 1, 2:
		return nil
	case 0:
		return fmt.Errorf("%w: %q", ErrPresenceNotOwner, presence.LoginName)
	default:
		return fmt.Errorf("%w: release returned invalid result %d", ErrRouteStoreUnavailable, removed)
	}
}

func validatePresenceIdentity(presence Presence) error {
	if presence.LoginName == "" || presence.GateID == "" || presence.ConnectionID == "" {
		return fmt.Errorf("%w: login name, gate id, and connection id are required", ErrDestinationInvalid)
	}
	return nil
}

func validatePresenceLease(presence Presence) error {
	if err := validatePresenceIdentity(presence); err != nil {
		return err
	}
	if presence.Epoch == 0 {
		return fmt.Errorf("%w: presence epoch is required", ErrDestinationInvalid)
	}
	return nil
}
