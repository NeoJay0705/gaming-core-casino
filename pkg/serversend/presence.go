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
	LeaseTTL time.Duration         `config:"lease_ttl" yaml:"lease_ttl"`
	Renewal  PresenceRenewalConfig `config:"renewal" yaml:"renewal"`
}

// PresenceRenewalConfig 控制 Gate 端 lease 排程。Interval 是正常成功續租的
// 間隔；retry backoff 刻意不放入此設定，避免暫時性 Redis 錯誤縮短正常週期。
type PresenceRenewalConfig struct {
	Interval time.Duration `config:"interval" yaml:"interval"`
	Buckets  int           `config:"buckets" yaml:"buckets"`
}

const defaultPresenceRenewalBuckets = 100

func (c PresenceConfig) normalized() (PresenceConfig, error) {
	if c.LeaseTTL <= 0 {
		return PresenceConfig{}, fmt.Errorf("%w: presence lease ttl must be positive", ErrDestinationInvalid)
	}
	if c.Renewal.Interval < 0 {
		return PresenceConfig{}, fmt.Errorf("%w: presence renewal interval must not be negative", ErrDestinationInvalid)
	}
	if c.Renewal.Interval == 0 {
		c.Renewal.Interval = c.LeaseTTL / 3
	}
	if c.Renewal.Interval <= 0 || c.Renewal.Interval > c.LeaseTTL/2 {
		return PresenceConfig{}, fmt.Errorf("%w: presence renewal interval must be positive and no greater than lease ttl / 2", ErrDestinationInvalid)
	}
	if c.Renewal.Buckets < 0 {
		return PresenceConfig{}, fmt.Errorf("%w: presence renewal buckets must not be negative", ErrDestinationInvalid)
	}
	if c.Renewal.Buckets == 0 {
		c.Renewal.Buckets = defaultPresenceRenewalBuckets
	}
	if c.Renewal.Interval/time.Duration(c.Renewal.Buckets) <= 0 {
		return PresenceConfig{}, fmt.Errorf("%w: presence renewal interval divided by buckets must be positive", ErrDestinationInvalid)
	}
	return c, nil
}

// NormalizePresenceConfig applies the ownership renewal defaults and validates
// the complete Gate-owned configuration. Gate lifecycle wiring uses this same
// normalization as PresenceRegistry so scheduler and Redis operations cannot
// observe different intervals or bucket counts.
func NormalizePresenceConfig(c PresenceConfig) (PresenceConfig, error) {
	return c.normalized()
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

// PresenceRenewResult 是單一輸入 lease 的結果。使用結果 slice 而非單一 aggregate
// error，因為 Redis Cluster pipeline 的 command 可能在不同節點獨立完成。
type PresenceRenewResult struct {
	Presence Presence
	Err      error
}

type presencePipelinedStore interface {
	Pipelined(context.Context, func(redis.Pipeliner) error) ([]redis.Cmder, error)
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
	return presenceRenewResultError(presence.LoginName, updated)
}

func presenceRenewResultError(loginName LoginName, updated int64) error {
	switch updated {
	case 1:
		return nil
	case 0:
		return fmt.Errorf("%w: %q", ErrPresenceNotOwner, loginName)
	default:
		return fmt.Errorf("%w: renew player presence %q returned unexpected result %d", ErrRouteStoreUnavailable, loginName, updated)
	}
}

// RenewMany 以 non-transactional pipeline 為每個合法 presence 執行一個 fenced、
// single-key EVAL。結果順序與輸入相同，讓 scheduler 能逐筆觀測 malformed entry
// 與 Redis command error。
func (r *PresenceRegistry) RenewMany(ctx context.Context, presences []Presence) []PresenceRenewResult {
	results := make([]PresenceRenewResult, len(presences))
	if len(presences) == 0 {
		return results
	}
	validCount := 0
	for index, presence := range presences {
		results[index].Presence = presence
		if err := validatePresenceLease(presence); err != nil {
			results[index].Err = err
			continue
		}
		validCount++
	}
	if validCount == 0 {
		return results
	}
	if r == nil || r.store == nil {
		err := fmt.Errorf("%w: presence registry is not configured", ErrRouteStoreUnavailable)
		for index := range results {
			if results[index].Err == nil {
				results[index].Err = err
			}
		}
		return results
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		for index := range results {
			if results[index].Err == nil {
				results[index].Err = err
			}
		}
		return results
	}
	store, ok := r.store.(presencePipelinedStore)
	if !ok {
		err := fmt.Errorf("%w: presence store does not support pipelining", ErrRouteStoreUnavailable)
		for index := range results {
			if results[index].Err == nil {
				results[index].Err = err
			}
		}
		return results
	}

	commands := make([]*redis.Cmd, len(presences))
	_, execErr := store.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for index, presence := range presences {
			if results[index].Err != nil {
				continue
			}
			commands[index] = pipe.Eval(ctx, renewPresenceScript, []string{r.keys.presence(presence.LoginName)},
				string(presence.GateID), string(presence.ConnectionID), strconv.FormatUint(presence.Epoch, 10), r.leaseTTL.Milliseconds())
		}
		return nil
	})
	for index, command := range commands {
		if results[index].Err != nil {
			continue
		}
		if command == nil {
			if execErr != nil {
				results[index].Err = routeStoreError("renew player presence batch", execErr)
			} else {
				results[index].Err = fmt.Errorf("%w: renewal command was not queued", ErrRouteStoreUnavailable)
			}
			continue
		}
		updated, err := command.Int64()
		if err != nil {
			results[index].Err = routeStoreError(fmt.Sprintf("renew player presence %q", presences[index].LoginName), err)
			continue
		}
		results[index].Err = presenceRenewResultError(presences[index].LoginName, updated)
	}
	return results
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
