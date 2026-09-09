package serversend

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestPresenceRegistryContractClaimsResolvesRenewsAndReleases(t *testing.T) {
	registry := newTestPresenceRegistry(t)
	claimed, err := registry.Claim(context.Background(), Presence{LoginName: " alice ", GateID: "gate-a", ConnectionID: "connection-1"})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Epoch != 1 {
		t.Fatalf("claim epoch = %d, want 1", claimed.Epoch)
	}
	resolved, err := registry.Resolve(context.Background(), " alice ")
	if err != nil {
		t.Fatalf("resolve opaque login name: %v", err)
	}
	if resolved != claimed {
		t.Fatalf("resolved presence = %#v, want %#v", resolved, claimed)
	}
	if _, err := registry.Resolve(context.Background(), "alice"); !errors.Is(err, ErrPresenceNotFound) {
		t.Fatalf("normalized login lookup error = %v, want ErrPresenceNotFound", err)
	}
	if err := registry.Renew(context.Background(), claimed); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if err := registry.Release(context.Background(), claimed); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := registry.Release(context.Background(), claimed); err != nil {
		t.Fatalf("second release must be idempotent: %v", err)
	}
	if _, err := registry.Resolve(context.Background(), claimed.LoginName); !errors.Is(err, ErrPresenceNotFound) {
		t.Fatalf("resolve released presence error = %v, want ErrPresenceNotFound", err)
	}
}

func TestPresenceRegistryContractStaleLeaseCannotReleaseReplacement(t *testing.T) {
	registry := newTestPresenceRegistry(t)
	oldPresence, err := registry.Claim(context.Background(), Presence{LoginName: "alice", GateID: "gate-a", ConnectionID: "connection-old"})
	if err != nil {
		t.Fatalf("claim old: %v", err)
	}
	replacement, err := registry.Claim(context.Background(), Presence{LoginName: "alice", GateID: "gate-b", ConnectionID: "connection-new"})
	if err != nil {
		t.Fatalf("claim replacement: %v", err)
	}
	if replacement.Epoch != oldPresence.Epoch+1 {
		t.Fatalf("replacement epoch = %d, want %d", replacement.Epoch, oldPresence.Epoch+1)
	}
	if err := registry.Renew(context.Background(), oldPresence); !errors.Is(err, ErrPresenceNotOwner) {
		t.Fatalf("stale renew error = %v, want ErrPresenceNotOwner", err)
	}
	if err := registry.Release(context.Background(), oldPresence); !errors.Is(err, ErrPresenceNotOwner) {
		t.Fatalf("stale release error = %v, want ErrPresenceNotOwner", err)
	}
	resolved, err := registry.Resolve(context.Background(), "alice")
	if err != nil {
		t.Fatalf("resolve replacement: %v", err)
	}
	if resolved != replacement {
		t.Fatalf("replacement was removed by stale lease: %#v, want %#v", resolved, replacement)
	}
}

func TestPresenceRegistryContractRejectsInvalidInputAndStoreFailure(t *testing.T) {
	keys, err := NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewPresenceRegistry(nil, keys, PresenceConfig{LeaseTTL: time.Second}); !errors.Is(err, ErrRouteStoreUnavailable) {
		t.Fatalf("nil store error = %v, want ErrRouteStoreUnavailable", err)
	}
	if _, err := NewPresenceRegistry(newMemoryPresenceStore(), keys, PresenceConfig{}); !errors.Is(err, ErrDestinationInvalid) {
		t.Fatalf("zero ttl error = %v, want ErrDestinationInvalid", err)
	}
	registry := newTestPresenceRegistry(t)
	if _, err := registry.Claim(context.Background(), Presence{LoginName: "alice", GateID: "gate-a"}); !errors.Is(err, ErrDestinationInvalid) {
		t.Fatalf("incomplete claim error = %v, want ErrDestinationInvalid", err)
	}
	if _, err := registry.Resolve(context.Background(), ""); !errors.Is(err, ErrDestinationInvalid) {
		t.Fatalf("empty resolve error = %v, want ErrDestinationInvalid", err)
	}
	if err := registry.Release(context.Background(), Presence{LoginName: "alice", GateID: "gate-a", ConnectionID: "connection-a"}); !errors.Is(err, ErrDestinationInvalid) {
		t.Fatalf("zero epoch release error = %v, want ErrDestinationInvalid", err)
	}

	store := newMemoryPresenceStore()
	store.err = errors.New("redis unavailable")
	failing, err := NewPresenceRegistry(store, keys, PresenceConfig{LeaseTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failing.Claim(context.Background(), Presence{LoginName: "alice", GateID: "gate-a", ConnectionID: "connection-a"}); !errors.Is(err, ErrRouteStoreUnavailable) {
		t.Fatalf("store claim error = %v, want ErrRouteStoreUnavailable", err)
	}
	store.err = context.Canceled
	if _, err := failing.Claim(context.Background(), Presence{LoginName: "alice", GateID: "gate-a", ConnectionID: "connection-a"}); !errors.Is(err, ErrRouteStoreUnavailable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled store claim error = %v, want route-store and context cancellation", err)
	}
	store.err = redis.Nil
	if _, err := failing.Resolve(context.Background(), "missing-from-store"); !errors.Is(err, ErrPresenceNotFound) || errors.Is(err, ErrRouteStoreUnavailable) {
		t.Fatalf("Redis nil resolve error = %v, want only ErrPresenceNotFound", err)
	}
}

func TestKeyspaceContractUsesOnePrefixAndOpaqueComponents(t *testing.T) {
	keys, err := NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	if keys.Prefix() != "core-casino:server-send" {
		t.Fatalf("prefix = %q, want core-casino:server-send", keys.Prefix())
	}
	for _, key := range []string{keys.presence("alice:one"), keys.gateEndpoint("gate/a"), keys.roomChannel("room a")} {
		if len(key) == 0 || key[:len("core-casino:server-send:")] != "core-casino:server-send:" {
			t.Fatalf("key = %q, want shared prefix", key)
		}
	}
	if _, err := NewKeyspace(""); !errors.Is(err, ErrDestinationInvalid) {
		t.Fatalf("empty prefix error = %v, want ErrDestinationInvalid", err)
	}
}

func TestRedisPresenceResolverContractDoesNotRequireLeaseTTL(t *testing.T) {
	keys, err := NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	store := newMemoryPresenceStore()
	resolver, err := NewRedisPresenceResolver(store, keys)
	if err != nil {
		t.Fatal(err)
	}
	store.values[keys.presence("alice")] = map[string]string{
		presenceGateIDField:       "gate-a",
		presenceConnectionIDField: "connection-a",
		presenceEpochField:        "1",
	}
	got, err := resolver.Resolve(context.Background(), "alice")
	if err != nil {
		t.Fatalf("resolve without lease config: %v", err)
	}
	if got != (Presence{LoginName: "alice", GateID: "gate-a", ConnectionID: "connection-a", Epoch: 1}) {
		t.Fatalf("resolved presence = %#v", got)
	}
}

func newTestPresenceRegistry(t *testing.T) *PresenceRegistry {
	t.Helper()
	keys, err := NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewPresenceRegistry(newMemoryPresenceStore(), keys, PresenceConfig{LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

type memoryPresenceStore struct {
	mu     sync.Mutex
	values map[string]map[string]string
	err    error
}

func newMemoryPresenceStore() *memoryPresenceStore {
	return &memoryPresenceStore{values: make(map[string]map[string]string)}
}

func (s *memoryPresenceStore) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return redis.NewCmdResult(nil, s.err)
	}
	if len(keys) != 1 {
		return redis.NewCmdResult(nil, errors.New("expected one key"))
	}
	key := keys[0]
	entry := s.values[key]
	switch script {
	case claimPresenceScript:
		if entry == nil {
			entry = make(map[string]string)
			s.values[key] = entry
		}
		epoch, _ := strconv.ParseUint(entry[presenceEpochField], 10, 64)
		epoch++
		entry[presenceGateIDField] = args[0].(string)
		entry[presenceConnectionIDField] = args[1].(string)
		entry[presenceEpochField] = strconv.FormatUint(epoch, 10)
		return redis.NewCmdResult(int64(epoch), nil)
	case renewPresenceScript:
		if memoryLeaseMatches(entry, args) {
			return redis.NewCmdResult(int64(1), nil)
		}
		return redis.NewCmdResult(int64(0), nil)
	case releasePresenceScript:
		if entry == nil {
			return redis.NewCmdResult(int64(2), nil)
		}
		if !memoryLeaseMatches(entry, args) {
			return redis.NewCmdResult(int64(0), nil)
		}
		delete(s.values, key)
		return redis.NewCmdResult(int64(1), nil)
	default:
		return redis.NewCmdResult(nil, errors.New("unexpected script"))
	}
}

func (s *memoryPresenceStore) HGetAll(_ context.Context, key string) *redis.MapStringStringCmd {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return redis.NewMapStringStringResult(nil, s.err)
	}
	result := make(map[string]string, len(s.values[key]))
	for field, value := range s.values[key] {
		result[field] = value
	}
	return redis.NewMapStringStringResult(result, nil)
}

func memoryLeaseMatches(entry map[string]string, args []any) bool {
	return entry != nil && entry[presenceGateIDField] == args[0].(string) && entry[presenceConnectionIDField] == args[1].(string) && entry[presenceEpochField] == args[2].(string)
}
