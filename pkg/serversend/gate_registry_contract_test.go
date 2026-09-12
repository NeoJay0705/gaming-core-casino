package serversend

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisGateDirectoryContractResolvesAddressRegistration(t *testing.T) {
	keys, err := NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	store := newMemoryEndpointStore()
	store.values[keys.gateEndpoint("gate-a")] = "127.0.0.1:9001"
	directory, err := NewRedisGateDirectory(store, keys)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := directory.Resolve(context.Background(), "gate-a")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if endpoint != (GateEndpoint{GateID: "gate-a", Address: "127.0.0.1:9001"}) {
		t.Fatalf("endpoint = %#v", endpoint)
	}
	if _, err := directory.Resolve(context.Background(), "missing"); !errors.Is(err, ErrGateEndpointNotFound) {
		t.Fatalf("missing endpoint error = %v, want ErrGateEndpointNotFound", err)
	}
	store.err = errors.New("redis down")
	if _, err := directory.Resolve(context.Background(), "gate-a"); !errors.Is(err, ErrRouteStoreUnavailable) {
		t.Fatalf("store endpoint error = %v, want ErrRouteStoreUnavailable", err)
	}
	store.err = context.Canceled
	if _, err := directory.Resolve(context.Background(), "gate-a"); !errors.Is(err, ErrRouteStoreUnavailable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled endpoint error = %v, want route-store and context cancellation", err)
	}
	store.err = nil
	store.values[keys.gateEndpoint("gate-a")] = "not-an-endpoint"
	if _, err := directory.Resolve(context.Background(), "gate-a"); !errors.Is(err, ErrDestinationInvalid) || !strings.Contains(err.Error(), "gate-a") {
		t.Fatalf("malformed endpoint error = %v, want destination error containing GateID", err)
	}
}

func TestEndpointRegistrarContractRegistersRefreshesAndRemovesLease(t *testing.T) {
	keys, err := NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	store := newMemoryEndpointStore()
	registrar, err := NewEndpointRegistrar(store, keys, GateEndpoint{GateID: "gate-a", Address: "127.0.0.1:9001"}, EndpointRegistrarConfig{TTL: time.Second, Refresh: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := registrar.Start(context.Background()); err != nil {
		t.Fatalf("start registrar: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for store.SetCount() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if store.SetCount() < 2 {
		t.Fatalf("registrar did not refresh endpoint; set count = %d", store.SetCount())
	}
	if value, exists := store.Value(keys.gateEndpoint("gate-a")); !exists || value != "127.0.0.1:9001" {
		t.Fatalf("registered endpoint = %q exists:%t", value, exists)
	}
	if err := registrar.Stop(context.Background()); err != nil {
		t.Fatalf("stop registrar: %v", err)
	}
	if _, exists := store.Value(keys.gateEndpoint("gate-a")); exists {
		t.Fatal("endpoint remained registered after stop")
	}
}

func TestRuntimeGateIdentityContractUsesDistinctProcessKeys(t *testing.T) {
	first, err := NewRuntimeGateIdentity()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRuntimeGateIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if first.GateID == "" || second.GateID == "" || first.GateID == second.GateID {
		t.Fatalf("runtime identities = %#v and %#v, want distinct non-empty IDs", first, second)
	}
}

func TestEndpointRegistrarContractPreservesStoreCancellationCause(t *testing.T) {
	keys, err := NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	store := newMemoryEndpointStore()
	store.err = context.DeadlineExceeded
	registrar, err := NewEndpointRegistrar(store, keys, GateEndpoint{GateID: "gate-a", Address: "127.0.0.1:9001"}, EndpointRegistrarConfig{TTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := registrar.Start(context.Background()); !errors.Is(err, ErrRouteStoreUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled endpoint registration error = %v, want route-store and deadline", err)
	}
}

type memoryEndpointStore struct {
	mu     sync.Mutex
	values map[string]string
	sets   int
	err    error
}

func newMemoryEndpointStore() *memoryEndpointStore {
	return &memoryEndpointStore{values: make(map[string]string)}
}

func (s *memoryEndpointStore) Set(_ context.Context, key string, value any, _ time.Duration) *redis.StatusCmd {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return redis.NewStatusResult("", s.err)
	}
	valueString, ok := value.(string)
	if !ok {
		return redis.NewStatusResult("", errors.New("unexpected endpoint value type"))
	}
	s.values[key] = valueString
	s.sets++
	return redis.NewStatusResult("OK", nil)
}

func (s *memoryEndpointStore) Get(_ context.Context, key string) *redis.StringCmd {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return redis.NewStringResult("", s.err)
	}
	value, ok := s.values[key]
	if !ok {
		return redis.NewStringResult("", redis.Nil)
	}
	return redis.NewStringResult(value, nil)
}

func (s *memoryEndpointStore) Del(_ context.Context, keys ...string) *redis.IntCmd {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return redis.NewIntResult(0, s.err)
	}
	var count int64
	for _, key := range keys {
		if _, exists := s.values[key]; exists {
			delete(s.values, key)
			count++
		}
	}
	return redis.NewIntResult(count, nil)
}

func (s *memoryEndpointStore) SetCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sets
}

func (s *memoryEndpointStore) Value(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, exists := s.values[key]
	return value, exists
}
