package serversend

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestPresenceRenewalConfigContractDefaultsAndBounds(t *testing.T) {
	normalized, err := NormalizePresenceConfig(PresenceConfig{LeaseTTL: 5 * time.Minute})
	if err != nil {
		t.Fatalf("normalize default renewal config: %v", err)
	}
	if normalized.Renewal.Interval != 100*time.Second {
		t.Fatalf("default renewal interval = %s, want 100s", normalized.Renewal.Interval)
	}
	if normalized.Renewal.Buckets != 100 {
		t.Fatalf("default renewal buckets = %d, want 100", normalized.Renewal.Buckets)
	}
	if got := normalized.Renewal.Interval / time.Duration(normalized.Renewal.Buckets); got != time.Second {
		t.Fatalf("derived renewal tick = %s, want 1s", got)
	}

	cases := []PresenceConfig{
		{LeaseTTL: time.Minute, Renewal: PresenceRenewalConfig{Interval: -time.Second}},
		{LeaseTTL: time.Minute, Renewal: PresenceRenewalConfig{Interval: 31 * time.Second}},
		{LeaseTTL: time.Minute, Renewal: PresenceRenewalConfig{Buckets: -1}},
		{LeaseTTL: time.Second, Renewal: PresenceRenewalConfig{Interval: time.Nanosecond, Buckets: 2}},
	}
	for index, cfg := range cases {
		if _, err := NormalizePresenceConfig(cfg); !errors.Is(err, ErrDestinationInvalid) {
			t.Errorf("invalid renewal config %d error = %v, want ErrDestinationInvalid", index, err)
		}
	}
}

func TestPresenceRenewResultContractClassifiesScriptValues(t *testing.T) {
	if err := presenceRenewResultError("alice", 1); err != nil {
		t.Fatalf("success result error = %v, want nil", err)
	}
	if err := presenceRenewResultError("alice", 0); !errors.Is(err, ErrPresenceNotOwner) {
		t.Fatalf("not-owner result error = %v, want ErrPresenceNotOwner", err)
	}
	if err := presenceRenewResultError("alice", 2); !errors.Is(err, ErrRouteStoreUnavailable) || errors.Is(err, ErrPresenceNotOwner) {
		t.Fatalf("unexpected result error = %v, want store error only", err)
	}
}

func TestPresenceRegistryRenewManyContractUsesFencedPipelineResults(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: miniRedis.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	keys, err := NewKeyspace("renewal-contract")
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewPresenceRegistry(client, keys, PresenceConfig{LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}

	alice, err := registry.Claim(context.Background(), Presence{LoginName: "alice", GateID: "gate-a", ConnectionID: "connection-alice"})
	if err != nil {
		t.Fatalf("claim alice: %v", err)
	}
	bob, err := registry.Claim(context.Background(), Presence{LoginName: "bob", GateID: "gate-a", ConnectionID: "connection-bob"})
	if err != nil {
		t.Fatalf("claim bob: %v", err)
	}
	results := registry.RenewMany(context.Background(), []Presence{alice, bob})
	if len(results) != 2 {
		t.Fatalf("renew result count = %d, want 2", len(results))
	}
	if results[0].Presence != alice || results[1].Presence != bob {
		t.Fatalf("renew result order = %#v, want alice then bob", results)
	}
	for index, result := range results {
		if result.Err != nil {
			t.Errorf("renew result %d error = %v, want nil", index, result.Err)
		}
	}

	replacement, err := registry.Claim(context.Background(), Presence{LoginName: "alice", GateID: "gate-b", ConnectionID: "connection-alice-new"})
	if err != nil {
		t.Fatalf("claim replacement: %v", err)
	}
	results = registry.RenewMany(context.Background(), []Presence{alice, replacement})
	if !errors.Is(results[0].Err, ErrPresenceNotOwner) {
		t.Fatalf("stale batch result error = %v, want ErrPresenceNotOwner", results[0].Err)
	}
	if results[1].Err != nil {
		t.Fatalf("replacement batch result error = %v, want nil", results[1].Err)
	}
	if err := registry.Release(context.Background(), bob); err != nil {
		t.Fatalf("release bob: %v", err)
	}
	if err := registry.Release(context.Background(), replacement); err != nil {
		t.Fatalf("release replacement: %v", err)
	}
}

func TestGatePresenceRegistryRenewManyContractRejectsForeignGateBeforeRedis(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: miniRedis.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	keys, err := NewKeyspace("gate-renewal-contract")
	if err != nil {
		t.Fatal(err)
	}
	presence, err := NewPresenceRegistry(client, keys, PresenceConfig{LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	gateRegistry, err := NewGatePresenceRegistry(presence, "gate-a")
	if err != nil {
		t.Fatal(err)
	}
	owned, err := gateRegistry.Claim(context.Background(), "alice", "connection-alice")
	if err != nil {
		t.Fatal(err)
	}
	foreign := owned
	foreign.GateID = "gate-b"
	results := gateRegistry.RenewMany(context.Background(), []Presence{foreign, owned})
	if len(results) != 2 {
		t.Fatalf("renew result count = %d, want 2", len(results))
	}
	if !errors.Is(results[0].Err, ErrPresenceNotOwner) {
		t.Fatalf("foreign Gate result error = %v, want ErrPresenceNotOwner", results[0].Err)
	}
	if results[1].Err != nil {
		t.Fatalf("owned Gate result error = %v, want nil", results[1].Err)
	}
	if err := gateRegistry.Release(context.Background(), owned); err != nil {
		t.Fatalf("release owned presence: %v", err)
	}
}
