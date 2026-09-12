package serversend

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisBatchResolversContractUseUniqueRouteKeysAndClassifyRecords(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: miniRedis.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	keys, err := NewKeyspace("batch-routing")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(context.Background(), keys.presence("alice"), "gate_id", "gate-a", "connection_id", "a", "epoch", "1").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(context.Background(), keys.presence("bob"), "gate_id", "gate-a", "connection_id", "b", "epoch", "2").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(context.Background(), keys.presence("malformed"), "gate_id", "gate-a").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(context.Background(), keys.gateEndpoint("gate-a"), "127.0.0.1:9001", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(context.Background(), keys.gateEndpoint("malformed-gate"), "invalid", 0).Err(); err != nil {
		t.Fatal(err)
	}

	presence, err := NewRedisPresenceResolver(client, keys)
	if err != nil {
		t.Fatal(err)
	}
	gotPresence, err := presence.ResolveMany(context.Background(), []LoginName{"alice", "alice", "missing", "bob"})
	if !errors.Is(err, ErrPresenceNotFound) {
		t.Fatalf("resolve batch missing error = %v, want ErrPresenceNotFound", err)
	}
	if len(gotPresence) != 2 || gotPresence["alice"].GateID != "gate-a" || gotPresence["bob"].ConnectionID != "b" {
		t.Fatalf("batch presence = %#v, want alice/bob", gotPresence)
	}
	gotPresence, err = presence.ResolveMany(context.Background(), []LoginName{"malformed"})
	if !errors.Is(err, ErrDestinationInvalid) || len(gotPresence) != 0 {
		t.Fatalf("malformed presence = %#v error:%v, want destination error", gotPresence, err)
	}

	directory, err := NewRedisGateDirectory(client, keys)
	if err != nil {
		t.Fatal(err)
	}
	gotEndpoints, err := directory.ResolveMany(context.Background(), []GateID{"gate-a", "gate-a", "missing"})
	if !errors.Is(err, ErrGateEndpointNotFound) {
		t.Fatalf("resolve endpoint missing error = %v, want ErrGateEndpointNotFound", err)
	}
	if len(gotEndpoints) != 1 || gotEndpoints["gate-a"].Address != "127.0.0.1:9001" {
		t.Fatalf("batch endpoints = %#v, want gate-a", gotEndpoints)
	}
	gotEndpoints, err = directory.ResolveMany(context.Background(), []GateID{"malformed-gate"})
	if !errors.Is(err, ErrDestinationInvalid) || len(gotEndpoints) != 0 {
		t.Fatalf("malformed endpoint = %#v error:%v, want destination error", gotEndpoints, err)
	}
}

func TestRedisBatchResolversContractClassifyRedisFailureForFallback(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: miniRedis.Addr()})
	keys, err := NewKeyspace("batch-routing-failure")
	if err != nil {
		t.Fatal(err)
	}
	presence, err := NewRedisPresenceResolver(client, keys)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := presence.ResolveMany(context.Background(), []LoginName{"alice"}); !errors.Is(err, ErrRouteStoreUnavailable) {
		t.Fatalf("Redis batch failure = %v, want ErrRouteStoreUnavailable", err)
	}
}
