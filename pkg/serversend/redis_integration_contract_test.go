package serversend

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	redisinfra "github.com/NeoJay0705/gaming-core-casino/pkg/infra/redis"
	"github.com/redis/go-redis/v9"
)

func TestRedisIntegrationContractPresenceIsAtomicFencedAndExpires(t *testing.T) {
	client, keys := newRedisIntegrationClient(t)
	registry, err := NewPresenceRegistry(client, keys, PresenceConfig{LeaseTTL: 250 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	old, err := registry.Claim(context.Background(), Presence{LoginName: "alice", GateID: "gate-a", ConnectionID: "connection-old"})
	if err != nil {
		t.Fatalf("claim old: %v", err)
	}
	replacement, err := registry.Claim(context.Background(), Presence{LoginName: "alice", GateID: "gate-b", ConnectionID: "connection-new"})
	if err != nil {
		t.Fatalf("claim replacement: %v", err)
	}
	if replacement.Epoch != old.Epoch+1 {
		t.Fatalf("replacement epoch = %d, want %d", replacement.Epoch, old.Epoch+1)
	}
	if err := registry.Renew(context.Background(), old); !errors.Is(err, ErrPresenceNotOwner) {
		t.Fatalf("stale renew = %v, want ErrPresenceNotOwner", err)
	}
	if err := registry.Release(context.Background(), old); !errors.Is(err, ErrPresenceNotOwner) {
		t.Fatalf("stale release = %v, want ErrPresenceNotOwner", err)
	}
	resolved, err := registry.Resolve(context.Background(), "alice")
	if err != nil || resolved != replacement {
		t.Fatalf("replacement resolve = %#v error:%v", resolved, err)
	}
	active, err := registry.Claim(context.Background(), Presence{LoginName: "active", GateID: "gate-a", ConnectionID: "connection-active"})
	if err != nil {
		t.Fatalf("claim active presence: %v", err)
	}
	if err := registry.Renew(context.Background(), active); err != nil {
		t.Fatalf("renew active presence: %v", err)
	}
	if err := registry.Release(context.Background(), active); err != nil {
		t.Fatalf("release active presence: %v", err)
	}
	if _, err := registry.Resolve(context.Background(), active.LoginName); !errors.Is(err, ErrPresenceNotFound) {
		t.Fatalf("released active presence = %v, want ErrPresenceNotFound", err)
	}

	atomicRegistry, err := NewPresenceRegistry(client, keys, PresenceConfig{LeaseTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	const claimCount = 8
	claims := make(chan Presence, claimCount)
	errs := make(chan error, claimCount)
	var waitGroup sync.WaitGroup
	waitGroup.Add(claimCount)
	for i := 0; i < claimCount; i++ {
		go func(i int) {
			defer waitGroup.Done()
			claimed, err := atomicRegistry.Claim(context.Background(), Presence{LoginName: "atomic", GateID: "gate-a", ConnectionID: ConnectionID(fmt.Sprintf("connection-%d", i))})
			if err != nil {
				errs <- err
				return
			}
			claims <- claimed
		}(i)
	}
	waitGroup.Wait()
	close(claims)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	seenEpochs := make(map[uint64]struct{}, claimCount)
	for claimed := range claims {
		seenEpochs[claimed.Epoch] = struct{}{}
	}
	if len(seenEpochs) != claimCount {
		t.Fatalf("atomic claim epochs = %v, want %d unique epochs", seenEpochs, claimCount)
	}

	shortRegistry, err := NewPresenceRegistry(client, keys, PresenceConfig{LeaseTTL: 60 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	short, err := shortRegistry.Claim(context.Background(), Presence{LoginName: "expires", GateID: "gate-a", ConnectionID: "connection-expires"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PTTL(context.Background(), keys.presence(short.LoginName)).Result(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := shortRegistry.Resolve(context.Background(), short.LoginName); !errors.Is(err, ErrPresenceNotFound) {
		t.Fatalf("expired presence = %v, want ErrPresenceNotFound", err)
	}
}

func TestRedisIntegrationContractEndpointRefreshAndExpiry(t *testing.T) {
	client, keys := newRedisIntegrationClient(t)
	directory, err := NewRedisGateDirectory(client, keys)
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewEndpointRegistrar(client, keys, GateEndpoint{GateID: "gate-a", Address: "127.0.0.1:9101"}, EndpointRegistrarConfig{TTL: 250 * time.Millisecond, Refresh: 40 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Stop(context.Background()) })
	second, err := NewEndpointRegistrar(client, keys, GateEndpoint{GateID: "gate-b", Address: "127.0.0.1:9102"}, EndpointRegistrarConfig{TTL: 250 * time.Millisecond, Refresh: 40 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Stop(context.Background()) })
	startupContext, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if err := first.Start(startupContext); err != nil {
		cancel()
		t.Fatalf("start first registrar: %v", err)
	}
	cancel()
	// This is longer than the 250ms TTL. Resolve succeeding proves the
	// registrar renewed the lease after the startup context expired.
	time.Sleep(320 * time.Millisecond)
	if _, err := directory.Resolve(context.Background(), "gate-a"); err != nil {
		t.Fatalf("endpoint did not survive startup context expiry: %v", err)
	}
	if err := second.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	if endpoint, err := directory.Resolve(context.Background(), "gate-b"); err != nil || endpoint.Address != "127.0.0.1:9102" {
		t.Fatalf("second endpoint = %#v error:%v", endpoint, err)
	}
	if endpoint, err := directory.Resolve(context.Background(), "gate-a"); err != nil || endpoint.Address != "127.0.0.1:9101" {
		t.Fatalf("first endpoint after second starts = %#v error:%v", endpoint, err)
	}
	if err := first.Stop(context.Background()); err != nil {
		t.Fatalf("stop first registrar: %v", err)
	}
	if _, err := directory.Resolve(context.Background(), "gate-a"); !errors.Is(err, ErrGateEndpointNotFound) {
		t.Fatalf("first endpoint after stop = %v, want ErrGateEndpointNotFound", err)
	}
	if err := second.Stop(context.Background()); err != nil {
		t.Fatalf("stop second registrar: %v", err)
	}
	if _, err := directory.Resolve(context.Background(), "gate-b"); !errors.Is(err, ErrGateEndpointNotFound) {
		t.Fatalf("second endpoint after stop = %v, want ErrGateEndpointNotFound", err)
	}

	if err := client.Set(context.Background(), keys.gateEndpoint("gate-expiring"), "127.0.0.1:9103", 70*time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)
	if _, err := directory.Resolve(context.Background(), "gate-expiring"); !errors.Is(err, ErrGateEndpointNotFound) {
		t.Fatalf("expired endpoint = %v, want ErrGateEndpointNotFound", err)
	}
}

func TestRedisIntegrationContractSubscriberSurvivesStartupContextExpiry(t *testing.T) {
	client, keys := newRedisIntegrationClient(t)
	receiver := &recordingReceiver{}
	subscriber, err := NewRedisBroadcastSubscriber(client, keys, receiver, RedisSubscriberConfig{MaxPayloadBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	startupContext, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if err := subscriber.Start(startupContext); err != nil {
		cancel()
		t.Fatalf("start subscriber: %v", err)
	}
	t.Cleanup(func() { _ = subscriber.Stop(context.Background()) })
	cancel()
	<-startupContext.Done()
	subscriber.mu.Lock()
	initialSubscription := subscriber.sub
	subscriber.mu.Unlock()
	sender, err := NewRedisBroadcastSender(client, keys, RedisBroadcastConfig{MaxPayloadBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.Broadcast(gatelink.WithGateRequestContext(context.Background(), gatelink.GateRequestContext{TraceID: "trace-real-redis"}), BroadcastMessage{RoomID: "room-real", Message: Message{CommandID: 12, Payload: []byte{0, 1, 255}}}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitForRedisIntegration(t, time.Second, func() bool {
		receiver.mu.Lock()
		got := receiver.broadcast.RoomID == "room-real"
		receiver.mu.Unlock()
		return got
	})
	receiver.mu.Lock()
	delivered := receiver.broadcast.RoomID == "room-real"
	traceID := receiver.traceID
	receiver.mu.Unlock()
	if !delivered {
		t.Fatal("subscriber did not receive Redis Pub/Sub message")
	}
	if traceID != "trace-real-redis" {
		t.Fatalf("subscriber trace = %q, want trace-real-redis", traceID)
	}
	if killed, err := client.Do(context.Background(), "CLIENT", "KILL", "TYPE", "pubsub").Int64(); err != nil || killed < 1 {
		t.Fatalf("kill Redis Pub/Sub connection = count:%d error:%v, want at least one", killed, err)
	}
	waitForRedisIntegration(t, time.Second, func() bool {
		subscriber.mu.Lock()
		defer subscriber.mu.Unlock()
		return subscriber.sub != nil && subscriber.sub != initialSubscription
	})
	if _, err := sender.Broadcast(gatelink.WithGateRequestContext(context.Background(), gatelink.GateRequestContext{TraceID: "trace-real-reconnect"}), BroadcastMessage{RoomID: "room-real-reconnect", Message: Message{CommandID: 13, Payload: []byte("reconnected")}}); err != nil {
		t.Fatalf("publish after reconnect: %v", err)
	}
	waitForRedisIntegration(t, time.Second, func() bool {
		receiver.mu.Lock()
		defer receiver.mu.Unlock()
		return receiver.broadcast.RoomID == "room-real-reconnect"
	})
	receiver.mu.Lock()
	traceID = receiver.traceID
	receiver.mu.Unlock()
	if traceID != "trace-real-reconnect" {
		t.Fatalf("reconnected subscriber trace = %q, want trace-real-reconnect", traceID)
	}
	if err := subscriber.Stop(context.Background()); err != nil {
		t.Fatalf("stop subscriber: %v", err)
	}
}

func waitForRedisIntegration(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("Redis integration condition was not reached")
}

func newRedisIntegrationClient(t *testing.T) (*redis.Client, Keyspace) {
	t.Helper()
	address := strings.TrimSpace(os.Getenv("SERVER_SEND_REDIS_ADDR"))
	if address == "" {
		address = strings.TrimSpace(os.Getenv("REDIS_ADDR"))
	}
	if address == "" {
		t.Skip("SERVER_SEND_REDIS_ADDR or REDIS_ADDR is not configured")
	}
	client := redis.NewClient(&redis.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("connect to integration Redis: %v", err)
	}
	prefix := fmt.Sprintf("contract-%d", time.Now().UnixNano())
	keys, err := NewKeyspace(redisinfra.KeyPrefix(prefix))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var cursor uint64
		for {
			matched, next, err := client.Scan(cleanupContext, cursor, prefix+":*", 0).Result()
			if err != nil {
				t.Logf("cleanup Redis integration keys: %v", err)
				return
			}
			if len(matched) > 0 {
				if err := client.Del(cleanupContext, matched...).Err(); err != nil {
					t.Logf("delete Redis integration keys: %v", err)
					return
				}
			}
			cursor = next
			if cursor == 0 {
				return
			}
		}
	})
	return client, keys
}
