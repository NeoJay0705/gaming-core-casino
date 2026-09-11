package gateproduct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

func TestGateApplicationStopReleasesSessionPresenceLease(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	configPath := filepath.Join(t.TempDir(), "gate.yaml")
	contents := observabilityTestYAML + "redis:\n  addr: " + miniRedis.Addr() + "\n  key_prefix: core-casino\nwebsocket:\n  client_addr: 127.0.0.1:0\ngrpc:\n  server:\n    listen_addr: 127.0.0.1:0\n  clients:\n    game:\n      target: dns:///gameproduct:9090\n  endpoint_registration:\n    ttl: 200ms\nsession_ownership:\n  lease_ttl: 200ms\n"
	if err := os.WriteFile(configPath, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	app, server, sessions, registered := newStartedSessionRegistryTestApp(t, configPath)
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = app.frameworkApp.Stop(context.Background())
		}
	})
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	registerSession(t, conn, "alice", registered)

	redisClient := redis.NewClient(&redis.Options{Addr: miniRedis.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	keys, err := serversend.NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	presence, err := serversend.NewPresenceRegistry(redisClient, keys, serversend.PresenceConfig{LeaseTTL: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := presence.Resolve(context.Background(), "alice"); err != nil {
		t.Fatalf("presence after registration: %v", err)
	}

	if err := app.frameworkApp.Stop(context.Background()); err != nil {
		t.Fatalf("stop Gate application: %v", err)
	}
	stopped = true
	assertSessionRemoved(t, sessions, "alice")
	if _, err := presence.Resolve(context.Background(), "alice"); !errors.Is(err, serversend.ErrPresenceNotFound) {
		t.Fatalf("presence after application stop = %v, want ErrPresenceNotFound", err)
	}
	time.Sleep(80 * time.Millisecond)
	if _, err := presence.Resolve(context.Background(), "alice"); !errors.Is(err, serversend.ErrPresenceNotFound) {
		t.Fatalf("presence after post-stop renewal interval = %v, want ErrPresenceNotFound", err)
	}
}
