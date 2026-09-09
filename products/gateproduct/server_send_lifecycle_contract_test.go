package gateproduct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestGateServerSendContractManagedRuntimeStartsThroughWebSocketDependency(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	path := filepath.Join(t.TempDir(), "gate.yaml")
	contents := "redis:\n  addr: " + miniRedis.Addr() + "\n  key_prefix: core-casino\ngate_to_game:\n  target: dns:///gameproduct:9090\nserver_send:\n  presence:\n    lease_ttl: 30s\n  gate:\n    listen_addr: 127.0.0.1:0\n    endpoint_ttl: 30s\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	var webSocket *WebSocketServer
	app, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GATE_SERVER_SEND_LIFECYCLE_TEST__"}, func(r framework.Registry) error {
		return r.AddHook(func(server *WebSocketServer) framework.Hook {
			webSocket = server
			return framework.Hook{Name: "capture-server-send-websocket", Phase: framework.PhaseService, OnStart: func(context.Context) error { return nil }}
		})
	})
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	if err := app.frameworkApp.Start(context.Background()); err != nil {
		t.Fatalf("start app: %v", err)
	}
	if webSocket == nil || webSocket.serverSend == nil {
		t.Fatal("server-send runtime was not resolved through the WebSocket dependency")
	}
	client := redis.NewClient(&redis.Options{Addr: miniRedis.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	keys, err := serversend.NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	directory, err := serversend.NewRedisGateDirectory(client, keys)
	if err != nil {
		t.Fatal(err)
	}
	gateID := serversend.GateID(webSocket.serverSend.Route())
	if gateID == "" {
		t.Fatal("server-send runtime did not expose a Gate identity")
	}
	if _, err := directory.Resolve(context.Background(), gateID); err != nil {
		t.Fatalf("endpoint after app start: %v", err)
	}
	identitySession := &registrySession{id: "identity-contract"}
	if err := webSocket.registry.Register(identitySession, "alice"); err != nil {
		t.Fatalf("register identity contract session: %v", err)
	}
	t.Cleanup(func() { webSocket.registry.Remove(identitySession) })
	presenceResolver, err := serversend.NewRedisPresenceResolver(client, keys)
	if err != nil {
		t.Fatal(err)
	}
	presence, err := presenceResolver.Resolve(context.Background(), "alice")
	if err != nil {
		t.Fatalf("resolve identity contract presence: %v", err)
	}
	if presence.GateID != gateID {
		t.Fatalf("presence Gate id = %q, endpoint/request Gate id = %q", presence.GateID, gateID)
	}
	if err := app.frameworkApp.Stop(context.Background()); err != nil {
		t.Fatalf("stop app: %v", err)
	}
	if _, err := directory.Resolve(context.Background(), gateID); !errors.Is(err, serversend.ErrGateEndpointNotFound) {
		t.Fatalf("endpoint after app stop = %v, want ErrGateEndpointNotFound", err)
	}
	// A second Stop must remain idempotent after the managed runtime was
	// resolved by WebSocketServer rather than by a no-op root hook.
	if err := app.frameworkApp.Stop(context.Background()); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}

func TestGateServerSendContractDoesNotSubscribeToRedisForGRPCPrimary(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	path := filepath.Join(t.TempDir(), "gate.yaml")
	contents := "redis:\n  addr: " + miniRedis.Addr() + "\n  key_prefix: core-casino\ngate_to_game:\n  target: dns:///gameproduct:9090\nserver_send:\n  broadcast:\n    primary: grpc\n  presence:\n    lease_ttl: 30s\n  gate:\n    listen_addr: 127.0.0.1:0\n    endpoint_ttl: 30s\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	var runtime *gateServerSendRuntime
	app, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GATE_SERVER_SEND_GRPC_PRIMARY_TEST__"}, func(r framework.Registry) error {
		return r.AddHook(func(value *gateServerSendRuntime) framework.Hook {
			runtime = value
			return framework.Hook{Name: "capture-grpc-primary-runtime", Phase: framework.PhaseService, OnStart: func(context.Context) error { return nil }}
		})
	})
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	if err := app.frameworkApp.Start(context.Background()); err != nil {
		t.Fatalf("start app: %v", err)
	}
	if runtime == nil {
		t.Fatal("gRPC-primary runtime was not resolved")
	}
	runtime.mu.RLock()
	subscriber := runtime.subscriber
	runtime.mu.RUnlock()
	if subscriber != nil {
		t.Fatal("gRPC-primary Gate created a Redis room subscriber")
	}
	if err := app.frameworkApp.Stop(context.Background()); err != nil {
		t.Fatalf("stop app: %v", err)
	}
}
