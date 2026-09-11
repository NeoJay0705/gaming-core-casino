package gateproduct

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/grpcserver"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestGateGRPCEndpointRegistrationUsesProductServerLifecycle(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	path := filepath.Join(t.TempDir(), "gate.yaml")
	contents := fmt.Sprintf("observability:\n  listen_addr: 127.0.0.1:0\nredis:\n  addr: %s\n  key_prefix: core-casino\ngrpc:\n  server:\n    listen_addr: 127.0.0.1:0\n  clients:\n    game:\n      target: dns:///gameproduct:9090\n  endpoint_registration:\n    ttl: 30s\nsession_ownership:\n  lease_ttl: 30s\n", miniRedis.Addr())
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	var server *grpcserver.Server
	var registration *gateGRPCEndpointRegistration
	app, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GATE_ENDPOINT_LIFECYCLE_TEST__"}, func(r framework.Registry) error {
		return r.AddHook(func(value *grpcserver.Server, endpoint *gateGRPCEndpointRegistration) framework.Hook {
			server, registration = value, endpoint
			return framework.Hook{Name: "capture-gate-endpoint-lifecycle", Phase: framework.PhaseService, OnStart: func(context.Context) error { return nil }}
		})
	})
	if err != nil {
		t.Fatalf("new Gate app: %v", err)
	}
	if err := app.frameworkApp.Start(context.Background()); err != nil {
		t.Fatalf("start Gate app: %v", err)
	}
	if server == nil || registration == nil || server.Addr() == "" {
		t.Fatal("Gate gRPC server or endpoint registration was not started")
	}

	redisClient := redis.NewClient(&redis.Options{Addr: miniRedis.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	keys, err := serversend.NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	directory, err := serversend.NewRedisGateDirectory(redisClient, keys)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := directory.Resolve(context.Background(), registration.gateID)
	if err != nil {
		t.Fatalf("resolve registered endpoint: %v", err)
	}
	listenerAddress, err := net.ResolveTCPAddr("tcp", server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	wantAddress, err := serversend.ResolveAdvertiseEndpoint(listenerAddress)
	if err != nil {
		t.Fatal(err)
	}
	if endpoint.Address != wantAddress {
		t.Fatalf("registered endpoint = %q, want product server address %q", endpoint.Address, wantAddress)
	}

	if err := app.frameworkApp.Stop(context.Background()); err != nil {
		t.Fatalf("stop Gate app: %v", err)
	}
	if _, err := directory.Resolve(context.Background(), registration.gateID); !errors.Is(err, serversend.ErrGateEndpointNotFound) {
		t.Fatalf("endpoint after stop = %v, want ErrGateEndpointNotFound", err)
	}
	if server.Addr() != "" {
		t.Fatalf("product server address after stop = %q, want empty", server.Addr())
	}
	if err := app.frameworkApp.Stop(context.Background()); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}

func TestGateServerSendBroadcastGRPCConfigDoesNotConstructRedisRuntime(t *testing.T) {
	snapshot := serverSendBroadcastSnapshot{primary: "grpc"}
	cfg, enabled, err := gateServerSendBroadcast(snapshot)
	if err != nil {
		t.Fatalf("validate gRPC broadcast config: %v", err)
	}
	if !enabled || cfg.Primary != "grpc" {
		t.Fatalf("broadcast config = %#v enabled=%t", cfg, enabled)
	}
	if _, err := newGateServerSendBroadcastRuntime(gateServerSendBroadcastConfig{Primary: "grpc"}, nil, nil, serversend.Keyspace{}); err == nil {
		t.Fatal("gRPC broadcast unexpectedly accepted Redis subscriber runtime")
	}
}

type serverSendBroadcastSnapshot struct{ primary string }

func (s serverSendBroadcastSnapshot) Bind(path string, target any, _ ...config.BindOption) error {
	if path == "server_send" {
		target.(*gateServerSendConfig).Broadcast.Primary = s.primary
	}
	return nil
}
func (serverSendBroadcastSnapshot) Has(path string) bool {
	return path == "server_send" || path == "server_send.broadcast"
}
func (serverSendBroadcastSnapshot) HasSource(string) bool { return false }
func (serverSendBroadcastSnapshot) BindSource(string, string, any, ...config.BindOption) error {
	return nil
}
