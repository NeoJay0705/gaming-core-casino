package gateproduct

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
)

func TestGateProductContractProvidesManagedGameRequestClient(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gate.yaml")
	if err := os.WriteFile(path, []byte(observabilityTestYAML+"gate_to_game:\n  target: dns:///gameproduct:9090\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var client *gatelink.Client
	app, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GATE_GRPC_TEST__"}, func(r framework.Registry) error {
		return r.AddHook(func(value *gatelink.Client) framework.Hook {
			client = value
			return framework.Hook{Name: "capture-gate-game-grpc-client", Phase: framework.PhaseService, OnStart: func(context.Context) error { return nil }}
		})
	})
	if err != nil {
		t.Fatalf("new gate app: %v", err)
	}
	if client == nil {
		t.Fatal("Gate product did not provide the Game request client")
	}
	if err := app.frameworkApp.Start(context.Background()); err != nil {
		t.Fatalf("start gate app: %v", err)
	}
	if err := app.frameworkApp.Stop(context.Background()); err != nil {
		t.Fatalf("stop gate app: %v", err)
	}
}

func TestGateProductRequiresGateToGameConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gate.yaml")
	if err := os.WriteFile(path, []byte(observabilityTestYAML+"product: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GATE_GRPC_REQUIRED_CONFIG_TEST__"})
	if err == nil || !strings.Contains(err.Error(), "gate_to_game is required") {
		t.Fatalf("new gate app error = %v, want required gate_to_game", err)
	}
}

func TestGateProductContractProvidesPresenceRegistryWhenServerSendIsConfigured(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gate.yaml")
	contents := observabilityTestYAML + "redis:\n  addr: redis:6379\n  key_prefix: core-casino\ngate_to_game:\n  target: dns:///gameproduct:9090\nserver_send:\n  presence:\n    lease_ttl: 30s\n  gate:\n    listen_addr: 127.0.0.1:0\n    endpoint_ttl: 30s\n    endpoint_refresh: 10s\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	var presence *serversend.GatePresenceRegistry
	_, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GATE_SERVER_SEND_TEST__"}, func(r framework.Registry) error {
		return r.AddHook(func(value *serversend.GatePresenceRegistry) framework.Hook {
			presence = value
			return framework.Hook{Name: "capture-gate-presence-registry", Phase: framework.PhaseService, OnStart: func(context.Context) error { return nil }}
		})
	})
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	if presence == nil {
		t.Fatal("Gate product did not provide GatePresenceRegistry")
	}
}
