package gateproduct

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/grpcserver"
	infraRedis "github.com/NeoJay0705/gaming-core-casino/pkg/infra/redis"
	"github.com/NeoJay0705/gaming-core-casino/pkg/observability"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/alicebob/miniredis/v2"
)

func TestModuleWithSnapshotBuildsRunnableNoopApp(t *testing.T) {
	app, err := framework.New(moduleWithSnapshot(newContractSnapshot(t)))
	if err != nil {
		t.Fatalf("build app: %v", err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("start app: %v", err)
	}
	if err := app.Stop(context.Background()); err != nil {
		t.Fatalf("stop app: %v", err)
	}
}

func TestNewAppUsesProductModule(t *testing.T) {
	called, started, stopped := false, false, false
	app, err := NewApp(context.Background(), AppOptions{Config: testInputs(t), EnvPrefix: "CORE_CASINO_GATE_TEST__"}, testProductModule(&called, &started, &stopped))
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	if !called {
		t.Fatal("product module was not used")
	}
	if err := app.frameworkApp.Start(context.Background()); err != nil {
		t.Fatalf("start app: %v", err)
	}
	if err := app.frameworkApp.Stop(context.Background()); err != nil {
		t.Fatalf("stop app: %v", err)
	}
	if !started || !stopped {
		t.Fatalf("product module lifecycle not used: started=%t stopped=%t", started, stopped)
	}
}

func TestNewAppProvidesSnapshotsToProductModule(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	path := filepath.Join(t.TempDir(), "app.yaml")
	contents := "observability:\n  listen_addr: 127.0.0.1:0\nproduct:\n  code: core-casino\ngrpc:\n  server:\n    listen_addr: 127.0.0.1:0\n  clients:\n    game:\n      target: dns:///gameproduct:9090\n  endpoint_registration:\n    ttl: 30s\nsession_ownership:\n  lease_ttl: 30s\nredis:\n  addr: " + miniRedis.Addr() + "\n  key_prefix: core-casino\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	var sourceCode, mergedCode string
	app, err := NewApp(context.Background(), AppOptions{
		Config:    config.ConfigInputs{MergedPaths: []string{path}},
		EnvPrefix: "CORE_CASINO_GATE_SNAPSHOT_TEST__",
	}, func(r framework.Registry) error {
		return r.AddHook(func(source config.SourceSnapshot, merged config.Snapshot) (framework.Hook, error) {
			var sourceValue, mergedValue struct {
				Code string `config:"code"`
			}
			if err := source.Bind("product", &sourceValue); err != nil {
				return framework.Hook{}, err
			}
			if err := merged.Bind("product", &mergedValue); err != nil {
				return framework.Hook{}, err
			}
			sourceCode, mergedCode = sourceValue.Code, mergedValue.Code
			return framework.Hook{
				Name:    "snapshot-consumer",
				Phase:   framework.PhaseService,
				OnStart: func(context.Context) error { return nil },
			}, nil
		})
	})
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	if sourceCode != "core-casino" || mergedCode != "core-casino" {
		t.Fatalf("snapshot values = source:%q merged:%q", sourceCode, mergedCode)
	}
	if err := app.frameworkApp.Start(context.Background()); err != nil {
		t.Fatalf("start app: %v", err)
	}
	if err := app.frameworkApp.Stop(context.Background()); err != nil {
		t.Fatalf("stop app: %v", err)
	}
}

func testInputs(t *testing.T) config.ConfigInputs {
	t.Helper()
	miniRedis := miniredis.RunT(t)
	path := filepath.Join(t.TempDir(), "app.yaml")
	contents := "observability:\n  listen_addr: 127.0.0.1:0\nproduct: {}\ngrpc:\n  server:\n    listen_addr: 127.0.0.1:0\n  clients:\n    game:\n      target: dns:///gameproduct:9090\n  endpoint_registration:\n    ttl: 30s\nsession_ownership:\n  lease_ttl: 30s\nredis:\n  addr: " + miniRedis.Addr() + "\n  key_prefix: core-casino\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return config.ConfigInputs{MergedPaths: []string{path}}
}

type contractSnapshot struct{ redisAddr string }

const observabilityTestYAML = "observability:\n  listen_addr: 127.0.0.1:0\n"

func (s contractSnapshot) Bind(path string, target any, _ ...config.BindOption) error {
	if path == "observability" {
		target.(*observability.Config).ListenAddr = "127.0.0.1:0"
	}
	if path == "grpc.server" {
		target.(*grpcserver.Config).ListenAddr = "127.0.0.1:0"
	}
	if path == "grpc.clients.game" {
		target.(*gateGRPCClientConfig).Target = "dns:///gameproduct:9090"
	}
	if path == "grpc.endpoint_registration" {
		target.(*gateEndpointRegistrationConfig).TTL = 30 * time.Second
	}
	if path == "session_ownership" {
		target.(*serversend.PresenceConfig).LeaseTTL = 30 * time.Second
	}
	if path == "redis" {
		value := target.(*infraRedis.Config)
		value.Addr = s.redisAddr
		value.KeyPrefix = "core-casino"
	}
	return nil
}
func (contractSnapshot) Has(path string) bool {
	return path == "grpc.server" || path == "grpc.clients.game" || path == "grpc.endpoint_registration" || path == "session_ownership" || path == "redis" || path == "observability"
}
func (contractSnapshot) HasSource(string) bool                                      { return false }
func (contractSnapshot) BindSource(string, string, any, ...config.BindOption) error { return nil }

func newContractSnapshot(t *testing.T) contractSnapshot {
	t.Helper()
	return contractSnapshot{redisAddr: miniredis.RunT(t).Addr()}
}

type contractNoop struct{}

func testProductModule(called, started, stopped *bool) framework.Module {
	return func(r framework.Registry) error {
		*called = true
		if err := r.Provide(func() contractNoop { return contractNoop{} }); err != nil {
			return err
		}
		return r.AddHook(func(contractNoop) framework.Hook {
			return framework.Hook{Name: "product-noop", Phase: framework.PhaseService, OnStart: func(context.Context) error { *started = true; return nil }, OnStop: func(context.Context) error { *stopped = true; return nil }}
		})
	}
}
