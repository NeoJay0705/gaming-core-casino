package infra

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/database"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/localmq"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/redis"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/rocketmq"
	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
)

func TestModuleContractRegistersAllInfrastructureAsManagedResources(t *testing.T) {
	registry := &recordingRegistry{}
	if err := Module(registry); err != nil {
		t.Fatalf("register infra module: %v", err)
	}
	if got, want := registry.names, []string{"redis", "database", "rocketmq", "localmq"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("managed names = %v, want %v", got, want)
	}
	if got, want := registry.phases, []framework.Phase{framework.PhaseInfrastructure, framework.PhaseInfrastructure, framework.PhaseInfrastructure, framework.PhaseInfrastructure}; !reflect.DeepEqual(got, want) {
		t.Fatalf("managed phases = %v, want %v", got, want)
	}
	for i, want := range []reflect.Type{
		reflect.TypeOf((*redis.Client)(nil)),
		reflect.TypeOf((*database.Client)(nil)),
		reflect.TypeOf((*rocketmq.Client)(nil)),
		reflect.TypeOf((*localmq.Client)(nil)),
	} {
		got := reflect.TypeOf(registry.constructors[i]).Out(0)
		if got != want {
			t.Fatalf("managed constructor %d result = %v, want %v", i, got, want)
		}
	}
}

func TestManagedConstructorsRegisterPoolMetricsOnlyWhenRequested(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool-metrics.yaml")
	contents := []byte("redis:\n  addr: redis:6379\n  key_prefix: core-casino\ndatabase:\n  dsn: app:password@tcp(tidb:4000)/gaming\n")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.LoadInputs(context.Background(), config.ConfigInputs{MergedPaths: []string{path}}, "CORE_CASINO_INFRA_METRICS_TEST__")
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if _, err := newRedis(redisInputs{Snapshot: snapshot}); err != nil {
		t.Fatalf("newRedis() without registerer: %v", err)
	}
	registry := prometheus.NewRegistry()
	if _, err := newRedis(redisInputs{Snapshot: snapshot, Registerer: registry}); err != nil {
		t.Fatalf("newRedis() with registerer: %v", err)
	}
	if _, err := newRedis(redisInputs{Snapshot: snapshot, Registerer: registry}); err == nil {
		t.Fatal("duplicate Redis pool collector registration error = nil")
	}
	databaseRegistry := prometheus.NewRegistry()
	if _, err := newDatabase(databaseInputs{Snapshot: snapshot, Registerer: databaseRegistry}); err != nil {
		t.Fatalf("newDatabase() with registerer: %v", err)
	}
	if _, err := newDatabase(databaseInputs{Snapshot: snapshot, Registerer: databaseRegistry}); err == nil {
		t.Fatal("duplicate database pool collector registration error = nil")
	}
}

func TestManagedPoolMetricsAreLazyUntilAResourceIsConsumed(t *testing.T) {
	t.Run("unused resources are not resolved", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "invalid-infra.yaml")
		if err := os.WriteFile(path, []byte("redis:\n  key_prefix: core-casino\ndatabase: {}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		snapshot, err := config.LoadInputs(context.Background(), config.ConfigInputs{MergedPaths: []string{path}}, "CORE_CASINO_INFRA_LAZY_UNUSED__")
		if err != nil {
			t.Fatalf("load snapshot: %v", err)
		}
		registerer := prometheus.NewRegistry()
		app, err := framework.New(Module, func(r framework.Registry) error {
			if err := r.Provide(func() config.SourceSnapshot { return snapshot }); err != nil {
				return err
			}
			if err := r.Provide(func() prometheus.Registerer { return registerer }); err != nil {
				return err
			}
			return r.AddHook(func() framework.Hook {
				return framework.Hook{Name: "readiness", Phase: framework.PhaseReadiness, OnStart: func(context.Context) error { return nil }}
			})
		})
		if err != nil {
			t.Fatalf("New() resolved an unused infrastructure resource: %v", err)
		}
		if err := app.Start(context.Background()); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		if err := app.Stop(context.Background()); err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
		assertNoPoolMetricFamilies(t, registerer)
	})

	t.Run("redis dependency resolves and registers metrics", func(t *testing.T) {
		server := miniredis.RunT(t)
		path := filepath.Join(t.TempDir(), "redis-infra.yaml")
		contents := fmt.Sprintf("redis:\n  addr: %q\n  key_prefix: core-casino\n", server.Addr())
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		snapshot, err := config.LoadInputs(context.Background(), config.ConfigInputs{MergedPaths: []string{path}}, "CORE_CASINO_INFRA_LAZY_USED__")
		if err != nil {
			t.Fatalf("load snapshot: %v", err)
		}
		registerer := prometheus.NewRegistry()
		app, err := framework.New(Module, func(r framework.Registry) error {
			if err := r.Provide(func() config.SourceSnapshot { return snapshot }); err != nil {
				return err
			}
			if err := r.Provide(func() prometheus.Registerer { return registerer }); err != nil {
				return err
			}
			if err := r.AddHook(func(*redis.Client) framework.Hook {
				return framework.Hook{Name: "redis-consumer", Phase: framework.PhaseService, OnStart: func(context.Context) error { return nil }}
			}); err != nil {
				return err
			}
			return r.AddHook(func() framework.Hook {
				return framework.Hook{Name: "readiness", Phase: framework.PhaseReadiness, OnStart: func(context.Context) error { return nil }}
			})
		})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if err := app.Start(context.Background()); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		assertPoolMetricNames(t, registerer, []string{
			"gaming_core_redis_pool_total_connections",
			"gaming_core_redis_pool_idle_connections",
			"gaming_core_redis_pool_pending_requests",
			"gaming_core_redis_pool_wait_total",
			"gaming_core_redis_pool_wait_seconds_total",
			"gaming_core_redis_pool_timeouts_total",
		})
		if err := app.Stop(context.Background()); err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
		assertNoPoolMetricFamilies(t, registerer)
	})
}

func assertNoPoolMetricFamilies(t *testing.T, registerer prometheus.Gatherer) {
	t.Helper()
	families, err := registerer.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range families {
		if strings.HasPrefix(family.GetName(), "gaming_core_redis_pool_") || strings.HasPrefix(family.GetName(), "gaming_core_database_pool_") {
			t.Fatalf("unexpected infrastructure pool metric family %q", family.GetName())
		}
	}
}

func assertPoolMetricNames(t *testing.T, registerer prometheus.Gatherer, names []string) {
	t.Helper()
	families, err := registerer.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	seen := make(map[string]bool, len(families))
	for _, family := range families {
		if len(family.Metric) != 0 {
			seen[family.GetName()] = true
		}
	}
	for _, name := range names {
		if !seen[name] {
			t.Errorf("metric family %q was not gathered", name)
		}
	}
}

func TestExampleConfigBuildsAllInfrastructureResources(t *testing.T) {
	path := filepath.Join("..", "..", "configs", "examples", "infra.yaml")
	snapshot, err := config.LoadInputs(context.Background(), config.ConfigInputs{MergedPaths: []string{path}}, "CORE_CASINO_INFRA_EXAMPLE_TEST__")
	if err != nil {
		t.Fatalf("load example config: %v", err)
	}
	if _, err := redis.New(snapshot); err != nil {
		t.Fatalf("build Redis resource: %v", err)
	}
	if _, err := database.New(snapshot); err != nil {
		t.Fatalf("build database resource: %v", err)
	}
	if _, err := rocketmq.New(snapshot); err != nil {
		t.Fatalf("build RocketMQ resource: %v", err)
	}
	if _, err := localmq.New(snapshot); err != nil {
		t.Fatalf("build local MQ resource: %v", err)
	}
}

type recordingRegistry struct {
	names        []string
	phases       []framework.Phase
	constructors []any
}

func (r *recordingRegistry) Provide(any) error   { return nil }
func (r *recordingRegistry) Configure(any) error { return nil }
func (r *recordingRegistry) ProvideManaged(name string, phase framework.Phase, constructor any) error {
	r.names = append(r.names, name)
	r.phases = append(r.phases, phase)
	r.constructors = append(r.constructors, constructor)
	return nil
}
func (r *recordingRegistry) AddHook(any) error { return nil }
