package gmsproduct

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
)

func TestModuleWithSnapshotBuildsRunnableNoopApp(t *testing.T) {
	app, err := framework.New(moduleWithSnapshot(contractSnapshot{}))
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
	app, err := NewApp(context.Background(), AppOptions{Config: testInputs(t), EnvPrefix: "CORE_CASINO_GMS_TEST__"}, testProductModule(&called, &started, &stopped))
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	if !called {
		t.Fatal("product module was not used")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := app.Run(ctx); err != nil {
		t.Fatalf("run app: %v", err)
	}
	if !started || !stopped {
		t.Fatalf("product module lifecycle not used: started=%t stopped=%t", started, stopped)
	}
}

func TestNewAppProvidesSnapshotsToProductModule(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.yaml")
	if err := os.WriteFile(path, []byte("product:\n  code: core-casino\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var sourceCode, mergedCode string
	app, err := NewApp(context.Background(), AppOptions{
		Config:    config.ConfigInputs{MergedPaths: []string{path}},
		EnvPrefix: "CORE_CASINO_GMS_SNAPSHOT_TEST__",
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
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := app.Run(ctx); err != nil {
		t.Fatalf("run app: %v", err)
	}
}

func testInputs(t *testing.T) config.ConfigInputs {
	t.Helper()
	path := filepath.Join(t.TempDir(), "app.yaml")
	if err := os.WriteFile(path, []byte("product: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return config.ConfigInputs{MergedPaths: []string{path}}
}

type contractSnapshot struct{}

func (contractSnapshot) Bind(string, any, ...config.BindOption) error               { return nil }
func (contractSnapshot) Has(string) bool                                            { return false }
func (contractSnapshot) HasSource(string) bool                                      { return false }
func (contractSnapshot) BindSource(string, string, any, ...config.BindOption) error { return nil }

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
