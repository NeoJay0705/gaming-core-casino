package apiproduct

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/observability"
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
	app, err := NewApp(context.Background(), AppOptions{Config: testInputs(t), EnvPrefix: "CORE_CASINO_API_TEST__"}, testProductModule(&called, &started, &stopped))
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
	path := filepath.Join(t.TempDir(), "app.yaml")
	if err := os.WriteFile(path, []byte("observability:\n  listen_addr: 127.0.0.1:0\nproduct:\n  code: core-casino\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var sourceCode, mergedCode string
	app, err := NewApp(context.Background(), AppOptions{
		Config:    config.ConfigInputs{MergedPaths: []string{path}},
		EnvPrefix: "CORE_CASINO_API_SNAPSHOT_TEST__",
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

func TestNewAppRejectsNonYAMLInputs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.json")
	if err := os.WriteFile(path, []byte(`{"product":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_API_FORMAT_TEST__"})
	if err == nil || !strings.Contains(err.Error(), "YAML/YML only") {
		t.Fatalf("error = %v, want YAML/YML rejection", err)
	}
}

func TestNewAppRequiresFileIntegrityWhenRequested(t *testing.T) {
	_, err := NewApp(context.Background(), AppOptions{
		Config:                testInputs(t),
		EnvPrefix:             "CORE_CASINO_API_INTEGRITY_REQUIRED_TEST__",
		RequireInputIntegrity: true,
	})
	if !errors.Is(err, config.ErrFileIntegrityRequired) {
		t.Fatalf("error = %v, want ErrFileIntegrityRequired", err)
	}
}

func TestNewAppValidatesFileIntegrityManifest(t *testing.T) {
	sourceBytes := []byte("profile: low\n")
	digest := md5.Sum(sourceBytes)
	for _, test := range []struct {
		name    string
		digest  string
		wantErr error
	}{
		{name: "mismatch", digest: "00000000000000000000000000000000", wantErr: config.ErrFileIntegrityMismatch},
		{name: "valid", digest: hex.EncodeToString(digest[:])},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			mergedPath := filepath.Join(dir, "app.yaml")
			sourcePath := filepath.Join(dir, "planner.yaml")
			merged := []byte("observability:\n  listen_addr: 127.0.0.1:0\nfile_integrity:\n  version: 1\n  sources:\n    - source: planner\n      md5: \"" + test.digest + "\"\n")
			if err := os.WriteFile(mergedPath, merged, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(sourcePath, sourceBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := NewApp(context.Background(), AppOptions{
				Config:                config.ConfigInputs{MergedPaths: []string{mergedPath}, SourcePaths: []config.NamedConfigPath{{Name: "planner", Path: sourcePath}}},
				EnvPrefix:             "CORE_CASINO_API_INTEGRITY_MANIFEST_TEST__",
				RequireInputIntegrity: true,
			})
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want errors.Is(..., %v)", err, test.wantErr)
			}
		})
	}
}

func testInputs(t *testing.T) config.ConfigInputs {
	t.Helper()
	path := filepath.Join(t.TempDir(), "app.yaml")
	if err := os.WriteFile(path, []byte("observability:\n  listen_addr: 127.0.0.1:0\nproduct: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return config.ConfigInputs{MergedPaths: []string{path}}
}

type contractSnapshot struct{}

func (contractSnapshot) Bind(path string, target any, _ ...config.BindOption) error {
	if path == "observability" {
		target.(*observability.Config).ListenAddr = "127.0.0.1:0"
	}
	return nil
}
func (contractSnapshot) Has(path string) bool {
	return path == "observability"
}
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
