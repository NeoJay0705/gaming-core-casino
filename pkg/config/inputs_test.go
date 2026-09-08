package config_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
)

func TestLoadInputsMergedAndNamedDocuments(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "base.json")
	override := filepath.Join(dir, "override.yaml")
	source := filepath.Join(dir, "source.json")
	for path, content := range map[string]string{
		base:     `{"server":{"name":"base"},"array":[1]}`,
		override: "server:\n  name: override\narray:\n  - 2\n",
		source:   `{"value":"source"}`,
	} {
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("INPUTS__SERVER__NAME", "environment")
	snapshot, err := config.LoadInputs(context.Background(), config.ConfigInputs{
		MergedPaths: []string{base, override},
		SourcePaths: []config.NamedConfigPath{{Name: "test.source", Path: source}},
	}, "INPUTS__")
	if err != nil {
		t.Fatal(err)
	}
	var merged struct {
		Server struct {
			Name string `config:"name"`
		} `config:"server"`
		Array []int `config:"array"`
	}
	if err := snapshot.Bind("", &merged, config.Strict()); err != nil {
		t.Fatal(err)
	}
	if merged.Server.Name != "environment" || len(merged.Array) != 1 || merged.Array[0] != 2 {
		t.Fatalf("merged = %+v", merged)
	}
	var sourceValue struct {
		Value string `config:"value"`
	}
	if err := snapshot.BindSource("test.source", "", &sourceValue, config.Strict()); err != nil {
		t.Fatal(err)
	}
	if sourceValue.Value != "source" {
		t.Fatalf("source = %+v", sourceValue)
	}
	if !snapshot.HasSource("test.source") {
		t.Fatal("test.source should be present")
	}
	if snapshot.HasSource("missing.source") {
		t.Fatal("missing.source should be absent")
	}
}

func TestLoadInputsRejectsXMLAndDuplicateMergedPath(t *testing.T) {
	dir := t.TempDir()
	xml := filepath.Join(dir, "config.xml")
	if err := os.WriteFile(xml, []byte(`<config/>`), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := config.LoadInputs(context.Background(), config.ConfigInputs{MergedPaths: []string{xml}}, "INPUTS__")
	if err == nil || !strings.Contains(err.Error(), "XML is not supported") {
		t.Fatalf("XML error = %v", err)
	}
	jsonPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(jsonPath, []byte(`{"ok":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	_, err = config.LoadInputs(context.Background(), config.ConfigInputs{MergedPaths: []string{jsonPath, jsonPath}}, "INPUTS__")
	if err == nil || !strings.Contains(err.Error(), "duplicate merged path") {
		t.Fatalf("duplicate error = %v", err)
	}
}

func TestLoadInputsRejectsNormalizedDuplicateMergedPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"ok":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(dir, "nested", "..", "config.json")
	_, err := config.LoadInputs(context.Background(), config.ConfigInputs{
		MergedPaths: []string{path, alias},
	}, "INPUTS__")
	if err == nil || !strings.Contains(err.Error(), "duplicate merged path") {
		t.Fatalf("normalized duplicate error = %v", err)
	}
}

func TestLoadInputsRejectsInvalidNamedSourceName(t *testing.T) {
	dir := t.TempDir()
	merged := filepath.Join(dir, "merged.json")
	if err := os.WriteFile(merged, []byte(`{"service":"test"}`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", " planner.low", "planner/low", "planner=low"} {
		t.Run(name, func(t *testing.T) {
			_, err := config.LoadInputs(context.Background(), config.ConfigInputs{
				MergedPaths: []string{merged},
				SourcePaths: []config.NamedConfigPath{{Name: name, Path: "source.json"}},
			}, "INPUTS__")
			if err == nil || !strings.Contains(err.Error(), "invalid named source name") {
				t.Fatalf("invalid source name error = %v", err)
			}
		})
	}
}

func TestLoadInputsRejectsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := config.LoadInputs(ctx, config.ConfigInputs{MergedPaths: []string{"config.json"}}, "INPUTS__")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v, want context.Canceled", err)
	}
}

func TestLoadInputsSourceBindIsTransactional(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "source.json")
	if err := os.WriteFile(path, []byte(`{"known":"bad","unknown":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.LoadInputs(context.Background(), config.ConfigInputs{MergedPaths: []string{path}, SourcePaths: []config.NamedConfigPath{{Name: "source", Path: path}}}, "INPUTS__")
	if err != nil {
		t.Fatal(err)
	}
	target := struct {
		Known int `config:"known"`
	}{Known: 7}
	if err := snapshot.BindSource("source", "", &target, config.Strict()); err == nil || target.Known != 7 {
		t.Fatalf("target = %+v, err = %v", target, err)
	}
}

func TestLoadInputsSupportsGenericNamedSourceViews(t *testing.T) {
	dir := t.TempDir()
	merged := filepath.Join(dir, "merged.json")
	left := filepath.Join(dir, "left.yaml")
	right := filepath.Join(dir, "right.json")
	for path, content := range map[string]string{
		merged: `{"service":{"name":"fake"}}`,
		left:   "value: left\n",
		right:  `{"value":"right"}`,
	} {
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := config.LoadInputs(context.Background(), config.ConfigInputs{
		MergedPaths: []string{merged},
		SourcePaths: []config.NamedConfigPath{{Name: "fake.left", Path: left}, {Name: "fake.right", Path: right}},
	}, "FAKE__")
	if err != nil {
		t.Fatal(err)
	}
	var service struct {
		Name string `config:"name"`
	}
	if err := snapshot.Bind("service", &service, config.Strict()); err != nil || service.Name != "fake" {
		t.Fatalf("merged fake view = %+v, err = %v", service, err)
	}
	for name, want := range map[string]string{"fake.left": "left", "fake.right": "right"} {
		var value struct {
			Value string `config:"value"`
		}
		if err := snapshot.BindSource(name, "", &value, config.Strict()); err != nil || value.Value != want {
			t.Fatalf("source %s = %+v, err = %v", name, value, err)
		}
	}
}

func TestLoadInputsAllowsNamedSourcesToSharePath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "source.json")
	if err := os.WriteFile(path, []byte(`{"value":"shared"}`), 0600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.LoadInputs(context.Background(), config.ConfigInputs{
		MergedPaths: []string{path},
		SourcePaths: []config.NamedConfigPath{
			{Name: "planner.low", Path: path},
			{Name: "planner.normal", Path: filepath.Join(dir, ".", "source.json")},
		},
	}, "INPUTS__")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"planner.low", "planner.normal"} {
		var value struct {
			Value string `config:"value"`
		}
		if err := snapshot.BindSource(name, "", &value, config.Strict()); err != nil {
			t.Fatalf("source %s bind error = %v", name, err)
		}
		if value.Value != "shared" {
			t.Fatalf("source %s value = %q, want shared", name, value.Value)
		}
	}
}

func TestLoadInputsMergeDoesNotMutateSharedNamedSource(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "base.json")
	override := filepath.Join(dir, "override.json")
	if err := os.WriteFile(base, []byte(`{"nested":{"base":1}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(override, []byte(`{"nested":{"override":2}}`), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := config.LoadInputsWithArtifacts(context.Background(), config.ConfigInputs{
		MergedPaths: []string{base, override},
		SourcePaths: []config.NamedConfigPath{{Name: "base", Path: base}},
	}, "CONFIG_SHARED_SOURCE_TEST__")
	if err != nil {
		t.Fatal(err)
	}
	var merged, source map[string]any
	if err := loaded.Snapshot.Bind("", &merged); err != nil {
		t.Fatal(err)
	}
	if err := loaded.Snapshot.BindSource("base", "", &source); err != nil {
		t.Fatal(err)
	}
	if _, ok := merged["nested"].(map[string]any)["override"]; !ok {
		t.Fatalf("merged tree = %#v", merged)
	}
	if _, ok := source["nested"].(map[string]any)["override"]; ok {
		t.Fatalf("named source was mutated by merge: %#v", source)
	}
	if source["nested"].(map[string]any)["base"] != int64(1) {
		t.Fatalf("named source = %#v", source)
	}
}

func TestBindSourceErrorIncludesSourceName(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "source.json")
	if err := os.WriteFile(path, []byte(`{"known":"bad"}`), 0600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.LoadInputs(context.Background(), config.ConfigInputs{
		MergedPaths: []string{path},
		SourcePaths: []config.NamedConfigPath{{Name: "planner.low", Path: path}},
	}, "CONFIG_SOURCE_ERROR_TEST__")
	if err != nil {
		t.Fatal(err)
	}
	var target struct {
		Known int `config:"known"`
	}
	err = snapshot.BindSource("planner.low", "", &target)
	if err == nil || !strings.Contains(err.Error(), `source "planner.low"`) {
		t.Fatalf("BindSource error = %v", err)
	}
}
