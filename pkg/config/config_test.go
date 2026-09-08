package config_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
)

type payment struct {
	Endpoint string        `config:"endpoint"`
	Timeout  time.Duration `config:"timeout"`
	Retries  int           `config:"retries"`
}

func TestLoadMergeBindAndHas(t *testing.T) {
	d := t.TempDir()
	base := filepath.Join(d, "base.yaml")
	if e := os.WriteFile(base, []byte("sdk:\n  payment:\n    endpoint: default\n    timeout: 3s\n    retries: 2\n"), 0600); e != nil {
		t.Fatal(e)
	}
	os.Setenv("CFG__SDK__PAYMENT__RETRIES", "4")
	t.Cleanup(func() { os.Unsetenv("CFG__SDK__PAYMENT__RETRIES") })
	s, e := config.Load(context.Background(), config.File(base), config.Env("CFG__"))
	if e != nil {
		t.Fatal(e)
	}
	if !s.Has("sdk.payment") || s.Has("missing") {
		t.Fatal("unexpected paths")
	}
	c := payment{Timeout: 10 * time.Second, Retries: 1}
	if e = s.Bind("sdk.payment", &c, config.Strict()); e != nil {
		t.Fatal(e)
	}
	if c.Endpoint != "default" || c.Timeout != 3*time.Second || c.Retries != 4 {
		t.Fatalf("got %+v", c)
	}
}
func TestBindIsTransactional(t *testing.T) {
	type C struct {
		N int  `config:"n"`
		B bool `config:"b"`
	}
	s, e := config.Load(context.Background(), testLayer{v: map[string]any{"n": "bad", "b": true}})
	if e != nil {
		t.Fatal(e)
	}
	c := C{N: 7}
	if e = s.Bind("", &c); e == nil || c.N != 7 || c.B {
		t.Fatalf("bind changed target: %+v err=%v", c, e)
	}
}

func TestJSONDuplicateKeyFails(t *testing.T) {
	d := t.TempDir()
	p := filepath.Join(d, "duplicate.json")
	if err := os.WriteFile(p, []byte(`{"a":1,"a":2}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := config.Load(context.Background(), config.File(p)); err == nil {
		t.Fatal("expected duplicate key error")
	}
}

func TestEnvParentChildConflictFails(t *testing.T) {
	os.Setenv("CFG__SERVER", "disabled")
	os.Setenv("CFG__SERVER__PORT", "8080")
	t.Cleanup(func() { os.Unsetenv("CFG__SERVER"); os.Unsetenv("CFG__SERVER__PORT") })
	if _, err := config.Load(context.Background(), config.Env("CFG__")); err == nil {
		t.Fatal("expected environment conflict")
	}
}

func TestXMLCanonicalization(t *testing.T) {
	d := t.TempDir()
	p := filepath.Join(d, "config.xml")
	x := `<server port="8080"><host>localhost</host><tag>a</tag><tag>b</tag></server>`
	if err := os.WriteFile(p, []byte(x), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := config.Load(context.Background(), config.File(p))
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Port string `config:"@port"`
		Host string
		Tag  []string
	}
	if err = s.Bind("server", &got); err != nil {
		t.Fatal(err)
	}
	if got.Port != "8080" || got.Host != "localhost" || len(got.Tag) != 2 {
		t.Fatalf("got %+v", got)
	}
}

func TestBindDefaultsNullAndArray(t *testing.T) {
	type nested struct {
		Timeout time.Duration `config:"timeout"`
		Retries int           `config:"retries"`
	}
	type cfg struct {
		Name   string `config:"name"`
		N      nested `config:"nested"`
		Values []int  `config:"values"`
	}
	s, err := config.Load(context.Background(), testLayer{v: map[string]any{"name": "new", "nested": map[string]any{"retries": "5"}, "values": []any{int64(1), int64(2)}}})
	if err != nil {
		t.Fatal(err)
	}
	c := cfg{Name: "old", N: nested{Timeout: 3 * time.Second, Retries: 1}, Values: []int{9}}
	if err := s.Bind("", &c); err != nil {
		t.Fatal(err)
	}
	if c.Name != "new" || c.N.Timeout != 3*time.Second || c.N.Retries != 5 || len(c.Values) != 2 || c.Values[0] != 1 {
		t.Fatalf("got %+v", c)
	}

	s, err = config.Load(context.Background(), testLayer{v: map[string]any{"nested": map[string]any{"timeout": nil}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Bind("", &c); err == nil {
		t.Fatal("expected null-to-duration error")
	}
}

func TestMergeMapScalarConflictFails(t *testing.T) {
	a := testLayer{v: map[string]any{"server": map[string]any{"port": int64(1)}}}
	b := testLayer{v: map[string]any{"server": "disabled"}}
	if _, err := config.Load(context.Background(), a, b); err == nil {
		t.Fatal("expected merge conflict")
	}
}

func TestBindPointerTransactionAndStrictPaths(t *testing.T) {
	type child struct {
		Known int `config:"known"`
	}
	type cfg struct {
		Child *child `config:"child"`
	}
	s, err := config.Load(context.Background(), testLayer{v: map[string]any{"child": map[string]any{"known": "bad"}}})
	if err != nil {
		t.Fatal(err)
	}
	c := cfg{Child: &child{Known: 7}}
	if err = s.Bind("", &c); err == nil || c.Child == nil || c.Child.Known != 7 {
		t.Fatalf("target mutated: %+v err=%v", c, err)
	}
	s, err = config.Load(context.Background(), testLayer{v: map[string]any{"z": 1, "a": map[string]any{"unknown": 2}}})
	if err != nil {
		t.Fatal(err)
	}
	var x struct {
		A struct{} `config:"a"`
	}
	err = s.Bind("", &x, config.Strict())
	if err == nil {
		t.Fatal("expected strict error")
	}
	if !strings.Contains(err.Error(), "a.unknown") || !strings.Contains(err.Error(), "z") {
		t.Fatalf("unexpected strict error: %v", err)
	}
}

func TestStrictChecksUnknownFieldsBehindPointerChains(t *testing.T) {
	type child struct {
		Known int `config:"known"`
	}
	type root struct {
		Child **child   `config:"child"`
		Items []**child `config:"items"`
	}
	s, err := config.Load(context.Background(), testLayer{v: map[string]any{
		"child": map[string]any{"known": int64(1), "unknown": true},
		"items": []any{map[string]any{"known": int64(2), "unknown": true}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	var target root
	err = s.Bind("", &target, config.Strict())
	if err == nil || !strings.Contains(err.Error(), "child.unknown") || !strings.Contains(err.Error(), "items[0].unknown") {
		t.Fatalf("strict pointer-chain error = %v", err)
	}
}

func TestBindPreservesNilDefaultsWhenFieldsAreMissing(t *testing.T) {
	s, err := config.Load(context.Background(), testLayer{v: map[string]any{"name": "new"}})
	if err != nil {
		t.Fatal(err)
	}
	type target struct {
		Name     string         `config:"name"`
		Defaults map[string]int `config:"defaults"`
		Items    []int          `config:"items"`
		Child    *struct{}      `config:"child"`
		Any      any            `config:"any"`
	}
	var value target
	if err := s.Bind("", &value); err != nil {
		t.Fatal(err)
	}
	if value.Name != "new" || value.Defaults != nil || value.Items != nil || value.Child != nil || value.Any != nil {
		t.Fatalf("missing fields changed defaults: %+v", value)
	}
}

func TestBuiltInLayersRejectNilAndCanceledContext(t *testing.T) {
	var nilContext context.Context
	if _, err := config.Env("CONFIG_CONTEXT_CONTRACT_NO_MATCH__").Load(nilContext); err == nil {
		t.Fatal("Env.Load(nil) returned nil error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := config.Env("CONFIG_CONTEXT_CONTRACT_NO_MATCH__").Load(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Env.Load(canceled) error = %v", err)
	}
}

func TestBindRootErrorUsesCanonicalPath(t *testing.T) {
	s, err := config.Load(context.Background(), testLayer{v: map[string]any{"n": "bad"}})
	if err != nil {
		t.Fatal(err)
	}
	var target struct {
		N int `config:"n"`
	}
	err = s.Bind("", &target)
	if err == nil || !strings.Contains(err.Error(), `path "n"`) || strings.Contains(err.Error(), `path ".n"`) {
		t.Fatalf("root bind path error = %v", err)
	}
}

func TestCanonicalizationErrorUsesFullPath(t *testing.T) {
	_, err := config.Load(context.Background(), testLayer{v: map[string]any{
		"server": map[string]any{"unsupported": struct{}{}},
	}})
	if err == nil || !strings.Contains(err.Error(), `server.unsupported`) {
		t.Fatalf("canonicalization path error = %v", err)
	}
}

func TestFileRulesAndPathValidation(t *testing.T) {
	d := t.TempDir()
	missing := filepath.Join(d, "missing.yaml")
	if _, err := config.Load(context.Background(), config.File(missing, config.Optional())); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(d, "bad.yaml")
	if err := os.WriteFile(p, []byte("a: ["), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := config.Load(context.Background(), config.File(p, config.Optional())); err == nil {
		t.Fatal("expected malformed optional file error")
	}
	s, err := config.Load(context.Background(), testLayer{v: map[string]any{"a": int64(1)}})
	if err != nil {
		t.Fatal(err)
	}
	var x int
	for _, path := range []string{".a", "a.", "a..b"} {
		if err := s.Bind(path, &x); err == nil {
			t.Fatalf("expected invalid path %q", path)
		}
	}
}

func TestBindConcurrencyAndSnapshotIsolation(t *testing.T) {
	s, err := config.Load(context.Background(), testLayer{v: map[string]any{"items": []any{int64(1)}}})
	if err != nil {
		t.Fatal(err)
	}
	var a, b struct {
		Items []int `config:"items"`
	}
	done := make(chan error, 2)
	go func() { done <- s.Bind("", &a) }()
	go func() { done <- s.Bind("", &b) }()
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	a.Items[0] = 9
	if b.Items[0] != 1 {
		t.Fatal("bind results share mutable data")
	}
}

func TestLoadContextAndTypedNilLayer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := config.Load(ctx, testLayer{v: map[string]any{}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	var l *testLayer
	if _, err := config.Load(context.Background(), l); err == nil {
		t.Fatal("expected typed nil layer error")
	}
}

func TestBuiltInFileLayerRejectsCancellationAfterRead(t *testing.T) {
	d := t.TempDir()
	path := filepath.Join(d, "config.json")
	if err := os.WriteFile(path, []byte(`{"value":1}`), 0600); err != nil {
		t.Fatal(err)
	}

	ctx := &cancelAfterChecksContext{Context: context.Background(), cancelAfter: 2}
	if _, err := config.File(path).Load(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("File.Load() error = %v, want context.Canceled", err)
	}
}

func TestLoadRejectsCancellationAfterCanonicalization(t *testing.T) {
	ctx := &cancelAfterChecksContext{Context: context.Background(), cancelAfter: 4}
	if _, err := config.Load(ctx, testLayer{v: map[string]any{"value": int64(1)}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Load() error = %v, want context.Canceled", err)
	}
}

func TestBindSubtreesToAnyDeepClones(t *testing.T) {
	s, err := config.Load(context.Background(), testLayer{v: map[string]any{
		"object": map[string]any{"nested": []any{int64(1)}},
		"items":  []any{map[string]any{"value": int64(2)}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	var object, items any
	if err := s.Bind("object", &object); err != nil {
		t.Fatal(err)
	}
	if err := s.Bind("items", &items); err != nil {
		t.Fatal(err)
	}
	object.(map[string]any)["nested"].([]any)[0] = int64(9)
	items.([]any)[0].(map[string]any)["value"] = int64(8)
	var fresh any
	if err := s.Bind("object", &fresh); err != nil {
		t.Fatal(err)
	}
	if fresh.(map[string]any)["nested"].([]any)[0] != int64(1) {
		t.Fatal("subtree bind was not cloned")
	}
}

func TestYAMLMultipleDocumentsFail(t *testing.T) {
	d := t.TempDir()
	p := filepath.Join(d, "multi.yaml")
	if err := os.WriteFile(p, []byte("a: 1\n---\nb: 2\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := config.Load(context.Background(), config.File(p)); err == nil {
		t.Fatal("expected multiple document error")
	}
}

func TestMergeConflictIncludesKinds(t *testing.T) {
	_, err := config.Load(context.Background(),
		testLayer{v: map[string]any{"server": map[string]any{"port": int64(1)}}},
		testLayer{v: map[string]any{"server": "disabled"}},
	)
	if err == nil || !strings.Contains(err.Error(), "map") || !strings.Contains(err.Error(), "string") {
		t.Fatalf("expected conflict kinds, err=%v", err)
	}
}

func TestSliceCycleFails(t *testing.T) {
	a := make([]any, 1)
	a[0] = a
	if _, err := config.Load(context.Background(), testLayer{v: map[string]any{"a": a}}); err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestYAMLAnchorAliasExpands(t *testing.T) {
	d := t.TempDir()
	p := filepath.Join(d, "anchors.yaml")
	// primary and secondary both alias the same anchored mapping.
	doc := "defaults: &def\n  timeout: 5s\n  retries: 3\nprimary: *def\nsecondary: *def\n"
	if err := os.WriteFile(p, []byte(doc), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := config.Load(context.Background(), config.File(p))
	if err != nil {
		t.Fatalf("anchor/alias should expand: %v", err)
	}
	type block struct {
		Timeout time.Duration `config:"timeout"`
		Retries int           `config:"retries"`
	}
	var primary, secondary block
	if err := s.Bind("primary", &primary); err != nil {
		t.Fatal(err)
	}
	if err := s.Bind("secondary", &secondary); err != nil {
		t.Fatal(err)
	}
	if primary.Timeout != 5*time.Second || primary.Retries != 3 {
		t.Fatalf("alias not expanded: %+v", primary)
	}
	if secondary.Timeout != 5*time.Second || secondary.Retries != 3 {
		t.Fatalf("reused alias not expanded: %+v", secondary)
	}
}

func TestYAMLRecursiveAnchorFails(t *testing.T) {
	d := t.TempDir()
	p := filepath.Join(d, "recursive.yaml")
	// &x anchors a sequence that aliases itself.
	if err := os.WriteFile(p, []byte("root: &x\n  - *x\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := config.Load(context.Background(), config.File(p)); err == nil {
		t.Fatal("expected recursive anchor error")
	}
}

func TestFieldNameSnakeCaseFallback(t *testing.T) {
	s, err := config.Load(context.Background(), testLayer{v: map[string]any{
		"max_retries":  int64(4),
		"endpoint_url": "a",
		"url_endpoint": "b",
	}})
	if err != nil {
		t.Fatal(err)
	}
	var c struct {
		MaxRetries  int
		EndpointURL string
		URLEndpoint string
	}
	if err := s.Bind("", &c); err != nil {
		t.Fatal(err)
	}
	if c.MaxRetries != 4 || c.EndpointURL != "a" || c.URLEndpoint != "b" {
		t.Fatalf("acronym-aware snake_case fallback wrong: %+v", c)
	}
}

type testLayer struct{ v map[string]any }

func (testLayer) Name() string                                   { return "test" }
func (x testLayer) Load(context.Context) (map[string]any, error) { return x.v, nil }

// cancelAfterChecksContext 讓測試可控制 cancellation 發生在不可中斷的 CPU 工作之後。
type cancelAfterChecksContext struct {
	context.Context
	cancelAfter int
	checks      int
}

func (c *cancelAfterChecksContext) Err() error {
	c.checks++
	if c.checks >= c.cancelAfter {
		return context.Canceled
	}
	return nil
}
