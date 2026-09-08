package config_test

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
)

type p0Layer map[string]any

func (p p0Layer) Name() string                                 { return "p0" }
func (p p0Layer) Load(context.Context) (map[string]any, error) { return p, nil }
func p0Snapshot(t *testing.T, v map[string]any) config.Snapshot {
	t.Helper()
	s, err := config.Load(context.Background(), p0Layer(v))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestBindCanonicalBool(t *testing.T) {
	type named bool
	s := p0Snapshot(t, map[string]any{"a": true, "b": false, "c": "true", "d": int64(1)})
	var a bool
	var b named
	var c bool
	d := true
	if err := s.Bind("a", &a); err != nil || !a {
		t.Fatalf("bool: %v %v", a, err)
	}
	if err := s.Bind("b", &b); err != nil || b {
		t.Fatalf("named bool: %v %v", b, err)
	}
	if err := s.Bind("c", &c); err != nil || !c {
		t.Fatalf("string bool: %v %v", c, err)
	}
	if err := s.Bind("d", &d); err == nil || !d {
		t.Fatalf("expected error and unchanged target: %v", d)
	}
}

func TestBindPointerKinds(t *testing.T) {
	type child struct {
		N int `config:"n"`
	}
	var _ = time.Second
	s := p0Snapshot(t, map[string]any{"b": true, "i": int64(3), "s": "x", "d": "2s", "xs": []any{int64(1)}, "c": map[string]any{"n": int64(4)}, "m": map[string]any{"x": "y"}})
	var b *bool
	var i *int
	var str *string
	var d *time.Duration
	var xs *[]int
	var c *child
	var m *map[string]string
	var chain **int
	for path, target := range map[string]any{"b": &b, "i": &i, "s": &str, "d": &d, "xs": &xs, "c": &c, "m": &m} {
		if err := s.Bind(path, target); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
	if err := s.Bind("i", &chain); err != nil || chain == nil || *(*chain) != 3 {
		t.Fatalf("chain: %v", err)
	}
	if !*b || *i != 3 || *str != "x" || *d != 2*time.Second || !reflect.DeepEqual(*xs, []int{1}) || c.N != 4 || (*m)["x"] != "y" {
		t.Fatal("pointer values incorrect")
	}
}

func TestBindPointerFailureIsTransactional(t *testing.T) {
	type C struct {
		N int `config:"n"`
	}
	s := p0Snapshot(t, map[string]any{"n": "bad"})
	old := &C{N: 7}
	target := old
	if err := s.Bind("", &target); err == nil || target != old || target.N != 7 {
		t.Fatalf("not transactional: %v %p %p", err, old, target)
	}
}

func TestBindEmptyInterfaceDeepClones(t *testing.T) {
	s := p0Snapshot(t, map[string]any{"x": map[string]any{"a": []any{int64(1)}}})
	var x any
	if err := s.Bind("x", &x); err != nil {
		t.Fatal(err)
	}
	x.(map[string]any)["a"].([]any)[0] = int64(9)
	var fresh any
	if err := s.Bind("x", &fresh); err != nil || fresh.(map[string]any)["a"].([]any)[0] != int64(1) {
		t.Fatal("snapshot leaked")
	}
}

func TestBindNonEmptyInterfaceReturnsErrorWithoutPanic(t *testing.T) {
	for _, src := range []map[string]any{{"x": "raw"}, {"x": map[string]any{"a": int64(1)}}} {
		s := p0Snapshot(t, src)
		var x fmt.Stringer
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic: %v", r)
				}
			}()
			err := s.Bind("x", &x)
			if err == nil || !strings.Contains(err.Error(), "x") || !strings.Contains(err.Error(), "fmt.Stringer") || strings.Contains(err.Error(), "raw") {
				t.Fatalf("bad error: %v", err)
			}
		}()
	}
}

func TestBindFloatToSignedIntegerBounds(t *testing.T) {
	for _, tc := range []struct {
		f       float64
		wantErr bool
	}{{-128, false}, {127, false}, {-129, true}, {128, true}, {1.5, true}} {
		var x int8
		err := p0Snapshot(t, map[string]any{"x": tc.f}).Bind("x", &x)
		if (err != nil) != tc.wantErr {
			t.Fatalf("%v err=%v", tc.f, err)
		}
	}
	var y int64
	for _, tc := range []struct {
		f       float64
		wantErr bool
	}{{math.Ldexp(-1, 63), false}, {math.Nextafter(math.Ldexp(1, 63), 0), false}, {math.Ldexp(1, 63), true}} {
		err := p0Snapshot(t, map[string]any{"x": tc.f}).Bind("x", &y)
		if (err != nil) != tc.wantErr {
			t.Fatalf("int64 %v err=%v", tc.f, err)
		}
	}
}

func TestBindFloatToUnsignedIntegerBounds(t *testing.T) {
	for _, tc := range []struct {
		f       float64
		wantErr bool
	}{{0, false}, {255, false}, {-1, true}, {256, true}, {1.5, true}} {
		var x uint8
		err := p0Snapshot(t, map[string]any{"x": tc.f}).Bind("x", &x)
		if (err != nil) != tc.wantErr {
			t.Fatalf("%v err=%v", tc.f, err)
		}
	}
	var x uint64
	for _, tc := range []struct {
		f       float64
		wantErr bool
	}{{math.Nextafter(math.Ldexp(1, 64), 0), false}, {math.Ldexp(1, 64), true}} {
		err := p0Snapshot(t, map[string]any{"x": tc.f}).Bind("x", &x)
		if (err != nil) != tc.wantErr {
			t.Fatalf("uint64 %v err=%v", tc.f, err)
		}
	}
}

func TestEmptySnapshotRootBindPreservesDefaults(t *testing.T) {
	s, err := config.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p := 3
	target := struct {
		N int
		P *int
		M map[string]int
		X []int
	}{7, &p, map[string]int{"a": 1}, []int{2}}
	if err := s.Bind("", &target); err != nil || target.N != 7 || target.P == nil || *target.P != p || target.M["a"] != 1 || target.X[0] != 2 {
		t.Fatalf("%v %+v", err, target)
	}
}

func TestEmptySnapshotHasRootIsFalse(t *testing.T) {
	s, _ := config.Load(context.Background())
	if s.Has("") {
		t.Fatal("empty root exists")
	}
	if !p0Snapshot(t, map[string]any{"x": int64(1)}).Has("") {
		t.Fatal("non-empty root missing")
	}
}
func TestMissingNonRootPathStillFails(t *testing.T) {
	for _, s := range []config.Snapshot{func() config.Snapshot { x, _ := config.Load(context.Background()); return x }(), p0Snapshot(t, map[string]any{"x": int64(1)})} {
		var x int
		if err := s.Bind("missing", &x); err == nil || x != 0 {
			t.Fatalf("missing path: %v", err)
		}
	}
}
