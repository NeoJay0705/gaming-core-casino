package config_test

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
)

func TestLoadInputsWithArtifactsHashesParsedNamedSource(t *testing.T) {
	dir := t.TempDir()
	merged := filepath.Join(dir, "gamesvr.json")
	source := filepath.Join(dir, "planner.json")
	dirtySource := filepath.Join(dir, "nested") +
		string(filepath.Separator) + ".." +
		string(filepath.Separator) + filepath.Base(source)
	sourceBytes := []byte("{\"profile\":\"low\"}\n")
	if err := os.WriteFile(merged, []byte(`{"file_integrity":{"version":"1","sources":[{"source":"planner.low","md5":"`+md5Hex(sourceBytes)+`"}]}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, sourceBytes, 0600); err != nil {
		t.Fatal(err)
	}

	loaded, err := config.LoadInputsWithArtifacts(context.Background(), config.ConfigInputs{
		MergedPaths: []string{merged},
		SourcePaths: []config.NamedConfigPath{{Name: "planner.low", Path: dirtySource}},
	}, "INTEGRITY_ARTIFACT_TEST__")
	if err != nil {
		t.Fatal(err)
	}
	artifact, ok := loaded.NamedSources["planner.low"]
	expectedPath, err := filepath.Abs(dirtySource)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || artifact.Name != "planner.low" || artifact.Path != filepath.Clean(expectedPath) || artifact.MD5 != md5Hex(sourceBytes) {
		t.Fatalf("artifact = %#v, want planner.low with path %s and md5 %s", artifact, expectedPath, md5Hex(sourceBytes))
	}
	if err := config.VerifyFileIntegrity(loaded.Snapshot, loaded.NamedSources, true); err != nil {
		t.Fatalf("VerifyFileIntegrity() error = %v", err)
	}

	// snapshot 與 artifact 刻意描述上方已載入的 bytes；載入後再修改檔案，
	// 不會偷偷改變這次已驗證的內容。
	if err := os.WriteFile(source, []byte("{\"profile\":\"changed\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := config.VerifyFileIntegrity(loaded.Snapshot, loaded.NamedSources, true); err != nil {
		t.Fatalf("VerifyFileIntegrity() after source mutation = %v", err)
	}
}

func TestLoadInputsWithArtifactsAllowsMissingDeclaredSourcesWhenIntegrityDeclared(t *testing.T) {
	dir := t.TempDir()
	merged := filepath.Join(dir, "gamesvr.json")
	low := filepath.Join(dir, "low.json")
	missingHigh := filepath.Join(dir, "high.json")
	lowBytes := []byte(`{"profile":"low"}`)
	if err := os.WriteFile(merged, []byte(`{"file_integrity":{"version":"1","sources":[{"source":"planner.low","md5":"`+md5Hex(lowBytes)+`"}]}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(low, lowBytes, 0600); err != nil {
		t.Fatal(err)
	}

	loaded, err := config.LoadInputsWithArtifacts(context.Background(), config.ConfigInputs{
		MergedPaths: []string{merged},
		SourcePaths: []config.NamedConfigPath{
			{Name: "planner.low", Path: low},
			{Name: "planner.high", Path: missingHigh},
		},
	}, "INTEGRITY_PARTIAL_LOAD_TEST__")
	if err != nil {
		t.Fatalf("LoadInputsWithArtifacts() error = %v", err)
	}
	if !loaded.Snapshot.HasSource("planner.low") {
		t.Fatal("planner.low should be present")
	}
	if loaded.Snapshot.HasSource("planner.high") {
		t.Fatal("planner.high should be absent")
	}
	if _, ok := loaded.NamedSources["planner.high"]; ok {
		t.Fatal("planner.high artifact should be absent")
	}
	if err := config.VerifyFileIntegrity(loaded.Snapshot, loaded.NamedSources, true); err != nil {
		t.Fatalf("VerifyFileIntegrity() error = %v", err)
	}

	var value struct {
		Profile string `config:"profile"`
	}
	if err := loaded.Snapshot.BindSource("planner.low", "", &value, config.Strict()); err != nil {
		t.Fatalf("BindSource(planner.low) error = %v", err)
	}
	if value.Profile != "low" {
		t.Fatalf("planner.low profile = %q, want low", value.Profile)
	}
	if err := loaded.Snapshot.BindSource("planner.high", "", &value); err == nil {
		t.Fatal("BindSource(planner.high) should fail for an undeployed source")
	}
}

func TestLoadInputsWithArtifactsStillRejectsMissingSourceWithoutIntegrity(t *testing.T) {
	dir := t.TempDir()
	merged := filepath.Join(dir, "gamesvr.json")
	missing := filepath.Join(dir, "source.json")
	if err := os.WriteFile(merged, []byte(`{"service":"test"}`), 0600); err != nil {
		t.Fatal(err)
	}

	_, err := config.LoadInputsWithArtifacts(context.Background(), config.ConfigInputs{
		MergedPaths: []string{merged},
		SourcePaths: []config.NamedConfigPath{{Name: "source", Path: missing}},
	}, "INTEGRITY_COMPAT_MISSING_TEST__")
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing source error = %v, want os.ErrNotExist", err)
	}
}

func TestLoadInputsWithArtifactsRejectsMalformedSourceWhenIntegrityDeclared(t *testing.T) {
	dir := t.TempDir()
	merged := filepath.Join(dir, "gamesvr.json")
	source := filepath.Join(dir, "planner.json")
	if err := os.WriteFile(merged, []byte(
		`{"file_integrity":{"version":"1","sources":[{"source":"planner.low","md5":"00000000000000000000000000000000"}]}}`,
	), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, []byte(`{`), 0600); err != nil {
		t.Fatal(err)
	}

	_, err := config.LoadInputsWithArtifacts(context.Background(), config.ConfigInputs{
		MergedPaths: []string{merged},
		SourcePaths: []config.NamedConfigPath{{Name: "planner.low", Path: source}},
	}, "INTEGRITY_MALFORMED_SOURCE_TEST__")
	if err == nil {
		t.Fatal("malformed source should fail")
	}
	if errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("malformed source error = %v, must not be classified as missing", err)
	}
}

func TestLoadInputsWithArtifactsRejectsSourceIOErrorWhenIntegrityDeclared(t *testing.T) {
	dir := t.TempDir()
	merged := filepath.Join(dir, "gamesvr.json")
	source := filepath.Join(dir, "planner.json")
	if err := os.WriteFile(merged, []byte(
		`{"file_integrity":{"version":"1","sources":[{"source":"planner.low","md5":"00000000000000000000000000000000"}]}}`,
	), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}

	_, err := config.LoadInputsWithArtifacts(context.Background(), config.ConfigInputs{
		MergedPaths: []string{merged},
		SourcePaths: []config.NamedConfigPath{{Name: "planner.low", Path: source}},
	}, "INTEGRITY_SOURCE_IO_TEST__")
	if err == nil {
		t.Fatal("source I/O error should fail")
	}
	if errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("source I/O error = %v, must not be classified as missing", err)
	}
}

func TestVerifyFileIntegrityRequiresExactSourceSetBeforeDigest(t *testing.T) {
	lowBytes := `{"profile":"low"}`
	highBytes := `{"profile":"high"}`
	lowMD5 := md5Hex([]byte(lowBytes))
	highMD5 := md5Hex([]byte(highBytes))

	tests := []struct {
		name        string
		declaration string
		sources     map[string]string
		want        error
		wantText    string
		wantNot     error
	}{
		{
			name:        "manifest is missing an actual source",
			declaration: `{"version":"1","sources":[{"source":"planner.low","md5":"` + lowMD5 + `"}]}`,
			sources:     map[string]string{"planner.low": lowBytes, "planner.high": highBytes},
			want:        config.ErrFileIntegrityInvalid,
			wantText:    "unexpected_sources=[planner.high]",
			wantNot:     config.ErrFileIntegrityMismatch,
		},
		{
			name:        "manifest has a source that is not deployed",
			declaration: `{"version":"1","sources":[{"source":"planner.low","md5":"` + lowMD5 + `"},{"source":"planner.high","md5":"` + highMD5 + `"}]}`,
			sources:     map[string]string{"planner.low": lowBytes},
			want:        config.ErrFileIntegrityInvalid,
			wantText:    "missing_sources=[planner.high]",
			wantNot:     config.ErrFileIntegrityMismatch,
		},
		{
			name:        "same count but different names",
			declaration: `{"version":"1","sources":[{"source":"planner.low","md5":"` + lowMD5 + `"},{"source":"planner.other","md5":"` + highMD5 + `"}]}`,
			sources:     map[string]string{"planner.low": lowBytes, "planner.high": highBytes},
			want:        config.ErrFileIntegrityInvalid,
			wantText:    "missing_sources=[planner.other] unexpected_sources=[planner.high]",
			wantNot:     config.ErrFileIntegrityMismatch,
		},
		{
			name:        "digest is checked after exact set",
			declaration: `{"version":"1","sources":[{"source":"planner.low","md5":"` + lowMD5 + `"},{"source":"planner.high","md5":"00000000000000000000000000000000"}]}`,
			sources:     map[string]string{"planner.low": lowBytes, "planner.high": highBytes},
			want:        config.ErrFileIntegrityMismatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loaded := loadIntegritySetFixture(t, tt.declaration, tt.sources)
			err := config.VerifyFileIntegrity(loaded.Snapshot, loaded.NamedSources, true)
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want errors.Is(..., %v)", err, tt.want)
			}
			if tt.wantText != "" && !strings.Contains(err.Error(), tt.wantText) {
				t.Fatalf("error = %v, want text %q", err, tt.wantText)
			}
			if tt.wantNot != nil && errors.Is(err, tt.wantNot) {
				t.Fatalf("error = %v, must not be errors.Is(..., %v)", err, tt.wantNot)
			}
		})
	}
}

func TestVerifyFileIntegrityRejectsDigestMismatch(t *testing.T) {
	loaded := loadIntegrityFixture(t, `{"version":"1","sources":[{"source":"planner.low","md5":"00000000000000000000000000000000"}]}`, "planner.low", `{"value":"planner"}`)
	err := config.VerifyFileIntegrity(loaded.Snapshot, loaded.NamedSources, true)
	if !errors.Is(err, config.ErrFileIntegrityMismatch) {
		t.Fatalf("error = %v, want ErrFileIntegrityMismatch", err)
	}
	if !strings.Contains(err.Error(), `source "planner.low"`) {
		t.Fatalf("error = %v, want source name", err)
	}
	optionalErr := config.VerifyFileIntegrity(loaded.Snapshot, loaded.NamedSources, false)
	if !errors.Is(optionalErr, config.ErrFileIntegrityMismatch) {
		t.Fatalf("optional verification error = %v, want ErrFileIntegrityMismatch", optionalErr)
	}
}

func TestVerifyFileIntegrityRequiredAndOptionalModes(t *testing.T) {
	loaded := loadIntegrityFixtureWithoutDeclaration(t, "planner.low", `{"value":"planner"}`)
	if err := config.VerifyFileIntegrity(loaded.Snapshot, loaded.NamedSources, false); err != nil {
		t.Fatalf("optional verification error = %v", err)
	}
	err := config.VerifyFileIntegrity(loaded.Snapshot, loaded.NamedSources, true)
	if !errors.Is(err, config.ErrFileIntegrityRequired) {
		t.Fatalf("required error = %v, want ErrFileIntegrityRequired", err)
	}
}

func TestVerifyFileIntegrityRejectsInvalidDeclarations(t *testing.T) {
	actualMD5 := md5Hex([]byte(`{"value":"planner"}`))
	tests := []struct {
		name        string
		declaration string
		want        error
	}{
		{name: "unknown field", declaration: `{"version":"1","sources":[{"source":"planner.low","md5":"` + actualMD5 + `","extra":true}]}`, want: config.ErrFileIntegrityInvalid},
		{name: "unknown version", declaration: `{"version":"2","sources":[{"source":"planner.low","md5":"` + actualMD5 + `"}]}`, want: config.ErrFileIntegrityInvalid},
		{name: "empty sources", declaration: `{"version":"1","sources":[]}`, want: config.ErrFileIntegrityInvalid},
		{name: "duplicate source", declaration: `{"version":"1","sources":[{"source":"planner.low","md5":"` + actualMD5 + `"},{"source":"planner.low","md5":"` + actualMD5 + `"}]}`, want: config.ErrFileIntegrityInvalid},
		{name: "uppercase md5", declaration: `{"version":"1","sources":[{"source":"planner.low","md5":"` + strings.ToUpper(actualMD5) + `"}]}`, want: config.ErrFileIntegrityInvalid},
		{name: "missing source", declaration: `{"version":"1","sources":[{"source":"planner.high","md5":"` + actualMD5 + `"}]}`, want: config.ErrFileIntegrityInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loaded := loadIntegrityFixture(t, tt.declaration, "planner.low", `{"value":"planner"}`)
			err := config.VerifyFileIntegrity(loaded.Snapshot, loaded.NamedSources, true)
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want errors.Is(..., %v)", err, tt.want)
			}
		})
	}
}

func TestLoadInputsAndLoadInputsWithArtifactsProduceSameSnapshot(t *testing.T) {
	dir := t.TempDir()
	merged := filepath.Join(dir, "merged.json")
	source := filepath.Join(dir, "source.json")
	if err := os.WriteFile(merged, []byte(`{"service":{"name":"test"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, []byte(`{"value":"source"}`), 0600); err != nil {
		t.Fatal(err)
	}
	inputs := config.ConfigInputs{MergedPaths: []string{merged}, SourcePaths: []config.NamedConfigPath{{Name: "source", Path: source}}}
	legacy, err := config.LoadInputs(context.Background(), inputs, "INTEGRITY_COMPAT_TEST__")
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := config.LoadInputsWithArtifacts(context.Background(), inputs, "INTEGRITY_COMPAT_TEST__")
	if err != nil {
		t.Fatal(err)
	}
	var legacyTree, loadedTree map[string]any
	if err := legacy.Bind("", &legacyTree); err != nil {
		t.Fatal(err)
	}
	if err := loaded.Snapshot.Bind("", &loadedTree); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(legacyTree, loadedTree) {
		t.Fatalf("legacy tree = %#v, loaded tree = %#v", legacyTree, loadedTree)
	}
	var legacySource, loadedSource map[string]any
	if err := legacy.BindSource("source", "", &legacySource); err != nil {
		t.Fatal(err)
	}
	if err := loaded.Snapshot.BindSource("source", "", &loadedSource); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(legacySource, loadedSource) {
		t.Fatalf("legacy source = %#v, loaded source = %#v", legacySource, loadedSource)
	}
}

func loadIntegrityFixture(t *testing.T, declaration, sourceName, sourceBody string) config.LoadedInputs {
	t.Helper()
	dir := t.TempDir()
	merged := filepath.Join(dir, "gamesvr.json")
	source := filepath.Join(dir, "planner.json")
	content := `{"file_integrity":` + declaration + `}`
	if err := os.WriteFile(merged, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, []byte(sourceBody), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := config.LoadInputsWithArtifacts(context.Background(), config.ConfigInputs{
		MergedPaths: []string{merged},
		SourcePaths: []config.NamedConfigPath{{Name: sourceName, Path: source}},
	}, "INTEGRITY_FIXTURE_TEST__")
	if err != nil {
		t.Fatal(err)
	}
	return loaded
}

func loadIntegrityFixtureWithoutDeclaration(t *testing.T, sourceName, sourceBody string) config.LoadedInputs {
	t.Helper()
	dir := t.TempDir()
	merged := filepath.Join(dir, "gamesvr.json")
	source := filepath.Join(dir, "planner.json")
	if err := os.WriteFile(merged, []byte(`{"service":"test"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, []byte(sourceBody), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := config.LoadInputsWithArtifacts(context.Background(), config.ConfigInputs{
		MergedPaths: []string{merged},
		SourcePaths: []config.NamedConfigPath{{Name: sourceName, Path: source}},
	}, "INTEGRITY_OPTIONAL_TEST__")
	if err != nil {
		t.Fatal(err)
	}
	return loaded
}

func loadIntegritySetFixture(t *testing.T, declaration string, sourceBodies map[string]string) config.LoadedInputs {
	t.Helper()
	dir := t.TempDir()
	merged := filepath.Join(dir, "gamesvr.json")
	if err := os.WriteFile(merged, []byte(`{"file_integrity":`+declaration+`}`), 0600); err != nil {
		t.Fatal(err)
	}

	names := make([]string, 0, len(sourceBodies))
	for name := range sourceBodies {
		names = append(names, name)
	}
	sort.Strings(names)
	sourcePaths := make([]config.NamedConfigPath, 0, len(names))
	for _, name := range names {
		path := filepath.Join(dir, name+".json")
		if err := os.WriteFile(path, []byte(sourceBodies[name]), 0600); err != nil {
			t.Fatal(err)
		}
		sourcePaths = append(sourcePaths, config.NamedConfigPath{Name: name, Path: path})
	}
	loaded, err := config.LoadInputsWithArtifacts(context.Background(), config.ConfigInputs{
		MergedPaths: []string{merged},
		SourcePaths: sourcePaths,
	}, "INTEGRITY_SOURCE_SET_TEST__")
	if err != nil {
		t.Fatal(err)
	}
	return loaded
}

func md5Hex(value []byte) string {
	digest := md5.Sum(value)
	return hex.EncodeToString(digest[:])
}
