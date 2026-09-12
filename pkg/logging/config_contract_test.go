package logging

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
)

func TestLoggingConfigContractDefaultsAndValidatesLevel(t *testing.T) {
	for _, test := range []struct {
		name    string
		body    string
		want    string
		wantErr string
	}{
		{name: "omitted", body: "product: {}\n", want: "info"},
		{name: "trimmed level", body: "logging:\n  level: ' WARN '\n", want: "warn"},
		{name: "invalid level", body: "logging:\n  level: verbose\n", wantErr: "invalid"},
		{name: "unknown field", body: "logging:\n  format: json\n", wantErr: "unknown config paths"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := newConfig(loadLoggingSnapshot(t, test.body))
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("newConfig() error = %v, want substring %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("newConfig() error = %v", err)
			}
			if cfg.Level != test.want {
				t.Fatalf("level = %q, want %q", cfg.Level, test.want)
			}
		})
	}
}

func loadLoggingSnapshot(t *testing.T, body string) config.SourceSnapshot {
	t.Helper()
	path := filepath.Join(t.TempDir(), "logging.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.LoadInputs(context.Background(), config.ConfigInputs{MergedPaths: []string{path}}, "CORE_CASINO_LOGGING_CONTRACT__")
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
