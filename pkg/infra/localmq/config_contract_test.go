package localmq

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
)

func TestNewBindsLocalMQDefaultsWithoutFilesystemIO(t *testing.T) {
	snapshot := localMQTestSnapshot(t, `local_mq:
  root_path: /var/lib/gaming-core/local-mq
  storage_id: 01993e11-8d3a-7c8f-a7ce-2ab21df8d410
`)
	client, err := New(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if got := client.Config().Consumer.WorkerConcurrency; got != defaultWorkerConcurrency {
		t.Fatalf("worker concurrency = %d, want %d", got, defaultWorkerConcurrency)
	}
}

func TestNewRejectsUnknownLocalMQConfig(t *testing.T) {
	snapshot := localMQTestSnapshot(t, `local_mq:
  root_path: /var/lib/gaming-core/local-mq
  storage_id: 01993e11-8d3a-7c8f-a7ce-2ab21df8d410
  not_a_v1_setting: true
`)
	_, err := New(snapshot)
	if err == nil || !strings.Contains(err.Error(), "not_a_v1_setting") {
		t.Fatalf("unknown setting error = %v", err)
	}
}

func localMQTestSnapshot(t *testing.T, contents string) config.SourceSnapshot {
	t.Helper()
	path := filepath.Join(t.TempDir(), "localmq.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.LoadInputs(context.Background(), config.ConfigInputs{MergedPaths: []string{path}}, "LOCALMQ_CONTRACT__")
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
