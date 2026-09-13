package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPushOrchestrationWaitsForStartMarker(t *testing.T) {
	dir := t.TempDir()
	orchestration, err := newPushOrchestration(dir)
	if err != nil {
		t.Fatalf("newPushOrchestration() error = %v", err)
	}
	if err := orchestration.writeClientsReady(10); err != nil {
		t.Fatalf("writeClientsReady() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, pushClientsReadyMarker)); err != nil {
		t.Fatalf("clients-ready marker: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- orchestration.waitForStart(ctx) }()
	select {
	case err := <-done:
		t.Fatalf("waitForStart() returned before marker: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	temporary := filepath.Join(dir, ".start.tmp")
	if err := os.WriteFile(temporary, []byte("start\n"), 0o640); err != nil {
		t.Fatalf("write temporary start marker: %v", err)
	}
	if err := os.Rename(temporary, filepath.Join(dir, pushStartMarker)); err != nil {
		t.Fatalf("rename start marker: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("waitForStart() error = %v", err)
	}
}

func TestPushOrchestrationRejectsStaleStartMarker(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, pushStartMarker), nil, 0o640); err != nil {
		t.Fatalf("write stale marker: %v", err)
	}
	if _, err := newPushOrchestration(dir); err == nil {
		t.Fatal("newPushOrchestration() error = nil")
	}
}

func TestPushOrchestrationRejectsStaleBaselineMarker(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, pushBaselineReadyMarker), []byte("{}\n"), 0o640); err != nil {
		t.Fatalf("write stale baseline marker: %v", err)
	}
	if _, err := newPushOrchestration(dir); err == nil {
		t.Fatal("newPushOrchestration() error = nil")
	}
}

func TestPushOrchestrationWarmupResultAllowsWorkloadFailures(t *testing.T) {
	dir := t.TempDir()
	orchestration, err := newPushOrchestration(dir)
	if err != nil {
		t.Fatalf("newPushOrchestration() error = %v", err)
	}
	want := pushWarmupSummary{planned: 312, attempted: 311, success: 309, partial: 1, err: 1, missed: 1}
	fixture := `{"timestamp":"2026-01-01T00:00:00.000000000Z","run_id":"warmup-1","planned":312,"attempted":311,"success":309,"partial":1,"error":1,"missed":1}`
	if err := os.WriteFile(filepath.Join(dir, pushWarmupResultMarker), []byte(fixture+"\n"), 0o640); err != nil {
		t.Fatalf("write warm-up result fixture: %v", err)
	}
	got, err := orchestration.waitForWarmupResult(context.Background(), time.Second, "warmup-1", want.planned)
	if err != nil {
		t.Fatalf("waitForWarmupResult() error = %v", err)
	}
	if got != want {
		t.Fatalf("warm-up result = %+v, want %+v", got, want)
	}
	if err := orchestration.writeWarmupDrained("warmup-1", pushLoadStats{received: 309000}, 3000); err != nil {
		t.Fatalf("writeWarmupDrained() error = %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, pushWarmupDrainedMarker))
	if err != nil {
		t.Fatalf("read warm-up drained marker: %v", err)
	}
	var marker pushOrchestrationMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		t.Fatalf("decode warm-up drained marker: %v", err)
	}
	if marker.RunID != "warmup-1" || marker.Received != 309000 || marker.Missing != 3000 {
		t.Fatalf("warm-up drained marker = %+v", marker)
	}
}

func TestPushOrchestrationRejectsInvalidWarmupResult(t *testing.T) {
	tests := map[string]struct {
		marker string
		expect string
	}{
		"stale run id": {
			marker: `{"run_id":"run-2","planned":4,"attempted":4,"success":4,"missed":0}`,
			expect: "run ID",
		},
		"invalid json": {
			marker: `{"run_id":"run-1","planned":4`,
			expect: "decode",
		},
		"negative counter": {
			marker: `{"run_id":"run-1","planned":4,"attempted":-1,"success":0,"missed":5}`,
			expect: "decode",
		},
		"counters do not cover planned ticks": {
			marker: `{"run_id":"run-1","planned":4,"attempted":3,"success":3,"missed":0}`,
			expect: "attempted and missed",
		},
		"result counters exceed attempted": {
			marker: `{"run_id":"run-1","planned":4,"attempted":4,"success":4,"partial":1,"error":0,"missed":0}`,
			expect: "counters",
		},
		"result counters do not sum to attempted": {
			marker: `{"run_id":"run-1","planned":4,"attempted":4,"success":3,"partial":0,"error":0,"missed":0}`,
			expect: "sum to attempted",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			orchestration, err := newPushOrchestration(dir)
			if err != nil {
				t.Fatalf("newPushOrchestration() error = %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, pushWarmupResultMarker), []byte(test.marker), 0o640); err != nil {
				t.Fatalf("write warm-up result marker: %v", err)
			}
			_, err = orchestration.waitForWarmupResult(context.Background(), time.Second, "run-1", 4)
			if err == nil {
				t.Fatal("waitForWarmupResult() error = nil")
			}
			if test.expect != "" && !strings.Contains(err.Error(), test.expect) {
				t.Fatalf("waitForWarmupResult() error = %q, want substring %q", err, test.expect)
			}
		})
	}
}

func TestPushOrchestrationRejectsMismatchedWarmupPlannedTicks(t *testing.T) {
	dir := t.TempDir()
	orchestration, err := newPushOrchestration(dir)
	if err != nil {
		t.Fatalf("newPushOrchestration() error = %v", err)
	}
	marker := `{"run_id":"run-1","planned":3,"attempted":3,"success":3,"partial":0,"error":0,"missed":0}`
	if err := os.WriteFile(filepath.Join(dir, pushWarmupResultMarker), []byte(marker), 0o640); err != nil {
		t.Fatalf("write warm-up result marker: %v", err)
	}
	_, err = orchestration.waitForWarmupResult(context.Background(), time.Second, "run-1", 4)
	if err == nil || !strings.Contains(err.Error(), "does not match response") {
		t.Fatalf("waitForWarmupResult() error = %v, want planned mismatch", err)
	}
	if _, err := orchestration.waitForWarmupResult(context.Background(), time.Second, "run-1", 0); err == nil || !strings.Contains(err.Error(), "must be positive") {
		t.Fatalf("waitForWarmupResult(expectedPlanned=0) error = %v, want positive validation", err)
	}
}

func TestPushOrchestrationWritesBoundedRunMarker(t *testing.T) {
	dir := t.TempDir()
	orchestration, err := newPushOrchestration(dir)
	if err != nil {
		t.Fatalf("newPushOrchestration() error = %v", err)
	}
	if err := orchestration.writeRunStarted(pushMeasuredStartedMarker, "run-1"); err != nil {
		t.Fatalf("writeRunStarted() error = %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, pushMeasuredStartedMarker))
	if err != nil {
		t.Fatalf("read run marker: %v", err)
	}
	var marker pushOrchestrationMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		t.Fatalf("decode run marker: %v", err)
	}
	if marker.RunID != "run-1" || marker.Timestamp == "" {
		t.Fatalf("run marker = %+v", marker)
	}
}

func TestPushOrchestrationWaitsForBaselineMarker(t *testing.T) {
	dir := t.TempDir()
	orchestration, err := newPushOrchestration(dir)
	if err != nil {
		t.Fatalf("newPushOrchestration() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- orchestration.waitForBaseline(ctx, time.Second, "run-1") }()
	select {
	case err := <-done:
		t.Fatalf("waitForBaseline() returned before marker: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	temporary := filepath.Join(dir, ".baseline.tmp")
	if err := os.WriteFile(temporary, []byte("{\"timestamp\":\"2026-01-01T00:00:00Z\",\"run_id\":\"run-1\"}\n"), 0o640); err != nil {
		t.Fatalf("write temporary baseline marker: %v", err)
	}
	if err := os.Rename(temporary, filepath.Join(dir, pushBaselineReadyMarker)); err != nil {
		t.Fatalf("rename baseline marker: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("waitForBaseline() error = %v", err)
	}
}

func TestPushOrchestrationFinalScrapeBarrierUsesRunID(t *testing.T) {
	dir := t.TempDir()
	orchestration, err := newPushOrchestration(dir)
	if err != nil {
		t.Fatalf("newPushOrchestration() error = %v", err)
	}
	if err := orchestration.writeFinalReady("run-1"); err != nil {
		t.Fatalf("writeFinalReady() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- orchestration.waitForFinalScrape(ctx, time.Second, "run-1") }()
	select {
	case err := <-done:
		t.Fatalf("waitForFinalScrape() returned before marker: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := orchestration.writeFinalScrape("run-1"); err != nil {
		t.Fatalf("writeFinalScrape() error = %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("waitForFinalScrape() error = %v", err)
	}
	if err := orchestration.waitForFinalScrape(context.Background(), time.Second, "run-2"); err == nil {
		t.Fatal("waitForFinalScrape(run-2) error = nil")
	}
}
