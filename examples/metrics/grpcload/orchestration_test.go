package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGRPCLoadOrchestrationLifecycle(t *testing.T) {
	orchestration, err := newGRPCLoadOrchestration(t.TempDir())
	if err != nil {
		t.Fatalf("newGRPCLoadOrchestration: %v", err)
	}
	if err := orchestration.writeReady(1, 1); err != nil {
		t.Fatalf("writeReady: %v", err)
	}
	var ready grpcLoadOrchestrationMarker
	readGRPCLoadMarker(t, filepath.Join(orchestration.dir, grpcLoadReadyMarker), &ready)
	if ready.RunID != orchestration.runID || ready.ClientConnections != 1 || ready.WarmupRequests != 1 {
		t.Fatalf("ready marker = %#v, want run ID %q and setup values", ready, orchestration.runID)
	}

	if err := orchestration.writeJSON(grpcLoadStartMarker, grpcLoadOrchestrationMarker{
		RunID: orchestration.runID, Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("write start marker: %v", err)
	}
	if err := orchestration.waitForStart(context.Background(), time.Second); err != nil {
		t.Fatalf("waitForStart: %v", err)
	}

	measurementStart := time.Now().Add(-time.Second)
	result := measuredGRPCLoadResult{
		measurementStart: measurementStart,
		admissionEnd:     measurementStart.Add(500 * time.Millisecond),
		measurementEnd:   measurementStart.Add(700 * time.Millisecond),
		successful:       42,
	}
	if err := orchestration.writeMeasured(result); err != nil {
		t.Fatalf("writeMeasured: %v", err)
	}
	var measured grpcLoadOrchestrationMarker
	readGRPCLoadMarker(t, filepath.Join(orchestration.dir, grpcLoadMeasuredMarker), &measured)
	if measured.RunID != orchestration.runID || measured.SuccessfulRequests != 42 || measured.FailedWorkers != 0 {
		t.Fatalf("measured marker = %#v, want matching result", measured)
	}
	if measured.AdmissionDuration <= 0 || measured.MeasuredDuration <= 0 || measured.TerminalDrain <= 0 {
		t.Fatalf("measured durations = %#v, want positive durations", measured)
	}

	if err := orchestration.writeFinalScraped(); err != nil {
		t.Fatalf("writeFinalScraped: %v", err)
	}
	if err := orchestration.waitForFinalScraped(context.Background(), time.Second); err != nil {
		t.Fatalf("waitForFinalScraped: %v", err)
	}
}

func TestGRPCLoadOrchestrationRejectsStaleAndInvalidMarkers(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, grpcLoadReadyMarker), []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write stale marker: %v", err)
	}
	if _, err := newGRPCLoadOrchestration(dir); err == nil || !strings.Contains(err.Error(), grpcLoadReadyMarker) {
		t.Fatalf("newGRPCLoadOrchestration() error = %v, want stale marker error", err)
	}

	cleanDir := t.TempDir()
	orchestration, err := newGRPCLoadOrchestration(cleanDir)
	if err != nil {
		t.Fatalf("newGRPCLoadOrchestration(clean): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = orchestration.waitForStart(ctx, time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForStart() error = %v, want context deadline exceeded", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := orchestration.waitForStart(canceled, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForStart(canceled) error = %v, want context canceled", err)
	}
	if err := orchestration.writeJSON(grpcLoadStartMarker, grpcLoadOrchestrationMarker{
		RunID: "stale", Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("write stale start marker: %v", err)
	}
	if err := orchestration.waitForStart(context.Background(), time.Second); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("waitForStart(stale) error = %v, want run ID mismatch", err)
	}

	if _, err := newGRPCLoadOrchestration(""); err != nil {
		t.Fatalf("newGRPCLoadOrchestration(blank): %v", err)
	}
}

func TestGRPCLoadOrchestrationValidatesMeasuredWindow(t *testing.T) {
	orchestration, err := newGRPCLoadOrchestration(t.TempDir())
	if err != nil {
		t.Fatalf("newGRPCLoadOrchestration: %v", err)
	}
	if err := orchestration.writeMeasured(measuredGRPCLoadResult{}); err == nil {
		t.Fatal("writeMeasured(zero result) error = nil")
	}
	start := time.Now()
	if err := orchestration.writeMeasured(measuredGRPCLoadResult{
		measurementStart: start,
		admissionEnd:     start.Add(-time.Second),
		measurementEnd:   start,
	}); err == nil {
		t.Fatal("writeMeasured(out of order) error = nil")
	}
}

func readGRPCLoadMarker(t *testing.T, path string, marker *grpcLoadOrchestrationMarker) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read marker %s: %v", path, err)
	}
	if err := json.Unmarshal(data, marker); err != nil {
		t.Fatalf("decode marker %s: %v", path, err)
	}
}
