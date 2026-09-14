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

func TestEchoOrchestrationLifecycleUsesMatchingRunID(t *testing.T) {
	dir := t.TempDir()
	orchestration, err := newEchoOrchestration(dir)
	if err != nil {
		t.Fatalf("newEchoOrchestration: %v", err)
	}
	if err := orchestration.writeReady(3, 1); err != nil {
		t.Fatalf("writeReady: %v", err)
	}
	var ready echoOrchestrationMarker
	readEchoMarker(t, filepath.Join(dir, echoReadyMarker), &ready)
	if ready.RunID != orchestration.runID || ready.Connections != 3 || ready.WarmupRequests != 1 {
		t.Fatalf("ready marker = %#v, want run ID %q, connections 3, warm-up 1", ready, orchestration.runID)
	}

	if err := orchestration.writeJSON(echoStartMarker, echoOrchestrationMarker{RunID: orchestration.runID, Timestamp: time.Now().UTC().Format(time.RFC3339Nano)}); err != nil {
		t.Fatalf("write start marker: %v", err)
	}
	if err := orchestration.waitForStart(context.Background(), time.Second); err != nil {
		t.Fatalf("waitForStart: %v", err)
	}

	started := time.Now().Add(-time.Second)
	admissionEnd := started.Add(500 * time.Millisecond)
	finished := admissionEnd.Add(100 * time.Millisecond)
	if err := orchestration.writeMeasured(started, admissionEnd, finished, 42, 0); err != nil {
		t.Fatalf("writeMeasured: %v", err)
	}
	var measured echoOrchestrationMarker
	readEchoMarker(t, filepath.Join(dir, echoMeasuredMarker), &measured)
	if measured.RunID != orchestration.runID || measured.SuccessfulRequests != 42 || measured.ConnectionFailures != 0 {
		t.Fatalf("measured marker = %#v, want matching run ID and 42 successes", measured)
	}
	if measured.AdmissionDuration <= 0 || measured.MeasuredDuration <= 0 || measured.TerminalDrain <= 0 {
		t.Fatalf("measured durations = %#v, want positive admission, measured, and drain durations", measured)
	}

	if err := orchestration.writeFinalScraped(); err != nil {
		t.Fatalf("writeFinalScraped: %v", err)
	}
	if err := orchestration.waitForFinalScraped(context.Background(), time.Second); err != nil {
		t.Fatalf("waitForFinalScraped: %v", err)
	}
}

func TestEchoOrchestrationRejectsStaleMarkerAndTimesOut(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, echoReadyMarker), []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write stale marker: %v", err)
	}
	if _, err := newEchoOrchestration(dir); err == nil || !strings.Contains(err.Error(), echoReadyMarker) {
		t.Fatalf("newEchoOrchestration() error = %v, want stale marker error", err)
	}

	cleanDir := t.TempDir()
	orchestration, err := newEchoOrchestration(cleanDir)
	if err != nil {
		t.Fatalf("newEchoOrchestration(clean): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = orchestration.waitForStart(ctx, time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForStart() error = %v, want context deadline exceeded", err)
	}
	if err := orchestration.writeJSON(echoStartMarker, echoOrchestrationMarker{RunID: "stale", Timestamp: time.Now().UTC().Format(time.RFC3339Nano)}); err != nil {
		t.Fatalf("write stale start marker: %v", err)
	}
	if err := orchestration.waitForStart(context.Background(), time.Second); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("waitForStart(stale) error = %v, want run ID mismatch", err)
	}
}

func TestEchoOrchestrationValidatesMeasuredWindow(t *testing.T) {
	orchestration, err := newEchoOrchestration(t.TempDir())
	if err != nil {
		t.Fatalf("newEchoOrchestration: %v", err)
	}
	start := time.Now()
	if err := orchestration.writeMeasured(time.Time{}, start, start, 0, 0); err == nil {
		t.Fatal("writeMeasured(zero start) error = nil")
	}
	if err := orchestration.writeMeasured(start, start.Add(-time.Second), start, 0, 0); err == nil {
		t.Fatal("writeMeasured(out of order) error = nil")
	}
	if err := orchestration.writeMeasured(start, start.Add(time.Second), start.Add(500*time.Millisecond), 0, 0); err != nil {
		t.Fatalf("writeMeasured(early terminal) error = %v, want marker for failed workers", err)
	}
	var measured echoOrchestrationMarker
	readEchoMarker(t, filepath.Join(orchestration.dir, echoMeasuredMarker), &measured)
	if measured.TerminalDrain != 0 {
		t.Fatalf("early terminal drain = %v, want zero", measured.TerminalDrain)
	}
	if err := orchestration.writeMeasured(start.Add(time.Second), start, start, 0, 0); err == nil {
		t.Fatal("writeMeasured(end before start) error = nil")
	}
	if _, err := newEchoOrchestration(""); err != nil {
		t.Fatalf("newEchoOrchestration(blank): %v", err)
	}
}

func readEchoMarker(t *testing.T, path string, marker *echoOrchestrationMarker) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read marker %s: %v", path, err)
	}
	if err := json.Unmarshal(data, marker); err != nil {
		t.Fatalf("decode marker %s: %v", path, err)
	}
}
