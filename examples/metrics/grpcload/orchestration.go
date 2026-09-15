package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	grpcLoadReadyMarker        = "grpc-ready.json"
	grpcLoadStartMarker        = "grpc-start.json"
	grpcLoadMeasuredMarker     = "grpc-measured.json"
	grpcLoadFinalScrapedMarker = "grpc-final-scraped.json"
	grpcLoadOrchestrationPoll  = 20 * time.Millisecond
	grpcLoadOrchestrationGrace = 2 * time.Minute
)

// grpcLoadOrchestration 是 direct gRPC validation 專用的 filesystem
// barrier；不屬於 framework API，也不改變未設定 directory 時的行為。
type grpcLoadOrchestration struct {
	dir   string
	runID string
}

type grpcLoadOrchestrationMarker struct {
	RunID              string  `json:"run_id"`
	Timestamp          string  `json:"timestamp"`
	ClientConnections  int     `json:"client_connections,omitempty"`
	WarmupRequests     int     `json:"warmup_requests,omitempty"`
	MeasurementStart   string  `json:"measurement_start,omitempty"`
	AdmissionEnd       string  `json:"admission_end,omitempty"`
	MeasurementEnd     string  `json:"measurement_end,omitempty"`
	AdmissionDuration  float64 `json:"admission_duration_seconds"`
	MeasuredDuration   float64 `json:"measured_duration_seconds"`
	TerminalDrain      float64 `json:"terminal_drain_duration_seconds"`
	SuccessfulRequests uint64  `json:"successful_requests"`
	FailedWorkers      uint64  `json:"failed_workers"`
}

func newGRPCLoadOrchestration(dir string) (*grpcLoadOrchestration, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create gRPC load orchestration directory: %w", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("stat gRPC load orchestration directory: %w", err)
	}
	if !info.IsDir() {
		return nil, errors.New("gRPC load orchestration path is not a directory")
	}
	for _, marker := range []string{grpcLoadReadyMarker, grpcLoadStartMarker, grpcLoadMeasuredMarker, grpcLoadFinalScrapedMarker} {
		if _, err := os.Lstat(filepath.Join(dir, marker)); err == nil {
			return nil, fmt.Errorf("gRPC load orchestration marker %q already exists", marker)
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("check gRPC load orchestration marker %q: %w", marker, err)
		}
	}
	return &grpcLoadOrchestration{
		dir:   dir,
		runID: fmt.Sprintf("grpc-load-%d", time.Now().UnixNano()),
	}, nil
}

func (o *grpcLoadOrchestration) writeReady(clientConnections, warmupRequests int) error {
	if o == nil {
		return nil
	}
	if clientConnections <= 0 {
		return errors.New("gRPC load orchestration client connections must be positive")
	}
	if warmupRequests < 0 {
		return errors.New("gRPC load orchestration warm-up requests must not be negative")
	}
	return o.writeJSON(grpcLoadReadyMarker, grpcLoadOrchestrationMarker{
		RunID:             o.runID,
		Timestamp:         time.Now().UTC().Format(time.RFC3339Nano),
		ClientConnections: clientConnections,
		WarmupRequests:    warmupRequests,
	})
}

func (o *grpcLoadOrchestration) waitForStart(ctx context.Context, timeout time.Duration) error {
	if o == nil {
		return nil
	}
	return o.waitForMarker(ctx, timeout, grpcLoadStartMarker, "start")
}

func (o *grpcLoadOrchestration) writeMeasured(result measuredGRPCLoadResult) error {
	if o == nil {
		return nil
	}
	if result.measurementStart.IsZero() || result.admissionEnd.IsZero() || result.measurementEnd.IsZero() {
		return errors.New("gRPC load orchestration measured timestamps are required")
	}
	if result.admissionEnd.Before(result.measurementStart) || result.measurementEnd.Before(result.measurementStart) {
		return errors.New("gRPC load orchestration measured timestamps are out of order")
	}
	terminalDrain := result.measurementEnd.Sub(result.admissionEnd)
	if terminalDrain < 0 {
		terminalDrain = 0
	}
	return o.writeJSON(grpcLoadMeasuredMarker, grpcLoadOrchestrationMarker{
		RunID:              o.runID,
		Timestamp:          time.Now().UTC().Format(time.RFC3339Nano),
		MeasurementStart:   result.measurementStart.UTC().Format(time.RFC3339Nano),
		AdmissionEnd:       result.admissionEnd.UTC().Format(time.RFC3339Nano),
		MeasurementEnd:     result.measurementEnd.UTC().Format(time.RFC3339Nano),
		AdmissionDuration:  result.admissionEnd.Sub(result.measurementStart).Seconds(),
		MeasuredDuration:   result.measurementEnd.Sub(result.measurementStart).Seconds(),
		TerminalDrain:      terminalDrain.Seconds(),
		SuccessfulRequests: result.successful,
		FailedWorkers:      result.failed,
	})
}

func (o *grpcLoadOrchestration) waitForFinalScraped(ctx context.Context, timeout time.Duration) error {
	if o == nil {
		return nil
	}
	return o.waitForMarker(ctx, timeout, grpcLoadFinalScrapedMarker, "final scrape")
}

func (o *grpcLoadOrchestration) writeFinalScraped() error {
	if o == nil {
		return nil
	}
	return o.writeJSON(grpcLoadFinalScrapedMarker, grpcLoadOrchestrationMarker{
		RunID:     o.runID,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func (o *grpcLoadOrchestration) waitForMarker(ctx context.Context, timeout time.Duration, name, description string) error {
	if timeout <= 0 {
		timeout = grpcLoadOrchestrationGrace
	}
	if ctx == nil {
		ctx = context.Background()
	}
	waitContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(grpcLoadOrchestrationPoll)
	defer ticker.Stop()
	for {
		data, err := os.ReadFile(filepath.Join(o.dir, name))
		if err == nil {
			var marker grpcLoadOrchestrationMarker
			if decodeErr := json.Unmarshal(data, &marker); decodeErr != nil {
				return fmt.Errorf("decode gRPC load orchestration %s marker: %w", description, decodeErr)
			}
			if marker.RunID == "" || marker.Timestamp == "" {
				return fmt.Errorf("gRPC load orchestration %s marker is missing run ID or timestamp", description)
			}
			if _, parseErr := time.Parse(time.RFC3339Nano, marker.Timestamp); parseErr != nil {
				return fmt.Errorf("gRPC load orchestration %s marker timestamp is invalid: %w", description, parseErr)
			}
			if marker.RunID != o.runID {
				return fmt.Errorf("gRPC load orchestration %s marker run ID %q does not match %q", description, marker.RunID, o.runID)
			}
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("read gRPC load orchestration %s marker: %w", description, err)
		}
		select {
		case <-ticker.C:
		case <-waitContext.Done():
			return waitContext.Err()
		}
	}
}

func (o *grpcLoadOrchestration) writeJSON(name string, marker grpcLoadOrchestrationMarker) error {
	if o == nil {
		return nil
	}
	data, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("marshal gRPC load orchestration marker: %w", err)
	}
	temporary, err := os.CreateTemp(o.dir, ".grpc-load-marker-*")
	if err != nil {
		return fmt.Errorf("create gRPC load orchestration marker: %w", err)
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()
	if err := temporary.Chmod(0o640); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("chmod gRPC load orchestration marker: %w", err)
	}
	if _, err := temporary.Write(append(data, '\n')); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write gRPC load orchestration marker: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync gRPC load orchestration marker: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close gRPC load orchestration marker: %w", err)
	}
	if err := os.Rename(temporaryName, filepath.Join(o.dir, name)); err != nil {
		return fmt.Errorf("publish gRPC load orchestration marker %q: %w", name, err)
	}
	return nil
}
