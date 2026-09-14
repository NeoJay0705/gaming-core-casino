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
	echoReadyMarker        = "echo-ready.json"
	echoStartMarker        = "echo-start.json"
	echoMeasuredMarker     = "echo-measured.json"
	echoFinalScrapedMarker = "echo-final-scraped.json"
	echoOrchestrationPoll  = 20 * time.Millisecond
	echoOrchestrationGrace = 2 * time.Minute
)

// echoOrchestration 是 example validation 專用的 filesystem barrier；不屬於
// framework API，也不改變未設定 orchestration directory 時的 load 行為。
type echoOrchestration struct {
	dir   string
	runID string
}

type echoOrchestrationMarker struct {
	RunID              string  `json:"run_id"`
	Timestamp          string  `json:"timestamp"`
	Connections        int     `json:"connections,omitempty"`
	WarmupRequests     int     `json:"warmup_requests,omitempty"`
	MeasurementStart   string  `json:"measurement_start,omitempty"`
	AdmissionEnd       string  `json:"admission_end,omitempty"`
	MeasurementEnd     string  `json:"measurement_end,omitempty"`
	AdmissionDuration  float64 `json:"admission_duration_seconds"`
	MeasuredDuration   float64 `json:"measured_duration_seconds"`
	TerminalDrain      float64 `json:"terminal_drain_duration_seconds"`
	SuccessfulRequests uint64  `json:"successful_echo_requests"`
	ConnectionFailures uint64  `json:"connection_failures"`
}

func newEchoOrchestration(dir string) (*echoOrchestration, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create echo orchestration directory: %w", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("stat echo orchestration directory: %w", err)
	}
	if !info.IsDir() {
		return nil, errors.New("echo orchestration path is not a directory")
	}
	for _, marker := range []string{echoReadyMarker, echoStartMarker, echoMeasuredMarker, echoFinalScrapedMarker} {
		if _, err := os.Lstat(filepath.Join(dir, marker)); err == nil {
			return nil, fmt.Errorf("echo orchestration marker %q already exists", marker)
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("check echo orchestration marker %q: %w", marker, err)
		}
	}
	return &echoOrchestration{
		dir:   dir,
		runID: fmt.Sprintf("echo-%d", time.Now().UnixNano()),
	}, nil
}

func (o *echoOrchestration) writeReady(connections, warmupRequests int) error {
	if o == nil {
		return nil
	}
	if connections <= 0 {
		return errors.New("echo orchestration connections must be positive")
	}
	if warmupRequests < 0 {
		return errors.New("echo orchestration warm-up requests must not be negative")
	}
	return o.writeJSON(echoReadyMarker, echoOrchestrationMarker{
		RunID:          o.runID,
		Timestamp:      time.Now().UTC().Format(time.RFC3339Nano),
		Connections:    connections,
		WarmupRequests: warmupRequests,
	})
}

func (o *echoOrchestration) waitForStart(ctx context.Context, timeout time.Duration) error {
	if o == nil {
		return nil
	}
	return o.waitForMarker(ctx, timeout, echoStartMarker, "start")
}

func (o *echoOrchestration) writeMeasured(measurementStart, admissionEnd, measurementEnd time.Time, successfulRequests, connectionFailures uint64) error {
	if o == nil {
		return nil
	}
	if measurementStart.IsZero() || admissionEnd.IsZero() || measurementEnd.IsZero() {
		return errors.New("echo orchestration measured timestamps are required")
	}
	if admissionEnd.Before(measurementStart) || measurementEnd.Before(measurementStart) {
		return errors.New("echo orchestration measured timestamps are out of order")
	}
	terminalDrain := measurementEnd.Sub(admissionEnd)
	if terminalDrain < 0 {
		// 連線錯誤可能讓 worker 在 admission window 結束前提早 terminal；
		// 仍保存 marker 供 script 判定 request error，不把診斷證據變成
		// orchestration timeout。
		terminalDrain = 0
	}
	return o.writeJSON(echoMeasuredMarker, echoOrchestrationMarker{
		RunID:              o.runID,
		Timestamp:          time.Now().UTC().Format(time.RFC3339Nano),
		MeasurementStart:   measurementStart.UTC().Format(time.RFC3339Nano),
		AdmissionEnd:       admissionEnd.UTC().Format(time.RFC3339Nano),
		MeasurementEnd:     measurementEnd.UTC().Format(time.RFC3339Nano),
		AdmissionDuration:  admissionEnd.Sub(measurementStart).Seconds(),
		MeasuredDuration:   measurementEnd.Sub(measurementStart).Seconds(),
		TerminalDrain:      terminalDrain.Seconds(),
		SuccessfulRequests: successfulRequests,
		ConnectionFailures: connectionFailures,
	})
}

func (o *echoOrchestration) waitForFinalScraped(ctx context.Context, timeout time.Duration) error {
	if o == nil {
		return nil
	}
	return o.waitForMarker(ctx, timeout, echoFinalScrapedMarker, "final scrape")
}

func (o *echoOrchestration) writeFinalScraped() error {
	if o == nil {
		return nil
	}
	return o.writeJSON(echoFinalScrapedMarker, echoOrchestrationMarker{
		RunID:     o.runID,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func (o *echoOrchestration) waitForMarker(ctx context.Context, timeout time.Duration, name, description string) error {
	if timeout <= 0 {
		timeout = echoOrchestrationGrace
	}
	if ctx == nil {
		ctx = context.Background()
	}
	waitContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(echoOrchestrationPoll)
	defer ticker.Stop()
	for {
		data, err := os.ReadFile(filepath.Join(o.dir, name))
		if err == nil {
			var marker echoOrchestrationMarker
			if decodeErr := json.Unmarshal(data, &marker); decodeErr != nil {
				return fmt.Errorf("decode echo orchestration %s marker: %w", description, decodeErr)
			}
			if marker.RunID == "" || marker.Timestamp == "" {
				return fmt.Errorf("echo orchestration %s marker is missing run ID or timestamp", description)
			}
			if _, parseErr := time.Parse(time.RFC3339Nano, marker.Timestamp); parseErr != nil {
				return fmt.Errorf("echo orchestration %s marker timestamp is invalid: %w", description, parseErr)
			}
			if marker.RunID != o.runID {
				return fmt.Errorf("echo orchestration %s marker run ID %q does not match %q", description, marker.RunID, o.runID)
			}
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("read echo orchestration %s marker: %w", description, err)
		}
		select {
		case <-ticker.C:
		case <-waitContext.Done():
			return waitContext.Err()
		}
	}
}

func (o *echoOrchestration) writeJSON(name string, marker echoOrchestrationMarker) error {
	if o == nil {
		return nil
	}
	data, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(o.dir, ".echo-marker-*")
	if err != nil {
		return fmt.Errorf("create echo orchestration marker: %w", err)
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()
	if err := temporary.Chmod(0o640); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("chmod echo orchestration marker: %w", err)
	}
	if _, err := temporary.Write(append(data, '\n')); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write echo orchestration marker: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync echo orchestration marker: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close echo orchestration marker: %w", err)
	}
	if err := os.Rename(temporaryName, filepath.Join(o.dir, name)); err != nil {
		return fmt.Errorf("publish echo orchestration marker %q: %w", name, err)
	}
	return nil
}
