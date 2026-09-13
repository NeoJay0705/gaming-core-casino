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
	pushClientsReadyMarker    = "clients-ready.json"
	pushStartMarker           = "start"
	pushWarmupStartedMarker   = "warmup-started.json"
	pushWarmupResultMarker    = "warmup-result.json"
	pushWarmupDrainedMarker   = "warmup-drained.json"
	pushBaselineReadyMarker   = "baseline-ready.json"
	pushMeasuredStartedMarker = "measured-started.json"
	pushFinalReadyMarker      = "final-ready.json"
	pushFinalScrapeMarker     = "final-scrape.json"
)

// pushOrchestration 是 example validation 的最小 filesystem barrier；不屬於
// framework API，也不取代 workload 的 control protocol。
type pushOrchestration struct {
	dir string
}

type pushOrchestrationMarker struct {
	Timestamp   string `json:"timestamp"`
	RunID       string `json:"run_id,omitempty"`
	Connections int    `json:"connections,omitempty"`
	Planned     uint64 `json:"planned"`
	Attempted   uint64 `json:"attempted"`
	Success     uint64 `json:"success"`
	Partial     uint64 `json:"partial"`
	Error       uint64 `json:"error"`
	Missed      uint64 `json:"missed"`
	Received    uint64 `json:"received"`
	Duplicate   uint64 `json:"duplicate"`
	SequenceGap uint64 `json:"sequence_gap"`
	Invalid     uint64 `json:"invalid"`
	Missing     uint64 `json:"missing"`
	ReaderFails uint64 `json:"reader_failures"`
}

func newPushOrchestration(dir string) (*pushOrchestration, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create orchestration directory: %w", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("stat orchestration directory: %w", err)
	}
	if !info.IsDir() {
		return nil, errors.New("orchestration path is not a directory")
	}
	for _, marker := range []string{pushStartMarker, pushWarmupResultMarker, pushWarmupDrainedMarker, pushBaselineReadyMarker, pushFinalReadyMarker, pushFinalScrapeMarker} {
		if _, err := os.Lstat(filepath.Join(dir, marker)); err == nil {
			return nil, fmt.Errorf("orchestration marker %q already exists", marker)
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("check orchestration marker %q: %w", marker, err)
		}
	}
	return &pushOrchestration{dir: dir}, nil
}

// waitForWarmupResult 等待 script 解析 Game completion；檢查 counters 的
// 邊界，避免截斷或 stale marker 讓下一個 phase 提前開始。
func (o *pushOrchestration) waitForWarmupResult(ctx context.Context, timeout time.Duration, runID string, expectedPlanned uint64) (pushWarmupSummary, error) {
	if o == nil {
		return pushWarmupSummary{}, nil
	}
	if timeout <= 0 {
		return pushWarmupSummary{}, errors.New("orchestration warm-up result timeout must be positive")
	}
	if strings.TrimSpace(runID) == "" {
		return pushWarmupSummary{}, errors.New("orchestration warm-up result run ID is required")
	}
	if expectedPlanned == 0 {
		return pushWarmupSummary{}, errors.New("orchestration warm-up result expected planned ticks must be positive")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	waitContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		data, err := os.ReadFile(filepath.Join(o.dir, pushWarmupResultMarker))
		if err == nil {
			var marker pushOrchestrationMarker
			if decodeErr := json.Unmarshal(data, &marker); decodeErr != nil {
				return pushWarmupSummary{}, fmt.Errorf("decode orchestration warm-up result: %w", decodeErr)
			}
			if marker.RunID != runID {
				return pushWarmupSummary{}, fmt.Errorf("orchestration warm-up result run ID %q does not match %q", marker.RunID, runID)
			}
			if err := marker.validateWarmupSummary(); err != nil {
				return pushWarmupSummary{}, err
			}
			if marker.Planned != expectedPlanned {
				return pushWarmupSummary{}, fmt.Errorf("orchestration warm-up result planned ticks %d does not match response %d", marker.Planned, expectedPlanned)
			}
			return pushWarmupSummary{
				planned: marker.Planned, attempted: marker.Attempted, success: marker.Success,
				partial: marker.Partial, err: marker.Error, missed: marker.Missed,
			}, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return pushWarmupSummary{}, fmt.Errorf("read orchestration warm-up result: %w", err)
		}
		select {
		case <-ticker.C:
		case <-waitContext.Done():
			return pushWarmupSummary{}, waitContext.Err()
		}
	}
}

func (m pushOrchestrationMarker) validateWarmupSummary() error {
	if m.Planned == 0 {
		return errors.New("orchestration warm-up result planned ticks must be positive")
	}
	if m.Attempted > m.Planned || m.Success > m.Attempted || m.Partial > m.Attempted || m.Error > m.Attempted || m.Missed > m.Planned {
		return errors.New("orchestration warm-up result counters are out of range")
	}
	remaining := m.Attempted
	if m.Success > remaining {
		return errors.New("orchestration warm-up result counters do not sum to attempted")
	}
	remaining -= m.Success
	if m.Partial > remaining {
		return errors.New("orchestration warm-up result counters do not sum to attempted")
	}
	remaining -= m.Partial
	if m.Error != remaining {
		return errors.New("orchestration warm-up result counters do not sum to attempted")
	}
	if m.Attempted != m.Planned-m.Missed {
		return errors.New("orchestration warm-up result attempted and missed do not cover planned ticks")
	}
	return nil
}

type pushWarmupSummary struct {
	planned, attempted, success, partial, err, missed uint64
}

// writeWarmupDrained 宣告 load reader 已完成 warm-up quiet drain。summary
// 保留 workload evidence，供 script 與 run-status 使用。
func (o *pushOrchestration) writeWarmupDrained(runID string, stats pushLoadStats, missing uint64) error {
	if o == nil {
		return nil
	}
	if strings.TrimSpace(runID) == "" {
		return errors.New("orchestration warm-up drained run ID is required")
	}
	return o.writeJSON(pushWarmupDrainedMarker, pushOrchestrationMarker{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano), RunID: runID,
		Received: stats.received, Duplicate: stats.duplicate, SequenceGap: stats.sequenceGap,
		Invalid: stats.invalid, Missing: missing, ReaderFails: stats.readerFailures,
	})
}

func (o *pushOrchestration) writeClientsReady(connections int) error {
	if o == nil {
		return nil
	}
	if connections <= 0 {
		return errors.New("orchestration connections must be positive")
	}
	return o.writeJSON(pushClientsReadyMarker, pushOrchestrationMarker{
		Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
		Connections: connections,
	})
}

func (o *pushOrchestration) waitForStart(ctx context.Context) error {
	if o == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		info, err := os.Stat(filepath.Join(o.dir, pushStartMarker))
		if err == nil {
			if info.IsDir() {
				return errors.New("orchestration start marker is a directory")
			}
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat orchestration start marker: %w", err)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// waitForBaseline 讓正式 baseline 的 measured control 僅能在外部 collector
// 已保存 warm-up 後 snapshot、完成 phase event 後送出。未使用 orchestration
// 的手動 run 不需要這個 barrier，也不應被當成可比較 baseline。
func (o *pushOrchestration) waitForBaseline(ctx context.Context, timeout time.Duration, runID string) error {
	if o == nil {
		return nil
	}
	if timeout <= 0 {
		return errors.New("orchestration baseline timeout must be positive")
	}
	if strings.TrimSpace(runID) == "" {
		return errors.New("orchestration baseline run ID is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	waitContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		info, err := os.Stat(filepath.Join(o.dir, pushBaselineReadyMarker))
		if err == nil {
			if info.IsDir() {
				return errors.New("orchestration baseline marker is a directory")
			}
			data, readErr := os.ReadFile(filepath.Join(o.dir, pushBaselineReadyMarker))
			if readErr != nil {
				return fmt.Errorf("read orchestration baseline marker: %w", readErr)
			}
			var marker pushOrchestrationMarker
			if readErr := json.Unmarshal(data, &marker); readErr != nil {
				return fmt.Errorf("decode orchestration baseline marker: %w", readErr)
			}
			if marker.RunID != runID {
				return fmt.Errorf("orchestration baseline marker run ID %q does not match %q", marker.RunID, runID)
			}
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat orchestration baseline marker: %w", err)
		}
		select {
		case <-ticker.C:
		case <-waitContext.Done():
			return waitContext.Err()
		}
	}
}

// writeFinalReady 宣告 load 已完成 measured drain 與 summary，並在
// orchestrator 保存 final metrics 前暫停關閉 reader／metrics listener。
func (o *pushOrchestration) writeFinalReady(runID string) error {
	if o == nil {
		return nil
	}
	if strings.TrimSpace(runID) == "" {
		return errors.New("orchestration final run ID is required")
	}
	return o.writeJSON(pushFinalReadyMarker, pushOrchestrationMarker{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		RunID:     runID,
	})
}

// waitForFinalScrape 等待 orchestrator 完成 final metrics snapshot；run ID
// 必須相同，避免 stale marker 讓 load 提前關閉 reader。
func (o *pushOrchestration) waitForFinalScrape(ctx context.Context, timeout time.Duration, runID string) error {
	if o == nil {
		return nil
	}
	if timeout <= 0 {
		return errors.New("orchestration final scrape timeout must be positive")
	}
	if strings.TrimSpace(runID) == "" {
		return errors.New("orchestration final scrape run ID is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	waitContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		data, err := os.ReadFile(filepath.Join(o.dir, pushFinalScrapeMarker))
		if err == nil {
			var marker pushOrchestrationMarker
			if decodeErr := json.Unmarshal(data, &marker); decodeErr != nil {
				return fmt.Errorf("decode orchestration final scrape marker: %w", decodeErr)
			}
			if marker.RunID != runID {
				return fmt.Errorf("orchestration final scrape marker run ID %q does not match %q", marker.RunID, runID)
			}
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("read orchestration final scrape marker: %w", err)
		}
		select {
		case <-ticker.C:
		case <-waitContext.Done():
			return waitContext.Err()
		}
	}
}

// writeFinalScrape 由 orchestrator 在 final metrics 已落盤後呼叫。
func (o *pushOrchestration) writeFinalScrape(runID string) error {
	if o == nil {
		return nil
	}
	if strings.TrimSpace(runID) == "" {
		return errors.New("orchestration final scrape run ID is required")
	}
	return o.writeJSON(pushFinalScrapeMarker, pushOrchestrationMarker{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		RunID:     runID,
	})
}

func (o *pushOrchestration) writeRunStarted(name, runID string) error {
	if o == nil {
		return nil
	}
	if runID == "" {
		return errors.New("orchestration run ID is required")
	}
	switch name {
	case pushWarmupStartedMarker, pushMeasuredStartedMarker:
	default:
		return fmt.Errorf("unsupported orchestration marker %q", name)
	}
	return o.writeJSON(name, pushOrchestrationMarker{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		RunID:     runID,
	})
}

func (o *pushOrchestration) writeJSON(name string, marker pushOrchestrationMarker) error {
	if o == nil {
		return nil
	}
	data, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(o.dir, ".marker-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer func() {
		_ = os.Remove(temporaryName)
	}()
	if err := temporary.Chmod(0o640); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(append(data, '\n')); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, filepath.Join(o.dir, name))
}
