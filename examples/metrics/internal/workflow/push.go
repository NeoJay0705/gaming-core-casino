package workflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/protocol"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/logging"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/dig"
	"google.golang.org/protobuf/proto"
)

const (
	maxPushRunIDBytes     = 128
	maxPushRoomIDBytes    = 128
	maxPushLoginNameBytes = 256
	maxPushPlayers        = 1000
	maxPushPayloadBytes   = serversend.DefaultMaxPayloadBytes
	maxPushDuration       = time.Hour

	pushModeBroadcast = "broadcast"
	pushModePlayer    = "player"
)

var (
	errPushRunnerNotStarted = errors.New("push runner is not started")
	errPushRunnerActive     = errors.New("push runner already has an active workload")
)

// pushClock 是 scheduler 的最小測試 seam；production 使用 realPushClock，
// contract test 可用 fake clock 驗證 tick cadence 而不等待實際秒數。
type pushClock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type realPushClock struct{}

func (realPushClock) Now() time.Time                                { return time.Now() }
func (realPushClock) After(duration time.Duration) <-chan time.Time { return time.After(duration) }

type pushRunnerMetrics struct {
	missedTicks *prometheus.CounterVec
}

func newPushRunnerMetrics(registerer prometheus.Registerer) (*pushRunnerMetrics, error) {
	if registerer == nil {
		return nil, errors.New("push runner metrics: registerer is nil")
	}
	metrics := &pushRunnerMetrics{
		missedTicks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gaming_core_example_push_missed_ticks_total",
			Help: "Total number of fixed-rate push ticks skipped because the previous sender call was still running.",
		}, []string{"mode"}),
	}
	if err := registerer.Register(metrics.missedTicks); err != nil {
		return nil, fmt.Errorf("register push runner metrics: %w", err)
	}
	// 讓 baseline scrape 在第一個 missed tick 前即可辨識完整 bounded
	// metric family；沒有 missed 時仍輸出零值，不增加 label cardinality。
	for _, mode := range []string{pushModeBroadcast, pushModePlayer} {
		metrics.missedTicks.WithLabelValues(mode)
	}
	return metrics, nil
}

type pushRunnerInputs struct {
	dig.In

	Broadcast  serversend.BroadcastSender
	Player     serversend.PlayerSender
	Registerer prometheus.Registerer
	Factory    *logging.Factory `optional:"true"`
}

// pushRunner 是 example-only 的 bounded workload lifecycle；它不是通用
// scheduler。每個 runner 一次只接受一個 workload，且 sender call 不重疊。
type pushRunner struct {
	broadcast serversend.BroadcastSender
	player    serversend.PlayerSender
	metrics   *pushRunnerMetrics
	logger    *logging.Logger
	clock     pushClock

	mu      sync.Mutex
	started bool
	active  bool
	cancel  context.CancelFunc
	done    chan struct{}
}

func newPushRunner(inputs pushRunnerInputs) (*pushRunner, error) {
	if inputs.Broadcast == nil || inputs.Player == nil {
		return nil, errors.New("push runner: sender dependencies are required")
	}
	metrics, err := newPushRunnerMetrics(inputs.Registerer)
	if err != nil {
		return nil, err
	}
	var logger *logging.Logger
	if inputs.Factory != nil {
		logger, err = inputs.Factory.Component("example.push")
		if err != nil {
			return nil, fmt.Errorf("push runner logger: %w", err)
		}
	}
	return &pushRunner{
		broadcast: inputs.Broadcast,
		player:    inputs.Player,
		metrics:   metrics,
		logger:    logger,
		clock:     realPushClock{},
	}, nil
}

func newPushRunnerHook(runner *pushRunner) framework.Hook {
	return framework.Hook{
		Name:  "example-push-runner",
		Phase: framework.PhaseService,
		OnStart: func(ctx context.Context) error {
			return runner.Start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			return runner.Stop(ctx)
		},
	}
}

func (r *pushRunner) Start(context.Context) error {
	if r == nil {
		return errors.New("push runner is nil")
	}
	r.mu.Lock()
	r.started = true
	r.mu.Unlock()
	return nil
}

func (r *pushRunner) Stop(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	r.started = false
	cancel := r.cancel
	done := r.done
	r.cancel = nil
	r.done = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type pushWorkload struct {
	runID       string
	mode        protocol.PushMode
	roomID      string
	loginNames  []serversend.LoginName
	interval    time.Duration
	duration    time.Duration
	payloadSize int
	planned     uint64
}

func (r *pushRunner) Submit(ctx context.Context, request *protocol.StartPushRequest) (uint64, error) {
	workload, err := normalizePushWorkload(request)
	if err != nil {
		return 0, err
	}
	if r == nil {
		return 0, errors.New("push runner is nil")
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.started {
		return 0, errPushRunnerNotStarted
	}
	if r.active {
		return 0, errPushRunnerActive
	}
	if r.clock == nil {
		r.clock = realPushClock{}
	}
	// Request context 會隨原始 unary 返回而取消；只保留 W3C trace，並由
	// runner 自己的 cancel 與 lifecycle Stop 管理這段 bounded workload。
	base := logging.Detach(ctx)
	runCtx, cancel := context.WithCancel(base)
	done := make(chan struct{})
	r.active = true
	r.cancel = cancel
	r.done = done
	go r.run(runCtx, workload, done)
	return workload.planned, nil
}

func normalizePushWorkload(request *protocol.StartPushRequest) (pushWorkload, error) {
	if request == nil {
		return pushWorkload{}, errors.New("push request is required")
	}
	runID := strings.TrimSpace(request.GetRunId())
	if runID == "" || len(runID) > maxPushRunIDBytes {
		return pushWorkload{}, fmt.Errorf("push request: run_id is required and must be at most %d bytes", maxPushRunIDBytes)
	}
	mode := request.GetMode()
	if mode != protocol.PushMode_PUSH_MODE_BROADCAST && mode != protocol.PushMode_PUSH_MODE_PLAYER {
		return pushWorkload{}, errors.New("push request: mode must be broadcast or player")
	}
	interval, err := millisDuration(request.GetIntervalMillis())
	if err != nil {
		return pushWorkload{}, fmt.Errorf("push request: interval: %w", err)
	}
	if interval != 16*time.Millisecond && interval != 33*time.Millisecond {
		return pushWorkload{}, errors.New("push request: interval must be 16ms or 33ms")
	}
	duration, err := millisDuration(request.GetDurationMillis())
	if err != nil {
		return pushWorkload{}, fmt.Errorf("push request: duration: %w", err)
	}
	if duration <= 0 || duration < interval {
		return pushWorkload{}, errors.New("push request: duration must be at least one interval")
	}
	if duration > maxPushDuration {
		return pushWorkload{}, fmt.Errorf("push request: duration must not exceed %s", maxPushDuration)
	}
	if request.GetPayloadBytes() > maxPushPayloadBytes {
		return pushWorkload{}, fmt.Errorf("push request: payload exceeds %d bytes", maxPushPayloadBytes)
	}
	roomID := strings.TrimSpace(request.GetRoomId())
	if mode == protocol.PushMode_PUSH_MODE_BROADCAST && (roomID == "" || len(roomID) > maxPushRoomIDBytes) {
		return pushWorkload{}, fmt.Errorf("push request: broadcast room_id is required and must be at most %d bytes", maxPushRoomIDBytes)
	}
	loginNames := make([]serversend.LoginName, 0, len(request.GetLoginNames()))
	seen := make(map[serversend.LoginName]struct{}, len(request.GetLoginNames()))
	for _, value := range request.GetLoginNames() {
		if value == "" || len(value) > maxPushLoginNameBytes || strings.TrimSpace(value) == "" {
			return pushWorkload{}, fmt.Errorf("push request: login name must be non-empty and at most %d bytes", maxPushLoginNameBytes)
		}
		name := serversend.LoginName(value)
		if _, exists := seen[name]; exists {
			return pushWorkload{}, fmt.Errorf("push request: duplicate login name %q", value)
		}
		seen[name] = struct{}{}
		loginNames = append(loginNames, name)
	}
	if len(loginNames) > maxPushPlayers {
		return pushWorkload{}, fmt.Errorf("push request: at most %d login names are allowed", maxPushPlayers)
	}
	if mode == protocol.PushMode_PUSH_MODE_PLAYER && len(loginNames) == 0 {
		return pushWorkload{}, fmt.Errorf("push request: 1 to %d login names are required for player mode", maxPushPlayers)
	}
	planned := uint64(duration / interval)
	if planned == 0 {
		return pushWorkload{}, errors.New("push request: planned tick count is zero")
	}
	// 接受 workload 前先驗證一筆 encoded client message。Player batch 仍可能
	// 由既有 sender 拆批，但每筆 opaque client payload 必須符合共用 1 MiB 上限。
	sample, err := proto.Marshal(&protocol.PushMessage{
		RunId:        runID,
		Sequence:     1,
		SentUnixNano: time.Now().UnixNano(),
		Payload:      make([]byte, request.GetPayloadBytes()),
	})
	if err != nil {
		return pushWorkload{}, fmt.Errorf("push request: encode payload: %w", err)
	}
	if len(sample) > serversend.DefaultMaxPayloadBytes {
		return pushWorkload{}, serversend.ErrPayloadTooLarge
	}
	return pushWorkload{
		runID:       runID,
		mode:        mode,
		roomID:      roomID,
		loginNames:  loginNames,
		interval:    interval,
		duration:    duration,
		payloadSize: int(request.GetPayloadBytes()),
		planned:     planned,
	}, nil
}

func millisDuration(value uint64) (time.Duration, error) {
	if value > uint64((time.Duration(1<<63-1))/time.Millisecond) {
		return 0, errors.New("value overflows time.Duration")
	}
	return time.Duration(value) * time.Millisecond, nil
}

func (r *pushRunner) run(ctx context.Context, workload pushWorkload, done chan<- struct{}) {
	defer close(done)
	defer func() {
		r.mu.Lock()
		r.active = false
		r.mu.Unlock()
	}()

	startedAt := r.clock.Now()
	lastCallEnd := time.Time{}
	sequence := uint64(0)
	var attempted, success, partial, failed, missed uint64
	var encodedPayloadBytes uint64
	mode := pushModeName(workload.mode)
	for tick := uint64(0); tick < workload.planned; {
		scheduledAt := startedAt.Add(time.Duration(tick) * workload.interval)
		if !r.waitUntil(ctx, scheduledAt) {
			return
		}
		// timer 喚醒可能稍晚；只有前一次同步 sender call 跨過下一個排程點時
		// 才跳過 ticks。
		if tick > 0 && lastCallEnd.After(scheduledAt) {
			next := uint64(lastCallEnd.Sub(startedAt)/workload.interval) + 1
			if next <= tick {
				next = tick + 1
			}
			if next > workload.planned {
				next = workload.planned
			}
			skipped := next - tick
			missed += skipped
			if r.metrics != nil {
				r.metrics.missedTicks.WithLabelValues(mode).Add(float64(skipped))
			}
			tick = next
			continue
		}

		sequence++
		attempted++
		sentAt := r.clock.Now()
		payload, err := proto.Marshal(&protocol.PushMessage{
			RunId:        workload.runID,
			Sequence:     sequence,
			SentUnixNano: sentAt.UnixNano(),
			Payload:      make([]byte, workload.payloadSize),
		})
		if err == nil {
			if encodedPayloadBytes == 0 {
				// 記錄第一個實際送出的 PushMessage wire size；不記錄
				// payload 內容，供 validation metadata 核對編碼邊界。
				encodedPayloadBytes = uint64(len(payload))
			}
			var receipt serversend.Receipt
			switch workload.mode {
			case protocol.PushMode_PUSH_MODE_BROADCAST:
				receipt, err = BroadcastRoom(ctx, r.broadcast, workload.roomID, []serversend.Message{{
					CommandID: protocol.PushMessageCommandID,
					Payload:   payload,
				}})
			case protocol.PushMode_PUSH_MODE_PLAYER:
				receipt, err = r.sendPlayers(ctx, workload.loginNames, payload)
			}
			if err == nil {
				success++
			} else if !receipt.AcceptedAt.IsZero() {
				partial++
			} else {
				failed++
			}
			if err != nil && r.logger != nil {
				r.logger.Error(ctx, "push_send", "example push sender failed", err,
					slog.String("run_id", workload.runID), slog.String("mode", mode), slog.Uint64("sequence", sequence))
			}
		} else {
			failed++
			if r.logger != nil {
				r.logger.Error(ctx, "push_encode", "encode example push message failed", err,
					slog.String("run_id", workload.runID), slog.String("mode", mode))
			}
		}
		lastCallEnd = r.clock.Now()
		tick++
	}
	if r.logger != nil {
		completedAt := r.clock.Now()
		r.logger.Info(ctx, "push_complete", "example push workload completed",
			slog.String("run_id", workload.runID), slog.String("mode", mode),
			slog.Duration("interval", workload.interval), slog.Duration("duration", workload.duration),
			slog.Time("started_at", startedAt), slog.Time("completed_at", completedAt),
			slog.Uint64("payload_bytes", uint64(workload.payloadSize)), slog.Uint64("encoded_payload_bytes", encodedPayloadBytes),
			slog.Uint64("targets", uint64(pushTargetCount(workload))), slog.Uint64("planned_ticks", workload.planned),
			slog.Uint64("attempted", attempted), slog.Uint64("success", success),
			slog.Uint64("partial", partial), slog.Uint64("error", failed), slog.Uint64("missed", missed))
	}
}

func (r *pushRunner) waitUntil(ctx context.Context, deadline time.Time) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	delay := deadline.Sub(r.clock.Now())
	if delay <= 0 {
		return ctx.Err() == nil
	}
	select {
	case <-r.clock.After(delay):
		return ctx.Err() == nil
	case <-ctx.Done():
		return false
	}
}

func (r *pushRunner) sendPlayers(ctx context.Context, loginNames []serversend.LoginName, payload []byte) (serversend.Receipt, error) {
	messages := make([]serversend.PlayerMessage, len(loginNames))
	for index, loginName := range loginNames {
		messages[index] = serversend.PlayerMessage{
			LoginName: loginName,
			Message: serversend.Message{
				CommandID: protocol.PushMessageCommandID,
				Payload:   append([]byte(nil), payload...),
			},
		}
	}
	return r.player.SendToPlayers(ctx, messages)
}

func pushModeName(mode protocol.PushMode) string {
	if mode == protocol.PushMode_PUSH_MODE_PLAYER {
		return pushModePlayer
	}
	return pushModeBroadcast
}

func pushTargetCount(workload pushWorkload) int {
	return len(workload.loginNames)
}
