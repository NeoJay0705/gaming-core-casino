package framework

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// 這個檔案對應設計 §14.4「Startup／rollback integration」。
//
// 用 fake hook 重現 §8.4 的 Game 啟動序列，驗證的是 framework 的編排契約——
// 順序、反向 rollback、one-shot state guard 與 Start／Stop 的序列化——而不是任何
// 具體 component 的 I/O。component 自身的 partial-failure 清理由各 package 的
// contract test 負責（§14.1）。

// gameStartupSequence 是 §8.4 的 Hook 名稱與 phase，順序即註冊順序。
var gameStartupSequence = []struct {
	name  string
	phase Phase
}{
	{"health-listener", PhaseInfrastructure},
	{"pprof-listener", PhaseInfrastructure},
	{"redis", PhaseInfrastructure},
	{"database", PhaseInfrastructure},
	{"rocketmq", PhaseInfrastructure},
	{"wallet", PhaseInfrastructure},
	{"metrics", PhaseInfrastructure},
	{"game-background-tasks", PhaseService},
	{"game-runtime-dependencies", PhaseService},
	{"game-order-schema", PhaseService},
	{"game-startup-steps", PhaseService},
	{"game-periodic-tasks", PhaseService},
	{"game-message-consumer", PhaseIngress},
	{"readiness", PhaseReadiness},
}

// lifecycleRecorder 收集 hook 回呼順序。App 的 Start／Stop 本身是序列化的，但
// concurrent 測試會從兩個 goroutine 觸發，因此記錄本身必須加鎖。
type lifecycleRecorder struct {
	mu     sync.Mutex
	events []string
	starts map[string]int
	stops  map[string]int
}

func newLifecycleRecorder() *lifecycleRecorder {
	return &lifecycleRecorder{starts: map[string]int{}, stops: map[string]int{}}
}

func (r *lifecycleRecorder) record(kind, name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, kind+":"+name)
	switch kind {
	case "start":
		r.starts[name]++
	case "stop":
		r.stops[name]++
	}
}

func (r *lifecycleRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

func (r *lifecycleRecorder) startCount(name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.starts[name]
}

func (r *lifecycleRecorder) stopCount(name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stops[name]
}

// newGameSequenceApp 依 §8.4 建出 App；failAt 非空時，該 hook 的 OnStart 回錯。
// delay 讓 concurrent 測試有機會在 Start 進行中插入 Stop。
func newGameSequenceApp(t *testing.T, recorder *lifecycleRecorder, failAt string, startErr error, delay time.Duration) *App {
	t.Helper()
	app, err := New(func(r Registry) error {
		for _, item := range gameStartupSequence {
			hook := item
			if err := r.AddHook(func() Hook {
				return Hook{
					Name:  hook.name,
					Phase: hook.phase,
					OnStart: func(context.Context) error {
						if delay > 0 {
							time.Sleep(delay)
						}
						recorder.record("start", hook.name)
						if hook.name == failAt {
							return startErr
						}
						return nil
					},
					OnStop: func(context.Context) error {
						recorder.record("stop", hook.name)
						return nil
					},
				}
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return app
}

func startEvents(names []string) []string {
	events := make([]string, 0, len(names))
	for _, name := range names {
		events = append(events, "start:"+name)
	}
	return events
}

func sequenceNames() []string {
	names := make([]string, 0, len(gameStartupSequence))
	for _, item := range gameStartupSequence {
		names = append(names, item.name)
	}
	return names
}

func assertEvents(t *testing.T, got, want []string) {
	t.Helper()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("event sequence mismatch\n got: %s\nwant: %s", strings.Join(got, "\n      "), strings.Join(want, "\n      "))
	}
}

// §14.4.1／§14.4.6：成功路徑的順序等於 §8.4，正常 Stop 為嚴格反序。
func TestGameSequenceStartsInOrderAndStopsInReverse(t *testing.T) {
	recorder := newLifecycleRecorder()
	app := newGameSequenceApp(t, recorder, "", nil, 0)
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	names := sequenceNames()
	assertEvents(t, recorder.snapshot(), startEvents(names))

	if err := app.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	want := startEvents(names)
	for i := len(names) - 1; i >= 0; i-- {
		want = append(want, "stop:"+names[i])
	}
	assertEvents(t, recorder.snapshot(), want)

	// consumer 早於 MQ、metrics 早於 Redis／Health、health 最後停止。
	events := recorder.snapshot()
	assertStopsBefore(t, events, "game-message-consumer", "rocketmq")
	assertStopsBefore(t, events, "metrics", "redis")
	assertStopsBefore(t, events, "metrics", "health-listener")
	if last := events[len(events)-1]; last != "stop:health-listener" {
		t.Errorf("last event = %q, want stop:health-listener", last)
	}
}

func assertStopsBefore(t *testing.T, events []string, first, second string) {
	t.Helper()
	firstIndex, secondIndex := indexOf(events, "stop:"+first), indexOf(events, "stop:"+second)
	if firstIndex < 0 || secondIndex < 0 {
		t.Fatalf("missing stop events for %q/%q", first, second)
	}
	if firstIndex > secondIndex {
		t.Errorf("%q stopped after %q", first, second)
	}
}

func indexOf(values []string, want string) int {
	for i, value := range values {
		if value == want {
			return i
		}
	}
	return -1
}

// §14.4.2～5：任一 infra hook 失敗時，先前成功的 hook 反向停止、後續 hook 不啟動，
// 且不會進入 Service／Ingress／Readiness。
func TestGameSequenceRollsBackOnInfraFailure(t *testing.T) {
	for _, failAt := range []string{"redis", "rocketmq", "metrics"} {
		t.Run(failAt, func(t *testing.T) {
			recorder := newLifecycleRecorder()
			startErr := fmt.Errorf("%s unavailable", failAt)
			app := newGameSequenceApp(t, recorder, failAt, startErr, 0)

			err := app.Start(context.Background())
			if !errors.Is(err, startErr) {
				t.Fatalf("Start() error = %v, want %v", err, startErr)
			}

			names := sequenceNames()
			failIndex := indexOf(names, failAt)
			started := names[:failIndex+1]
			want := startEvents(started)
			// framework 不會停止「正在啟動且回錯」的 hook（設計 §1.4），
			// 因此 rollback 從失敗者的前一個開始反向執行。
			for i := failIndex - 1; i >= 0; i-- {
				want = append(want, "stop:"+names[i])
			}
			assertEvents(t, recorder.snapshot(), want)

			for _, name := range names[failIndex+1:] {
				if recorder.startCount(name) != 0 {
					t.Errorf("hook %q started after %q failed", name, failAt)
				}
			}
			if recorder.stopCount(failAt) != 0 {
				t.Errorf("failing hook %q received OnStop", failAt)
			}
			if recorder.startCount("readiness") != 0 {
				t.Error("readiness was reached despite an infra failure")
			}
		})
	}
}

// §14.4.8／9／AC12：duplicate Start、failed 後 retry、Stop 後 Start 都必須回
// ErrAppNotStartable 且不再執行任何 hook。
func TestGameSequenceStartIsOneShot(t *testing.T) {
	recorder := newLifecycleRecorder()
	app := newGameSequenceApp(t, recorder, "", nil, 0)
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	before := len(recorder.snapshot())
	if err := app.Start(context.Background()); !errors.Is(err, ErrAppNotStartable) {
		t.Fatalf("duplicate Start() error = %v, want ErrAppNotStartable", err)
	}
	if got := len(recorder.snapshot()); got != before {
		t.Fatalf("duplicate Start executed hooks: events %d → %d", before, got)
	}
	if err := app.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	afterStop := recorder.snapshot()
	if err := app.Start(context.Background()); !errors.Is(err, ErrAppNotStartable) {
		t.Fatalf("restart after Stop error = %v, want ErrAppNotStartable", err)
	}
	// §14.4.9：重複 Stop 冪等，不重跑 OnStop。
	if err := app.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop() error = %v", err)
	}
	assertEvents(t, recorder.snapshot(), afterStop)

	failing := newLifecycleRecorder()
	startErr := errors.New("wallet unavailable")
	failedApp := newGameSequenceApp(t, failing, "wallet", startErr, 0)
	if err := failedApp.Start(context.Background()); !errors.Is(err, startErr) {
		t.Fatalf("Start() error = %v, want %v", err, startErr)
	}
	rollback := failing.snapshot()
	if err := failedApp.Start(context.Background()); !errors.Is(err, ErrAppNotStartable) {
		t.Fatalf("retry after failed Start error = %v, want ErrAppNotStartable", err)
	}
	// failed 之後的 Stop 只回傳已保存的 rollback 結果，不重跑 hook。
	if err := failedApp.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() after failed Start = %v", err)
	}
	assertEvents(t, failing.snapshot(), rollback)
}

// §14.4.10：concurrent Start／Stop 不得讓 OnStop 插進兩個 OnStart 之間。
func TestGameSequenceConcurrentStartAndStopDoNotInterleave(t *testing.T) {
	recorder := newLifecycleRecorder()
	app := newGameSequenceApp(t, recorder, "", nil, time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := app.Start(context.Background()); err != nil {
			t.Errorf("Start() error = %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		// 讓 Start 先取得 lifecycleMu，Stop 必須等整段 Start 完成。
		time.Sleep(2 * time.Millisecond)
		if err := app.Stop(context.Background()); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	}()
	wg.Wait()

	events := recorder.snapshot()
	stopped := false
	for _, event := range events {
		if strings.HasPrefix(event, "stop:") {
			stopped = true
			continue
		}
		if stopped {
			t.Fatalf("OnStart ran after OnStop began: %v", events)
		}
	}
	for _, item := range gameStartupSequence {
		if recorder.startCount(item.name) != 1 {
			t.Errorf("hook %q start count = %d, want 1", item.name, recorder.startCount(item.name))
		}
		if got := recorder.stopCount(item.name); got > 1 {
			t.Errorf("hook %q stop count = %d, want at most 1", item.name, got)
		}
	}
}
