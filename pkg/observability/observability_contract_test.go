package observability

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/prometheus/client_golang/prometheus"
)

func TestConfigContractValidatesListenAddress(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want string
	}{
		{name: "missing path", body: "product: {}\n", want: "observability is required"},
		{name: "missing address", body: "observability: {}\n", want: "listen_addr is required"},
		{name: "invalid address", body: "observability:\n  listen_addr: 127.0.0.1\n", want: "invalid listen_addr"},
		{name: "missing port", body: "observability:\n  listen_addr: '127.0.0.1:'\n", want: "invalid listen_addr"},
		{name: "unknown field", body: "observability:\n  listen_addr: 127.0.0.1:0\n  port: 8081\n", want: "unknown config paths"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := newConfig(loadSnapshot(t, test.body))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("newConfig() error = %v, want substring %q", err, test.want)
			}
		})
	}

	cfg, err := newConfig(loadSnapshot(t, "observability:\n  listen_addr: ' 127.0.0.1:0 '\n"))
	if err != nil {
		t.Fatalf("newConfig(valid) error = %v", err)
	}
	if cfg.ListenAddr != "127.0.0.1:0" {
		t.Fatalf("ListenAddr = %q, want 127.0.0.1:0", cfg.ListenAddr)
	}
}

func TestHTTPServerContractServesHealthReadinessAndMetrics(t *testing.T) {
	owner := newRegistryOwner()
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "observability_contract_events_total",
		Help: "Total events recorded by the observability contract test.",
	})
	if err := owner.registry.Register(counter); err != nil {
		t.Fatalf("register counter: %v", err)
	}
	counter.Add(2)

	server, err := newHTTPServer(Config{ListenAddr: "127.0.0.1:0"}, owner)
	if err != nil {
		t.Fatalf("newHTTPServer() error = %v", err)
	}

	t.Run("health", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		server.server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, healthPath, nil))
		assertProbeResponse(t, recorder, http.StatusOK, healthBody)
	})
	t.Run("readiness before start", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		server.server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, readinessPath, nil))
		assertProbeResponse(t, recorder, http.StatusServiceUnavailable, notReadyBody)
	})
	t.Run("readiness after transition", func(t *testing.T) {
		server.setReady(true)
		recorder := httptest.NewRecorder()
		server.server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, readinessPath, nil))
		assertProbeResponse(t, recorder, http.StatusOK, readyBody)
	})
	t.Run("metrics", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		server.server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, metricsPath, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("metrics status = %d, want %d; body=%q", recorder.Code, http.StatusOK, recorder.Body.String())
		}
		body := recorder.Body.String()
		if !strings.Contains(body, "observability_contract_events_total 2") {
			t.Fatalf("metrics body does not contain registered counter: %q", body)
		}
		if strings.Contains(body, "go_goroutines") || strings.Contains(body, "process_cpu_seconds_total") {
			t.Fatalf("custom registry unexpectedly exposed default collectors: %q", body)
		}
	})
	t.Run("unknown route", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		server.server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/unknown", nil))
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("unknown route status = %d, want %d", recorder.Code, http.StatusNotFound)
		}
	})
}

func TestRegistryContractIsAppLocalAndReportsDuplicateRegistration(t *testing.T) {
	first, second := newRegistryOwner(), newRegistryOwner()
	firstMetric := prometheus.NewCounter(prometheus.CounterOpts{Name: "same_app_local_total", Help: "App-local counter."})
	secondMetric := prometheus.NewCounter(prometheus.CounterOpts{Name: "same_app_local_total", Help: "App-local counter."})
	if err := first.registry.Register(firstMetric); err != nil {
		t.Fatalf("register first metric: %v", err)
	}
	if err := second.registry.Register(secondMetric); err != nil {
		t.Fatalf("register same metric in second registry: %v", err)
	}
	duplicate := prometheus.NewCounter(prometheus.CounterOpts{Name: "same_app_local_total", Help: "App-local counter."})
	err := first.registry.Register(duplicate)
	if err == nil {
		t.Fatal("duplicate registration error = nil")
	}
	var alreadyRegistered prometheus.AlreadyRegisteredError
	if !errors.As(err, &alreadyRegistered) {
		t.Fatalf("duplicate registration error = %v, want AlreadyRegisteredError", err)
	}
	firstMetric.Add(3)
	secondMetric.Add(7)
	if got := gatheredCounterValue(t, first.registry, "same_app_local_total"); got != 3 {
		t.Fatalf("first registry value = %v, want 3", got)
	}
	if got := gatheredCounterValue(t, second.registry, "same_app_local_total"); got != 7 {
		t.Fatalf("second registry value = %v, want 7", got)
	}
}

func TestHTTPServerStartRejectsOccupiedAddress(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	server, err := newHTTPServer(Config{ListenAddr: listener.Addr().String()}, newRegistryOwner())
	if err != nil {
		t.Fatalf("newHTTPServer() error = %v", err)
	}
	if err := server.Start(context.Background()); err == nil {
		t.Fatal("Start() error = nil for occupied address")
	}
	if server.addr() != "" {
		t.Fatalf("server address after failed start = %q", server.addr())
	}
}

func TestHTTPServerStopIsIdempotent(t *testing.T) {
	server, err := newHTTPServer(Config{ListenAddr: "127.0.0.1:0"}, newRegistryOwner())
	if err != nil {
		t.Fatalf("newHTTPServer() error = %v", err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	addr := server.addr()
	if addr == "" {
		t.Fatal("server address is empty after Start()")
	}
	if err := server.Stop(context.Background()); err != nil {
		t.Fatalf("first Stop() error = %v", err)
	}
	if err := server.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop() error = %v", err)
	}
	if server.addr() != "" {
		t.Fatalf("server address after Stop() = %q", server.addr())
	}
	if err := server.Start(context.Background()); err == nil {
		t.Fatal("Start() after Stop() error = nil")
	}
}

func TestModuleContractDrivesReadinessAcrossLifecycle(t *testing.T) {
	snapshot := loadSnapshot(t, "observability:\n  listen_addr: 127.0.0.1:0\n")
	serviceStarted := make(chan struct{})
	releaseService := make(chan struct{})
	serviceStopped := make(chan int, 1)
	var (
		serverMu sync.Mutex
		server   *httpServer
	)

	app, err := framework.New(
		func(r framework.Registry) error {
			if err := r.Provide(func() config.SourceSnapshot { return snapshot }); err != nil {
				return err
			}
			return Module(r)
		},
		func(r framework.Registry) error {
			return r.AddHook(func(s *httpServer) framework.Hook {
				serverMu.Lock()
				server = s
				serverMu.Unlock()
				return framework.Hook{
					Name:  "observability-contract-service",
					Phase: framework.PhaseService,
					OnStart: func(ctx context.Context) error {
						close(serviceStarted)
						select {
						case <-releaseService:
							return nil
						case <-ctx.Done():
							return ctx.Err()
						}
					},
					OnStop: func(context.Context) error {
						serverMu.Lock()
						value := server
						serverMu.Unlock()
						if value == nil {
							return errors.New("observability server was not resolved")
						}
						status, err := requestStatus(value.addr(), readinessPath)
						if err != nil {
							return err
						}
						serviceStopped <- status
						return nil
					},
				}
			})
		},
	)
	if err != nil {
		t.Fatalf("framework.New() error = %v", err)
	}

	startDone := make(chan error, 1)
	go func() { startDone <- app.Start(context.Background()) }()
	select {
	case <-serviceStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("service hook did not start")
	}
	serverMu.Lock()
	addr := server.addr()
	serverMu.Unlock()
	if got := mustRequestStatus(t, addr, readinessPath); got != http.StatusServiceUnavailable {
		t.Fatalf("readiness during startup = %d, want %d", got, http.StatusServiceUnavailable)
	}
	close(releaseService)
	if err := <-startDone; err != nil {
		t.Fatalf("App.Start() error = %v", err)
	}
	if got := mustRequestStatus(t, addr, readinessPath); got != http.StatusOK {
		t.Fatalf("readiness after startup = %d, want %d", got, http.StatusOK)
	}
	if err := app.Stop(context.Background()); err != nil {
		t.Fatalf("App.Stop() error = %v", err)
	}
	select {
	case got := <-serviceStopped:
		if got != http.StatusServiceUnavailable {
			t.Fatalf("readiness during shutdown = %d, want %d", got, http.StatusServiceUnavailable)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("service hook did not stop")
	}
}

func TestModuleRegistersOneManagedServerAndReadinessHook(t *testing.T) {
	var registry recordingRegistry
	if err := Module(&registry); err != nil {
		t.Fatalf("Module() error = %v", err)
	}
	if len(registry.provides) != 3 {
		t.Fatalf("Provide calls = %d, want 3", len(registry.provides))
	}
	if len(registry.managed) != 1 {
		t.Fatalf("managed resources = %d, want 1", len(registry.managed))
	}
	if got := registry.managed[0]; got.name != "observability-http" || got.phase != framework.PhaseInfrastructure || !got.hasConstructor {
		t.Fatalf("managed registration = %+v, want observability-http at infrastructure", got)
	}
	if len(registry.hookConstructors) != 1 {
		t.Fatalf("hook registrations = %d, want 1 readiness hook", len(registry.hookConstructors))
	}
	hookConstructor, ok := registry.hookConstructors[0].(func(*httpServer) framework.Hook)
	if !ok {
		t.Fatalf("readiness hook constructor type = %T", registry.hookConstructors[0])
	}
	hook := hookConstructor(nil)
	if hook.Name != "readiness" || hook.Phase != framework.PhaseReadiness || hook.OnStart == nil || hook.OnStop == nil {
		t.Fatalf("readiness hook = %+v, want named start/stop hook at readiness phase", hook)
	}
}

func TestModuleRollsBackListenerWhenInfrastructureStartupFails(t *testing.T) {
	snapshot := loadSnapshot(t, "observability:\n  listen_addr: 127.0.0.1:0\n")
	startErr := errors.New("dependency unavailable")
	var (
		server    *httpServer
		boundAddr string
	)
	app, err := framework.New(func(r framework.Registry) error {
		if err := r.Provide(func() config.SourceSnapshot { return snapshot }); err != nil {
			return err
		}
		if err := Module(r); err != nil {
			return err
		}
		return r.AddHook(func(value *httpServer) framework.Hook {
			server = value
			return framework.Hook{
				Name:  "failing-infrastructure",
				Phase: framework.PhaseInfrastructure,
				OnStart: func(context.Context) error {
					boundAddr = value.addr()
					return startErr
				},
			}
		})
	})
	if err != nil {
		t.Fatalf("framework.New() error = %v", err)
	}
	if server == nil {
		t.Fatal("observability server was not resolved")
	}
	if err := app.Start(context.Background()); !errors.Is(err, startErr) {
		t.Fatalf("App.Start() error = %v, want %v", err, startErr)
	}
	if boundAddr == "" {
		t.Fatal("observability server did not bind before infrastructure failure")
	}
	if server.addr() != "" {
		t.Fatalf("server address after rollback = %q, want empty", server.addr())
	}
	listener, err := net.Listen("tcp", boundAddr)
	if err != nil {
		t.Fatalf("listener was not released after rollback: %v", err)
	}
	_ = listener.Close()
}

func TestHTTPServerStopForcesCloseWhenContextIsCancelled(t *testing.T) {
	server, err := newHTTPServer(Config{ListenAddr: "127.0.0.1:0"}, newRegistryOwner())
	if err != nil {
		t.Fatalf("newHTTPServer() error = %v", err)
	}
	accepted := make(chan struct{}, 1)
	server.server.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			select {
			case accepted <- struct{}{}:
			default:
			}
		}
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	addr := server.addr()
	connection, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer connection.Close()
	_, _ = connection.Write([]byte("GET /health HTTP/1.1\r\nHost: localhost\r\n"))
	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not accept the held connection")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := server.Stop(ctx); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop(cancelled context) error = %v, want context.Canceled", err)
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listener was not released after forced close: %v", err)
	}
	_ = listener.Close()
}

func TestModuleRejectsNilRegistry(t *testing.T) {
	if err := Module(nil); err == nil || !strings.Contains(err.Error(), "registry is nil") {
		t.Fatalf("Module(nil) error = %v, want nil registry error", err)
	}
	var registry *testRegistry
	if err := Module(registry); err == nil || !strings.Contains(err.Error(), "registry is nil") {
		t.Fatalf("Module(typed nil) error = %v, want nil registry error", err)
	}
}

func assertProbeResponse(t *testing.T, recorder *httptest.ResponseRecorder, status int, body string) {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("status = %d, want %d; body=%q", recorder.Code, status, recorder.Body.String())
	}
	if got := recorder.Body.String(); got != body {
		t.Fatalf("body = %q, want %q", got, body)
	}
	if got := recorder.Header().Get("Content-Type"); got != probeContent {
		t.Fatalf("Content-Type = %q, want %q", got, probeContent)
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
}

func requestStatus(addr, path string) (int, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://" + addr + path)
	if err != nil {
		return 0, fmt.Errorf("GET %s%s: %w", addr, path, err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode, nil
}

func mustRequestStatus(t *testing.T, addr, path string) int {
	t.Helper()
	status, err := requestStatus(addr, path)
	if err != nil {
		t.Fatal(err)
	}
	return status
}

func gatheredCounterValue(t *testing.T, gatherer prometheus.Gatherer, name string) float64 {
	t.Helper()
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range families {
		if family.GetName() == name && len(family.Metric) == 1 {
			return family.Metric[0].GetCounter().GetValue()
		}
	}
	t.Fatalf("metric family %q not found", name)
	return 0
}

func loadSnapshot(t *testing.T, body string) config.SourceSnapshot {
	t.Helper()
	path := filepath.Join(t.TempDir(), "observability.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.LoadInputs(context.Background(), config.ConfigInputs{MergedPaths: []string{path}}, fmt.Sprintf("OBSERVABILITY_CONTRACT_%d__", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

type testRegistry struct{}

func (*testRegistry) Provide(any) error                                 { return nil }
func (*testRegistry) ProvideManaged(string, framework.Phase, any) error { return nil }
func (*testRegistry) Configure(any) error                               { return nil }
func (*testRegistry) AddHook(any) error                                 { return nil }

type recordingRegistry struct {
	provides         []any
	managed          []managedRegistration
	hookConstructors []any
}

type managedRegistration struct {
	name           string
	phase          framework.Phase
	hasConstructor bool
}

func (r *recordingRegistry) Provide(constructor any) error {
	r.provides = append(r.provides, constructor)
	return nil
}

func (r *recordingRegistry) ProvideManaged(name string, phase framework.Phase, constructor any) error {
	r.managed = append(r.managed, managedRegistration{name: name, phase: phase, hasConstructor: constructor != nil})
	return nil
}

func (*recordingRegistry) Configure(any) error { return nil }

func (r *recordingRegistry) AddHook(constructor any) error {
	r.hookConstructors = append(r.hookConstructors, constructor)
	return nil
}
