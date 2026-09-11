package profilehttp

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestValidateLoopbackAddressAcceptsSupportedForms(t *testing.T) {
	for _, address := range []string{"127.0.0.1:0", "[::1]:0", "localhost:0"} {
		t.Run(address, func(t *testing.T) {
			if err := ValidateLoopbackAddress(address); err != nil {
				t.Fatalf("ValidateLoopbackAddress(%q) error = %v", address, err)
			}
		})
	}
}

func TestStartDisabled(t *testing.T) {
	server, err := Start("   ")
	if err != nil {
		t.Fatalf("Start(disabled) error = %v", err)
	}
	if server != nil {
		t.Fatal("Start(disabled) returned a server")
	}
	if err := server.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown(nil) error = %v", err)
	}
}

func TestStartRejectsNonLoopbackAddresses(t *testing.T) {
	for _, address := range []string{
		":0",
		"0.0.0.0:0",
		"[::]:0",
		"example.invalid:0",
		"127.0.0.1",
	} {
		t.Run(address, func(t *testing.T) {
			server, err := Start(address)
			if err == nil {
				_ = server.Shutdown(context.Background())
				t.Fatalf("Start(%q) error = nil", address)
			}
			if !strings.Contains(err.Error(), "loopback") && !strings.Contains(err.Error(), "invalid listen address") {
				t.Fatalf("Start(%q) error = %v, want address validation error", address, err)
			}
		})
	}
}

func TestServerServesPprofAndShutsDown(t *testing.T) {
	server, err := Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})
	if server.Addr() == "" {
		t.Fatal("Addr() is empty after Start")
	}
	host, _, err := net.SplitHostPort(server.Addr())
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", server.Addr(), err)
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		t.Fatalf("bound address %q is not loopback", server.Addr())
	}

	client := &http.Client{Timeout: time.Second}
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/goroutine?debug=1", "/debug/pprof/symbol"} {
		response, err := client.Get("http://" + server.Addr() + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		body, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("GET %s body: read=%v close=%v", path, readErr, closeErr)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want %d; body=%q", path, response.StatusCode, http.StatusOK, body)
		}
	}
	response, err := client.Get("http://" + server.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /metrics status = %d, want %d", response.StatusCode, http.StatusNotFound)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if server.Addr() != "" {
		t.Fatalf("Addr() after Shutdown = %q, want empty", server.Addr())
	}
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown() error = %v", err)
	}
}

func TestShutdownForcesCloseWhenContextIsCancelled(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	handlerStarted := make(chan struct{})
	httpServer := &http.Server{Handler: http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(handlerStarted)
		<-request.Context().Done()
	})}
	server := &Server{
		server:       httpServer,
		listener:     listener,
		done:         make(chan error, 1),
		shutdownDone: make(chan struct{}),
	}
	go func() {
		serveErr := httpServer.Serve(listener)
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		server.done <- serveErr
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})

	addr := server.Addr()
	connection, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer connection.Close()
	if _, err := io.WriteString(connection, "GET /held HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
		t.Fatalf("write held request: %v", err)
	}
	select {
	case <-handlerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not start the held request")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := server.Shutdown(ctx); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown(cancelled context) error = %v, want context.Canceled", err)
	}
	if server.Addr() != "" {
		t.Fatalf("Addr() after forced shutdown = %q, want empty", server.Addr())
	}
	rebound, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listener was not released after forced shutdown: %v", err)
	}
	_ = rebound.Close()
}

func TestStartFailsWhenPortIsOccupied(t *testing.T) {
	first, err := Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("first Start() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = first.Shutdown(ctx)
	})
	second, err := Start(first.Addr())
	if err == nil {
		_ = second.Shutdown(context.Background())
		t.Fatal("second Start() error = nil")
	}
	if !strings.Contains(err.Error(), "listen") {
		t.Fatalf("second Start() error = %v, want listen error", err)
	}
}
