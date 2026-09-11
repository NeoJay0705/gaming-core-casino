// Package profilehttp 提供範例專用的 loopback pprof HTTP server。
//
// 這個 package 不屬於 framework observability contract；呼叫端必須明確
// 傳入 listen address 才會啟動，避免把 profiling endpoint 暴露給正式
// product listener。
package profilehttp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"sync"
	"time"
)

const (
	readHeaderTimeout = 5 * time.Second
)

// Server 擁有一個可選的 loopback-only pprof listener。
type Server struct {
	server   *http.Server
	listener net.Listener
	done     chan error

	mu           sync.RWMutex
	shutdownOnce sync.Once
	shutdownDone chan struct{}
	shutdownErr  error
}

// Start 啟動 pprof listener。空白 address 停用 profiling 並回傳 nil, nil；非空
// address 必須明確指定 localhost 或 loopback IP，wildcard 與 public address 會拒絕。
func Start(listenAddr string) (*Server, error) {
	listenAddr = strings.TrimSpace(listenAddr)
	if listenAddr == "" {
		return nil, nil
	}
	if err := validateLoopbackAddress(listenAddr); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("profilehttp: listen on %s: %w", listenAddr, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	server := &Server{
		server:       &http.Server{Handler: mux, ReadHeaderTimeout: readHeaderTimeout},
		listener:     listener,
		done:         make(chan error, 1),
		shutdownDone: make(chan struct{}),
	}
	go func() {
		serveErr := server.server.Serve(listener)
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		server.done <- serveErr
	}()
	return server, nil
}

func validateLoopbackAddress(listenAddr string) error {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return fmt.Errorf("profilehttp: invalid listen address %q: %w", listenAddr, err)
	}
	if port == "" {
		return fmt.Errorf("profilehttp: invalid listen address %q: port is required", listenAddr)
	}
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("profilehttp: listen address %q must be loopback", listenAddr)
	}
	return nil
}

// Addr returns the bound address, or an empty string after shutdown/when the
// server is disabled.
func (s *Server) Addr() string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	listener := s.listener
	s.mu.RUnlock()
	if listener == nil {
		return ""
	}
	return listener.Addr().String()
}

// Shutdown 優雅停止 pprof listener；操作具 idempotent 特性且允許 nil receiver。
// 呼叫端應提供有上限的 context。
func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.shutdownOnce.Do(func() {
		shutdownErr := s.server.Shutdown(ctx)
		if shutdownErr != nil {
			shutdownErr = errors.Join(shutdownErr, s.server.Close())
		}
		s.mu.Lock()
		listener := s.listener
		s.listener = nil
		s.mu.Unlock()
		if listener != nil {
			if closeErr := listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
				shutdownErr = errors.Join(shutdownErr, closeErr)
			}
		}
		serveErr := <-s.done
		s.shutdownErr = errors.Join(shutdownErr, serveErr)
		close(s.shutdownDone)
	})
	<-s.shutdownDone
	return s.shutdownErr
}
