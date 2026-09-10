package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestExpectedLoadTerminationOnlyAcceptsDeadlineTimeout(t *testing.T) {
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancelExpired)
	future, cancelFuture := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	t.Cleanup(cancelFuture)

	for _, test := range []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{name: "deadline exceeded", ctx: expired, err: context.DeadlineExceeded, want: true},
		{name: "wrapped timeout", ctx: expired, err: fmt.Errorf("read: %w", loadTimeoutError{}), want: true},
		{name: "ordinary error after deadline", ctx: expired, err: errors.New("unexpected EOF"), want: false},
		{name: "timeout before deadline", ctx: future, err: loadTimeoutError{}, want: false},
		{name: "nil error", ctx: expired, err: nil, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := isExpectedLoadTermination(test.ctx, test.err); got != test.want {
				t.Fatalf("isExpectedLoadTermination() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestCloseLoadConnectionSendsNormalClosure(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	serverError := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			serverError <- err
			return
		}
		defer connection.Close()
		_, _, err = connection.ReadMessage()
		serverError <- err
	}))
	t.Cleanup(server.Close)

	connection, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial test server: %v", err)
	}
	closeLoadConnection(connection)

	select {
	case err := <-serverError:
		var closeError *websocket.CloseError
		if !errors.As(err, &closeError) || closeError.Code != websocket.CloseNormalClosure {
			t.Fatalf("server close error = %v, want normal closure", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe normal closure")
	}
}

func TestExpiredWriteDeadlineStopsWrite(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		<-release
	}))
	t.Cleanup(func() {
		close(release)
		server.Close()
	})

	connection, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial test server: %v", err)
	}
	defer connection.Close()
	if err := connection.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("set expired write deadline: %v", err)
	}
	if err := connection.WriteMessage(websocket.BinaryMessage, []byte("expired")); err == nil {
		t.Fatal("WriteMessage() error = nil with expired deadline")
	}
}

type loadTimeoutError struct{}

func (loadTimeoutError) Error() string   { return "load timeout" }
func (loadTimeoutError) Timeout() bool   { return true }
func (loadTimeoutError) Temporary() bool { return true }
