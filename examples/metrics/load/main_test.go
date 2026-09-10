package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/protocol"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gateproto"
	"github.com/NeoJay0705/gaming-core-casino/products/gateproduct"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
)

func TestRunBoundedRespectsConcurrencyLimit(t *testing.T) {
	const (
		count       = 8
		concurrency = 2
	)
	started := make(chan struct{}, count)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- runBounded(context.Background(), count, concurrency, func(context.Context, int) error {
			current := active.Add(1)
			for {
				previous := maximum.Load()
				if current <= previous || maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			started <- struct{}{}
			<-release
			active.Add(-1)
			return nil
		})
	}()

	for i := 0; i < concurrency; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("bounded jobs did not start")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("runBounded() error = %v", err)
	}
	if got := maximum.Load(); got > concurrency {
		t.Fatalf("maximum active jobs = %d, want <= %d", got, concurrency)
	}
}

func TestRunBoundedStopsQueuedJobsAfterFirstError(t *testing.T) {
	sentinel := errors.New("setup failed")
	var first atomic.Bool
	var later atomic.Int32
	err := runBounded(context.Background(), 8, 1, func(context.Context, int) error {
		if first.CompareAndSwap(false, true) {
			return sentinel
		}
		later.Add(1)
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("runBounded() error = %v, want %v", err, sentinel)
	}
	if got := later.Load(); got != 0 {
		t.Fatalf("jobs after first error = %d, want 0", got)
	}
}

func TestRunBoundedCancelsActiveJobsAfterFirstError(t *testing.T) {
	sentinel := errors.New("setup failed")
	secondStarted := make(chan struct{})
	var cancellationObserved atomic.Bool

	err := runBounded(context.Background(), 2, 2, func(jobContext context.Context, index int) error {
		if index == 0 {
			<-secondStarted
			return sentinel
		}
		close(secondStarted)
		select {
		case <-jobContext.Done():
			cancellationObserved.Store(true)
			return jobContext.Err()
		case <-time.After(time.Second):
			return errors.New("active job was not cancelled")
		}
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("runBounded() error = %v, want %v", err, sentinel)
	}
	if !cancellationObserved.Load() {
		t.Fatal("active job did not observe child context cancellation")
	}
}

func TestLoadSetupPhasesCompleteBeforeNextPhaseAndWarmupIsUnobserved(t *testing.T) {
	const connectionCount = 2
	testServer := newLoadProtocolTestServer(t, connectionCount)
	setupContext, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	prepared, err := prepareConnections(setupContext, testServer.serverURL(), connectionCount, connectionCount)
	if err != nil {
		t.Fatalf("prepareConnections() error = %v", err)
	}
	t.Cleanup(func() { closePreparedConnections(prepared) })
	select {
	case <-testServer.handshakeReady:
	case <-time.After(time.Second):
		t.Fatalf("completed handshakes = %d, want %d", testServer.handshakes.Load(), connectionCount)
	}
	if got := testServer.messages.Load(); got != 0 {
		t.Fatalf("messages before initialization = %d, want 0", got)
	}
	if err := initializeConnections(setupContext, prepared, connectionCount, time.Second); err != nil {
		t.Fatalf("initializeConnections() error = %v", err)
	}
	if got := testServer.logins.Load(); got != connectionCount {
		t.Fatalf("login requests = %d, want %d", got, connectionCount)
	}
	if err := warmupConnections(setupContext, prepared, []byte("warmup"), connectionCount, 1, time.Second); err != nil {
		t.Fatalf("warmupConnections() error = %v", err)
	}
	if got := testServer.enters.Load(); got != connectionCount {
		t.Fatalf("enter-room requests = %d, want %d", got, connectionCount)
	}
	if got := testServer.echoes.Load(); got != connectionCount {
		t.Fatalf("warm-up Echo requests = %d, want %d", got, connectionCount)
	}
	select {
	case err := <-testServer.violations:
		t.Fatal(err)
	default:
	}

	registry := prometheus.NewRegistry()
	metrics, err := newLoadMetrics(registry)
	if err != nil {
		t.Fatalf("newLoadMetrics() error = %v", err)
	}
	if got := countMetricFamilies(t, registry, "gaming_core_example_load_echo_round_trips_total"); got != 0 {
		t.Fatalf("warm-up metric families = %d, want 0", got)
	}
	requests, err := runMeasuredConnection(context.Background(), &prepared[0], []byte("measured"), time.Now().Add(20*time.Millisecond), time.Second, metrics)
	if err != nil {
		t.Fatalf("runMeasuredConnection() error = %v", err)
	}
	if requests == 0 {
		t.Fatal("runMeasuredConnection() sent no measured requests")
	}
	if got := counterSampleValue(t, registry, "gaming_core_example_load_echo_round_trips_total", loadResultSuccess); got != float64(requests) {
		t.Fatalf("measured success counter = %v, want %d", got, requests)
	}
}

func TestMeasuredConnectionDrainsRequestAfterAdmissionEnd(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	requestReceived := make(chan struct{})
	releaseResponse := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		if _, _, err := connection.ReadMessage(); err != nil {
			return
		}
		close(requestReceived)
		<-releaseResponse
		_ = connection.WriteMessage(websocket.BinaryMessage, gateproduct.EncodeWebSocketPacket(gateproduct.WebSocketPacket{CommandID: protocol.EchoResponseCommandID}))
	}))
	t.Cleanup(server.Close)

	connection, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial test server: %v", err)
	}
	prepared := preparedConnection{conn: connection, sequence: 1}
	admissionEnd := time.Now().Add(20 * time.Millisecond)
	done := make(chan struct {
		requests uint64
		err      error
	}, 1)
	go func() {
		requests, err := runMeasuredConnection(context.Background(), &prepared, []byte("drain"), admissionEnd, time.Second, nil)
		done <- struct {
			requests uint64
			err      error
		}{requests: requests, err: err}
	}()
	select {
	case <-requestReceived:
	case <-time.After(time.Second):
		t.Fatal("measured request was not sent")
	}
	timer := time.NewTimer(time.Until(admissionEnd) + 20*time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-done:
		t.Fatal("measured connection stopped before admission window ended")
	}
	close(releaseResponse)
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatalf("runMeasuredConnection() error = %v", result.err)
		}
		if result.requests != 1 {
			t.Fatalf("measured requests = %d, want 1", result.requests)
		}
	case <-time.After(time.Second):
		t.Fatal("measured connection did not drain in-flight request")
	}
	closeLoadConnection(connection)
}

func TestMeasuredRequestTimeoutRecordsErrorAndClosesConnection(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	requestReceived := make(chan struct{})
	clientClosed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		if _, _, err := connection.ReadMessage(); err != nil {
			return
		}
		close(requestReceived)
		_, _, _ = connection.ReadMessage()
		close(clientClosed)
	}))
	t.Cleanup(server.Close)

	connection, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial test server: %v", err)
	}
	defer connection.Close()
	registry := prometheus.NewRegistry()
	metrics, err := newLoadMetrics(registry)
	if err != nil {
		t.Fatalf("newLoadMetrics() error = %v", err)
	}
	requests, err := runMeasuredConnection(context.Background(), &preparedConnection{conn: connection, sequence: 1}, []byte("timeout"), time.Now().Add(time.Second), 20*time.Millisecond, metrics)
	if err == nil {
		t.Fatal("runMeasuredConnection() error = nil, want request timeout")
	}
	if requests != 0 {
		t.Fatalf("successful requests = %d, want 0", requests)
	}
	if got := counterSampleValue(t, registry, "gaming_core_example_load_echo_round_trips_total", loadResultError); got != 1 {
		t.Fatalf("error counter = %v, want 1", got)
	}
	if got := histogramSampleCount(t, registry, "gaming_core_example_load_echo_round_trip_duration_seconds", loadResultError); got != 1 {
		t.Fatalf("error duration count = %d, want 1", got)
	}
	if got := gaugeSampleValue(t, registry, "gaming_core_example_load_echo_round_trips_in_flight"); got != 0 {
		t.Fatalf("in-flight = %v, want 0", got)
	}
	select {
	case <-requestReceived:
	case <-time.After(time.Second):
		t.Fatal("test server did not receive Echo request")
	}
	select {
	case <-clientClosed:
	case <-time.After(time.Second):
		t.Fatal("request timeout did not close WebSocket connection")
	}
}

func TestMeasuredRoundTripOverwritesPreviousDeadlines(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		for {
			_, data, err := connection.ReadMessage()
			if err != nil {
				return
			}
			if len(data) < 12 {
				return
			}
			sequence := binary.BigEndian.Uint32(data[8:12])
			if err := connection.WriteMessage(websocket.BinaryMessage, gateproduct.EncodeWebSocketPacket(gateproduct.WebSocketPacket{CommandID: protocol.EchoResponseCommandID, Sequence: sequence})); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)

	connection, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial test server: %v", err)
	}
	defer connection.Close()
	if err := connection.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("set expired read deadline: %v", err)
	}
	if err := connection.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("set expired write deadline: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := echoRoundTrip(ctx, connection, []byte("deadline"), 1, nil); err != nil {
		t.Fatalf("echoRoundTrip() error = %v, want deadline reset to succeed", err)
	}
}

func TestRunLoadSetupFailureClosesConnectionsAndSkipsMeasurement(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var failLogin atomic.Bool
	var echoRequests atomic.Int32
	closed := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = connection.Close()
			closed <- struct{}{}
		}()
		for {
			_, data, err := connection.ReadMessage()
			if err != nil {
				return
			}
			if len(data) < 12 {
				return
			}
			commandID := binary.BigEndian.Uint32(data[0:4])
			sequence := binary.BigEndian.Uint32(data[8:12])
			if commandID == gateproto.LoginRequestCommandID && failLogin.CompareAndSwap(false, true) {
				return
			}
			var responseCommandID uint32
			switch commandID {
			case gateproto.LoginRequestCommandID:
				responseCommandID = gateproto.LoginResponseCommandID
			case protocol.EnterRoomRequestCommandID:
				responseCommandID = protocol.EnterRoomResponseCommandID
			case protocol.EchoRequestCommandID:
				echoRequests.Add(1)
				responseCommandID = protocol.EchoResponseCommandID
			default:
				return
			}
			if err := connection.WriteMessage(websocket.BinaryMessage, gateproduct.EncodeWebSocketPacket(gateproduct.WebSocketPacket{CommandID: responseCommandID, Sequence: sequence})); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)

	err := runLoad(context.Background(), "ws"+strings.TrimPrefix(server.URL, "http"), 2, 20*time.Millisecond, 8, "127.0.0.1:0", 2*time.Second, 2, 1, 100*time.Millisecond)
	if err == nil {
		t.Fatal("runLoad() error = nil, want setup failure")
	}
	if got := echoRequests.Load(); got != 0 {
		t.Fatalf("Echo requests after setup failure = %d, want 0", got)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-closed:
		case <-time.After(time.Second):
			t.Fatalf("closed connections = %d, want 2", i)
		}
	}
}

func TestRunLoadStartsMeasurementAfterWarmup(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	warmupReceived := make(chan struct{})
	measuredReceived := make(chan struct{})
	releaseWarmup := make(chan struct{})
	var releaseOnce sync.Once
	var measuredOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		for {
			_, data, err := connection.ReadMessage()
			if err != nil {
				return
			}
			if len(data) < 12 {
				return
			}
			commandID := binary.BigEndian.Uint32(data[0:4])
			sequence := binary.BigEndian.Uint32(data[8:12])
			var responseCommandID uint32
			switch commandID {
			case gateproto.LoginRequestCommandID:
				responseCommandID = gateproto.LoginResponseCommandID
			case protocol.EnterRoomRequestCommandID:
				responseCommandID = protocol.EnterRoomResponseCommandID
			case protocol.EchoRequestCommandID:
				if sequence == 3 {
					close(warmupReceived)
					<-releaseWarmup
				} else {
					measuredOnce.Do(func() { close(measuredReceived) })
				}
				responseCommandID = protocol.EchoResponseCommandID
			default:
				return
			}
			if err := connection.WriteMessage(websocket.BinaryMessage, gateproduct.EncodeWebSocketPacket(gateproduct.WebSocketPacket{CommandID: responseCommandID, Sequence: sequence})); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	release := func() { releaseOnce.Do(func() { close(releaseWarmup) }) }
	t.Cleanup(release)

	done := make(chan error, 1)
	go func() {
		done <- runLoad(context.Background(), "ws"+strings.TrimPrefix(server.URL, "http"), 1, 20*time.Millisecond, 8, "127.0.0.1:0", 2*time.Second, 1, 1, time.Second)
	}()
	select {
	case <-warmupReceived:
	case <-time.After(time.Second):
		t.Fatal("warm-up Echo was not received")
	}
	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case <-timer.C:
		release()
	case <-done:
		t.Fatal("load completed before warm-up was released")
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	select {
	case <-measuredReceived:
	case <-time.After(time.Second):
		t.Fatal("measurement did not start after warm-up completed")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runLoad() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runLoad() did not finish")
	}
}

type loadProtocolTestServer struct {
	server         *httptest.Server
	expected       int
	handshakes     atomic.Int32
	messages       atomic.Int32
	logins         atomic.Int32
	enters         atomic.Int32
	echoes         atomic.Int32
	violations     chan error
	handshakeReady chan struct{}
}

func newLoadProtocolTestServer(t *testing.T, expected int) *loadProtocolTestServer {
	t.Helper()
	testServer := &loadProtocolTestServer{expected: expected, violations: make(chan error, expected*2), handshakeReady: make(chan struct{})}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	testServer.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			testServer.report(err)
			return
		}
		defer connection.Close()
		if testServer.handshakes.Add(1) == int32(testServer.expected) {
			close(testServer.handshakeReady)
		}
		for {
			messageType, data, err := connection.ReadMessage()
			if err != nil {
				return
			}
			if messageType != websocket.BinaryMessage || len(data) < 16 {
				testServer.report(errors.New("invalid protocol test request"))
				return
			}
			testServer.messages.Add(1)
			commandID := binary.BigEndian.Uint32(data[0:4])
			sequence := binary.BigEndian.Uint32(data[8:12])
			responseCommandID := uint32(0)
			var responsePayload []byte
			switch commandID {
			case gateproto.LoginRequestCommandID:
				if testServer.handshakes.Load() != int32(testServer.expected) {
					testServer.report(errors.New("Login arrived before all WebSocket handshakes"))
				}
				testServer.logins.Add(1)
				responseCommandID = gateproto.LoginResponseCommandID
			case protocol.EnterRoomRequestCommandID:
				testServer.enters.Add(1)
				responseCommandID = protocol.EnterRoomResponseCommandID
			case protocol.EchoRequestCommandID:
				if testServer.enters.Load() != int32(testServer.expected) {
					testServer.report(errors.New("Echo arrived before all EnterRoom responses"))
				}
				var echo protocol.EchoRequest
				if err := proto.Unmarshal(data[16:], &echo); err != nil {
					testServer.report(err)
					return
				}
				testServer.echoes.Add(1)
				responseCommandID = protocol.EchoResponseCommandID
				responsePayload, err = proto.Marshal(&protocol.EchoResponse{Payload: echo.GetPayload()})
				if err != nil {
					testServer.report(err)
					return
				}
			default:
				testServer.report(fmt.Errorf("unexpected command %d", commandID))
				return
			}
			if err := connection.WriteMessage(websocket.BinaryMessage, gateproduct.EncodeWebSocketPacket(gateproduct.WebSocketPacket{CommandID: responseCommandID, Sequence: sequence, Payload: responsePayload})); err != nil {
				return
			}
		}
	}))
	t.Cleanup(testServer.server.Close)
	return testServer
}

func (s *loadProtocolTestServer) report(err error) {
	select {
	case s.violations <- err:
	default:
	}
}

func (s *loadProtocolTestServer) serverURL() string {
	return "ws" + strings.TrimPrefix(s.server.URL, "http")
}

func countMetricFamilies(t *testing.T, gatherer prometheus.Gatherer, name string) int {
	t.Helper()
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	count := 0
	for _, family := range families {
		if family.GetName() == name {
			count++
		}
	}
	return count
}

func TestExpectedLoadTerminationOnlyAcceptsCancelledContext(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	future, cancelFuture := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	t.Cleanup(cancelFuture)
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancelExpired)

	for _, test := range []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{name: "cancelled context", ctx: cancelled, err: errors.New("connection closed"), want: true},
		{name: "deadline exceeded is request error", ctx: expired, err: context.DeadlineExceeded, want: false},
		{name: "wrapped timeout is request error", ctx: expired, err: fmt.Errorf("read: %w", loadTimeoutError{}), want: false},
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
