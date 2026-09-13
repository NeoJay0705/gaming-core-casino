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

func TestParseEchoRoute(t *testing.T) {
	tests := []struct {
		name           string
		value          string
		wantRoute      echoRoute
		wantRequestID  uint32
		wantResponseID uint32
		wantError      bool
	}{
		{name: "game default", value: "game", wantRoute: echoRouteGame, wantRequestID: protocol.EchoRequestCommandID, wantResponseID: protocol.EchoResponseCommandID},
		{name: "local", value: "local", wantRoute: echoRouteLocal, wantRequestID: protocol.LocalEchoRequestCommandID, wantResponseID: protocol.LocalEchoResponseCommandID},
		{name: "trimmed game", value: "  game ", wantRoute: echoRouteGame, wantRequestID: protocol.EchoRequestCommandID, wantResponseID: protocol.EchoResponseCommandID},
		{name: "trimmed local", value: "\tlocal\n", wantRoute: echoRouteLocal, wantRequestID: protocol.LocalEchoRequestCommandID, wantResponseID: protocol.LocalEchoResponseCommandID},
		{name: "empty", value: "", wantError: true},
		{name: "unknown", value: "other", wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseEchoRoute(test.value)
			if test.wantError {
				if err == nil {
					t.Fatal("parseEchoRoute() error = nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseEchoRoute() error = %v", err)
			}
			if got.route != test.wantRoute || got.requestCommandID != test.wantRequestID || got.responseCommandID != test.wantResponseID {
				t.Fatalf("parseEchoRoute() = %+v, want route=%q request=%#x response=%#x", got, test.wantRoute, test.wantRequestID, test.wantResponseID)
			}
		})
	}
}

func TestParsePushWorkload(t *testing.T) {
	for _, value := range []string{"broadcast", "PLAYER", " player "} {
		if mode, err := parsePushWorkload(value); err != nil || (mode != pushWorkloadBroadcast && mode != pushWorkloadPlayer) {
			t.Fatalf("parsePushWorkload(%q) = %q, %v", value, mode, err)
		}
	}
	if _, err := parsePushWorkload("echo"); err == nil {
		t.Fatal("parsePushWorkload(echo) error = nil")
	}
}

func TestDecodeLoadPacketValidatesHeaderAndPreservesFields(t *testing.T) {
	want := gateproduct.EncodeWebSocketPacket(gateproduct.WebSocketPacket{
		CommandID: protocol.PushMessageCommandID,
		Sequence:  9,
		Session:   3,
		Version:   4,
		Payload:   []byte{1, 2, 3},
	})
	got, err := decodeLoadPacket(want)
	if err != nil {
		t.Fatalf("decodeLoadPacket() error = %v", err)
	}
	if got.CommandID != protocol.PushMessageCommandID || got.Sequence != 9 || got.Session != 3 || got.Version != 4 || string(got.Payload) != string([]byte{1, 2, 3}) {
		t.Fatalf("decodeLoadPacket() = %+v", got)
	}
	for _, malformed := range [][]byte{
		{},
		make([]byte, 15),
		append(append([]byte(nil), want...), 0),
	} {
		if _, err := decodeLoadPacket(malformed); err == nil {
			t.Fatalf("decodeLoadPacket(%d bytes) error = nil", len(malformed))
		}
	}
}

func TestPushClientDemultiplexesControlAndPushFramesWithOneReader(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	serverDone := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			serverDone <- err
			return
		}
		defer connection.Close()
		_, data, err := connection.ReadMessage()
		if err != nil {
			serverDone <- err
			return
		}
		packet, err := decodeLoadPacket(data)
		if err != nil {
			serverDone <- err
			return
		}
		requestMessage := new(protocol.StartPushRequest)
		if err := proto.Unmarshal(packet.Payload, requestMessage); err != nil {
			serverDone <- err
			return
		}
		response, err := proto.Marshal(&protocol.StartPushResponse{RunId: requestMessage.GetRunId(), PlannedTicks: 2})
		if err != nil {
			serverDone <- err
			return
		}
		write := func(commandID uint32, payload []byte) error {
			return connection.WriteMessage(websocket.BinaryMessage, gateproduct.EncodeWebSocketPacket(gateproduct.WebSocketPacket{CommandID: commandID, Payload: payload}))
		}
		if err := write(protocol.StartPushResponseCommandID, response); err != nil {
			serverDone <- err
			return
		}
		for _, message := range []*protocol.PushMessage{
			{RunId: requestMessage.GetRunId(), Sequence: 1, SentUnixNano: time.Now().UnixNano(), Payload: []byte("data")},
			{RunId: requestMessage.GetRunId(), Sequence: 2, SentUnixNano: time.Now().UnixNano(), Payload: []byte("data")},
			{RunId: requestMessage.GetRunId(), Sequence: 2, SentUnixNano: time.Now().UnixNano(), Payload: []byte("data")},
			{RunId: requestMessage.GetRunId(), Sequence: 4, SentUnixNano: time.Now().UnixNano(), Payload: []byte("data")},
		} {
			payload, marshalErr := proto.Marshal(message)
			writeErr := write(protocol.PushMessageCommandID, payload)
			if marshalErr != nil || writeErr != nil {
				if marshalErr != nil {
					serverDone <- marshalErr
				} else {
					serverDone <- writeErr
				}
				return
			}
		}
		if err := write(protocol.PushMessageCommandID, []byte{0xff}); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}))
	t.Cleanup(server.Close)

	connection, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial test server: %v", err)
	}
	prepared := []preparedConnection{{conn: connection, sequence: 1, name: "alice"}}
	registry := prometheus.NewRegistry()
	metrics, err := newLoadMetrics(registry)
	if err != nil {
		t.Fatalf("newLoadMetrics: %v", err)
	}
	client, err := newPushClient(context.Background(), prepared, metrics, pushWorkloadBroadcast, 4)
	if err != nil {
		t.Fatalf("newPushClient: %v", err)
	}
	if err := client.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	request := newPushStartRequest("reader-test", pushWorkloadBroadcast, []string{"alice"}, 33*time.Millisecond, time.Second, 4)
	planned, err := client.startRun(context.Background(), &prepared[0], request, true, time.Second)
	if err != nil {
		client.Stop()
		t.Fatalf("startRun: %v", err)
	}
	if planned != 2 {
		client.Stop()
		t.Fatalf("planned ticks = %d, want 2", planned)
	}
	deadline := time.Now().Add(time.Second)
	for {
		stats := client.snapshot()
		if stats.received == 2 && stats.duplicate == 1 && stats.sequenceGap == 1 && stats.invalid == 1 {
			break
		}
		if time.Now().After(deadline) {
			client.Stop()
			t.Fatalf("push stats = %+v", stats)
		}
		time.Sleep(time.Millisecond)
	}
	client.Stop()
	if err := <-serverDone; err != nil {
		t.Fatalf("test server: %v", err)
	}
	if got := gaugeSampleValue(t, registry, "gaming_core_example_load_push_readers"); got != 0 {
		t.Fatalf("push readers = %v, want 0", got)
	}
	if got := pushHistogramSampleCount(t, registry, pushWorkloadBroadcast); got != 4 {
		t.Fatalf("push delivery duration count = %d, want 4", got)
	}
	if got := client.missingAtLeast(4); got != 1 {
		t.Fatalf("push missing sequence count = %d, want 1", got)
	}
}

func TestRunLoadRejectsInvalidEchoRouteBeforeObserver(t *testing.T) {
	err := runLoad(context.Background(), "", 1, time.Second, 8, ":invalid", time.Second, 1, 0, time.Second, "invalid")
	if err == nil || !strings.Contains(err.Error(), `echo-route must be "game" or "local"`) {
		t.Fatalf("runLoad() error = %v, want invalid echo-route error", err)
	}
}

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
	if err := warmupConnections(setupContext, prepared, []byte("warmup"), connectionCount, 1, time.Second, testEchoCommands(t, string(echoRouteGame))); err != nil {
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
	requests, err := runMeasuredConnection(context.Background(), &prepared[0], []byte("measured"), time.Now().Add(20*time.Millisecond), time.Second, testEchoCommands(t, string(echoRouteGame)), metrics)
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

func TestRunLoadSupportsLocalEchoRoute(t *testing.T) {
	testServer := newLoadProtocolTestServer(t, 1)
	err := runLoad(context.Background(), testServer.serverURL(), 1, 50*time.Millisecond, 8, "127.0.0.1:0", 2*time.Second, 1, 1, time.Second, string(echoRouteLocal))
	if err != nil {
		t.Fatalf("runLoad() error = %v", err)
	}
	if got := testServer.firstEchoCommand.Load(); got != protocol.LocalEchoRequestCommandID {
		t.Fatalf("first Echo command = %#x, want local %#x", got, protocol.LocalEchoRequestCommandID)
	}
	if got := testServer.lastEchoCommand.Load(); got != protocol.LocalEchoRequestCommandID {
		t.Fatalf("last Echo command = %#x, want local %#x", got, protocol.LocalEchoRequestCommandID)
	}
	if got := testServer.echoes.Load(); got < 2 {
		t.Fatalf("Echo requests = %d, want warm-up plus measurement", got)
	}
	select {
	case err := <-testServer.violations:
		t.Fatal(err)
	default:
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
		requests, err := runMeasuredConnection(context.Background(), &prepared, []byte("drain"), admissionEnd, time.Second, testEchoCommands(t, string(echoRouteGame)), nil)
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
	requests, err := runMeasuredConnection(context.Background(), &preparedConnection{conn: connection, sequence: 1}, []byte("timeout"), time.Now().Add(time.Second), 20*time.Millisecond, testEchoCommands(t, string(echoRouteGame)), metrics)
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
	if err := echoRoundTrip(ctx, connection, []byte("deadline"), 1, testEchoCommands(t, string(echoRouteGame)), nil); err != nil {
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

	err := runLoad(context.Background(), "ws"+strings.TrimPrefix(server.URL, "http"), 2, 20*time.Millisecond, 8, "127.0.0.1:0", 2*time.Second, 2, 1, 100*time.Millisecond, string(echoRouteGame))
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
		done <- runLoad(context.Background(), "ws"+strings.TrimPrefix(server.URL, "http"), 1, 20*time.Millisecond, 8, "127.0.0.1:0", 2*time.Second, 1, 1, time.Second, string(echoRouteGame))
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
	server           *httptest.Server
	expected         int
	handshakes       atomic.Int32
	messages         atomic.Int32
	logins           atomic.Int32
	enters           atomic.Int32
	echoes           atomic.Int32
	firstEchoCommand atomic.Uint32
	lastEchoCommand  atomic.Uint32
	violations       chan error
	handshakeReady   chan struct{}
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
			case protocol.EchoRequestCommandID, protocol.LocalEchoRequestCommandID:
				if testServer.enters.Load() != int32(testServer.expected) {
					testServer.report(errors.New("Echo arrived before all EnterRoom responses"))
				}
				testServer.lastEchoCommand.Store(commandID)
				if first := testServer.firstEchoCommand.Load(); first == 0 {
					testServer.firstEchoCommand.CompareAndSwap(0, commandID)
				} else if first != commandID {
					testServer.report(fmt.Errorf("Echo command changed from %#x to %#x", first, commandID))
				}
				var echo protocol.EchoRequest
				if err := proto.Unmarshal(data[16:], &echo); err != nil {
					testServer.report(err)
					return
				}
				testServer.echoes.Add(1)
				responseCommandID = protocol.EchoResponseCommandID
				if commandID == protocol.LocalEchoRequestCommandID {
					responseCommandID = protocol.LocalEchoResponseCommandID
				}
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

func testEchoCommands(t *testing.T, route string) echoCommands {
	t.Helper()
	commands, err := parseEchoRoute(route)
	if err != nil {
		t.Fatalf("parseEchoRoute(%q): %v", route, err)
	}
	return commands
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
