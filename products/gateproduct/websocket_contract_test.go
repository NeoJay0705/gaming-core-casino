package gateproduct

import (
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"github.com/gorilla/websocket"
)

const gateToGameTestYAML = "gate_to_game:\n  target: dns:///gameproduct:9090\n"

func TestGateWebSocketContractDispatchesReadsWritesAndStops(t *testing.T) {
	configPath := writeWebSocketConfig(t)
	packets := make(chan []byte, 4)
	const (
		requestSequence = 17
		requestSession  = 23
		requestVersion  = 5
	)
	app, server := newStartedWebSocketTestApp(t, configPath, dispatcher.Registration{
		Channel: WebSocketChannel, CommandID: 0xE10003,
		Handler: func(ctx context.Context, payload []byte) error {
			webSocketContext, ok := WebSocketRequestContextFrom(ctx)
			if !ok {
				return errors.New("missing WebSocket request context")
			}
			requestContext, ok := gatelink.GateRequestContextFrom(ctx)
			if !ok || requestContext != webSocketContext.Request || requestContext.Source.ConnectionID == "" {
				return errors.New("missing WebSocket request source context")
			}
			if packet := webSocketContext.Packet; packet.CommandID != 0xE10003 || packet.Sequence != requestSequence || packet.Session != requestSession || packet.Version != requestVersion {
				return errors.New("missing WebSocket packet header context")
			}
			packets <- append([]byte(nil), payload...)
			return webSocketContext.Session.SendBinary(testWebSocketPacket(0xE10004, []byte("response")))
		},
	})
	t.Cleanup(func() { _ = app.frameworkApp.Stop(context.Background()) })

	if conn, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+"/ws", http.Header{"Origin": []string{"https://attacker.example"}}); err == nil {
		_ = conn.Close()
		t.Fatal("cross-origin websocket connection succeeded")
	}
	for _, path := range []string{"/ws", "/ws/tcp/legacy-client", "/wss/tcp/stage-client"} {
		t.Run(path, func(t *testing.T) {
			conn, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+path, nil)
			if err != nil {
				t.Fatalf("dial %s: %v", path, err)
			}
			defer conn.Close()
			request := testWebSocketPacketWithHeader(0xE10003, requestSequence, requestSession, requestVersion, []byte("request"))
			if err := conn.WriteMessage(websocket.BinaryMessage, request[:8]); err != nil {
				t.Fatalf("write packet prefix: %v", err)
			}
			if err := conn.WriteMessage(websocket.BinaryMessage, request[8:]); err != nil {
				t.Fatalf("write packet suffix: %v", err)
			}
			select {
			case payload := <-packets:
				if string(payload) != "request" {
					t.Fatalf("handler payload = %q, want request", payload)
				}
			case <-time.After(time.Second):
				t.Fatal("dispatcher handler did not receive packet")
			}
			_ = conn.SetReadDeadline(time.Now().Add(time.Second))
			messageType, data, err := conn.ReadMessage()
			if err != nil {
				t.Fatalf("read response: %v", err)
			}
			wantResponse := testWebSocketPacket(0xE10004, []byte("response"))
			if messageType != websocket.BinaryMessage || string(data) != string(wantResponse) {
				t.Fatalf("response = type:%d data:%x, want binary %x", messageType, data, wantResponse)
			}
		})
	}

	conn, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+"/ws", nil)
	if err != nil {
		t.Fatalf("dial shutdown connection: %v", err)
	}
	if err := app.frameworkApp.Stop(context.Background()); err != nil {
		t.Fatalf("stop app: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("connection remained readable after app stop")
	}
	_ = conn.Close()
}

func TestGateWebSocketContractForwardsUnhandledCommandToGame(t *testing.T) {
	type receivedRequest struct {
		request gatelink.Request
		context gatelink.GateRequestContext
	}
	received := make(chan receivedRequest, 2)
	gameServer, err := gatelink.NewServer(gatelink.ServerConfig{ListenAddr: "127.0.0.1:0"}, gatelink.RequestHandlerFunc(func(ctx context.Context, request gatelink.Request) error {
		requestContext, ok := gatelink.GateRequestContextFrom(ctx)
		if !ok {
			return errors.New("missing Gate request context")
		}
		received <- receivedRequest{request: request, context: requestContext}
		return nil
	}))
	if err != nil {
		t.Fatalf("new Game server: %v", err)
	}
	if err := gameServer.Start(context.Background()); err != nil {
		t.Fatalf("start Game server: %v", err)
	}
	t.Cleanup(func() { _ = gameServer.Stop(context.Background()) })

	localHandled := make(chan WebSocketRequestContext, 1)
	app, server := newStartedWebSocketTestApp(t, writeWebSocketConfigWithGameTarget(t, gameServer.Addr()), dispatcher.Registration{
		Channel:   WebSocketChannel,
		CommandID: 100,
		Handler: func(ctx context.Context, _ []byte) error {
			requestContext, ok := WebSocketRequestContextFrom(ctx)
			if !ok {
				return errors.New("missing WebSocket request context")
			}
			localHandled <- requestContext
			return nil
		},
	})
	t.Cleanup(func() { _ = app.frameworkApp.Stop(context.Background()) })
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+"/ws", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if err := conn.WriteMessage(websocket.BinaryMessage, testWebSocketPacket(99, []byte("forwarded"))); err != nil {
		t.Fatalf("write unregistered command: %v", err)
	}
	var forwarded receivedRequest
	select {
	case forwarded = <-received:
		if forwarded.request.CommandID != 99 || string(forwarded.request.Payload) != "forwarded" {
			t.Fatalf("forwarded request = %#v, want command 99 and original payload", forwarded.request)
		}
		if forwarded.context.Source.ConnectionID == "" {
			t.Fatal("forwarded request is missing WebSocket connection ID")
		}
	case <-time.After(time.Second):
		t.Fatal("Game did not receive unhandled WebSocket command")
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, testWebSocketPacketWithHeader(100, 7, 11, 13, nil)); err != nil {
		t.Fatalf("write locally handled command: %v", err)
	}
	select {
	case local := <-localHandled:
		if local.Request.Source.ConnectionID != forwarded.context.Source.ConnectionID {
			t.Fatalf("local connection ID = %q, want forwarded connection ID %q", local.Request.Source.ConnectionID, forwarded.context.Source.ConnectionID)
		}
		if packet := local.Packet; packet.Sequence != 7 || packet.Session != 11 || packet.Version != 13 {
			t.Fatalf("local packet header = %#v, want sequence/session/version 7/11/13", packet)
		}
	case <-time.After(time.Second):
		t.Fatal("locally registered command was not handled")
	}
	select {
	case unexpected := <-received:
		t.Fatalf("locally handled command was also forwarded to Game: %#v", unexpected.request)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestNewAppRejectsConfiguredWebSocketWithoutClientAddr(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "gate.yaml")
	if err := os.WriteFile(configPath, []byte("websocket:\n  write_chan_size: 1\n"+gateToGameTestYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{configPath}}, EnvPrefix: "CORE_CASINO_GATE_WEBSOCKET_REQUIRED_TEST__"})
	if err == nil || !strings.Contains(err.Error(), "client_addr is required") {
		t.Fatalf("NewApp() error = %v, want missing client_addr", err)
	}
}

func TestGateProductExampleConfigBuildsApp(t *testing.T) {
	_, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{filepath.Join("..", "..", "configs", "examples", "gateproduct.yaml")}}, EnvPrefix: "CORE_CASINO_GATE_EXAMPLE_TEST__"})
	if err != nil {
		t.Fatalf("build app from Gate example config: %v", err)
	}
}

func TestGateWebSocketContractContainsDispatcherPanic(t *testing.T) {
	app, server := newStartedWebSocketTestApp(t, writeWebSocketConfig(t), dispatcher.Registration{
		Channel: WebSocketChannel, CommandID: 1,
		Handler: func(context.Context, []byte) error { panic("test dispatcher panic") },
	})
	t.Cleanup(func() { _ = app.frameworkApp.Stop(context.Background()) })
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+"/ws", nil)
	if err != nil {
		t.Fatalf("dial panic test: %v", err)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, testWebSocketPacket(1, nil)); err != nil {
		t.Fatalf("write packet: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("connection remained open after dispatcher panic")
	}
	_ = conn.Close()
}

func TestGateWebSocketContractClosesSessionOnDispatcherError(t *testing.T) {
	app, server := newStartedWebSocketTestApp(t, writeWebSocketConfig(t), dispatcher.Registration{
		Channel:   WebSocketChannel,
		CommandID: 1,
		Handler:   func(context.Context, []byte) error { return errors.New("handler failed") },
	})
	t.Cleanup(func() { _ = app.frameworkApp.Stop(context.Background()) })
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+"/ws", nil)
	if err != nil {
		t.Fatalf("dial error test: %v", err)
	}
	defer conn.Close()
	if err := conn.WriteMessage(websocket.BinaryMessage, testWebSocketPacket(1, nil)); err != nil {
		t.Fatalf("write packet: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("connection remained open after dispatcher error")
	}
}

func TestGateWebSocketContractCancelsBlockingDispatcherOnStop(t *testing.T) {
	started, canceled := make(chan struct{}), make(chan struct{})
	var startedOnce, canceledOnce sync.Once
	app, server := newStartedWebSocketTestApp(t, writeWebSocketConfig(t), dispatcher.Registration{
		Channel: WebSocketChannel, CommandID: 1,
		Handler: func(ctx context.Context, _ []byte) error {
			startedOnce.Do(func() { close(started) })
			<-ctx.Done()
			canceledOnce.Do(func() { close(canceled) })
			return ctx.Err()
		},
	})
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+"/ws", nil)
	if err != nil {
		t.Fatalf("dial blocking test: %v", err)
	}
	defer conn.Close()
	if err := conn.WriteMessage(websocket.BinaryMessage, testWebSocketPacket(1, nil)); err != nil {
		t.Fatalf("write packet: %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("blocking dispatcher handler was not called")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := app.frameworkApp.Stop(stopCtx); err != nil {
		t.Fatalf("stop app: %v", err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("blocking dispatcher handler did not receive cancellation")
	}
}

func writeWebSocketConfig(t *testing.T) string {
	return writeWebSocketConfigWithGameTarget(t, "dns:///gameproduct:9090")
}

func writeWebSocketConfigWithGameTarget(t *testing.T, target string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gate.yaml")
	contents := "websocket:\n  client_addr: 127.0.0.1:0\ngate_to_game:\n  target: " + target + "\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func newStartedWebSocketTestApp(t *testing.T, configPath string, registrations ...dispatcher.Registration) (*App, *WebSocketServer) {
	t.Helper()
	var server *WebSocketServer
	modules := append([]framework.Module{dispatcher.Register(registrations...)}, func(r framework.Registry) error {
		return r.AddHook(func(value *WebSocketServer) framework.Hook {
			server = value
			return framework.Hook{Name: "websocket-contract-observer", Phase: framework.PhaseIngress, OnStart: func(context.Context) error { return nil }}
		})
	})
	app, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{configPath}}, EnvPrefix: "CORE_CASINO_GATE_WEBSOCKET_TEST__"}, modules...)
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	if err := app.frameworkApp.Start(context.Background()); err != nil {
		t.Fatalf("start app: %v", err)
	}
	return app, server
}

func testWebSocketPacket(commandID uint32, payload []byte) []byte {
	return testWebSocketPacketWithHeader(commandID, 0, 0, 0, payload)
}

func testWebSocketPacketWithHeader(commandID, sequence uint32, session, version uint16, payload []byte) []byte {
	packet := make([]byte, webSocketPacketHeaderSize+len(payload))
	binary.BigEndian.PutUint32(packet[0:4], commandID)
	binary.BigEndian.PutUint32(packet[4:8], uint32(len(packet)))
	binary.BigEndian.PutUint32(packet[8:12], sequence)
	binary.BigEndian.PutUint16(packet[12:14], session)
	binary.BigEndian.PutUint16(packet[14:16], version)
	copy(packet[webSocketPacketHeaderSize:], payload)
	return packet
}
