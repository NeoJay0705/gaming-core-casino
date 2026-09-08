package gateproduct

import (
	"context"
	"encoding/binary"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/gorilla/websocket"
)

func TestGateWebSocketContractReadsWritesAndStops(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "gate.yaml")
	if err := os.WriteFile(configPath, []byte("websocket:\n  client_addr: 127.0.0.1:0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ingress := &recordingWebSocketIngress{packets: make(chan WebSocketPacket, 4), events: make(chan string, 8), closedWriteErrs: make(chan error, 8)}
	var server *WebSocketServer
	app, err := NewApp(context.Background(), AppOptions{
		Config:    config.ConfigInputs{MergedPaths: []string{configPath}},
		EnvPrefix: "CORE_CASINO_GATE_WEBSOCKET_TEST__",
	}, testWebSocketModule(ingress, &server))
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	if server == nil {
		t.Fatal("product module did not receive WebSocketServer")
	}
	if err := app.frameworkApp.Start(context.Background()); err != nil {
		t.Fatalf("start app: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = app.frameworkApp.Stop(ctx)
	})
	if conn, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+"/ws", http.Header{"Origin": []string{"https://attacker.example"}}); err == nil {
		_ = conn.Close()
		t.Fatal("cross-origin websocket connection succeeded")
	}
	sameOriginConn, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+"/ws", http.Header{"Origin": []string{"http://" + server.Addr()}})
	if err != nil {
		t.Fatalf("dial same-origin websocket: %v", err)
	}
	assertWebSocketEvent(t, ingress.events, "connect")
	if err := sameOriginConn.Close(); err != nil {
		t.Fatalf("close same-origin client: %v", err)
	}
	assertWebSocketEvent(t, ingress.events, "disconnect")
	assertClosedWriteRejected(t, ingress.closedWriteErrs)

	for _, path := range []string{"/ws", "/ws/tcp/legacy-client", "/wss/tcp/stage-client"} {
		t.Run(path, func(t *testing.T) {
			conn, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+path, http.Header{})
			if err != nil {
				t.Fatalf("dial %s: %v", path, err)
			}
			assertWebSocketEvent(t, ingress.events, "connect")

			request := testWebSocketPacket(0xE10003, []byte("request"))
			if err := conn.WriteMessage(websocket.BinaryMessage, request[:8]); err != nil {
				t.Fatalf("write packet prefix: %v", err)
			}
			if err := conn.WriteMessage(websocket.BinaryMessage, request[8:]); err != nil {
				t.Fatalf("write packet suffix: %v", err)
			}

			select {
			case packet := <-ingress.packets:
				if packet.CommandID != 0xE10003 || string(packet.Payload) != "request" {
					t.Fatalf("read packet = %#v", packet)
				}
			case <-time.After(time.Second):
				t.Fatal("ingress did not receive a packet")
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
			if err := conn.Close(); err != nil {
				t.Fatalf("close client: %v", err)
			}
			assertWebSocketEvent(t, ingress.events, "disconnect")
			assertClosedWriteRejected(t, ingress.closedWriteErrs)
		})
	}

	conn, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+"/ws", http.Header{})
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

func TestNewAppRejectsConfiguredWebSocketWithoutClientAddr(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "gate.yaml")
	if err := os.WriteFile(configPath, []byte("websocket:\n  write_chan_size: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := NewApp(context.Background(), AppOptions{
		Config:    config.ConfigInputs{MergedPaths: []string{configPath}},
		EnvPrefix: "CORE_CASINO_GATE_WEBSOCKET_REQUIRED_TEST__",
	})
	if err == nil || !strings.Contains(err.Error(), "client_addr is required") {
		t.Fatalf("NewApp() error = %v, want missing client_addr", err)
	}
}

func TestGateProductExampleConfigBuildsApp(t *testing.T) {
	_, err := NewApp(context.Background(), AppOptions{
		Config:    config.ConfigInputs{MergedPaths: []string{filepath.Join("..", "..", "configs", "examples", "gateproduct.yaml")}},
		EnvPrefix: "CORE_CASINO_GATE_EXAMPLE_TEST__",
	})
	if err != nil {
		t.Fatalf("build app from Gate example config: %v", err)
	}
}

func TestGateWebSocketContractContainsIngressPanic(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "gate.yaml")
	if err := os.WriteFile(configPath, []byte("websocket:\n  client_addr: 127.0.0.1:0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	app, server := newStartedWebSocketTestApp(t, configPath, panickingWebSocketIngress{})
	t.Cleanup(func() { _ = app.frameworkApp.Stop(context.Background()) })

	conn, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+"/ws", nil)
	if err != nil {
		t.Fatalf("dial panicking ingress: %v", err)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, testWebSocketPacket(1, nil)); err != nil {
		t.Fatalf("write packet: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("connection remained open after ingress panic")
	}
	_ = conn.Close()

	secondConn, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+"/ws", nil)
	if err != nil {
		t.Fatalf("listener did not survive ingress panic: %v", err)
	}
	_ = secondConn.Close()
}

func TestGateWebSocketContractCancelsBlockingIngressOnStop(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "gate.yaml")
	if err := os.WriteFile(configPath, []byte("websocket:\n  client_addr: 127.0.0.1:0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ingress := &blockingWebSocketIngress{started: make(chan struct{}), canceled: make(chan struct{})}
	app, server := newStartedWebSocketTestApp(t, configPath, ingress)

	conn, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+"/ws", nil)
	if err != nil {
		t.Fatalf("dial blocking ingress: %v", err)
	}
	defer conn.Close()
	if err := conn.WriteMessage(websocket.BinaryMessage, testWebSocketPacket(1, nil)); err != nil {
		t.Fatalf("write packet: %v", err)
	}
	select {
	case <-ingress.started:
	case <-time.After(time.Second):
		t.Fatal("blocking ingress was not called")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := app.frameworkApp.Stop(stopCtx); err != nil {
		t.Fatalf("stop app: %v", err)
	}
	select {
	case <-ingress.canceled:
	case <-time.After(time.Second):
		t.Fatal("blocking ingress did not receive cancellation")
	}
}

type recordingWebSocketIngress struct {
	packets         chan WebSocketPacket
	events          chan string
	closedWriteErrs chan error
	mu              sync.Mutex
}

func (i *recordingWebSocketIngress) OnConnect(context.Context, WebSocketSession) {
	i.events <- "connect"
}

func (i *recordingWebSocketIngress) OnPacket(_ context.Context, session WebSocketSession, packet WebSocketPacket) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.packets <- packet
	_ = session.SendBinary(testWebSocketPacket(packet.CommandID+1, []byte("response")))
}

func (i *recordingWebSocketIngress) OnDisconnect(_ context.Context, session WebSocketSession) {
	i.closedWriteErrs <- session.SendBinary(testWebSocketPacket(1, nil))
	i.events <- "disconnect"
}

func assertWebSocketEvent(t *testing.T, events <-chan string, want string) {
	t.Helper()
	select {
	case got := <-events:
		if got != want {
			t.Fatalf("websocket event = %q, want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("websocket event = none, want %q", want)
	}
}

func assertClosedWriteRejected(t *testing.T, errors <-chan error) {
	t.Helper()
	select {
	case err := <-errors:
		if err == nil {
			t.Fatal("SendBinary() succeeded after session close")
		}
	case <-time.After(time.Second):
		t.Fatal("missing post-close SendBinary() result")
	}
}

func newStartedWebSocketTestApp(t *testing.T, configPath string, ingress WebSocketIngress) (*App, *WebSocketServer) {
	t.Helper()
	var server *WebSocketServer
	app, err := NewApp(context.Background(), AppOptions{
		Config:    config.ConfigInputs{MergedPaths: []string{configPath}},
		EnvPrefix: "CORE_CASINO_GATE_WEBSOCKET_TEST__",
	}, testWebSocketModule(ingress, &server))
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	if err := app.frameworkApp.Start(context.Background()); err != nil {
		t.Fatalf("start app: %v", err)
	}
	return app, server
}

type panickingWebSocketIngress struct{}

func (panickingWebSocketIngress) OnConnect(context.Context, WebSocketSession) {}
func (panickingWebSocketIngress) OnPacket(context.Context, WebSocketSession, WebSocketPacket) {
	panic("test ingress panic")
}
func (panickingWebSocketIngress) OnDisconnect(context.Context, WebSocketSession) {}

type blockingWebSocketIngress struct {
	started      chan struct{}
	canceled     chan struct{}
	startedOnce  sync.Once
	canceledOnce sync.Once
}

func (*blockingWebSocketIngress) OnConnect(context.Context, WebSocketSession) {}
func (i *blockingWebSocketIngress) OnPacket(ctx context.Context, _ WebSocketSession, _ WebSocketPacket) {
	i.startedOnce.Do(func() { close(i.started) })
	<-ctx.Done()
	i.canceledOnce.Do(func() { close(i.canceled) })
}
func (*blockingWebSocketIngress) OnDisconnect(context.Context, WebSocketSession) {}

func testWebSocketModule(ingress WebSocketIngress, server **WebSocketServer) framework.Module {
	return func(r framework.Registry) error {
		if err := r.Provide(func() WebSocketIngress { return ingress }); err != nil {
			return err
		}
		return r.AddHook(func(value *WebSocketServer) framework.Hook {
			*server = value
			return framework.Hook{
				Name:    "websocket-contract-observer",
				Phase:   framework.PhaseIngress,
				OnStart: func(context.Context) error { return nil },
			}
		})
	}
}

func testWebSocketPacket(commandID uint32, payload []byte) []byte {
	packet := make([]byte, webSocketPacketHeaderSize+len(payload))
	binary.BigEndian.PutUint32(packet[0:4], commandID)
	binary.BigEndian.PutUint32(packet[4:8], uint32(len(packet)))
	copy(packet[webSocketPacketHeaderSize:], payload)
	return packet
}
