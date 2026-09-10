package gateproduct

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
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
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/NeoJay0705/gaming-core-casino/products/gameproduct"
	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
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
	sessionRegistered := make(chan struct{}, 1)
	var sessions *SessionRegistry
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
	}, dispatcher.Registration{
		Channel:   WebSocketChannel,
		CommandID: 101,
		Handler: func(ctx context.Context, payload []byte) error {
			requestContext, ok := WebSocketRequestContextFrom(ctx)
			if !ok || sessions == nil {
				return errors.New("missing WebSocket request context or registry")
			}
			closable, ok := requestContext.Session.(ClosableWebSocketSession)
			if !ok {
				return errors.New("session is not closable")
			}
			if err := sessions.Register(closable, LoginName(payload)); err != nil {
				return err
			}
			if err := sessions.EnterRoom(LoginName(payload), RoomID("test-room")); err != nil {
				return err
			}
			sessionRegistered <- struct{}{}
			return nil
		},
	})
	t.Cleanup(func() { _ = app.frameworkApp.Stop(context.Background()) })
	sessions = server.registry
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+"/ws", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if err := conn.WriteMessage(websocket.BinaryMessage, testWebSocketPacket(101, []byte("alice"))); err != nil {
		t.Fatalf("write login command: %v", err)
	}
	select {
	case <-sessionRegistered:
	case <-time.After(time.Second):
		t.Fatal("session was not registered")
	}
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

func TestGateWebSocketContractDirectGameReplyReturnsToOriginalConnection(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	deliveryListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deliveryAddress := deliveryListener.Addr().String()
	if err := deliveryListener.Close(); err != nil {
		t.Fatal(err)
	}
	redisClient := redis.NewClient(&redis.Options{Addr: miniRedis.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	keys, err := serversend.NewKeyspace("core-casino")
	if err != nil {
		t.Fatal(err)
	}
	registrar, err := serversend.NewEndpointRegistrar(redisClient, keys, serversend.GateEndpoint{GateID: "gate-direct-reply", Address: deliveryAddress}, serversend.EndpointRegistrarConfig{TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if err := registrar.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registrar.Stop(context.Background()) })

	gameConfig := filepath.Join(t.TempDir(), "game.yaml")
	gameYAML := observabilityTestYAML + "redis:\n  addr: " + miniRedis.Addr() + "\n  key_prefix: core-casino\ngate_to_game:\n  listen_addr: 127.0.0.1:0\nserver_send:\n  presence:\n    lease_ttl: 30s\n  gate:\n    listen_addr: 127.0.0.1:0\n    endpoint_ttl: 30s\n  broadcast:\n    primary: redis\n"
	if err := os.WriteFile(gameConfig, []byte(gameYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	var requestSender serversend.RequestPlayerSender
	var gameServer *gatelink.Server
	gameReady := make(chan struct{})
	gameApp, err := gameproduct.NewApp(context.Background(), gameproduct.AppOptions{Config: config.ConfigInputs{MergedPaths: []string{gameConfig}}, EnvPrefix: "CORE_CASINO_GATE_DIRECT_REPLY_GAME_TEST__"}, dispatcher.Register(dispatcher.Registration{
		Channel:   gameproduct.GateRequestChannel,
		CommandID: 99,
		Handler: func(ctx context.Context, _ []byte) error {
			_, err := requestSender.SendToRequestPlayer(ctx, serversend.RequestPlayerMessage{Message: serversend.Message{CommandID: 0xE20010, Payload: []byte("reply")}})
			return err
		},
	}), func(r framework.Registry) error {
		return r.AddHook(func(sender serversend.RequestPlayerSender, server *gatelink.Server) framework.Hook {
			requestSender, gameServer = sender, server
			return framework.Hook{Name: "capture-direct-reply-game-server", Phase: framework.PhaseIngress, OnStart: func(context.Context) error {
				close(gameReady)
				return nil
			}}
		})
	})
	if err != nil {
		t.Fatalf("new Game app: %v", err)
	}
	gameCtx, stopGame := context.WithCancel(context.Background())
	gameDone := make(chan error, 1)
	go func() { gameDone <- gameApp.Run(gameCtx) }()
	select {
	case <-gameReady:
	case <-time.After(time.Second):
		t.Fatal("Game app did not start")
	}
	t.Cleanup(func() {
		stopGame()
		select {
		case err := <-gameDone:
			if err != nil {
				t.Errorf("stop Game app: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("Game app did not stop")
		}
	})
	if requestSender == nil || gameServer == nil || gameServer.Addr() == "" {
		t.Fatal("Game server-send direct reply dependencies were not started")
	}

	gateConfig := writeWebSocketConfigWithGameTarget(t, gameServer.Addr())
	registered := make(chan struct{}, 1)
	var gateServer *WebSocketServer
	gateApp, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{gateConfig}}, EnvPrefix: "CORE_CASINO_GATE_DIRECT_REPLY_GATE_TEST__"}, func(r framework.Registry) error {
		if err := r.Configure(func(sessions *SessionRegistry, commandDispatcher *dispatcher.Dispatcher) error {
			return commandDispatcher.Register(WebSocketChannel, 100, func(ctx context.Context, payload []byte) error {
				requestContext, ok := WebSocketRequestContextFrom(ctx)
				if !ok {
					return errors.New("missing WebSocket request context")
				}
				session, ok := requestContext.Session.(ClosableWebSocketSession)
				if !ok {
					return errors.New("WebSocket session cannot be registered")
				}
				if err := sessions.Register(session, LoginName(payload)); err != nil {
					return err
				}
				if err := sessions.EnterRoom(LoginName(payload), RoomID("test-room")); err != nil {
					return err
				}
				registered <- struct{}{}
				return nil
			})
		}); err != nil {
			return err
		}
		if err := r.Provide(func(sessions *SessionRegistry) (*serversend.ReceiverServer, error) {
			receiver, err := newGateServerSendReceiver(sessions)
			if err != nil {
				return nil, err
			}
			return serversend.NewReceiverServer(serversend.ReceiverConfig{ListenAddr: deliveryAddress}, receiver)
		}); err != nil {
			return err
		}
		if err := r.Provide(func() *gateServerSendRuntime {
			return &gateServerSendRuntime{gateID: "gate-direct-reply", started: true}
		}); err != nil {
			return err
		}
		if err := r.AddHook(func(receiver *serversend.ReceiverServer) framework.Hook {
			return framework.Hook{Name: "direct-reply-gate-receiver", Phase: framework.PhaseInfrastructure, OnStart: receiver.Start, OnStop: receiver.Stop}
		}); err != nil {
			return err
		}
		return r.AddHook(func(server *WebSocketServer) framework.Hook {
			gateServer = server
			return framework.Hook{Name: "capture-direct-reply-gate-websocket", Phase: framework.PhaseIngress, OnStart: func(context.Context) error { return nil }}
		})
	})
	if err != nil {
		t.Fatalf("new Gate app: %v", err)
	}
	if err := gateApp.frameworkApp.Start(context.Background()); err != nil {
		t.Fatalf("start Gate app: %v", err)
	}
	t.Cleanup(func() { _ = gateApp.frameworkApp.Stop(context.Background()) })
	if gateServer == nil || gateServer.Addr() == "" {
		t.Fatal("Gate WebSocket server did not start")
	}

	conn, _, err := websocket.DefaultDialer.Dial("ws://"+gateServer.Addr()+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.WriteMessage(websocket.BinaryMessage, testWebSocketPacket(100, []byte("alice"))); err != nil {
		t.Fatalf("register WebSocket session: %v", err)
	}
	select {
	case <-registered:
	case <-time.After(time.Second):
		t.Fatal("Gate did not register WebSocket session")
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, testWebSocketPacket(99, []byte("request"))); err != nil {
		t.Fatalf("forward request: %v", err)
	}
	assertWebSocketPacket(t, conn, testWebSocketPacket(0xE20010, []byte("reply")))
}

func TestGateWebSocketContractBroadcastsAndKicksRegisteredSessions(t *testing.T) {
	app, server, sessions, registered := newStartedSessionRegistryTestApp(t, writeWebSocketConfig(t))
	t.Cleanup(func() { _ = app.frameworkApp.Stop(context.Background()) })

	alice, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+"/ws", nil)
	if err != nil {
		t.Fatalf("dial alice: %v", err)
	}
	defer alice.Close()
	bob, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+"/ws", nil)
	if err != nil {
		t.Fatalf("dial bob: %v", err)
	}
	defer bob.Close()
	registerSession(t, alice, "alice", registered)
	registerSession(t, bob, "bob", registered)
	for _, loginName := range []LoginName{"alice", "bob"} {
		if err := sessions.EnterRoom(loginName, "room-a"); err != nil {
			t.Fatalf("enter room for %s: %v", loginName, err)
		}
	}
	packet := encodeWebSocketPacket(WebSocketPacket{CommandID: 0xE10006, Payload: []byte("room-broadcast")})
	if delivered, err := sessions.BroadcastRoom("room-a", packet); err != nil || delivered != 2 {
		t.Fatalf("broadcast room = delivered:%d error:%v, want 2/nil", delivered, err)
	}
	assertWebSocketPacket(t, alice, packet)
	assertWebSocketPacket(t, bob, packet)
	if err := sessions.KickLoginName("alice"); err != nil {
		t.Fatalf("kick alice: %v", err)
	}
	assertWebSocketClosed(t, alice)
	packet = encodeWebSocketPacket(WebSocketPacket{CommandID: 0xE10006, Payload: []byte("bob-only")})
	if delivered, err := sessions.BroadcastRoom("room-a", packet); err != nil || delivered != 1 {
		t.Fatalf("broadcast after alice kick = delivered:%d error:%v, want 1/nil", delivered, err)
	}
	assertWebSocketPacket(t, bob, packet)
	if kicked, err := sessions.KickRoom("room-a"); err != nil || kicked != 1 {
		t.Fatalf("kick room = kicked:%d error:%v, want 1/nil", kicked, err)
	}
	assertWebSocketClosed(t, bob)
}

func TestGateWebSocketContractCleansRegisteredSessionAfterDisconnect(t *testing.T) {
	app, server, sessions, registered := newStartedSessionRegistryTestApp(t, writeWebSocketConfig(t))
	t.Cleanup(func() { _ = app.frameworkApp.Stop(context.Background()) })
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+"/ws", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	registerSession(t, conn, "alice", registered)
	if err := conn.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}
	assertSessionRemoved(t, sessions, "alice")
}

func TestGateWebSocketContractCleansRegisteredSessionOnApplicationStop(t *testing.T) {
	app, server, sessions, registered := newStartedSessionRegistryTestApp(t, writeWebSocketConfig(t))
	t.Cleanup(func() { _ = app.frameworkApp.Stop(context.Background()) })
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr()+"/ws", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	registerSession(t, conn, "alice", registered)
	if err := sessions.EnterRoom("alice", "room-a"); err != nil {
		t.Fatalf("enter room: %v", err)
	}
	if err := app.frameworkApp.Stop(context.Background()); err != nil {
		t.Fatalf("stop app: %v", err)
	}
	assertSessionRemoved(t, sessions, "alice")
	assertRoomMemberRemoved(t, sessions, "alice", "room-a")
	assertWebSocketClosed(t, conn)
}

func TestEncodeWebSocketPacketContract(t *testing.T) {
	packet := WebSocketPacket{
		CommandID: 0xE10004,
		Sequence:  17,
		Session:   23,
		Version:   5,
		Payload:   []byte("response"),
	}
	data := encodeWebSocketPacket(packet)
	if got, want := len(data), webSocketPacketHeaderSize+len(packet.Payload); got != want {
		t.Fatalf("packet length = %d, want %d", got, want)
	}
	if got := binary.BigEndian.Uint32(data[0:4]); got != packet.CommandID {
		t.Fatalf("command id = %#x, want %#x", got, packet.CommandID)
	}
	if got := binary.BigEndian.Uint32(data[4:8]); got != uint32(len(data)) {
		t.Fatalf("declared packet length = %d, want %d", got, len(data))
	}
	if got := binary.BigEndian.Uint32(data[8:12]); got != packet.Sequence {
		t.Fatalf("sequence = %d, want %d", got, packet.Sequence)
	}
	if got := binary.BigEndian.Uint16(data[12:14]); got != packet.Session {
		t.Fatalf("session = %d, want %d", got, packet.Session)
	}
	if got := binary.BigEndian.Uint16(data[14:16]); got != packet.Version {
		t.Fatalf("version = %d, want %d", got, packet.Version)
	}
	if got := data[webSocketPacketHeaderSize:]; string(got) != string(packet.Payload) {
		t.Fatalf("payload = %q, want %q", got, packet.Payload)
	}
}

func TestNewAppRejectsConfiguredWebSocketWithoutClientAddr(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "gate.yaml")
	if err := os.WriteFile(configPath, []byte(observabilityTestYAML+"websocket:\n  write_chan_size: 1\n"+gateToGameTestYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{configPath}}, EnvPrefix: "CORE_CASINO_GATE_WEBSOCKET_REQUIRED_TEST__"})
	if err == nil || !strings.Contains(err.Error(), "client_addr is required") {
		t.Fatalf("NewApp() error = %v, want missing client_addr", err)
	}
}

func TestGateProductExampleConfigBuildsApp(t *testing.T) {
	_, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{
		filepath.Join("..", "..", "configs", "examples", "infra.yaml"),
		filepath.Join("..", "..", "configs", "examples", "gateproduct.yaml"),
	}}, EnvPrefix: "CORE_CASINO_GATE_EXAMPLE_TEST__"})
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
	contents := observabilityTestYAML + "websocket:\n  client_addr: 127.0.0.1:0\ngate_to_game:\n  target: " + target + "\n"
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

func newStartedSessionRegistryTestApp(t *testing.T, configPath string) (*App, *WebSocketServer, *SessionRegistry, <-chan struct{}) {
	t.Helper()
	var server *WebSocketServer
	var sessions *SessionRegistry
	registered := make(chan struct{}, 2)
	app, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{configPath}}, EnvPrefix: "CORE_CASINO_GATE_SESSION_REGISTRY_TEST__"}, func(r framework.Registry) error {
		if err := r.Configure(func(registry *SessionRegistry, commandDispatcher *dispatcher.Dispatcher) error {
			sessions = registry
			return commandDispatcher.Register(WebSocketChannel, 0xE10005, func(ctx context.Context, payload []byte) error {
				requestContext, ok := WebSocketRequestContextFrom(ctx)
				if !ok {
					return errors.New("missing WebSocket request context")
				}
				session, ok := requestContext.Session.(ClosableWebSocketSession)
				if !ok {
					return errors.New("WebSocket session cannot be closed")
				}
				if err := registry.Register(session, LoginName(payload)); err != nil {
					return err
				}
				registered <- struct{}{}
				return nil
			})
		}); err != nil {
			return err
		}
		return r.AddHook(func(value *WebSocketServer) framework.Hook {
			server = value
			return framework.Hook{Name: "session-registry-contract-observer", Phase: framework.PhaseIngress, OnStart: func(context.Context) error { return nil }}
		})
	})
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	if err := app.frameworkApp.Start(context.Background()); err != nil {
		t.Fatalf("start app: %v", err)
	}
	return app, server, sessions, registered
}

func assertSessionRemoved(t *testing.T, sessions *SessionRegistry, loginName LoginName) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		sessions.mu.RLock()
		_, exists := sessions.byLoginName[loginName]
		sessions.mu.RUnlock()
		if !exists {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("session %q remained registered after WebSocket disconnect", loginName)
}

func assertRoomMemberRemoved(t *testing.T, sessions *SessionRegistry, loginName LoginName, roomID RoomID) {
	t.Helper()
	sessions.mu.RLock()
	_, exists := sessions.byRoomID[roomID][loginName]
	sessions.mu.RUnlock()
	if exists {
		t.Fatalf("%q remained in room %q", loginName, roomID)
	}
}

func registerSession(t *testing.T, conn *websocket.Conn, loginName string, registered <-chan struct{}) {
	t.Helper()
	if err := conn.WriteMessage(websocket.BinaryMessage, testWebSocketPacket(0xE10005, []byte(loginName))); err != nil {
		t.Fatalf("write registration packet for %s: %v", loginName, err)
	}
	select {
	case <-registered:
	case <-time.After(time.Second):
		t.Fatalf("session registration handler was not called for %s", loginName)
	}
}

func assertWebSocketPacket(t *testing.T, conn *websocket.Conn, want []byte) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	messageType, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read WebSocket packet: %v", err)
	}
	if messageType != websocket.BinaryMessage || string(data) != string(want) {
		t.Fatalf("WebSocket packet = type:%d data:%x, want binary %x", messageType, data, want)
	}
}

func assertWebSocketClosed(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("WebSocket connection remained readable after closure")
	} else {
		var networkError net.Error
		if errors.As(err, &networkError) && networkError.Timeout() {
			t.Fatalf("WebSocket read timed out; connection was not closed: %v", err)
		}
	}
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
