package metrics_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/protocol"
	"github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/workflow"
	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gateproto"
	"github.com/NeoJay0705/gaming-core-casino/pkg/grpcserver"
	"github.com/NeoJay0705/gaming-core-casino/products/gameproduct"
	"github.com/NeoJay0705/gaming-core-casino/products/gateproduct"
	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

func TestMetricsExampleGateGameFlowContract(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	gameConfig := writeFlowConfig(t, fmt.Sprintf(`observability:
  listen_addr: "127.0.0.1:0"
grpc:
  server:
    listen_addr: "127.0.0.1:0"
  clients:
    gate:
      timeout: "2s"
      fanout:
        target: "dns:///gate-grpc-headless:9091"
server_send:
  broadcast:
    primary: "redis"
redis:
  addr: %q
  key_prefix: "metrics-flow"
`, miniRedis.Addr()))

	gameAddress := make(chan string, 1)
	var gameRegisterer prometheus.Registerer
	gameApp, err := gameproduct.NewApp(context.Background(), gameproduct.AppOptions{
		Config:    config.ConfigInputs{MergedPaths: []string{gameConfig}},
		EnvPrefix: "CORE_CASINO_METRICS_FLOW_GAME__",
	}, workflow.GameModule(), captureGameServer(gameAddress), captureRegisterer(&gameRegisterer))
	if err != nil {
		t.Fatalf("build Game app: %v", err)
	}
	gameContext, cancelGame := context.WithCancel(context.Background())
	gameDone := runApp(t, gameApp.Run, gameContext)
	t.Cleanup(func() {
		cancelGame()
		waitApp(t, gameDone)
	})
	gameAddr := waitForAddress(t, gameAddress, gameDone)

	gateConfig := writeFlowConfig(t, fmt.Sprintf(`observability:
  listen_addr: "127.0.0.1:0"
websocket:
  client_addr: "127.0.0.1:0"
grpc:
  server:
    listen_addr: "127.0.0.1:0"
  clients:
    game:
      target: %q
      timeout: "2s"
    gate:
      timeout: "2s"
      fanout:
        target: "dns:///gate-grpc-headless:9091"
  endpoint_registration:
    ttl: "30s"
session_ownership:
  lease_ttl: "30s"
server_send:
  broadcast:
    primary: "redis"
redis:
  addr: %q
  key_prefix: "metrics-flow"
`, gameAddr, miniRedis.Addr()))

	gateAddress := make(chan string, 1)
	var gateRegisterer prometheus.Registerer
	gateApp, err := gateproduct.NewApp(context.Background(), gateproduct.AppOptions{
		Config:    config.ConfigInputs{MergedPaths: []string{gateConfig}},
		EnvPrefix: "CORE_CASINO_METRICS_FLOW_GATE__",
	}, workflow.GateModule(), captureGateServer(gateAddress), captureRegisterer(&gateRegisterer))
	if err != nil {
		t.Fatalf("build Gate app: %v", err)
	}
	gateContext, cancelGate := context.WithCancel(context.Background())
	gateDone := runApp(t, gateApp.Run, gateContext)
	t.Cleanup(func() {
		cancelGate()
		waitApp(t, gateDone)
	})
	gateAddr := waitForAddress(t, gateAddress, gateDone)

	conn, _, err := websocket.DefaultDialer.Dial("ws://"+gateAddr+"/ws", nil)
	if err != nil {
		t.Fatalf("dial Gate: %v", err)
	}
	defer conn.Close()
	if err := flowRoundTrip(conn, gateproto.LoginRequestCommandID, &gateproto.LoginRequest{LoginName: "flow-player"}, gateproto.LoginResponseCommandID); err != nil {
		t.Fatalf("login flow: %v", err)
	}
	if err := flowRoundTrip(conn, protocol.EnterRoomRequestCommandID, &protocol.EnterRoomRequest{RoomId: "flow-room"}, protocol.EnterRoomResponseCommandID); err != nil {
		t.Fatalf("enter-room flow: %v", err)
	}
	broadcastPayload := mustFlowMarshal(&protocol.EchoResponse{Payload: []byte("metrics-broadcast")})
	broadcastCommand := &protocol.BroadcastRoomCommand{
		RoomId:          "flow-room",
		ClientCommandId: protocol.EchoResponseCommandID,
		ClientPayload:   broadcastPayload,
	}
	if err := writeFlowPacket(conn, protocol.BroadcastRoomCommandID, 3, mustFlowMarshal(broadcastCommand)); err != nil {
		t.Fatalf("write room broadcast: %v", err)
	}
	broadcastResponse, err := readFlowPacket(conn, protocol.EchoResponseCommandID)
	if err != nil {
		t.Fatalf("read room broadcast: %v", err)
	}
	var broadcastEcho protocol.EchoResponse
	if err := proto.Unmarshal(broadcastResponse, &broadcastEcho); err != nil {
		t.Fatalf("decode room broadcast: %v", err)
	}
	if string(broadcastEcho.GetPayload()) != "metrics-broadcast" {
		t.Fatalf("room broadcast payload = %q, want metrics-broadcast", broadcastEcho.GetPayload())
	}
	echoPayload := []byte("metrics-flow")
	echoRequest, err := proto.Marshal(&protocol.EchoRequest{Payload: echoPayload})
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFlowPacket(conn, protocol.EchoRequestCommandID, 3, echoRequest); err != nil {
		t.Fatalf("write Echo: %v", err)
	}
	response, err := readFlowPacket(conn, protocol.EchoResponseCommandID)
	if err != nil {
		t.Fatalf("read Echo response: %v", err)
	}
	echo := new(protocol.EchoResponse)
	if err := proto.Unmarshal(response, echo); err != nil {
		t.Fatalf("decode Echo response: %v", err)
	}
	if string(echo.GetPayload()) != string(echoPayload) {
		t.Fatalf("Echo payload = %q, want %q", echo.GetPayload(), echoPayload)
	}
	localPayload := []byte("metrics-local")
	if err := writeFlowPacket(conn, protocol.LocalEchoRequestCommandID, 4, mustFlowMarshal(&protocol.EchoRequest{Payload: localPayload})); err != nil {
		t.Fatalf("write local Echo: %v", err)
	}
	localResponse, err := readFlowPacket(conn, protocol.LocalEchoResponseCommandID)
	if err != nil {
		t.Fatalf("read local Echo response: %v", err)
	}
	localEcho := new(protocol.EchoResponse)
	if err := proto.Unmarshal(localResponse, localEcho); err != nil {
		t.Fatalf("decode local Echo response: %v", err)
	}
	if string(localEcho.GetPayload()) != string(localPayload) {
		t.Fatalf("local Echo payload = %q, want %q", localEcho.GetPayload(), localPayload)
	}

	unauthenticated, _, err := websocket.DefaultDialer.Dial("ws://"+gateAddr+"/ws", nil)
	if err != nil {
		t.Fatalf("dial unauthenticated client: %v", err)
	}
	if err := writeFlowPacket(unauthenticated, protocol.EnterRoomRequestCommandID, 1, mustFlowMarshal(&protocol.EnterRoomRequest{RoomId: "flow-room"})); err != nil {
		t.Fatalf("write unauthenticated EnterRoom: %v", err)
	}
	if err := expectFlowConnectionClose(unauthenticated); err != nil {
		t.Fatalf("unauthenticated EnterRoom close: %v", err)
	}
	_ = unauthenticated.Close()

	localUnauthenticated, _, err := websocket.DefaultDialer.Dial("ws://"+gateAddr+"/ws", nil)
	if err != nil {
		t.Fatalf("dial local unauthenticated client: %v", err)
	}
	if err := writeFlowPacket(localUnauthenticated, protocol.LocalEchoRequestCommandID, 1, mustFlowMarshal(&protocol.EchoRequest{Payload: []byte("unauthenticated-local")})); err != nil {
		t.Fatalf("write unauthenticated local Echo: %v", err)
	}
	if err := expectFlowConnectionClose(localUnauthenticated); err != nil {
		t.Fatalf("unauthenticated local Echo close: %v", err)
	}
	_ = localUnauthenticated.Close()

	loginOnly, _, err := websocket.DefaultDialer.Dial("ws://"+gateAddr+"/ws", nil)
	if err != nil {
		t.Fatalf("dial login-only client: %v", err)
	}
	if err := flowRoundTrip(loginOnly, gateproto.LoginRequestCommandID, &gateproto.LoginRequest{LoginName: "login-only"}, gateproto.LoginResponseCommandID); err != nil {
		t.Fatalf("login-only flow: %v", err)
	}
	if err := writeFlowPacket(loginOnly, protocol.EchoRequestCommandID, 2, mustFlowMarshal(&protocol.EchoRequest{Payload: []byte("blocked")})); err != nil {
		t.Fatalf("write login-only Echo: %v", err)
	}
	if err := expectFlowConnectionClose(loginOnly); err != nil {
		t.Fatalf("login-only Echo close: %v", err)
	}
	_ = loginOnly.Close()

	localLoginOnly, _, err := websocket.DefaultDialer.Dial("ws://"+gateAddr+"/ws", nil)
	if err != nil {
		t.Fatalf("dial local login-only client: %v", err)
	}
	if err := flowRoundTrip(localLoginOnly, gateproto.LoginRequestCommandID, &gateproto.LoginRequest{LoginName: "local-login-only"}, gateproto.LoginResponseCommandID); err != nil {
		t.Fatalf("local login-only flow: %v", err)
	}
	if err := writeFlowPacket(localLoginOnly, protocol.LocalEchoRequestCommandID, 2, mustFlowMarshal(&protocol.EchoRequest{Payload: []byte("blocked-local")})); err != nil {
		t.Fatalf("write local login-only Echo: %v", err)
	}
	if err := expectFlowConnectionClose(localLoginOnly); err != nil {
		t.Fatalf("local login-only Echo close: %v", err)
	}
	_ = localLoginOnly.Close()
	closeFlowConnection(conn)
	waitForGaugeZero(t, gateRegisterer, "gaming_core_gate_websocket_connections")

	assertCounterSample(t, gateRegisterer, "gaming_core_gate_websocket_commands_total", map[string]string{
		"route": "local", "command": strconv.FormatUint(uint64(gateproto.LoginRequestCommandID), 10), "result": "success",
	}, 3)
	assertCounterSample(t, gateRegisterer, "gaming_core_gate_websocket_commands_total", map[string]string{
		"route": "local", "command": strconv.FormatUint(uint64(protocol.EnterRoomRequestCommandID), 10), "result": "success",
	}, 1)
	assertCounterSample(t, gateRegisterer, "gaming_core_gate_websocket_commands_total", map[string]string{
		"route": "local", "command": strconv.FormatUint(uint64(protocol.EnterRoomRequestCommandID), 10), "result": "error",
	}, 1)
	assertCounterSample(t, gateRegisterer, "gaming_core_gate_websocket_commands_total", map[string]string{
		"route": "game", "command": "forward", "result": "success",
	}, 2)
	assertCounterSample(t, gateRegisterer, "gaming_core_gate_websocket_commands_total", map[string]string{
		"route": "game", "command": "forward", "result": "error",
	}, 1)
	assertCounterSample(t, gateRegisterer, "gaming_core_gate_websocket_commands_total", map[string]string{
		"route": "local", "command": strconv.FormatUint(uint64(protocol.LocalEchoRequestCommandID), 10), "result": "success",
	}, 1)
	assertCounterSample(t, gateRegisterer, "gaming_core_gate_websocket_commands_total", map[string]string{
		"route": "local", "command": strconv.FormatUint(uint64(protocol.LocalEchoRequestCommandID), 10), "result": "error",
	}, 2)
	assertCounterSample(t, gateRegisterer, "gaming_core_gate_game_grpc_requests_total", map[string]string{"code": "OK"}, 2)
	assertCounterSample(t, gateRegisterer, "gaming_core_gate_websocket_writes_total", map[string]string{"source": "handler", "result": "success"}, 5)
	assertCounterSample(t, gateRegisterer, "gaming_core_gate_websocket_writes_total", map[string]string{"source": "server_send", "result": "success"}, 2)
	assertHistogramSample(t, gateRegisterer, "gaming_core_gate_server_send_delivery_duration_seconds", map[string]string{"target": "connection", "result": "success"}, 1)
	assertHistogramSample(t, gateRegisterer, "gaming_core_gate_server_send_delivery_duration_seconds", map[string]string{"target": "room", "result": "success"}, 1)
	assertCounterSample(t, gateRegisterer, "gaming_core_gate_websocket_connection_closes_total", map[string]string{"reason": "login_required"}, 2)
	assertCounterSample(t, gateRegisterer, "gaming_core_gate_websocket_connection_closes_total", map[string]string{"reason": "room_required"}, 2)
	assertCounterSample(t, gateRegisterer, "gaming_core_gate_websocket_connection_closes_total", map[string]string{"reason": "client_closed"}, 1)
	assertGatheredFamily(t, gateRegisterer, "gaming_core_gate_websocket_commands_total")
	assertGatheredFamily(t, gateRegisterer, "gaming_core_gate_server_send_delivery_duration_seconds")
	assertGaugeZero(t, gateRegisterer, "gaming_core_gate_websocket_commands_in_flight")
	assertGaugeZero(t, gateRegisterer, "gaming_core_gate_game_grpc_in_flight")
	assertGaugeZero(t, gateRegisterer, "gaming_core_gate_websocket_writes_in_flight")
	assertGaugeZero(t, gateRegisterer, "gaming_core_gate_websocket_write_queue_messages")
	assertGatheredFamily(t, gameRegisterer, "gaming_core_game_gate_commands_total")
	assertGatheredFamily(t, gameRegisterer, "gaming_core_game_server_send_requests_total")
	assertCounterSample(t, gameRegisterer, "gaming_core_game_gate_commands_total", map[string]string{
		"command": strconv.FormatUint(uint64(protocol.EchoRequestCommandID), 10),
		"result":  "success",
	}, 1)
	assertCounterSample(t, gameRegisterer, "gaming_core_game_gate_commands_total", map[string]string{
		"command": strconv.FormatUint(uint64(protocol.BroadcastRoomCommandID), 10),
		"result":  "success",
	}, 1)
	assertOnlyCounterSample(t, gameRegisterer, "gaming_core_game_server_send_requests_total", map[string]string{
		"operation": "request_player",
		"result":    "success",
	}, 1)
	assertGaugeZero(t, gameRegisterer, "gaming_core_game_gate_commands_in_flight")
}

func captureGameServer(address chan<- string) framework.Module {
	return func(r framework.Registry) error {
		return r.AddHook(func(server *grpcserver.Server) framework.Hook {
			return framework.Hook{Name: "capture-flow-game-server", Phase: framework.PhaseIngress, OnStart: func(context.Context) error {
				if value := server.Addr(); value != "" {
					address <- value
				}
				return nil
			}}
		})
	}
}

func captureGateServer(address chan<- string) framework.Module {
	return func(r framework.Registry) error {
		return r.AddHook(func(server *gateproduct.WebSocketServer) framework.Hook {
			return framework.Hook{Name: "capture-flow-gate-server", Phase: framework.PhaseIngress, OnStart: func(context.Context) error {
				if value := server.Addr(); value != "" {
					address <- value
				}
				return nil
			}}
		})
	}
}

func captureRegisterer(target *prometheus.Registerer) framework.Module {
	return func(r framework.Registry) error {
		return r.Configure(func(registerer prometheus.Registerer) error {
			*target = registerer
			return nil
		})
	}
}

func writeFlowConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metrics-flow.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func runApp(t *testing.T, run func(context.Context) error, ctx context.Context) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	return done
}

func waitForAddress(t *testing.T, address <-chan string, done <-chan error) string {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	select {
	case addressValue := <-address:
		return addressValue
	case err := <-done:
		t.Fatalf("app stopped before listener became ready: %v", err)
	case <-deadline.C:
		t.Fatal("listener address was not captured")
	}
	return ""
}

func waitApp(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("app stop: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("app did not stop")
	}
}

func flowRoundTrip(conn *websocket.Conn, commandID uint32, request proto.Message, responseCommandID uint32) error {
	if err := writeFlowPacket(conn, commandID, 1, mustFlowMarshal(request)); err != nil {
		return err
	}
	_, err := readFlowPacket(conn, responseCommandID)
	return err
}

func writeFlowPacket(conn *websocket.Conn, commandID, sequence uint32, payload []byte) error {
	return conn.WriteMessage(websocket.BinaryMessage, gateproduct.EncodeWebSocketPacket(gateproduct.WebSocketPacket{CommandID: commandID, Sequence: sequence, Payload: payload}))
}

func readFlowPacket(conn *websocket.Conn, commandID uint32) ([]byte, error) {
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			return nil, err
		}
		if messageType != websocket.BinaryMessage || len(data) < 16 || binary.BigEndian.Uint32(data[0:4]) != commandID {
			continue
		}
		size := int(binary.BigEndian.Uint32(data[4:8]))
		if size < 16 || size > len(data) {
			return nil, fmt.Errorf("invalid flow packet size %d", size)
		}
		return data[16:size], nil
	}
}

func expectFlowConnectionClose(conn *websocket.Conn) error {
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := conn.ReadMessage()
	if err == nil {
		return errors.New("connection remained readable")
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return errors.New("timed out waiting for connection close")
	}
	return nil
}

func assertGatheredFamily(t *testing.T, registerer prometheus.Registerer, name string) {
	t.Helper()
	gatherer, ok := registerer.(prometheus.Gatherer)
	if !ok {
		t.Fatalf("registerer %T does not expose test gatherer", registerer)
	}
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("gather %s: %v", name, err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return
		}
	}
	t.Fatalf("metric family %q was not gathered", name)
}

func assertGaugeZero(t *testing.T, registerer prometheus.Registerer, name string) {
	t.Helper()
	gatherer, ok := registerer.(prometheus.Gatherer)
	if !ok {
		t.Fatalf("registerer %T does not expose test gatherer", registerer)
	}
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("gather %s: %v", name, err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			if metric.GetGauge().GetValue() != 0 {
				t.Fatalf("gauge %s = %v, want 0", name, metric.GetGauge().GetValue())
			}
		}
		return
	}
	t.Fatalf("gauge family %q was not gathered", name)
}

func waitForGaugeZero(t *testing.T, registerer prometheus.Registerer, name string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	interval := time.NewTicker(10 * time.Millisecond)
	defer interval.Stop()
	for {
		if value, ok := gatheredGaugeValue(t, registerer, name); ok && value == 0 {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("gauge %q did not reach zero", name)
		}
		<-interval.C
	}
}

func gatheredGaugeValue(t *testing.T, registerer prometheus.Registerer, name string) (float64, bool) {
	t.Helper()
	gatherer, ok := registerer.(prometheus.Gatherer)
	if !ok {
		t.Fatalf("registerer %T does not expose test gatherer", registerer)
	}
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("gather %s: %v", name, err)
	}
	for _, family := range families {
		if family.GetName() == name && len(family.Metric) != 0 {
			return family.Metric[0].GetGauge().GetValue(), true
		}
	}
	return 0, false
}

func assertCounterSample(t *testing.T, registerer prometheus.Registerer, name string, wantLabels map[string]string, wantValue float64) {
	t.Helper()
	metric := findFlowMetric(t, registerer, name, wantLabels)
	if got := metric.GetCounter().GetValue(); got != wantValue {
		t.Fatalf("counter %s = %v, want %v", name, got, wantValue)
	}
}

func assertHistogramSample(t *testing.T, registerer prometheus.Registerer, name string, wantLabels map[string]string, wantCount uint64) {
	t.Helper()
	metric := findFlowMetric(t, registerer, name, wantLabels)
	if got := metric.GetHistogram().GetSampleCount(); got != wantCount {
		t.Fatalf("histogram %s count = %d, want %d", name, got, wantCount)
	}
}

func findFlowMetric(t *testing.T, registerer prometheus.Registerer, name string, wantLabels map[string]string) *dto.Metric {
	t.Helper()
	gatherer, ok := registerer.(prometheus.Gatherer)
	if !ok {
		t.Fatalf("registerer %T does not expose test gatherer", registerer)
	}
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("gather %s: %v", name, err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			labels := make(map[string]string, len(metric.Label))
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if len(labels) != len(wantLabels) {
				continue
			}
			matched := true
			for key, want := range wantLabels {
				if labels[key] != want {
					matched = false
					break
				}
			}
			if matched {
				return metric
			}
		}
	}
	t.Fatalf("metric %q with labels %#v was not gathered", name, wantLabels)
	return nil
}

func closeFlowConnection(conn *websocket.Conn) {
	if conn == nil {
		return
	}
	_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
	_ = conn.Close()
}

func assertOnlyCounterSample(t *testing.T, registerer prometheus.Registerer, name string, wantLabels map[string]string, wantValue float64) {
	t.Helper()
	gatherer, ok := registerer.(prometheus.Gatherer)
	if !ok {
		t.Fatalf("registerer %T does not expose test gatherer", registerer)
	}
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("gather %s: %v", name, err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		if len(family.Metric) != 1 {
			t.Fatalf("counter family %s has %d samples, want one", name, len(family.Metric))
		}
		metric := family.Metric[0]
		labels := make(map[string]string, len(metric.Label))
		for _, label := range metric.Label {
			labels[label.GetName()] = label.GetValue()
		}
		if len(labels) != len(wantLabels) {
			t.Fatalf("counter %s labels = %#v, want %#v", name, labels, wantLabels)
		}
		for key, want := range wantLabels {
			if labels[key] != want {
				t.Fatalf("counter %s label %s = %q, want %q", name, key, labels[key], want)
			}
		}
		if got := metric.GetCounter().GetValue(); got != wantValue {
			t.Fatalf("counter %s value = %v, want %v", name, got, wantValue)
		}
		return
	}
	t.Fatalf("counter family %q was not gathered", name)
}

func mustFlowMarshal(message proto.Message) []byte {
	payload, err := proto.Marshal(message)
	if err != nil {
		panic(err)
	}
	return payload
}
