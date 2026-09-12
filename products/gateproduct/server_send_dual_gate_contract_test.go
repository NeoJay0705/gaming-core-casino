package gateproduct

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/grpcserver"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func TestGameServerSendContractDeliversAcrossTwoGates(t *testing.T) {
	gateASessions, gateADispatcher, gateAEndpoint := startContractGateReceiver(t, "gate-a")
	gateBSessions, gateBDispatcher, gateBEndpoint := startContractGateReceiver(t, "gate-b")

	alice := &registrySession{id: "gate-a-alice"}
	carol := &registrySession{id: "gate-a-carol"}
	bob := &registrySession{id: "gate-b-bob"}
	dave := &registrySession{id: "gate-b-dave"}
	for loginName, session := range map[LoginName]*registrySession{
		"alice": alice,
		"carol": carol,
	} {
		if err := gateASessions.Register(session, loginName); err != nil {
			t.Fatalf("register Gate A %s: %v", loginName, err)
		}
	}
	for loginName, session := range map[LoginName]*registrySession{
		"bob":  bob,
		"dave": dave,
	} {
		if err := gateBSessions.Register(session, loginName); err != nil {
			t.Fatalf("register Gate B %s: %v", loginName, err)
		}
	}
	if err := gateASessions.EnterRoom("alice", "room-a"); err != nil {
		t.Fatal(err)
	}
	if err := gateBSessions.EnterRoom("bob", "room-a"); err != nil {
		t.Fatal(err)
	}

	directory := dualGateDirectory{endpoints: []serversend.GateEndpoint{gateAEndpoint, gateBEndpoint}}
	transport, err := serversend.NewGRPCTransport(serversend.TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })

	presence := dualGatePresenceResolver{presences: map[serversend.LoginName]serversend.Presence{
		"bob": {LoginName: "bob", GateID: "gate-b", ConnectionID: "gate-b-bob", Epoch: 1},
	}}
	routed, err := serversend.NewBatchPlayerSender(presence, directory, directory, transport)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := routed.SendToPlayers(context.Background(), []serversend.PlayerMessage{{LoginName: "bob", Message: serversend.Message{CommandID: 102, Payload: []byte("private")}}}); err != nil {
		t.Fatalf("routed player send: %v", err)
	}
	assertServerSendPacket(t, bob, 102, "private")
	assertNoServerSendPacket(t, alice, 102)

	miniRedis := miniredis.RunT(t)
	subscriberClient := redis.NewClient(&redis.Options{Addr: miniRedis.Addr()})
	t.Cleanup(func() { _ = subscriberClient.Close() })
	producerClient := redis.NewClient(&redis.Options{Addr: miniRedis.Addr()})
	t.Cleanup(func() { _ = producerClient.Close() })
	keys, err := serversend.NewKeyspace("dual-gate")
	if err != nil {
		t.Fatal(err)
	}
	gateASubscriber, err := serversend.NewRedisBroadcastSubscriber(subscriberClient, keys, gateADispatcher)
	if err != nil {
		t.Fatal(err)
	}
	gateBSubscriber, err := serversend.NewRedisBroadcastSubscriber(subscriberClient, keys, gateBDispatcher)
	if err != nil {
		t.Fatal(err)
	}
	for name, subscriber := range map[string]*serversend.RedisBroadcastSubscriber{
		"gate-a": gateASubscriber,
		"gate-b": gateBSubscriber,
	} {
		if err := subscriber.Start(context.Background()); err != nil {
			t.Fatalf("start %s Redis subscriber: %v", name, err)
		}
		t.Cleanup(func() { _ = subscriber.Stop(context.Background()) })
	}
	fanout, err := serversend.NewFanoutSender(directory, transport)
	if err != nil {
		t.Fatal(err)
	}
	redisSender, err := serversend.NewRedisBroadcastSender(producerClient, keys)
	if err != nil {
		t.Fatal(err)
	}
	broadcastSender, err := serversend.NewFallbackBroadcastSender(
		redisSender,
		fanout,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := broadcastSender.Broadcast(context.Background(), serversend.Message{CommandID: 103, Payload: []byte("redis")}); err != nil {
		t.Fatalf("Redis broadcast: %v", err)
	}
	waitForServerSendPacket(t, alice, 103, "redis")
	waitForServerSendPacket(t, bob, 103, "redis")
	assertNoServerSendPacket(t, carol, 103)
	assertNoServerSendPacket(t, dave, 103)

	if err := producerClient.Close(); err != nil {
		t.Fatalf("close Redis producer: %v", err)
	}
	if _, err := broadcastSender.Broadcast(context.Background(), serversend.Message{CommandID: 103, Payload: []byte("grpc-fallback")}); err != nil {
		t.Fatalf("gRPC fallback broadcast: %v", err)
	}
	assertServerSendPacket(t, alice, 103, "grpc-fallback")
	assertServerSendPacket(t, bob, 103, "grpc-fallback")
	assertNoServerSendPacket(t, carol, 103)
	assertNoServerSendPacket(t, dave, 103)
}

func TestGameServerSendContractReportsUnavailableGateInPartialFanout(t *testing.T) {
	gateSessions, _, endpoint := startContractGateReceiver(t, "gate-live")
	session := &registrySession{id: "gate-live-alice"}
	if err := gateSessions.Register(session, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := gateSessions.EnterRoom("alice", "room-a"); err != nil {
		t.Fatal(err)
	}

	downListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	downAddress := downListener.Addr().String()
	if err := downListener.Close(); err != nil {
		t.Fatal(err)
	}

	transport, err := serversend.NewGRPCTransport(serversend.TransportConfig{RequestTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	fanout, err := serversend.NewFanoutSender(dualGateDirectory{endpoints: []serversend.GateEndpoint{
		endpoint,
		{GateID: "gate-down", Address: downAddress},
	}}, transport)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fanout.Broadcast(context.Background(), serversend.Message{CommandID: 104, Payload: []byte("partial")}); err == nil || !strings.Contains(err.Error(), "gate-down") {
		t.Fatalf("partial fan-out error = %v, want gate-down identity", err)
	}
	assertServerSendPacket(t, session, 104, "partial")
}

func TestGameServerSendContractPlayerRouteReturnsStalePrimaryError(t *testing.T) {
	_, _, gateAEndpoint := startContractGateReceiver(t, "gate-a")
	gateBSessions, _, gateBEndpoint := startContractGateReceiver(t, "gate-b")
	bob := &registrySession{id: "gate-b-bob"}
	if err := gateBSessions.Register(bob, "bob"); err != nil {
		t.Fatal(err)
	}
	directory := dualGateDirectory{endpoints: []serversend.GateEndpoint{gateAEndpoint, gateBEndpoint}}
	transport, err := serversend.NewGRPCTransport(serversend.TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	routed, err := serversend.NewBatchPlayerSender(dualGatePresenceResolver{presences: map[serversend.LoginName]serversend.Presence{
		"bob": {LoginName: "bob", GateID: "stale-gate", ConnectionID: "gate-b-bob", Epoch: 1},
	}}, dualGateDirectory{endpoints: directory.endpoints}, directory, transport)
	if err != nil {
		t.Fatal(err)
	}
	if receipt, err := routed.SendToPlayers(context.Background(), []serversend.PlayerMessage{{LoginName: "bob", Message: serversend.Message{CommandID: 105, Payload: []byte("route")}}}); !errors.Is(err, serversend.ErrGateEndpointNotFound) || !receipt.AcceptedAt.IsZero() {
		t.Fatalf("stale player route = receipt:%#v error:%v, want empty/ErrGateEndpointNotFound", receipt, err)
	}
	if len(bob.Sent()) != 0 {
		t.Fatalf("stale player route unexpectedly delivered: %x", bob.Sent())
	}
}

func startContractGateReceiver(t *testing.T, gateID serversend.GateID) (*SessionRegistry, *dispatcher.Dispatcher, serversend.GateEndpoint) {
	t.Helper()
	sessions := NewSessionRegistry()
	commandDispatcher := dispatcher.New()
	if err := registerGatePlayerDeliveryCommand(sessions, nil, commandDispatcher); err != nil {
		t.Fatal(err)
	}
	for _, commandID := range []uint32{102, 103, 104, 105} {
		if err := commandDispatcher.Register(serversend.RemoteCommandChannel, dispatcher.CommandID(commandID), func(ctx context.Context, payload []byte) error {
			_, err := sessions.BroadcastRoom(ctx, "room-a", EncodeWebSocketPacket(WebSocketPacket{CommandID: commandID, Payload: append([]byte(nil), payload...)}))
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	service, err := serversend.NewGateDeliveryService(commandDispatcher)
	if err != nil {
		t.Fatal(err)
	}
	server, err := grpcserver.New(grpcserver.Config{ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Register(serversend.GateDelivery_ServiceDesc.ServiceName, func(registrar grpc.ServiceRegistrar) {
		serversend.RegisterGateDeliveryServer(registrar, service)
	}); err != nil {
		t.Fatal(err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })
	return sessions, commandDispatcher, serversend.GateEndpoint{GateID: gateID, Address: server.Addr()}
}

type dualGateDirectory struct {
	endpoints []serversend.GateEndpoint
}

func (d dualGateDirectory) Resolve(_ context.Context, gateID serversend.GateID) (serversend.GateEndpoint, error) {
	for _, endpoint := range d.endpoints {
		if endpoint.GateID == gateID {
			return endpoint, nil
		}
	}
	return serversend.GateEndpoint{}, serversend.ErrGateEndpointNotFound
}

func (d dualGateDirectory) List(context.Context) ([]serversend.GateEndpoint, error) {
	return append([]serversend.GateEndpoint(nil), d.endpoints...), nil
}

func (d dualGateDirectory) ResolveMany(_ context.Context, gateIDs []serversend.GateID) (map[serversend.GateID]serversend.GateEndpoint, error) {
	result := make(map[serversend.GateID]serversend.GateEndpoint, len(gateIDs))
	for _, gateID := range gateIDs {
		for _, endpoint := range d.endpoints {
			if endpoint.GateID == gateID {
				result[gateID] = endpoint
				break
			}
		}
	}
	return result, nil
}

type dualGatePresenceResolver struct {
	presences map[serversend.LoginName]serversend.Presence
}

func (r dualGatePresenceResolver) ResolveMany(_ context.Context, names []serversend.LoginName) (map[serversend.LoginName]serversend.Presence, error) {
	result := make(map[serversend.LoginName]serversend.Presence, len(names))
	for _, name := range names {
		if presence, ok := r.presences[name]; ok {
			result[name] = presence
		}
	}
	return result, nil
}

func assertServerSendPacket(t *testing.T, session *registrySession, commandID uint32, payload string) {
	t.Helper()
	if hasServerSendPacket(session, commandID, payload) {
		return
	}
	t.Fatalf("session %q did not receive command %#x payload %q; packets=%x", session.ID(), commandID, payload, session.Sent())
}

func waitForServerSendPacket(t *testing.T, session *registrySession, commandID uint32, payload string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if hasServerSendPacket(session, commandID, payload) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	assertServerSendPacket(t, session, commandID, payload)
}

func hasServerSendPacket(session *registrySession, commandID uint32, payload string) bool {
	for _, packet := range session.Sent() {
		if len(packet) >= webSocketPacketHeaderSize && binary.BigEndian.Uint32(packet[:4]) == commandID && string(packet[webSocketPacketHeaderSize:]) == payload {
			return true
		}
	}
	return false
}

func assertNoServerSendPacket(t *testing.T, session *registrySession, commandID uint32) {
	t.Helper()
	for _, packet := range session.Sent() {
		if len(packet) >= webSocketPacketHeaderSize && binary.BigEndian.Uint32(packet[:4]) == commandID {
			t.Fatalf("session %q unexpectedly received command %#x", session.ID(), commandID)
		}
	}
}
