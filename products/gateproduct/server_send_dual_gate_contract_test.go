package gateproduct

import (
	"context"
	"encoding/binary"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
)

func TestGameServerSendContractDeliversAcrossTwoGates(t *testing.T) {
	gateASessions, gateAEndpoint := startContractGateReceiver(t, "gate-a")
	gateBSessions, gateBEndpoint := startContractGateReceiver(t, "gate-b")

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

	direct, err := serversend.NewDirectRequestPlayerSender(transport)
	if err != nil {
		t.Fatal(err)
	}
	requestContext := gatelink.WithGateRequestContext(context.Background(), gatelink.GateRequestContext{
		Source: gatelink.RequestSource{GateID: "gate-a", ConnectionID: "gate-a-alice", ReplyEndpoint: gateAEndpoint.Address},
	})
	if _, err := direct.SendToRequestPlayer(requestContext, serversend.RequestPlayerMessage{Message: serversend.Message{CommandID: 101, Payload: []byte("direct")}}); err != nil {
		t.Fatalf("direct request reply: %v", err)
	}
	assertServerSendPacket(t, alice, 101, "direct")
	assertNoServerSendPacket(t, bob, 101)

	routed, err := serversend.NewRoutedPlayerSender(dualGatePresenceResolver{presence: serversend.Presence{LoginName: "bob", GateID: "gate-b", ConnectionID: "gate-b-bob", Epoch: 1}}, directory, transport, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := routed.SendToPlayer(context.Background(), serversend.PlayerMessage{LoginName: "bob", Message: serversend.Message{CommandID: 102, Payload: []byte("private")}}); err != nil {
		t.Fatalf("routed player send: %v", err)
	}
	assertServerSendPacket(t, bob, 102, "private")
	assertNoServerSendPacket(t, alice, 102)

	fanout, err := serversend.NewFanoutSender(directory, transport)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fanout.Broadcast(context.Background(), serversend.BroadcastMessage{RoomID: "room-a", Message: serversend.Message{CommandID: 103, Payload: []byte("room")}}); err != nil {
		t.Fatalf("room fan-out: %v", err)
	}
	assertServerSendPacket(t, alice, 103, "room")
	assertServerSendPacket(t, bob, 103, "room")
	assertNoServerSendPacket(t, carol, 103)
	assertNoServerSendPacket(t, dave, 103)
}

func TestGameServerSendContractReportsUnavailableGateInPartialFanout(t *testing.T) {
	gateSessions, endpoint := startContractGateReceiver(t, "gate-live")
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
	if _, err := fanout.Broadcast(context.Background(), serversend.BroadcastMessage{RoomID: "room-a", Message: serversend.Message{CommandID: 104, Payload: []byte("partial")}}); err == nil || !strings.Contains(err.Error(), "gate-down") {
		t.Fatalf("partial fan-out error = %v, want gate-down identity", err)
	}
	assertServerSendPacket(t, session, 104, "partial")
}

func TestGameServerSendContractPlayerFallbackFindsOwnerAfterStalePrimaryRoute(t *testing.T) {
	_, gateAEndpoint := startContractGateReceiver(t, "gate-a")
	gateBSessions, gateBEndpoint := startContractGateReceiver(t, "gate-b")
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
	fanout, err := serversend.NewFanoutSender(directory, transport)
	if err != nil {
		t.Fatal(err)
	}
	routed, err := serversend.NewRoutedPlayerSender(dualGatePresenceResolver{presence: serversend.Presence{LoginName: "bob", GateID: "stale-gate", ConnectionID: "gate-b-bob", Epoch: 1}}, directory, transport, fanout)
	if err != nil {
		t.Fatal(err)
	}
	if receipt, err := routed.SendToPlayer(context.Background(), serversend.PlayerMessage{LoginName: "bob", Message: serversend.Message{CommandID: 105, Payload: []byte("fallback")}}); err != nil || receipt.AcceptedAt.IsZero() {
		t.Fatalf("player fallback = receipt:%#v error:%v, want accepted/nil", receipt, err)
	}
	assertServerSendPacket(t, bob, 105, "fallback")
}

func TestGameServerSendContractPlayerFallbackMayReachDuplicateGateLocalSessionsUntilGlobalOwnershipExists(t *testing.T) {
	gateASessions, gateAEndpoint := startContractGateReceiver(t, "gate-a")
	gateBSessions, gateBEndpoint := startContractGateReceiver(t, "gate-b")
	aliceA := &registrySession{id: "gate-a-alice"}
	aliceB := &registrySession{id: "gate-b-alice"}
	if err := gateASessions.Register(aliceA, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := gateBSessions.Register(aliceB, "alice"); err != nil {
		t.Fatal(err)
	}
	directory := dualGateDirectory{endpoints: []serversend.GateEndpoint{gateAEndpoint, gateBEndpoint}}
	transport, err := serversend.NewGRPCTransport(serversend.TransportConfig{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stop(context.Background()) })
	fanout, err := serversend.NewFanoutSender(directory, transport)
	if err != nil {
		t.Fatal(err)
	}
	routed, err := serversend.NewRoutedPlayerSender(dualGatePresenceResolver{presence: serversend.Presence{LoginName: "alice", GateID: "missing-gate", Epoch: 1}}, directory, transport, fanout)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := routed.SendToPlayer(context.Background(), serversend.PlayerMessage{LoginName: "alice", Message: serversend.Message{CommandID: 106, Payload: []byte("duplicate-limited")}}); err != nil {
		t.Fatalf("duplicate local-session fallback: %v", err)
	}
	assertServerSendPacket(t, aliceA, 106, "duplicate-limited")
	assertServerSendPacket(t, aliceB, 106, "duplicate-limited")
}

func startContractGateReceiver(t *testing.T, gateID serversend.GateID) (*SessionRegistry, serversend.GateEndpoint) {
	t.Helper()
	sessions := NewSessionRegistry()
	receiver, err := newGateServerSendReceiver(sessions)
	if err != nil {
		t.Fatal(err)
	}
	server, err := serversend.NewReceiverServer(serversend.ReceiverConfig{ListenAddr: "127.0.0.1:0"}, receiver)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })
	return sessions, serversend.GateEndpoint{GateID: gateID, Address: server.Addr()}
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

type dualGatePresenceResolver struct {
	presence serversend.Presence
}

func (r dualGatePresenceResolver) Resolve(context.Context, serversend.LoginName) (serversend.Presence, error) {
	return r.presence, nil
}

func assertServerSendPacket(t *testing.T, session *registrySession, commandID uint32, payload string) {
	t.Helper()
	for _, packet := range session.Sent() {
		if len(packet) >= webSocketPacketHeaderSize && binary.BigEndian.Uint32(packet[:4]) == commandID && string(packet[webSocketPacketHeaderSize:]) == payload {
			return
		}
	}
	t.Fatalf("session %q did not receive command %#x payload %q; packets=%x", session.ID(), commandID, payload, session.Sent())
}

func assertNoServerSendPacket(t *testing.T, session *registrySession, commandID uint32) {
	t.Helper()
	for _, packet := range session.Sent() {
		if len(packet) >= webSocketPacketHeaderSize && binary.BigEndian.Uint32(packet[:4]) == commandID {
			t.Fatalf("session %q unexpectedly received command %#x", session.ID(), commandID)
		}
	}
}
