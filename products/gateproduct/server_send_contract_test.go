package gateproduct

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
)

func TestGateServerSendReceiverContractDeliversFramesAndIgnoresNonLocalTargets(t *testing.T) {
	registry := NewSessionRegistry()
	alice := &registrySession{id: "connection-alice"}
	bob := &registrySession{id: "connection-bob"}
	if err := registry.Register(alice, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(bob, "bob"); err != nil {
		t.Fatal(err)
	}
	if err := registry.EnterRoom("alice", "room-a"); err != nil {
		t.Fatal(err)
	}
	receiver, err := newGateServerSendReceiver(registry)
	if err != nil {
		t.Fatal(err)
	}
	message := serversend.Message{CommandID: 0xE20001, Payload: []byte("payload")}
	statusValue, err := receiver.SendToConnection(context.Background(), "connection-alice", "alice", message)
	if err != nil || statusValue != serversend.DeliveryStatus_DELIVERY_STATUS_DELIVERED {
		t.Fatalf("direct delivery = status:%s error:%v, want delivered/nil", statusValue, err)
	}
	statusValue, err = receiver.SendToConnection(context.Background(), "connection-alice", "bob", message)
	if err != nil || statusValue != serversend.DeliveryStatus_DELIVERY_STATUS_IGNORED {
		t.Fatalf("mismatched direct delivery = status:%s error:%v, want ignored/nil", statusValue, err)
	}
	statusValue, err = receiver.SendToPlayer(context.Background(), serversend.PlayerMessage{LoginName: "missing", Message: message})
	if err != nil || statusValue != serversend.DeliveryStatus_DELIVERY_STATUS_IGNORED {
		t.Fatalf("missing player delivery = status:%s error:%v, want ignored/nil", statusValue, err)
	}
	if delivered, err := receiver.BroadcastRoom(context.Background(), serversend.BroadcastMessage{RoomID: "room-a", Message: message}); err != nil || delivered != 1 {
		t.Fatalf("room broadcast = delivered:%d error:%v, want 1/nil", delivered, err)
	}
	if len(alice.Sent()) != 2 || len(bob.Sent()) != 0 {
		t.Fatalf("frame delivery = alice:%d bob:%d, want 2/0", len(alice.Sent()), len(bob.Sent()))
	}
	packet := alice.Sent()[0]
	if got := binary.BigEndian.Uint32(packet[0:4]); got != message.CommandID {
		t.Fatalf("frame command id = %#x, want %#x", got, message.CommandID)
	}
	if got := packet[webSocketPacketHeaderSize:]; string(got) != string(message.Payload) {
		t.Fatalf("frame payload = %q, want %q", got, message.Payload)
	}
}
