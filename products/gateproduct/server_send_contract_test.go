package gateproduct

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"google.golang.org/protobuf/proto"
)

func TestGatePlayerDeliveryCommandDeliversFramesAndIgnoresNonLocalTargets(t *testing.T) {
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
	message := serversend.Message{CommandID: 0xE20001, Payload: []byte("payload")}
	payload, err := proto.Marshal(&serversend.SendPlayersCommand{Messages: []*serversend.PlayerDelivery{
		{LoginName: "alice", ClientCommandId: message.CommandID, ClientPayload: message.Payload},
		{LoginName: "missing", ClientCommandId: message.CommandID, ClientPayload: message.Payload},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := handlePlayerDeliveryCommand(registry, nil, context.Background(), payload); err != nil {
		t.Fatalf("player delivery command: %v", err)
	}
	if len(alice.Sent()) != 1 || len(bob.Sent()) != 0 {
		t.Fatalf("frame delivery = alice:%d bob:%d, want 1/0", len(alice.Sent()), len(bob.Sent()))
	}
	packet := alice.Sent()[0]
	if got := binary.BigEndian.Uint32(packet[0:4]); got != message.CommandID {
		t.Fatalf("frame command id = %#x, want %#x", got, message.CommandID)
	}
	if got := packet[webSocketPacketHeaderSize:]; string(got) != string(message.Payload) {
		t.Fatalf("frame payload = %q, want %q", got, message.Payload)
	}
}

func TestGatePlayerDeliveryCommandKeepsClientPayloadOpaque(t *testing.T) {
	registry := NewSessionRegistry()
	alice := &registrySession{id: "connection-alice"}
	if err := registry.Register(alice, "alice"); err != nil {
		t.Fatal(err)
	}
	opaque := []byte{0x00, 0xff, 0x01, 0xfe}
	payload, err := proto.Marshal(&serversend.SendPlayersCommand{Messages: []*serversend.PlayerDelivery{{
		LoginName: "alice", ClientCommandId: 0xE20002, ClientPayload: opaque,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := handlePlayerDeliveryCommand(registry, nil, context.Background(), payload); err != nil {
		t.Fatalf("opaque player delivery command: %v", err)
	}
	packet := alice.Sent()[0]
	if got := packet[webSocketPacketHeaderSize:]; string(got) != string(opaque) {
		t.Fatalf("opaque payload = %x, want %x", got, opaque)
	}
}

func TestGatePlayerDeliveryCommandContinuesAfterQueueFailure(t *testing.T) {
	registry := NewSessionRegistry()
	failed := &registrySession{id: "connection-failed", sendErr: errors.New("queue full")}
	success := &registrySession{id: "connection-success"}
	if err := registry.Register(failed, "failed"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(success, "success"); err != nil {
		t.Fatal(err)
	}
	payload, err := proto.Marshal(&serversend.SendPlayersCommand{Messages: []*serversend.PlayerDelivery{
		{LoginName: "failed", ClientCommandId: 0xE20003, ClientPayload: []byte("first")},
		{LoginName: "success", ClientCommandId: 0xE20004, ClientPayload: []byte("second")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := handlePlayerDeliveryCommand(registry, nil, context.Background(), payload); err == nil {
		t.Fatal("queue failure returned nil error")
	}
	if len(failed.Sent()) != 0 {
		t.Fatalf("failed session received %d frames, want 0", len(failed.Sent()))
	}
	if len(success.Sent()) != 1 {
		t.Fatalf("successful session received %d frames, want 1", len(success.Sent()))
	}
}
