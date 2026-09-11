package workflow

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/protocol"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/NeoJay0705/gaming-core-casino/products/gateproduct"
	"google.golang.org/protobuf/proto"
)

func TestBroadcastRoomHandlerDecodesOnlyRoutingEnvelope(t *testing.T) {
	registry := gateproduct.NewSessionRegistry()
	alice := &workflowSession{id: "alice"}
	bob := &workflowSession{id: "bob"}
	if err := registry.Register(alice, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(bob, "bob"); err != nil {
		t.Fatal(err)
	}
	if err := registry.EnterRoom("alice", "room-a"); err != nil {
		t.Fatal(err)
	}
	if err := registry.EnterRoom("bob", "room-b"); err != nil {
		t.Fatal(err)
	}
	opaquePayload := []byte{0xff, 0x00, 0x7f}
	encoded, err := proto.Marshal(&protocol.BroadcastRoomCommand{
		RoomId:          "room-a",
		ClientCommandId: protocol.EchoResponseCommandID,
		ClientPayload:   opaquePayload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := broadcastRoomHandler(registry, context.Background(), encoded); err != nil {
		t.Fatalf("broadcast room handler: %v", err)
	}
	if got := alice.Messages(); len(got) != 1 {
		t.Fatalf("alice messages = %d, want 1", len(got))
	} else {
		packet := got[0]
		if len(packet) != gateproductWebSocketHeaderSize+len(opaquePayload) {
			t.Fatalf("packet size = %d, want %d", len(packet), gateproductWebSocketHeaderSize+len(opaquePayload))
		}
		if commandID := binary.BigEndian.Uint32(packet[:4]); commandID != protocol.EchoResponseCommandID {
			t.Fatalf("packet command = %#x, want %#x", commandID, protocol.EchoResponseCommandID)
		}
		if total := binary.BigEndian.Uint32(packet[4:8]); total != uint32(len(packet)) {
			t.Fatalf("packet total length = %d, want %d", total, len(packet))
		}
		if sequence := binary.BigEndian.Uint32(packet[8:12]); sequence != 0 {
			t.Fatalf("packet sequence = %d, want 0", sequence)
		}
		if session := binary.BigEndian.Uint16(packet[12:14]); session != 0 {
			t.Fatalf("packet session = %d, want 0", session)
		}
		if version := binary.BigEndian.Uint16(packet[14:16]); version != 0 {
			t.Fatalf("packet version = %d, want 0", version)
		}
		if string(packet[16:]) != string(opaquePayload) {
			t.Fatalf("packet payload = %x, want %x", packet[16:], opaquePayload)
		}
	}
	if got := bob.Messages(); len(got) != 0 {
		t.Fatalf("bob messages = %d, want 0", len(got))
	}
}

func TestBroadcastRoomProducerUsesGenericMessage(t *testing.T) {
	sender := &recordingWorkflowBroadcastSender{}
	payload := []byte{0xff, 0x01}
	if _, err := BroadcastRoom(context.Background(), sender, "room-a", protocol.EchoResponseCommandID, payload); err != nil {
		t.Fatalf("BroadcastRoom: %v", err)
	}
	if sender.message.CommandID != protocol.BroadcastRoomCommandID {
		t.Fatalf("transport command = %#x, want %#x", sender.message.CommandID, protocol.BroadcastRoomCommandID)
	}
	var command protocol.BroadcastRoomCommand
	if err := proto.Unmarshal(sender.message.Payload, &command); err != nil {
		t.Fatalf("decode outer command: %v", err)
	}
	if command.GetRoomId() != "room-a" || command.GetClientCommandId() != protocol.EchoResponseCommandID || string(command.GetClientPayload()) != string(payload) {
		t.Fatalf("outer command = %#v", &command)
	}
}

const gateproductWebSocketHeaderSize = 16

type workflowSession struct {
	id       gateproduct.WebSocketConnectionID
	mu       sync.Mutex
	messages [][]byte
}

func (s *workflowSession) ID() gateproduct.WebSocketConnectionID { return s.id }

func (s *workflowSession) SendBinary(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, append([]byte(nil), data...))
	return nil
}

func (s *workflowSession) Close() error { return nil }

func (s *workflowSession) Messages() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]byte(nil), s.messages...)
}

type recordingWorkflowBroadcastSender struct {
	message serversend.Message
}

func (s *recordingWorkflowBroadcastSender) Broadcast(_ context.Context, message serversend.Message) (serversend.Receipt, error) {
	s.message = serversend.Message{CommandID: message.CommandID, Payload: append([]byte(nil), message.Payload...)}
	return serversend.Receipt{}, nil
}
