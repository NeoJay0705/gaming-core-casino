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
	secondPayload := []byte{0x01, 0xfe, 0x02}
	encoded, err := proto.Marshal(&protocol.BroadcastRoomCommand{
		RoomId: "room-a",
		Messages: []*protocol.BroadcastClientMessage{
			{ClientCommandId: protocol.EchoResponseCommandID, ClientPayload: opaquePayload},
			{ClientCommandId: protocol.LocalEchoResponseCommandID, ClientPayload: secondPayload},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := broadcastRoomHandler(registry, context.Background(), encoded); err != nil {
		t.Fatalf("broadcast room handler: %v", err)
	}
	got := alice.Messages()
	if len(got) != 2 {
		t.Fatalf("alice messages = %d, want 2", len(got))
	}
	for index, want := range []struct {
		commandID uint32
		payload   []byte
	}{
		{commandID: protocol.EchoResponseCommandID, payload: opaquePayload},
		{commandID: protocol.LocalEchoResponseCommandID, payload: secondPayload},
	} {
		packet := got[index]
		if len(packet) != gateproductWebSocketHeaderSize+len(want.payload) {
			t.Fatalf("packet %d size = %d, want %d", index, len(packet), gateproductWebSocketHeaderSize+len(want.payload))
		}
		if commandID := binary.BigEndian.Uint32(packet[:4]); commandID != want.commandID {
			t.Fatalf("packet %d command = %#x, want %#x", index, commandID, want.commandID)
		}
		if total := binary.BigEndian.Uint32(packet[4:8]); total != uint32(len(packet)) {
			t.Fatalf("packet %d total length = %d, want %d", index, total, len(packet))
		}
		if sequence := binary.BigEndian.Uint32(packet[8:12]); sequence != 0 {
			t.Fatalf("packet %d sequence = %d, want 0", index, sequence)
		}
		if session := binary.BigEndian.Uint16(packet[12:14]); session != 0 {
			t.Fatalf("packet %d session = %d, want 0", index, session)
		}
		if version := binary.BigEndian.Uint16(packet[14:16]); version != 0 {
			t.Fatalf("packet %d version = %d, want 0", index, version)
		}
		if string(packet[16:]) != string(want.payload) {
			t.Fatalf("packet %d payload = %x, want %x", index, packet[16:], want.payload)
		}
	}
	if got := bob.Messages(); len(got) != 0 {
		t.Fatalf("bob messages = %d, want 0", len(got))
	}
}

func TestBroadcastRoomProducerUsesGenericMessage(t *testing.T) {
	sender := &recordingWorkflowBroadcastSender{}
	firstPayload := []byte{0xff, 0x01}
	secondPayload := []byte{0x02, 0xfe}
	if _, err := BroadcastRoom(context.Background(), sender, "room-a", []serversend.Message{
		{CommandID: protocol.EchoResponseCommandID, Payload: firstPayload},
		{CommandID: protocol.LocalEchoResponseCommandID, Payload: secondPayload},
	}); err != nil {
		t.Fatalf("BroadcastRoom: %v", err)
	}
	if sender.message.CommandID != protocol.BroadcastRoomCommandID {
		t.Fatalf("transport command = %#x, want %#x", sender.message.CommandID, protocol.BroadcastRoomCommandID)
	}
	var command protocol.BroadcastRoomCommand
	if err := proto.Unmarshal(sender.message.Payload, &command); err != nil {
		t.Fatalf("decode outer command: %v", err)
	}
	if command.GetRoomId() != "room-a" || len(command.GetMessages()) != 2 || command.GetMessages()[0].GetClientCommandId() != protocol.EchoResponseCommandID || string(command.GetMessages()[0].GetClientPayload()) != string(firstPayload) || command.GetMessages()[1].GetClientCommandId() != protocol.LocalEchoResponseCommandID || string(command.GetMessages()[1].GetClientPayload()) != string(secondPayload) {
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

func (s *workflowSession) SendBinary(_ context.Context, data []byte) error {
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
