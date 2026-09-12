package gateproduct

import (
	"context"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
)

func TestWebSocketForwardStateContractClosesBeforeGameCall(t *testing.T) {
	commandDispatcher := dispatcher.New()
	registry := NewSessionRegistry()
	server := &WebSocketServer{dispatcher: commandDispatcher, registry: registry}

	unauthenticated := newWebSocketConnection(nil, 1, time.Second)
	if server.dispatchPacket(context.Background(), unauthenticated, WebSocketPacket{CommandID: 99}) {
		t.Fatal("unauthenticated forward was reported as handled")
	}
	if got := unauthenticated.currentCloseReason(); got != closeReasonLoginRequired {
		t.Fatalf("unauthenticated close reason = %q, want %q", got, closeReasonLoginRequired)
	}

	authenticated := newWebSocketConnection(nil, 1, time.Second)
	if err := registry.Register(authenticated, LoginName("alice")); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if server.dispatchPacket(context.Background(), authenticated, WebSocketPacket{CommandID: 99}) {
		t.Fatal("forward without room was reported as handled")
	}
	if got := authenticated.currentCloseReason(); got != closeReasonRoomRequired {
		t.Fatalf("room-less close reason = %q, want %q", got, closeReasonRoomRequired)
	}
}

func TestWebSocketLocalStateErrorUsesLoginReason(t *testing.T) {
	commandDispatcher := dispatcher.New()
	if err := commandDispatcher.Register(WebSocketChannel, 1, func(context.Context, []byte) error { return ErrLoginRequired }); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	server := &WebSocketServer{dispatcher: commandDispatcher, registry: NewSessionRegistry()}
	session := newWebSocketConnection(nil, 1, time.Second)
	if server.dispatchPacket(context.Background(), session, WebSocketPacket{CommandID: 1}) {
		t.Fatal("state error was reported as handled")
	}
	if got := session.currentCloseReason(); got != closeReasonLoginRequired {
		t.Fatalf("local state error close reason = %q, want %q", got, closeReasonLoginRequired)
	}
}

func TestWebSocketForwardUsesAuthenticatedLoginNameAffinity(t *testing.T) {
	registry := NewSessionRegistry()
	session := newWebSocketConnection(nil, 1, time.Second)
	if err := registry.Register(session, LoginName("alice")); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := registry.EnterRoom(LoginName("alice"), RoomID("room-1")); err != nil {
		t.Fatalf("EnterRoom() error = %v", err)
	}

	forwarder := &recordingGameForwarder{}
	server := &WebSocketServer{
		dispatcher: dispatcher.New(),
		gameClient: forwarder,
		registry:   registry,
	}
	packet := WebSocketPacket{CommandID: 99, Payload: []byte("opaque")}
	if !server.dispatchPacket(context.Background(), session, packet) {
		t.Fatal("forwarded packet was not handled")
	}
	if forwarder.affinityKey != "alice" {
		t.Fatalf("affinity key = %q, want authenticated login name %q", forwarder.affinityKey, "alice")
	}
	if forwarder.request.CommandID != packet.CommandID || string(forwarder.request.Payload) != string(packet.Payload) {
		t.Fatalf("forwarded request = %#v, want command %d and opaque payload", forwarder.request, packet.CommandID)
	}
}

type recordingGameForwarder struct {
	affinityKey string
	request     gatelink.Request
}

func (f *recordingGameForwarder) Forward(ctx context.Context, request gatelink.Request) (*gatelink.Reply, error) {
	key, ok := gatelink.AffinityKeyFromContext(ctx)
	if !ok {
		return nil, context.Canceled
	}
	f.affinityKey = key
	f.request = request
	return nil, nil
}
