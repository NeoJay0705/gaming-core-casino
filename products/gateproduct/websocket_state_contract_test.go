package gateproduct

import (
	"context"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
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
