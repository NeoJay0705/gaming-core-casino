package gateproduct

import (
	"testing"
)

func TestSessionRegistryStateContract(t *testing.T) {
	registry := NewSessionRegistry()
	session := &stateTestSession{id: "connection-state"}
	if err := registry.Register(session, LoginName("alice")); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	state, ok := registry.State(session.ID())
	if !ok || state.LoginName != LoginName("alice") || state.RoomID != "" {
		t.Fatalf("State() = %#v, %t", state, ok)
	}
	if err := registry.EnterRoom(LoginName("alice"), RoomID("room-1")); err != nil {
		t.Fatalf("EnterRoom() error = %v", err)
	}
	state, ok = registry.State(session.ID())
	if !ok || state.RoomID != RoomID("room-1") {
		t.Fatalf("State() after room = %#v, %t", state, ok)
	}
	registry.Remove(session)
	if _, ok := registry.State(session.ID()); ok {
		t.Fatal("State() remained available after Remove()")
	}
}

type stateTestSession struct{ id WebSocketConnectionID }

func (s *stateTestSession) ID() WebSocketConnectionID { return s.id }
func (*stateTestSession) SendBinary([]byte) error     { return nil }
func (*stateTestSession) Close() error                { return nil }
