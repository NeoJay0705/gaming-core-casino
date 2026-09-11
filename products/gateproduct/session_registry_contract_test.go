package gateproduct

import (
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestSessionRegistryContractRejectsInvalidOperations(t *testing.T) {
	registry := NewSessionRegistry()

	if err := registry.Register(nil, "alice"); !errors.Is(err, ErrSessionRegistrationInvalid) {
		t.Fatalf("nil session error = %v", err)
	}
	if err := registry.Register(&registrySession{}, "alice"); !errors.Is(err, ErrSessionRegistrationInvalid) {
		t.Fatalf("empty connection id error = %v", err)
	}
	if err := registry.Register(&registrySession{id: "connection-1"}, ""); !errors.Is(err, ErrSessionRegistrationInvalid) {
		t.Fatalf("empty login name error = %v", err)
	}

	alice := &registrySession{id: "connection-alice"}
	if err := registry.Register(alice, "alice"); err != nil {
		t.Fatal(err)
	}

	if err := registry.EnterRoom("", "room-a"); !errors.Is(err, ErrLoginSessionNotFound) {
		t.Fatalf("empty login error = %v", err)
	}
	if err := registry.EnterRoom("missing", "room-a"); !errors.Is(err, ErrLoginSessionNotFound) {
		t.Fatalf("missing login error = %v", err)
	}
	if err := registry.EnterRoom("alice", ""); !errors.Is(err, ErrRoomIDInvalid) {
		t.Fatalf("empty room error = %v", err)
	}
	if err := registry.LeaveRoom("missing"); !errors.Is(err, ErrLoginSessionNotFound) {
		t.Fatalf("leave missing login error = %v", err)
	}

	if delivered, err := registry.BroadcastRoom("", nil); delivered != 0 || !errors.Is(err, ErrRoomIDInvalid) {
		t.Fatalf("empty room broadcast = %d, %v", delivered, err)
	}
	if delivered, err := registry.BroadcastRoom("missing", nil); delivered != 0 || err != nil {
		t.Fatalf("missing room broadcast = %d, %v", delivered, err)
	}
	if kicked, err := registry.KickRoom("missing"); kicked != 0 || err != nil {
		t.Fatalf("missing room kick = %d, %v", kicked, err)
	}
}

func TestSessionRegistryContractLeaveRoomIsIdempotent(t *testing.T) {
	registry := NewSessionRegistry()
	session := &registrySession{id: "connection-alice"}

	if err := registry.Register(session, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := registry.EnterRoom("alice", "room-a"); err != nil {
		t.Fatal(err)
	}
	if err := registry.LeaveRoom("alice"); err != nil {
		t.Fatal(err)
	}
	if err := registry.LeaveRoom("alice"); err != nil {
		t.Fatalf("second leave: %v", err)
	}
	if err := registry.SendToLoginName("alice", []byte("still-connected")); err != nil {
		t.Fatalf("session disappeared after leaving room: %v", err)
	}
}

func TestSessionRegistryContractRegistersAndDelivers(t *testing.T) {
	registry := NewSessionRegistry()
	alice := &registrySession{id: "connection-alice"}
	if err := registry.Register(alice, "alice"); err != nil {
		t.Fatalf("register alice: %v", err)
	}
	packet := []byte{1, 2, 3}
	if err := registry.SendToLoginName("alice", packet); err != nil {
		t.Fatalf("send to alice: %v", err)
	}
	if sent := alice.Sent(); len(sent) != 1 || string(sent[0]) != string(packet) {
		t.Fatalf("alice sent packets = %x, want %x", sent, packet)
	}
	if err := registry.SendToLoginName("missing", packet); !errors.Is(err, ErrLoginSessionNotFound) {
		t.Fatalf("send missing login error = %v, want ErrLoginSessionNotFound", err)
	}
}

func TestSessionRegistryContractTreatsIdentityAsOpaque(t *testing.T) {
	registry := NewSessionRegistry()
	session := &registrySession{id: "connection-1"}
	if err := registry.Register(session, " alice "); err != nil {
		t.Fatalf("register opaque identity: %v", err)
	}
	if err := registry.SendToLoginName("alice", []byte("message")); !errors.Is(err, ErrLoginSessionNotFound) {
		t.Fatalf("normalized lookup error = %v, want ErrLoginSessionNotFound", err)
	}
}

func TestSessionRegistryContractRegisterIsIdempotentForSameConnectionAndIdentity(t *testing.T) {
	registry := NewSessionRegistry()
	session := &registrySession{id: "connection-1"}
	if err := registry.Register(session, "alice"); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if err := registry.Register(session, "alice"); err != nil {
		t.Fatalf("idempotent register: %v", err)
	}
	if session.CloseCount() != 0 {
		t.Fatalf("idempotent register closed session %d times", session.CloseCount())
	}
}

func TestSessionRegistryContractRemoveDoesNotCloseAlreadyDisconnectedSession(t *testing.T) {
	registry := NewSessionRegistry()
	session := &registrySession{id: "connection-disconnected"}
	if err := registry.Register(session, "alice"); err != nil {
		t.Fatal(err)
	}
	registry.Remove(session)
	if session.CloseCount() != 0 {
		t.Fatalf("Remove closed disconnected session %d times, want 0", session.CloseCount())
	}
}

func TestSessionRegistryContractRejectsIdentitySwitchOnOneConnection(t *testing.T) {
	registry := NewSessionRegistry()
	session := &registrySession{id: "connection-1"}
	if err := registry.Register(session, "alice"); err != nil {
		t.Fatalf("register alice: %v", err)
	}
	if err := registry.Register(session, "bob"); !errors.Is(err, ErrSessionAlreadyRegistered) {
		t.Fatalf("switch identity error = %v, want ErrSessionAlreadyRegistered", err)
	}
}

func TestSessionRegistryContractReplacementSurvivesStaleDisconnectAndCloseFailure(t *testing.T) {
	closeErr := errors.New("old connection close failed")
	registry := NewSessionRegistry()
	oldSession := &registrySession{id: "connection-old", closeErr: closeErr}
	if err := registry.Register(oldSession, "alice"); err != nil {
		t.Fatalf("register old session: %v", err)
	}
	if err := registry.EnterRoom("alice", "room-a"); err != nil {
		t.Fatalf("enter old session room: %v", err)
	}
	replacement := &registrySession{id: "connection-new"}
	if err := registry.Register(replacement, "alice"); err != nil {
		t.Fatalf("register replacement: %v", err)
	}
	if oldSession.CloseCount() != 1 {
		t.Fatalf("old session close count = %d, want 1", oldSession.CloseCount())
	}
	registry.Remove(oldSession)
	if err := registry.SendToLoginName("alice", []byte("replacement-only")); err != nil {
		t.Fatalf("send to replacement: %v", err)
	}
	if len(oldSession.Sent()) != 0 || len(replacement.Sent()) != 1 {
		t.Fatalf("replacement delivery = old:%d new:%d, want 0/1", len(oldSession.Sent()), len(replacement.Sent()))
	}
	if delivered, err := registry.BroadcastRoom("room-a", nil); delivered != 0 || err != nil {
		t.Fatalf("old room after replacement = delivered:%d error:%v, want 0/nil", delivered, err)
	}
	registry.Remove(replacement)
	if err := registry.SendToLoginName("alice", nil); !errors.Is(err, ErrLoginSessionNotFound) {
		t.Fatalf("authoritative disconnect error = %v, want ErrLoginSessionNotFound", err)
	}
}

func TestSessionRegistryContractPropagatesSendAndKickErrors(t *testing.T) {
	sendErr := errors.New("send failed")
	closeErr := errors.New("close failed")
	registry := NewSessionRegistry()
	session := &registrySession{id: "connection-alice", sendErr: sendErr, closeErr: closeErr}
	if err := registry.Register(session, "alice"); err != nil {
		t.Fatalf("register alice: %v", err)
	}
	if err := registry.SendToLoginName("alice", nil); !errors.Is(err, sendErr) {
		t.Fatalf("send error = %v, want send error", err)
	}
	if err := registry.KickLoginName("alice"); !errors.Is(err, closeErr) {
		t.Fatalf("kick error = %v, want close error", err)
	}
	if err := registry.SendToLoginName("alice", nil); !errors.Is(err, ErrLoginSessionNotFound) {
		t.Fatalf("kicked mapping error = %v, want ErrLoginSessionNotFound", err)
	}
	if err := registry.KickLoginName("alice"); err != nil {
		t.Fatalf("second kick: %v", err)
	}
	if session.CloseCount() != 1 {
		t.Fatalf("close count = %d, want 1", session.CloseCount())
	}
}

func TestSessionRegistryContractRoomMembershipAndBroadcast(t *testing.T) {
	registry := NewSessionRegistry()
	alice := &registrySession{id: "connection-alice"}
	bob := &registrySession{id: "connection-bob"}
	carol := &registrySession{id: "connection-carol"}
	for loginName, session := range map[LoginName]*registrySession{"alice": alice, "bob": bob, "carol": carol} {
		if err := registry.Register(session, loginName); err != nil {
			t.Fatalf("register %s: %v", loginName, err)
		}
	}
	for loginName, roomID := range map[LoginName]RoomID{"alice": "room-a", "bob": "room-a", "carol": "room-b"} {
		if err := registry.EnterRoom(loginName, roomID); err != nil {
			t.Fatalf("enter room %s/%s: %v", loginName, roomID, err)
		}
	}
	if err := registry.EnterRoom("alice", "room-a"); err != nil {
		t.Fatalf("idempotent enter room: %v", err)
	}
	if delivered, err := registry.BroadcastRoom("room-a", []byte("a-1")); err != nil || delivered != 2 {
		t.Fatalf("broadcast room-a = delivered:%d error:%v, want 2/nil", delivered, err)
	}
	if len(alice.Sent()) != 1 || len(bob.Sent()) != 1 || len(carol.Sent()) != 0 {
		t.Fatalf("first broadcast delivery = alice:%d bob:%d carol:%d, want 1/1/0", len(alice.Sent()), len(bob.Sent()), len(carol.Sent()))
	}
	if err := registry.EnterRoom("alice", "room-b"); err != nil {
		t.Fatalf("move alice to room-b: %v", err)
	}
	if delivered, err := registry.BroadcastRoom("room-a", []byte("a-2")); err != nil || delivered != 1 {
		t.Fatalf("second room-a broadcast = delivered:%d error:%v, want 1/nil", delivered, err)
	}
	if delivered, err := registry.BroadcastRoom("room-b", []byte("b-1")); err != nil || delivered != 2 {
		t.Fatalf("room-b broadcast = delivered:%d error:%v, want 2/nil", delivered, err)
	}
	if err := registry.LeaveRoom("bob"); err != nil {
		t.Fatalf("leave room: %v", err)
	}
	if delivered, err := registry.BroadcastRoom("room-a", []byte("a-3")); err != nil || delivered != 0 {
		t.Fatalf("broadcast after leave = delivered:%d error:%v, want 0/nil", delivered, err)
	}
	if err := registry.SendToLoginName("bob", []byte("targeted")); err != nil {
		t.Fatalf("target bob after leave: %v", err)
	}
}

func TestSessionRegistryContractDisconnectAndBroadcastFailureCleanUpRoomMembership(t *testing.T) {
	sendErr := errors.New("alice send failed")
	registry := NewSessionRegistry()
	alice := &registrySession{id: "connection-alice", sendErr: sendErr}
	bob := &registrySession{id: "connection-bob"}
	for loginName, session := range map[LoginName]*registrySession{"alice": alice, "bob": bob} {
		if err := registry.Register(session, loginName); err != nil {
			t.Fatalf("register %s: %v", loginName, err)
		}
		if err := registry.EnterRoom(loginName, "room-a"); err != nil {
			t.Fatalf("enter room %s: %v", loginName, err)
		}
	}
	if delivered, err := registry.BroadcastRoom("room-a", []byte("broadcast")); delivered != 1 || !errors.Is(err, sendErr) {
		t.Fatalf("broadcast error = delivered:%d error:%v, want 1/send error", delivered, err)
	} else if !strings.Contains(err.Error(), `room "room-a"`) || !strings.Contains(err.Error(), `login "alice"`) {
		t.Fatalf("broadcast error lacks room/login context: %v", err)
	}
	if len(bob.Sent()) != 1 {
		t.Fatalf("bob did not receive broadcast after alice failure")
	}
	registry.Remove(bob)
	if delivered, err := registry.BroadcastRoom("room-a", []byte("after-disconnect")); delivered != 0 || !errors.Is(err, sendErr) {
		t.Fatalf("broadcast after bob disconnect = delivered:%d error:%v, want 0/send error", delivered, err)
	}
	registry.Remove(alice)
	if delivered, err := registry.BroadcastRoom("room-a", nil); delivered != 0 || err != nil {
		t.Fatalf("broadcast after room cleanup = delivered:%d error:%v, want 0/nil", delivered, err)
	}
}

func TestSessionRegistryContractKickLoginAndRoom(t *testing.T) {
	closeErr := errors.New("bob close failed")
	registry := NewSessionRegistry()
	alice := &registrySession{id: "connection-alice"}
	bob := &registrySession{id: "connection-bob", closeErr: closeErr}
	carol := &registrySession{id: "connection-carol"}
	for loginName, session := range map[LoginName]*registrySession{"alice": alice, "bob": bob, "carol": carol} {
		if err := registry.Register(session, loginName); err != nil {
			t.Fatalf("register %s: %v", loginName, err)
		}
	}
	if err := registry.EnterRoom("alice", "room-a"); err != nil {
		t.Fatal(err)
	}
	if err := registry.EnterRoom("bob", "room-a"); err != nil {
		t.Fatal(err)
	}
	if err := registry.EnterRoom("carol", "room-b"); err != nil {
		t.Fatal(err)
	}
	if err := registry.KickLoginName("alice"); err != nil {
		t.Fatalf("kick alice: %v", err)
	}
	if alice.CloseCount() != 1 {
		t.Fatalf("alice close count = %d, want 1", alice.CloseCount())
	}
	if delivered, err := registry.BroadcastRoom("room-a", nil); delivered != 1 || err != nil {
		t.Fatalf("room-a after alice kick = delivered:%d error:%v, want 1/nil", delivered, err)
	}
	if err := registry.SendToLoginName("alice", nil); !errors.Is(err, ErrLoginSessionNotFound) {
		t.Fatalf("alice mapping after kick = %v, want ErrLoginSessionNotFound", err)
	}
	dave := &registrySession{id: "connection-dave"}
	if err := registry.Register(dave, "dave"); err != nil {
		t.Fatal(err)
	}
	if err := registry.EnterRoom("dave", "room-a"); err != nil {
		t.Fatal(err)
	}
	if kicked, err := registry.KickRoom("room-a"); kicked != 2 || !errors.Is(err, closeErr) {
		t.Fatalf("kick room-a = kicked:%d error:%v, want 2/close error", kicked, err)
	} else if !strings.Contains(err.Error(), `room "room-a"`) || !strings.Contains(err.Error(), `login "bob"`) {
		t.Fatalf("kick room error lacks room/login context: %v", err)
	}
	if bob.CloseCount() != 1 || dave.CloseCount() != 1 {
		t.Fatalf("room kick close counts = bob:%d dave:%d, want 1/1", bob.CloseCount(), dave.CloseCount())
	}
	if err := registry.SendToLoginName("bob", nil); !errors.Is(err, ErrLoginSessionNotFound) {
		t.Fatalf("bob mapping after room kick = %v, want ErrLoginSessionNotFound", err)
	}
	if err := registry.SendToLoginName("carol", []byte("still connected")); err != nil {
		t.Fatalf("carol was affected by room-a kick: %v", err)
	}
	if _, err := registry.KickRoom(""); !errors.Is(err, ErrRoomIDInvalid) {
		t.Fatalf("empty room kick error = %v, want ErrRoomIDInvalid", err)
	}
}

type registrySession struct {
	id WebSocketConnectionID

	mu         sync.Mutex
	sent       [][]byte
	closeCount int
	sendErr    error
	closeErr   error
}

func (s *registrySession) ID() WebSocketConnectionID { return s.id }

func (s *registrySession) SendBinary(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendErr != nil {
		return s.sendErr
	}
	s.sent = append(s.sent, append([]byte(nil), data...))
	return nil
}

func (s *registrySession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCount++
	return s.closeErr
}

func (s *registrySession) Sent() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]byte(nil), s.sent...)
}

func (s *registrySession) CloseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCount
}
