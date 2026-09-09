package gateproduct

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrLoginSessionNotFound indicates the requested login name has no current
	// local WebSocket session.
	ErrLoginSessionNotFound = errors.New("gate session: login name is not connected")
	// ErrSessionRegistrationInvalid indicates a malformed session registration.
	ErrSessionRegistrationInvalid = errors.New("gate session: registration is invalid")
	// ErrSessionAlreadyRegistered prevents one WebSocket connection from
	// switching identity after its caller has registered it.
	ErrSessionAlreadyRegistered = errors.New("gate session: connection is already registered")
	// ErrRoomIDInvalid indicates a room operation without a room identity.
	ErrRoomIDInvalid = errors.New("gate session: room id is invalid")
)

// LoginName identifies one caller-authenticated player identity.
type LoginName string

// RoomID identifies one caller-authorized local room membership.
type RoomID string

type registeredLoginSession struct {
	connectionID WebSocketConnectionID
	roomID       RoomID
	session      ClosableWebSocketSession
}

type roomTarget struct {
	loginName LoginName
	session   ClosableWebSocketSession
}

// SessionRegistry stores canonical identities already authenticated by its
// caller. It is process-local: delivery, room broadcast, and kick operations
// only affect sessions accepted by this Gate instance. It does not authenticate
// login names or tokens.
type SessionRegistry struct {
	mu           sync.RWMutex
	byLoginName  map[LoginName]registeredLoginSession
	byConnection map[WebSocketConnectionID]LoginName
	byRoomID     map[RoomID]map[LoginName]struct{}
}

func NewSessionRegistry() *SessionRegistry {
	return &SessionRegistry{
		byLoginName:  make(map[LoginName]registeredLoginSession),
		byConnection: make(map[WebSocketConnectionID]LoginName),
		byRoomID:     make(map[RoomID]map[LoginName]struct{}),
	}
}

// Register makes session the authoritative local delivery target for the
// caller-authenticated loginName. Re-registering the same connection and
// identity is idempotent; a newer connection for the same login replaces and
// closes the prior local session.
func (r *SessionRegistry) Register(session ClosableWebSocketSession, loginName LoginName) error {
	if session == nil {
		return fmt.Errorf("%w: session is nil", ErrSessionRegistrationInvalid)
	}
	connectionID := session.ID()
	if connectionID == "" || loginName == "" {
		return fmt.Errorf("%w: connection id and login name are required", ErrSessionRegistrationInvalid)
	}

	var previous ClosableWebSocketSession
	r.mu.Lock()
	if existingLoginName, exists := r.byConnection[connectionID]; exists {
		r.mu.Unlock()
		if existingLoginName == loginName {
			return nil
		}
		return fmt.Errorf("%w: %q", ErrSessionAlreadyRegistered, existingLoginName)
	}
	if existing, exists := r.byLoginName[loginName]; exists {
		previous = existing.session
		r.removeLocked(loginName, existing)
	}
	r.byLoginName[loginName] = registeredLoginSession{
		connectionID: connectionID,
		session:      session,
	}
	r.byConnection[connectionID] = loginName
	r.mu.Unlock()

	if previous != nil {
		// Replacement is authoritative before closing the stale connection. A
		// socket that already disconnected may report a close error, which must
		// not invalidate the new authoritative mapping.
		_ = previous.Close()
	}
	return nil
}

// Remove forgets a disconnected WebSocket session. A stale disconnect cannot
// remove a replacement because the connection ID must still match.
func (r *SessionRegistry) Remove(session WebSocketSession) {
	if session == nil {
		return
	}
	r.mu.Lock()
	loginName := r.byConnection[session.ID()]
	entry, exists := r.byLoginName[loginName]
	if exists && entry.connectionID == session.ID() {
		r.removeLocked(loginName, entry)
	}
	r.mu.Unlock()
}

// EnterRoom places the authoritative local session in exactly one room. Room
// admission must be validated by the caller before invoking this method.
func (r *SessionRegistry) EnterRoom(loginName LoginName, roomID RoomID) error {
	if loginName == "" {
		return ErrLoginSessionNotFound
	}
	if roomID == "" {
		return ErrRoomIDInvalid
	}
	r.mu.Lock()
	entry, exists := r.byLoginName[loginName]
	if !exists {
		r.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrLoginSessionNotFound, loginName)
	}
	if entry.roomID == roomID {
		r.mu.Unlock()
		return nil
	}
	r.removeFromRoomLocked(loginName, entry.roomID)
	entry.roomID = roomID
	r.byLoginName[loginName] = entry
	r.addToRoomLocked(loginName, roomID)
	r.mu.Unlock()
	return nil
}

// LeaveRoom removes the authoritative local session from its current room
// while keeping its login session available for targeted delivery.
func (r *SessionRegistry) LeaveRoom(loginName LoginName) error {
	r.mu.Lock()
	entry, exists := r.byLoginName[loginName]
	if !exists {
		r.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrLoginSessionNotFound, loginName)
	}
	if entry.roomID == "" {
		r.mu.Unlock()
		return nil
	}
	r.removeFromRoomLocked(loginName, entry.roomID)
	entry.roomID = ""
	r.byLoginName[loginName] = entry
	r.mu.Unlock()
	return nil
}

// SendToLoginName sends one complete client wire packet to the current
// authoritative local session for loginName.
func (r *SessionRegistry) SendToLoginName(loginName LoginName, data []byte) error {
	r.mu.RLock()
	entry, exists := r.byLoginName[loginName]
	r.mu.RUnlock()
	if !exists {
		return fmt.Errorf("%w: %q", ErrLoginSessionNotFound, loginName)
	}
	return entry.session.SendBinary(data)
}

// BroadcastRoom sends one complete client wire packet to every current local
// member of roomID. A failed member does not prevent delivery to others.
func (r *SessionRegistry) BroadcastRoom(roomID RoomID, data []byte) (int, error) {
	if roomID == "" {
		return 0, ErrRoomIDInvalid
	}
	r.mu.RLock()
	members := r.byRoomID[roomID]
	targets := make([]roomTarget, 0, len(members))
	for loginName := range members {
		if entry, exists := r.byLoginName[loginName]; exists {
			targets = append(targets, roomTarget{loginName: loginName, session: entry.session})
		}
	}
	r.mu.RUnlock()

	delivered := 0
	var errs []error
	for _, target := range targets {
		if err := target.session.SendBinary(data); err != nil {
			errs = append(errs, fmt.Errorf("broadcast room %q to login %q: %w", roomID, target.loginName, err))
			continue
		}
		delivered++
	}
	return delivered, errors.Join(errs...)
}

// KickLoginName removes and closes the current local session for loginName.
// A missing login name is an idempotent no-op.
func (r *SessionRegistry) KickLoginName(loginName LoginName) error {
	r.mu.Lock()
	entry, exists := r.byLoginName[loginName]
	if exists {
		r.removeLocked(loginName, entry)
	}
	r.mu.Unlock()
	if !exists {
		return nil
	}
	return entry.session.Close()
}

// KickRoom removes and closes every current local session in roomID. The
// returned count is the number of selected sessions, regardless of close
// errors. Other rooms are unaffected.
func (r *SessionRegistry) KickRoom(roomID RoomID) (int, error) {
	if roomID == "" {
		return 0, ErrRoomIDInvalid
	}
	r.mu.Lock()
	members := r.byRoomID[roomID]
	targets := make([]roomTarget, 0, len(members))
	for loginName := range members {
		entry, exists := r.byLoginName[loginName]
		if !exists {
			continue
		}
		targets = append(targets, roomTarget{loginName: loginName, session: entry.session})
		r.removeLocked(loginName, entry)
	}
	r.mu.Unlock()

	var errs []error
	for _, target := range targets {
		if err := target.session.Close(); err != nil {
			errs = append(errs, fmt.Errorf("kick room %q login %q: %w", roomID, target.loginName, err))
		}
	}
	return len(targets), errors.Join(errs...)
}

func (r *SessionRegistry) addToRoomLocked(loginName LoginName, roomID RoomID) {
	members := r.byRoomID[roomID]
	if members == nil {
		members = make(map[LoginName]struct{})
		r.byRoomID[roomID] = members
	}
	members[loginName] = struct{}{}
}

func (r *SessionRegistry) removeFromRoomLocked(loginName LoginName, roomID RoomID) {
	if roomID == "" {
		return
	}
	members := r.byRoomID[roomID]
	delete(members, loginName)
	if len(members) == 0 {
		delete(r.byRoomID, roomID)
	}
}

func (r *SessionRegistry) removeLocked(loginName LoginName, entry registeredLoginSession) {
	delete(r.byLoginName, loginName)
	delete(r.byConnection, entry.connectionID)
	r.removeFromRoomLocked(loginName, entry.roomID)
}
