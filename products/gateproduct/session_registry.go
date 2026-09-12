package gateproduct

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/logging"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
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
	// ErrLoginRequired indicates that a connection has not completed login.
	ErrLoginRequired = errors.New("gate session: login is required")
	// ErrRoomRequired indicates that a connection has not entered a room.
	ErrRoomRequired = errors.New("gate session: room is required")
)

// LoginName identifies one caller-authenticated player identity.
type LoginName = serversend.LoginName

// RoomID identifies one caller-authorized local room membership.
type RoomID string

// SessionState is a read-only snapshot of the canonical connection state.
type SessionState struct {
	LoginName LoginName
	RoomID    RoomID
}

type registeredLoginSession struct {
	connectionID WebSocketConnectionID
	roomID       RoomID
	session      ClosableWebSocketSession
}

type roomTarget struct {
	loginName LoginName
	session   ClosableWebSocketSession
}

// sessionPresence is the optional distributed owner lease used by a Gate
// session registry. Authentication remains outside this type.
type sessionPresence interface {
	Claim(context.Context, serversend.LoginName, serversend.ConnectionID) (serversend.Presence, error)
	Release(context.Context, serversend.Presence) error
}

type sessionPresenceLease struct {
	presence serversend.Presence
	owner    sessionPresence
}

type detachedSession struct {
	session ClosableWebSocketSession
	lease   *sessionPresenceLease
}

// SessionRegistry stores canonical identities already authenticated by its
// caller. It is process-local for delivery and room membership, and can
// optionally maintain the same identity's distributed Gate owner lease.
// It does not authenticate login names or tokens.
type SessionRegistry struct {
	mu           sync.RWMutex
	byLoginName  map[LoginName]registeredLoginSession
	byConnection map[WebSocketConnectionID]LoginName
	byRoomID     map[RoomID]map[LoginName]struct{}

	registrationMu     sync.Mutex
	presence           sessionPresence
	presenceTTL        time.Duration
	presenceScheduler  sessionPresenceScheduler
	leasesByConnection map[WebSocketConnectionID]*sessionPresenceLease
	logger             *logging.Logger
}

func NewSessionRegistry() *SessionRegistry {
	return newLocalSessionRegistry()
}

// State returns a copy of the canonical login and room state for one
// connection. It never exposes the session pointer or performs authentication.
func (r *SessionRegistry) State(connectionID WebSocketConnectionID) (SessionState, bool) {
	if r == nil || connectionID == "" {
		return SessionState{}, false
	}
	r.mu.RLock()
	loginName, exists := r.byConnection[connectionID]
	entry, current := r.byLoginName[loginName]
	r.mu.RUnlock()
	if !exists || !current || entry.connectionID != connectionID {
		return SessionState{}, false
	}
	return SessionState{LoginName: loginName, RoomID: entry.roomID}, true
}

func newLocalSessionRegistry() *SessionRegistry {
	return &SessionRegistry{
		byLoginName:        make(map[LoginName]registeredLoginSession),
		byConnection:       make(map[WebSocketConnectionID]LoginName),
		byRoomID:           make(map[RoomID]map[LoginName]struct{}),
		leasesByConnection: make(map[WebSocketConnectionID]*sessionPresenceLease),
	}
}

func newSessionRegistryWithScheduler(presence sessionPresence, cfg serversend.PresenceConfig, logger *logging.Logger, scheduler sessionPresenceScheduler) (*SessionRegistry, error) {
	if presence == nil {
		if cfg.LeaseTTL > 0 {
			return nil, fmt.Errorf("gate session: presence owner is required when lease ttl is configured")
		}
		return newLocalSessionRegistry(), nil
	}
	normalized, err := serversend.NormalizePresenceConfig(cfg)
	if err != nil {
		return nil, err
	}
	if scheduler == nil {
		return nil, fmt.Errorf("gate session: presence renewal scheduler is required")
	}
	registry := newLocalSessionRegistry()
	registry.presence = presence
	registry.presenceTTL = normalized.LeaseTTL
	registry.presenceScheduler = scheduler
	registry.logger = logger
	return registry, nil
}

// Register makes session the authoritative local delivery target for the
// caller-authenticated loginName. With presence enabled, the distributed
// claim succeeds before this local mapping changes. Re-registering the same
// connection and identity is idempotent; a newer connection for the same
// login replaces and closes the prior local session.
func (r *SessionRegistry) Register(session ClosableWebSocketSession, loginName LoginName) error {
	if session == nil {
		return fmt.Errorf("%w: session is nil", ErrSessionRegistrationInvalid)
	}
	connectionID := session.ID()
	if connectionID == "" || loginName == "" {
		return fmt.Errorf("%w: connection id and login name are required", ErrSessionRegistrationInvalid)
	}

	r.registrationMu.Lock()

	r.mu.RLock()
	existingLoginName, connectionRegistered := r.byConnection[connectionID]
	r.mu.RUnlock()
	if connectionRegistered {
		r.registrationMu.Unlock()
		if existingLoginName == loginName {
			return nil
		}
		return fmt.Errorf("%w: %q", ErrSessionAlreadyRegistered, existingLoginName)
	}

	var claimed serversend.Presence
	var err error
	if r.presence != nil {
		operationCtx, cancel := presenceOperationContext(context.Background(), r.presenceTTL)
		claimed, err = r.presence.Claim(operationCtx, serversend.LoginName(loginName), serversend.ConnectionID(connectionID))
		cancel()
		if err != nil {
			r.registrationMu.Unlock()
			return err
		}
		if r.presenceScheduler == nil {
			r.registrationMu.Unlock()
			_ = r.releasePresence(context.Background(), claimed)
			return errors.New("gate session: presence renewal scheduler is not configured")
		}
		if err := r.presenceScheduler.Schedule(claimed); err != nil {
			r.registrationMu.Unlock()
			_ = r.releasePresence(context.Background(), claimed)
			return err
		}
	}

	r.mu.Lock()
	// registrationMu serializes all mutations, but keep the identity check at
	// the commit point so this invariant remains local to the write boundary.
	if existingLoginName, exists := r.byConnection[connectionID]; exists {
		r.mu.Unlock()
		r.registrationMu.Unlock()
		if r.presence != nil {
			r.presenceScheduler.Remove(claimed)
			_ = r.releasePresence(context.Background(), claimed)
		}
		if existingLoginName == loginName {
			return nil
		}
		return fmt.Errorf("%w: %q", ErrSessionAlreadyRegistered, existingLoginName)
	}
	previous, replaced := r.detachLoginLocked(loginName)
	r.byLoginName[loginName] = registeredLoginSession{connectionID: connectionID, session: session}
	r.byConnection[connectionID] = loginName
	if r.presence != nil {
		lease := &sessionPresenceLease{presence: claimed, owner: r.presence}
		r.leasesByConnection[connectionID] = lease
	}
	r.mu.Unlock()
	r.registrationMu.Unlock()

	if replaced {
		// The new mapping is authoritative before stale socket cleanup. Its old
		// lease is fenced by epoch, so cleanup cannot release the replacement.
		if err := r.cleanupDetached(context.Background(), previous, true); err != nil {
			if r.logger != nil {
				r.logger.Error(context.Background(), "session_cleanup", "replaced session cleanup failed", err,
					slog.String("login_name", string(loginName)))
			}
		}
	}
	return nil
}

// Remove forgets a disconnected WebSocket session. A stale disconnect cannot
// remove a replacement because the connection ID must still match.
func (r *SessionRegistry) Remove(session WebSocketSession) {
	if session == nil {
		return
	}
	r.registrationMu.Lock()
	r.mu.Lock()
	loginName, exists := r.byConnection[session.ID()]
	entry, current := r.byLoginName[loginName]
	var detached detachedSession
	if exists && current && entry.connectionID == session.ID() {
		detached, _ = r.detachLoginLocked(loginName)
	}
	r.mu.Unlock()
	r.registrationMu.Unlock()
	if detached.session != nil {
		if err := r.cleanupDetached(context.Background(), detached, false); err != nil {
			if r.logger != nil {
				r.logger.Error(context.Background(), "session_cleanup", "disconnected session cleanup failed", err,
					slog.String("login_name", string(loginName)))
			}
		}
	}
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
	return r.sendToLoginNameAt(loginName, data, time.Now(), serverSendTargetPlayer)
}

func (r *SessionRegistry) sendToLoginNameAt(loginName LoginName, data []byte, receivedAt time.Time, target serverSendTarget) error {
	return r.sendToLoginNameWithContext(context.Background(), loginName, data, receivedAt, target)
}

func (r *SessionRegistry) sendToLoginNameWithContext(ctx context.Context, loginName LoginName, data []byte, receivedAt time.Time, target serverSendTarget) error {
	r.mu.RLock()
	entry, exists := r.byLoginName[loginName]
	r.mu.RUnlock()
	if !exists {
		return fmt.Errorf("%w: %q", ErrLoginSessionNotFound, loginName)
	}
	return sendOutbound(entry.session, outboundMessage{data: data, source: outboundSourceServerSend, receivedAt: receivedAt, target: target, ctx: ctx})
}

// BroadcastRoom sends one complete client wire packet to every current local
// member of roomID. A failed member does not prevent delivery to others.
func (r *SessionRegistry) BroadcastRoom(ctx context.Context, roomID RoomID, data []byte) (int, error) {
	return r.broadcastRoomAt(ctx, roomID, data, time.Now(), serverSendTargetRoom)
}

func (r *SessionRegistry) broadcastRoomAt(ctx context.Context, roomID RoomID, data []byte, receivedAt time.Time, deliveryTarget serverSendTarget) (int, error) {
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
	for _, member := range targets {
		if err := sendOutbound(member.session, outboundMessage{data: data, source: outboundSourceServerSend, receivedAt: receivedAt, target: deliveryTarget, ctx: ctx}); err != nil {
			errs = append(errs, fmt.Errorf("broadcast room %q to login %q: %w", roomID, member.loginName, err))
			continue
		}
		delivered++
	}
	return delivered, errors.Join(errs...)
}

// KickLoginName removes and closes the current local session for loginName.
// A missing login name is an idempotent no-op.
func (r *SessionRegistry) KickLoginName(loginName LoginName) error {
	r.registrationMu.Lock()
	r.mu.Lock()
	detached, exists := r.detachLoginLocked(loginName)
	r.mu.Unlock()
	r.registrationMu.Unlock()
	if !exists {
		return nil
	}
	return r.cleanupDetached(context.Background(), detached, true)
}

// KickRoom removes and closes every current local session in roomID. The
// returned count is the number of selected sessions, regardless of close
// errors. Other rooms are unaffected.
func (r *SessionRegistry) KickRoom(roomID RoomID) (int, error) {
	if roomID == "" {
		return 0, ErrRoomIDInvalid
	}
	r.registrationMu.Lock()
	r.mu.Lock()
	members := r.byRoomID[roomID]
	targets := make([]roomTarget, 0, len(members))
	detached := make([]detachedSession, 0, len(members))
	for loginName := range members {
		entry, exists := r.byLoginName[loginName]
		if !exists {
			continue
		}
		targets = append(targets, roomTarget{loginName: loginName, session: entry.session})
		item, _ := r.detachLoginLocked(loginName)
		detached = append(detached, item)
	}
	r.mu.Unlock()
	r.registrationMu.Unlock()

	return len(targets), r.cleanupDetachedBatch(context.Background(), detached, true, func(index int, err error) error {
		return fmt.Errorf("kick room %q login %q: %w", roomID, targets[index].loginName, err)
	})
}

func (r *SessionRegistry) detachLoginLocked(loginName LoginName) (detachedSession, bool) {
	entry, exists := r.byLoginName[loginName]
	if !exists {
		return detachedSession{}, false
	}
	delete(r.byLoginName, loginName)
	delete(r.byConnection, entry.connectionID)
	r.removeFromRoomLocked(loginName, entry.roomID)
	lease := r.leasesByConnection[entry.connectionID]
	delete(r.leasesByConnection, entry.connectionID)
	return detachedSession{session: entry.session, lease: lease}, true
}

func (r *SessionRegistry) cleanupDetached(ctx context.Context, detached detachedSession, closeSession bool) error {
	return r.cleanupDetachedBatch(ctx, []detachedSession{detached}, closeSession, nil)
}

// cleanupDetachedBatch applies the fixed detach cleanup ordering to all room
// members: remove leases from the scheduler, close every socket, then release
// every presence lease under one shared deadline. A failure for one member
// never prevents cleanup of the remaining members.
func (r *SessionRegistry) cleanupDetachedBatch(ctx context.Context, detached []detachedSession, closeSession bool, wrap func(int, error) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var errs []error
	for _, item := range detached {
		if item.lease == nil {
			continue
		}
		if r.presenceScheduler != nil {
			r.presenceScheduler.Remove(item.lease.presence)
		}
	}
	for index, item := range detached {
		if closeSession && item.session != nil {
			if err := item.session.Close(); err != nil {
				if wrap != nil {
					errs = append(errs, wrap(index, err))
				} else {
					errs = append(errs, err)
				}
			}
		}
	}
	releaseCtx, cancel := presenceReleaseContext(ctx, r.presenceTTL)
	defer cancel()
	for index, item := range detached {
		if item.lease == nil {
			continue
		}
		if err := item.lease.owner.Release(releaseCtx, item.lease.presence); err != nil && !errors.Is(err, serversend.ErrPresenceNotOwner) {
			if wrap != nil {
				errs = append(errs, wrap(index, err))
			} else {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (r *SessionRegistry) releasePresence(ctx context.Context, presence serversend.Presence) error {
	if r.presence == nil {
		return nil
	}
	releaseCtx, cancel := presenceReleaseContext(ctx, r.presenceTTL)
	defer cancel()
	err := r.presence.Release(releaseCtx, presence)
	if errors.Is(err, serversend.ErrPresenceNotOwner) {
		return nil
	}
	return err
}

func presenceOperationContext(parent context.Context, leaseTTL time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	timeout := leaseTTL / 3
	if timeout <= 0 {
		timeout = time.Millisecond
	}
	if timeout > 5*time.Second {
		timeout = 5 * time.Second
	}
	return context.WithTimeout(parent, timeout)
}

func presenceReleaseContext(parent context.Context, leaseTTL time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if parent.Err() != nil {
		parent = context.WithoutCancel(parent)
	}
	return presenceOperationContext(parent, leaseTTL)
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
