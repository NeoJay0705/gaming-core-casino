package gateproduct

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
)

func newSessionRegistry(presence sessionPresence, leaseTTL time.Duration) (*SessionRegistry, error) {
	if presence == nil {
		return newSessionRegistryWithScheduler(nil, serversend.PresenceConfig{}, nil, nil)
	}
	scheduler := &sessionPresenceSchedulerFake{}
	return newSessionRegistryWithScheduler(presence, serversend.PresenceConfig{LeaseTTL: leaseTTL}, nil, scheduler)
}

type sessionPresenceSchedulerFake struct {
	mu          sync.Mutex
	scheduled   []serversend.Presence
	removed     []serversend.Presence
	scheduleErr error
}

func (s *sessionPresenceSchedulerFake) Start(context.Context) error { return nil }

func (s *sessionPresenceSchedulerFake) Stop(context.Context) error { return nil }

func (s *sessionPresenceSchedulerFake) Schedule(presence serversend.Presence) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.scheduleErr != nil {
		return s.scheduleErr
	}
	s.scheduled = append(s.scheduled, presence)
	return nil
}

func (s *sessionPresenceSchedulerFake) Remove(presence serversend.Presence) {
	s.mu.Lock()
	s.removed = append(s.removed, presence)
	s.mu.Unlock()
}

func (s *sessionPresenceSchedulerFake) scheduledSnapshot() []serversend.Presence {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]serversend.Presence(nil), s.scheduled...)
}

func (s *sessionPresenceSchedulerFake) removedSnapshot() []serversend.Presence {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]serversend.Presence(nil), s.removed...)
}

func TestSessionRegistryPresenceContractClaimsAndSchedulesAuthenticatedSession(t *testing.T) {
	presence := &sessionPresenceFake{gateID: "gate-a"}
	registry, err := newSessionRegistry(presence, 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	session := &registrySession{id: "connection-alice"}
	if err := registry.Register(session, "alice"); err != nil {
		t.Fatalf("register: %v", err)
	}
	claimed := presence.claimedSnapshot()
	if len(claimed) != 1 || claimed[0].LoginName != "alice" || claimed[0].ConnectionID != "connection-alice" || claimed[0].GateID != "gate-a" {
		t.Fatalf("claims = %#v, want exact authenticated identity", claimed)
	}
	scheduler := registry.presenceScheduler.(*sessionPresenceSchedulerFake)
	if scheduled := scheduler.scheduledSnapshot(); len(scheduled) != 1 || scheduled[0] != claimed[0] {
		t.Fatalf("scheduled leases = %#v, want one claimed lease", scheduled)
	}
	if err := registry.Register(session, "alice"); err != nil {
		t.Fatalf("idempotent register: %v", err)
	}
	if scheduled := scheduler.scheduledSnapshot(); len(scheduled) != 1 {
		t.Fatalf("idempotent register scheduled %d leases, want 1", len(scheduled))
	}

	registry.Remove(session)
	if got := len(presence.releasedSnapshot()); got != 1 {
		t.Fatalf("release count after disconnect = %d, want 1", got)
	}
	if removed := scheduler.removedSnapshot(); len(removed) != 1 || removed[0] != claimed[0] {
		t.Fatalf("removed leases = %#v, want one claimed lease", removed)
	}
}

func TestSessionRegistryPresenceContractClaimFailurePreservesExistingLocalSession(t *testing.T) {
	claimErr := errors.New("presence store unavailable")
	presence := &sessionPresenceFake{gateID: "gate-a"}
	registry, err := newSessionRegistry(presence, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	oldSession := &registrySession{id: "connection-old"}
	if err := registry.Register(oldSession, "alice"); err != nil {
		t.Fatal(err)
	}
	presence.mu.Lock()
	presence.claimErr = claimErr
	presence.mu.Unlock()
	newSession := &registrySession{id: "connection-new"}
	if err := registry.Register(newSession, "alice"); !errors.Is(err, claimErr) {
		t.Fatalf("claim failure = %v, want %v", err, claimErr)
	}
	if newSession.CloseCount() != 0 {
		t.Fatalf("failed registration closed new session %d times", newSession.CloseCount())
	}
	if err := registry.SendToLoginName("alice", []byte("old-is-still-authoritative")); err != nil {
		t.Fatalf("old session was not preserved: %v", err)
	}
	if len(oldSession.Sent()) != 1 {
		t.Fatalf("old session sends = %d, want 1", len(oldSession.Sent()))
	}
	registry.Remove(oldSession)
}

func TestSessionRegistryPresenceContractKickReleasesEveryRoomLease(t *testing.T) {
	presence := &sessionPresenceFake{gateID: "gate-a"}
	registry, err := newSessionRegistry(presence, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	alice := &registrySession{id: "connection-alice"}
	bob := &registrySession{id: "connection-bob"}
	for loginName, session := range map[LoginName]*registrySession{"alice": alice, "bob": bob} {
		if err := registry.Register(session, loginName); err != nil {
			t.Fatal(err)
		}
		if err := registry.EnterRoom(loginName, "room-a"); err != nil {
			t.Fatal(err)
		}
	}
	if kicked, err := registry.KickRoom("room-a"); err != nil || kicked != 2 {
		t.Fatalf("kick room = %d, %v; want 2/nil", kicked, err)
	}
	if got := len(presence.releasedSnapshot()); got != 2 {
		t.Fatalf("release count after room kick = %d, want 2", got)
	}
	if alice.CloseCount() != 1 || bob.CloseCount() != 1 {
		t.Fatalf("close counts = alice:%d bob:%d, want 1/1", alice.CloseCount(), bob.CloseCount())
	}
}

func TestSessionRegistryPresenceContractReplacementFencesStaleDisconnect(t *testing.T) {
	presence := &sessionPresenceFake{gateID: "gate-a"}
	registry, err := newSessionRegistry(presence, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	oldSession := &registrySession{id: "connection-old"}
	newSession := &registrySession{id: "connection-new"}
	if err := registry.Register(oldSession, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(newSession, "alice"); err != nil {
		t.Fatal(err)
	}
	claimed := presence.claimedSnapshot()
	released := presence.releasedSnapshot()
	if len(claimed) != 2 || len(released) != 1 || released[0].Epoch != claimed[0].Epoch {
		t.Fatalf("replacement lease transition = claimed:%#v released:%#v", claimed, released)
	}
	registry.Remove(oldSession)
	if releasedAfterStaleDisconnect := len(presence.releasedSnapshot()); releasedAfterStaleDisconnect != 1 {
		t.Fatalf("stale disconnect released current lease: releases=%d", releasedAfterStaleDisconnect)
	}
	if err := registry.SendToLoginName("alice", []byte("replacement")); err != nil {
		t.Fatalf("replacement session unavailable: %v", err)
	}
	registry.Remove(newSession)
}

func TestSessionRegistryPresenceContractWithoutPresenceKeepsLocalBehavior(t *testing.T) {
	registry, err := newSessionRegistry(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	session := &registrySession{id: "connection-local"}
	if err := registry.Register(session, "alice"); err != nil {
		t.Fatal(err)
	}
	if len(registry.leasesByConnection) != 0 {
		t.Fatalf("local registry created presence leases: %d", len(registry.leasesByConnection))
	}
	registry.Remove(session)
}

func TestSessionRegistryPresenceContractScheduleFailureRollsBackClaim(t *testing.T) {
	presence := &sessionPresenceFake{gateID: "gate-a"}
	scheduleErr := errors.New("scheduler is stopped")
	scheduler := &sessionPresenceSchedulerFake{scheduleErr: scheduleErr}
	registry, err := newSessionRegistryWithScheduler(presence, serversend.PresenceConfig{LeaseTTL: 30 * time.Millisecond}, nil, scheduler)
	if err != nil {
		t.Fatal(err)
	}
	session := &registrySession{id: "connection-schedule-failure"}
	if err := registry.Register(session, "alice"); !errors.Is(err, scheduleErr) {
		t.Fatalf("register schedule failure = %v, want %v", err, scheduleErr)
	}
	if _, ok := registry.State(session.ID()); ok {
		t.Fatal("schedule failure created local session")
	}
	claimed := presence.claimedSnapshot()
	released := presence.releasedSnapshot()
	if len(claimed) != 1 || len(released) != 1 || released[0] != claimed[0] {
		t.Fatalf("schedule rollback leases = claimed:%#v released:%#v", claimed, released)
	}
}

func TestSessionRegistryPresenceContractClosesSocketBeforeBlockedRelease(t *testing.T) {
	events := &lifecycleEventLog{}
	presence := newLifecyclePresenceFake()
	presence.events = events
	presence.releaseContinue = make(chan struct{})
	registry, err := newSessionRegistry(presence, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	session := newBlockingCloseSession("connection-kick", events)
	if err := registry.Register(session, "alice"); err != nil {
		t.Fatal(err)
	}

	kicked := make(chan error, 1)
	go func() { kicked <- registry.KickLoginName("alice") }()
	select {
	case <-session.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("KickLoginName did not close the socket")
	}
	select {
	case <-presence.releaseStarted:
	case <-time.After(time.Second):
		t.Fatal("KickLoginName did not reach presence release")
	}
	if got := events.snapshot(); len(got) < 2 || got[0] != "close:connection-kick" || got[1] != "release" {
		t.Fatalf("kick cleanup order = %v, want close before release", got)
	}
	close(presence.releaseContinue)
	if err := <-kicked; err != nil {
		t.Fatalf("kick after release unblock: %v", err)
	}
}

func TestSessionRegistryPresenceContractKickRoomClosesAllBeforeReleaseFailures(t *testing.T) {
	events := &lifecycleEventLog{}
	releaseErr := errors.New("release failed")
	presence := newLifecyclePresenceFake()
	presence.events = events
	presence.releaseContinue = make(chan struct{})
	presence.releaseErr = releaseErr
	registry, err := newSessionRegistry(presence, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	alice := newBlockingCloseSession("connection-alice", events)
	bob := newBlockingCloseSession("connection-bob", events)
	for loginName, session := range map[LoginName]*blockingCloseSession{"alice": alice, "bob": bob} {
		if err := registry.Register(session, loginName); err != nil {
			t.Fatal(err)
		}
		if err := registry.EnterRoom(loginName, "room-a"); err != nil {
			t.Fatal(err)
		}
	}

	kicked := make(chan struct {
		count int
		err   error
	}, 1)
	go func() {
		count, err := registry.KickRoom("room-a")
		kicked <- struct {
			count int
			err   error
		}{count: count, err: err}
	}()
	select {
	case <-presence.releaseStarted:
	case <-time.After(time.Second):
		t.Fatal("KickRoom did not reach presence release")
	}
	if alice.CloseCount() != 1 || bob.CloseCount() != 1 {
		t.Fatalf("room cleanup close counts before release = alice:%d bob:%d, want 1/1", alice.CloseCount(), bob.CloseCount())
	}
	close(presence.releaseContinue)
	result := <-kicked
	if result.count != 2 || !errors.Is(result.err, releaseErr) || presence.releaseCount() != 2 {
		t.Fatalf("room cleanup result = count:%d error:%v releases:%d, want 2/release error/2", result.count, result.err, presence.releaseCount())
	}
}

func TestSessionRegistryPresenceContractReplacementDoesNotHoldTransitionLockDuringClose(t *testing.T) {
	events := &lifecycleEventLog{}
	registry, err := newSessionRegistry(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	oldSession := newBlockingCloseSession("connection-old", events)
	oldSession.closeContinue = make(chan struct{})
	if err := registry.Register(oldSession, "alice"); err != nil {
		t.Fatal(err)
	}

	replacementDone := make(chan error, 1)
	go func() {
		replacementDone <- registry.Register(newBlockingCloseSession("connection-new", events), "alice")
	}()
	select {
	case <-oldSession.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("replacement did not start old-session cleanup")
	}

	otherDone := make(chan error, 1)
	go func() { otherDone <- registry.Register(newBlockingCloseSession("connection-other", events), "bob") }()
	select {
	case err := <-otherDone:
		if err != nil {
			t.Fatalf("registration during replacement cleanup: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("registration was blocked by old-session cleanup")
	}
	close(oldSession.closeContinue)
	if err := <-replacementDone; err != nil {
		t.Fatalf("replacement registration: %v", err)
	}
}

type sessionPresenceFake struct {
	mu        sync.Mutex
	gateID    serversend.GateID
	nextEpoch uint64
	claimed   []serversend.Presence
	released  []serversend.Presence
	claimErr  error
}

func (p *sessionPresenceFake) Claim(_ context.Context, loginName serversend.LoginName, connectionID serversend.ConnectionID) (serversend.Presence, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.claimErr != nil {
		return serversend.Presence{}, p.claimErr
	}
	p.nextEpoch++
	presence := serversend.Presence{LoginName: loginName, GateID: p.gateID, ConnectionID: connectionID, Epoch: p.nextEpoch}
	p.claimed = append(p.claimed, presence)
	return presence, nil
}

func (p *sessionPresenceFake) Release(_ context.Context, presence serversend.Presence) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.released = append(p.released, presence)
	return nil
}

func (p *sessionPresenceFake) claimedSnapshot() []serversend.Presence {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]serversend.Presence(nil), p.claimed...)
}

func (p *sessionPresenceFake) releasedSnapshot() []serversend.Presence {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]serversend.Presence(nil), p.released...)
}

type lifecycleEventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *lifecycleEventLog) add(event string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.events = append(l.events, event)
	l.mu.Unlock()
}

func (l *lifecycleEventLog) snapshot() []string {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

type blockingCloseSession struct {
	id            WebSocketConnectionID
	events        *lifecycleEventLog
	closeStarted  chan struct{}
	closeOnce     sync.Once
	closeContinue chan struct{}
	closeErr      error

	mu         sync.Mutex
	closeCount int
}

func newBlockingCloseSession(id WebSocketConnectionID, events *lifecycleEventLog) *blockingCloseSession {
	return &blockingCloseSession{id: id, events: events, closeStarted: make(chan struct{})}
}

func (s *blockingCloseSession) ID() WebSocketConnectionID { return s.id }

func (s *blockingCloseSession) SendBinary(context.Context, []byte) error { return nil }

func (s *blockingCloseSession) Close() error {
	s.closeOnce.Do(func() {
		close(s.closeStarted)
		s.events.add("close:" + string(s.id))
	})
	s.mu.Lock()
	s.closeCount++
	s.mu.Unlock()
	if s.closeContinue != nil {
		<-s.closeContinue
	}
	return s.closeErr
}

func (s *blockingCloseSession) CloseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCount
}

type lifecyclePresenceFake struct {
	mu sync.Mutex

	gateID    serversend.GateID
	nextEpoch uint64
	releases  []serversend.Presence

	events          *lifecycleEventLog
	releaseStarted  chan struct{}
	releaseOnce     sync.Once
	releaseContinue chan struct{}
	releaseErr      error
}

func newLifecyclePresenceFake() *lifecyclePresenceFake {
	return &lifecyclePresenceFake{
		gateID:         "gate-a",
		releaseStarted: make(chan struct{}),
	}
}

func (p *lifecyclePresenceFake) Claim(_ context.Context, loginName serversend.LoginName, connectionID serversend.ConnectionID) (serversend.Presence, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nextEpoch++
	return serversend.Presence{LoginName: loginName, GateID: p.gateID, ConnectionID: connectionID, Epoch: p.nextEpoch}, nil
}

func (p *lifecyclePresenceFake) Release(ctx context.Context, presence serversend.Presence) error {
	p.mu.Lock()
	p.releases = append(p.releases, presence)
	releaseNumber := len(p.releases)
	p.mu.Unlock()
	p.events.add("release")
	p.releaseOnce.Do(func() { close(p.releaseStarted) })
	if releaseNumber == 1 && p.releaseContinue != nil {
		select {
		case <-p.releaseContinue:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return p.releaseErr
}

func (p *lifecyclePresenceFake) releasedSnapshot() []serversend.Presence {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]serversend.Presence(nil), p.releases...)
}

func (p *lifecyclePresenceFake) releaseCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.releases)
}
