package serversend

import (
	"context"
	"errors"
	"testing"
)

func TestGatePresenceRegistryContractBindsClaimsToItsGate(t *testing.T) {
	presence := newTestPresenceRegistry(t)
	registry, err := NewGatePresenceRegistry(presence, "gate-a")
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := registry.Claim(context.Background(), "alice", "connection-1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.GateID != "gate-a" {
		t.Fatalf("claimed Gate id = %q, want gate-a", claimed.GateID)
	}
	if err := registry.Release(context.Background(), Presence{LoginName: "alice", GateID: "gate-b", ConnectionID: "connection-1", Epoch: claimed.Epoch}); !errors.Is(err, ErrPresenceNotOwner) {
		t.Fatalf("foreign Gate release error = %v, want ErrPresenceNotOwner", err)
	}
	if _, err := presence.Resolve(context.Background(), "alice"); err != nil {
		t.Fatalf("foreign Gate release removed lease: %v", err)
	}
}
