package serversend

import (
	"context"
	"fmt"
)

// GatePresenceRegistry binds one Gate identity to the generic distributed
// presence registry, so login handling cannot accidentally claim another Gate.
type GatePresenceRegistry struct {
	presence *PresenceRegistry
	gateID   GateID
}

func NewGatePresenceRegistry(presence *PresenceRegistry, gateID GateID) (*GatePresenceRegistry, error) {
	if presence == nil {
		return nil, fmt.Errorf("%w: presence registry is required", ErrRouteStoreUnavailable)
	}
	if gateID == "" {
		return nil, fmt.Errorf("%w: Gate id is required", ErrDestinationInvalid)
	}
	return &GatePresenceRegistry{presence: presence, gateID: gateID}, nil
}

func (r *GatePresenceRegistry) Claim(ctx context.Context, loginName LoginName, connectionID ConnectionID) (Presence, error) {
	if r == nil || r.presence == nil {
		return Presence{}, fmt.Errorf("%w: Gate presence registry is not configured", ErrRouteStoreUnavailable)
	}
	return r.presence.Claim(ctx, Presence{LoginName: loginName, GateID: r.gateID, ConnectionID: connectionID})
}

func (r *GatePresenceRegistry) Renew(ctx context.Context, presence Presence) error {
	if r == nil || r.presence == nil {
		return fmt.Errorf("%w: Gate presence registry is not configured", ErrRouteStoreUnavailable)
	}
	if presence.GateID != r.gateID {
		return fmt.Errorf("%w: lease Gate id %q does not match %q", ErrPresenceNotOwner, presence.GateID, r.gateID)
	}
	return r.presence.Renew(ctx, presence)
}

func (r *GatePresenceRegistry) Release(ctx context.Context, presence Presence) error {
	if r == nil || r.presence == nil {
		return fmt.Errorf("%w: Gate presence registry is not configured", ErrRouteStoreUnavailable)
	}
	if presence.GateID != r.gateID {
		return fmt.Errorf("%w: lease Gate id %q does not match %q", ErrPresenceNotOwner, presence.GateID, r.gateID)
	}
	return r.presence.Release(ctx, presence)
}
