package serversend

import (
	"context"
	"fmt"
)

// GatePresenceRegistry 將單一 Gate identity 綁定到 distributed presence registry，
// 避免 login 流程誤 claim 其他 Gate。
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

// RenewMany 將批次續租限制在本 process 的 Gate identity，並保留輸入順序以分類
// partial pipeline result。
func (r *GatePresenceRegistry) RenewMany(ctx context.Context, presences []Presence) []PresenceRenewResult {
	results := make([]PresenceRenewResult, len(presences))
	if r == nil || r.presence == nil {
		err := fmt.Errorf("%w: Gate presence registry is not configured", ErrRouteStoreUnavailable)
		for index, presence := range presences {
			results[index] = PresenceRenewResult{Presence: presence, Err: err}
		}
		return results
	}
	validIndexes := make([]int, 0, len(presences))
	validPresences := make([]Presence, 0, len(presences))
	for index, presence := range presences {
		results[index].Presence = presence
		if presence.GateID == "" {
			results[index].Err = fmt.Errorf("%w: Gate id is required", ErrDestinationInvalid)
			continue
		}
		if presence.GateID != r.gateID {
			results[index].Err = fmt.Errorf("%w: lease Gate id %q does not match %q", ErrPresenceNotOwner, presence.GateID, r.gateID)
			continue
		}
		validIndexes = append(validIndexes, index)
		validPresences = append(validPresences, presence)
	}
	if len(validPresences) == 0 {
		return results
	}
	batchResults := r.presence.RenewMany(ctx, validPresences)
	for index, result := range batchResults {
		if index >= len(validIndexes) {
			break
		}
		results[validIndexes[index]] = result
	}
	if len(batchResults) < len(validIndexes) {
		err := fmt.Errorf("%w: batch renewal returned %d results for %d leases", ErrRouteStoreUnavailable, len(batchResults), len(validIndexes))
		for _, index := range validIndexes[len(batchResults):] {
			results[index].Err = err
		}
	}
	return results
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
