package serversend

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// pipelinedRouteStore 由 framework Redis adapter 實作。與 single-key store
// 分開可讓既有 lifecycle operation 維持窄 contract，同時讓 batch routing
// 每個 lookup phase 只使用一次 Redis round trip。
type pipelinedRouteStore interface {
	Pipelined(context.Context, func(redis.Pipeliner) error) ([]redis.Cmder, error)
}

// ResolveMany 在一個 pipeline 讀取所有指定玩家 ownership record。結果 map
// 保留可解析的 owner，遺失與 malformed record 以 typed validation error 回報；
// Redis operation error 轉為 ErrRouteStoreUnavailable，不符合 player-send
// fallback 條件的資料錯誤不會被偽裝成 store failure。
func (r *RedisPresenceResolver) ResolveMany(ctx context.Context, loginNames []LoginName) (map[LoginName]Presence, error) {
	if r == nil || r.store == nil {
		return nil, fmt.Errorf("%w: presence resolver is not configured", ErrRouteStoreUnavailable)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store, ok := r.store.(pipelinedRouteStore)
	if !ok {
		return nil, fmt.Errorf("%w: presence store does not support pipelining", ErrRouteStoreUnavailable)
	}
	unique := uniqueLoginNames(loginNames)
	commands := make(map[LoginName]*redis.MapStringStringCmd, len(unique))
	_, execErr := store.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, loginName := range unique {
			commands[loginName] = pipe.HGetAll(ctx, r.keys.presence(loginName))
		}
		return nil
	})
	if execErr != nil && !errors.Is(execErr, redis.Nil) {
		return nil, routeStoreError("resolve player presence batch", execErr)
	}

	result := make(map[LoginName]Presence, len(commands))
	var errs []error
	for _, loginName := range unique {
		values, err := commands[loginName].Result()
		if errors.Is(err, redis.Nil) {
			errs = append(errs, fmt.Errorf("%w: %q", ErrPresenceNotFound, loginName))
			continue
		}
		if err != nil {
			errs = append(errs, routeStoreError(fmt.Sprintf("resolve player presence %q", loginName), err))
			continue
		}
		if len(values) == 0 {
			errs = append(errs, fmt.Errorf("%w: %q", ErrPresenceNotFound, loginName))
			continue
		}
		presence, err := parsePresence(loginName, values)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		result[loginName] = presence
	}
	return result, errors.Join(errs...)
}

func uniqueLoginNames(loginNames []LoginName) []LoginName {
	seen := make(map[LoginName]struct{}, len(loginNames))
	unique := make([]LoginName, 0, len(loginNames))
	for _, loginName := range loginNames {
		if _, exists := seen[loginName]; exists {
			continue
		}
		seen[loginName] = struct{}{}
		unique = append(unique, loginName)
	}
	return unique
}

func parsePresence(loginName LoginName, values map[string]string) (Presence, error) {
	epoch, err := parsePresenceEpoch(values[presenceEpochField])
	if err != nil || epoch == 0 || values[presenceGateIDField] == "" || values[presenceConnectionIDField] == "" {
		return Presence{}, fmt.Errorf("%w: malformed presence for %q", ErrDestinationInvalid, loginName)
	}
	return Presence{
		LoginName:    loginName,
		GateID:       GateID(values[presenceGateIDField]),
		ConnectionID: ConnectionID(values[presenceConnectionIDField]),
		Epoch:        epoch,
	}, nil
}

// ResolveMany 在一個 pipeline 讀取每個不重複的 Gate endpoint。結果 map
// 保留可解析的 endpoint，遺失或 malformed record 以不可 fallback 的 typed
// error 返回。
func (d *RedisGateDirectory) ResolveMany(ctx context.Context, gateIDs []GateID) (map[GateID]GateEndpoint, error) {
	if d == nil || d.store == nil {
		return nil, fmt.Errorf("%w: Gate directory is not configured", ErrRouteStoreUnavailable)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store, ok := d.store.(pipelinedRouteStore)
	if !ok {
		return nil, fmt.Errorf("%w: Gate endpoint store does not support pipelining", ErrRouteStoreUnavailable)
	}
	unique := uniqueGateIDs(gateIDs)
	commands := make(map[GateID]*redis.StringCmd, len(unique))
	_, execErr := store.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, gateID := range unique {
			commands[gateID] = pipe.Get(ctx, d.keys.gateEndpoint(gateID))
		}
		return nil
	})
	if execErr != nil && !errors.Is(execErr, redis.Nil) {
		return nil, routeStoreError("resolve Gate endpoint batch", execErr)
	}

	result := make(map[GateID]GateEndpoint, len(commands))
	var errs []error
	for _, gateID := range unique {
		address, err := commands[gateID].Result()
		if errors.Is(err, redis.Nil) {
			errs = append(errs, fmt.Errorf("%w: %q", ErrGateEndpointNotFound, gateID))
			continue
		}
		if err != nil {
			errs = append(errs, routeStoreError(fmt.Sprintf("resolve Gate endpoint %q", gateID), err))
			continue
		}
		endpoint, err := (GateEndpoint{GateID: gateID, Address: address}).validated()
		if err != nil {
			errs = append(errs, fmt.Errorf("%w: validate Gate endpoint %q: %v", ErrDestinationInvalid, gateID, err))
			continue
		}
		result[gateID] = endpoint
	}
	return result, errors.Join(errs...)
}

func uniqueGateIDs(gateIDs []GateID) []GateID {
	seen := make(map[GateID]struct{}, len(gateIDs))
	unique := make([]GateID, 0, len(gateIDs))
	for _, gateID := range gateIDs {
		if _, exists := seen[gateID]; exists {
			continue
		}
		seen[gateID] = struct{}{}
		unique = append(unique, gateID)
	}
	return unique
}
