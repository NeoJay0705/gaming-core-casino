// Package redisstore adapts the framework Redis resource for Server Send.
// It intentionally lives outside the serversend core package so delivery
// contracts do not depend on one concrete infrastructure implementation.
package redisstore

import (
	"context"
	"errors"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/redis"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	clientredis "github.com/redis/go-redis/v9"
)

// Store adapts the framework-managed Redis pool to the narrow Server Send
// stores. It resolves the raw client per operation, so construction remains
// lazy until the framework starts the Redis resource.
type Store struct{ client *redis.Client }

func New(client *redis.Client) *Store { return &Store{client: client} }

func (s *Store) Client() (clientredis.UniversalClient, error) {
	if s == nil || s.client == nil {
		return nil, errors.New("server send: infrastructure Redis client is nil")
	}
	return s.client.Client()
}

func (s *Store) Eval(ctx context.Context, script string, keys []string, args ...any) *clientredis.Cmd {
	client, err := s.Client()
	if err != nil {
		return clientredis.NewCmdResult(nil, err)
	}
	return client.Eval(ctx, script, keys, args...)
}

func (s *Store) HGetAll(ctx context.Context, key string) *clientredis.MapStringStringCmd {
	client, err := s.Client()
	if err != nil {
		return clientredis.NewMapStringStringResult(nil, err)
	}
	return client.HGetAll(ctx, key)
}

func (s *Store) Set(ctx context.Context, key string, value any, ttl time.Duration) *clientredis.StatusCmd {
	client, err := s.Client()
	if err != nil {
		return clientredis.NewStatusResult("", err)
	}
	return client.Set(ctx, key, value, ttl)
}

func (s *Store) Get(ctx context.Context, key string) *clientredis.StringCmd {
	client, err := s.Client()
	if err != nil {
		return clientredis.NewStringResult("", err)
	}
	return client.Get(ctx, key)
}

func (s *Store) Del(ctx context.Context, keys ...string) *clientredis.IntCmd {
	client, err := s.Client()
	if err != nil {
		return clientredis.NewIntResult(0, err)
	}
	return client.Del(ctx, keys...)
}

func (s *Store) Publish(ctx context.Context, channel string, message any) *clientredis.IntCmd {
	client, err := s.Client()
	if err != nil {
		return clientredis.NewIntResult(0, err)
	}
	return client.Publish(ctx, channel, message)
}

var _ serversend.PresenceStore = (*Store)(nil)
var _ serversend.GateEndpointStore = (*Store)(nil)
var _ serversend.RoomPublisher = (*Store)(nil)
