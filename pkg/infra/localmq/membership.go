package localmq

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	infraredis "github.com/NeoJay0705/gaming-core-casino/pkg/infra/redis"
	"github.com/redis/go-redis/v9"
)

const membershipKeySegment = "localmq/membership"

var heartbeatScript = redis.NewScript(`
local now = redis.call('TIME')
local millis = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
redis.call('ZADD', KEYS[1], millis, ARGV[1])
return millis
`)

type membership struct {
	store      membershipStore
	key        string
	instanceID string
	timeout    time.Duration
}

type membershipFactory func(topic, group, instanceID string, timeout time.Duration) (*membership, error)

func redisMembershipFactory(client *infraredis.Client) membershipFactory {
	return func(topic, group, instanceID string, timeout time.Duration) (*membership, error) {
		return newMembership(client, topic, group, instanceID, timeout)
	}
}

// membershipStore 將 Redis membership primitive 隔離成最小 package-private
// seam；production 使用 redisMembershipStore，conformance test 可注入受控
// 的 fake store，而不改變公開 API。
type membershipStore interface {
	heartbeat(context.Context, string, string) error
	activeMembers(context.Context, string, time.Duration) ([]string, error)
	remove(context.Context, string, string) error
}

type redisMembershipStore struct {
	client redis.UniversalClient
}

func (s *redisMembershipStore) heartbeat(ctx context.Context, key, instanceID string) error {
	if s == nil || s.client == nil {
		return errors.New("localmq membership Redis client is nil")
	}
	_, err := heartbeatScript.Run(ctx, s.client, []string{key}, instanceID).Int64()
	return err
}

func (s *redisMembershipStore) activeMembers(ctx context.Context, key string, timeout time.Duration) ([]string, error) {
	if s == nil || s.client == nil {
		return nil, errors.New("localmq membership Redis client is nil")
	}
	now, err := s.client.Time(ctx).Result()
	if err != nil {
		return nil, err
	}
	minimum := now.Add(-timeout).UnixMilli()
	members, err := s.client.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min: strconv.FormatInt(minimum, 10),
		Max: "+inf",
	}).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(members)
	return members, nil
}

func (s *redisMembershipStore) remove(ctx context.Context, key, instanceID string) error {
	if s == nil || s.client == nil {
		return errors.New("localmq membership Redis client is nil")
	}
	return s.client.ZRem(ctx, key, instanceID).Err()
}

func newMembership(client *infraredis.Client, topic, group, instanceID string, timeout time.Duration) (*membership, error) {
	if client == nil {
		return nil, errors.New("localmq membership Redis client is nil")
	}
	if err := validateTopicAndGroup(topic, group); err != nil {
		return nil, err
	}
	if instanceID == "" {
		return nil, errors.New("localmq membership instance id is required")
	}
	redisClient, err := client.Client()
	if err != nil {
		return nil, err
	}
	return newMembershipWithStore(&redisMembershipStore{client: redisClient}, membershipKey(client.KeyPrefix(), topic, group), instanceID, timeout)
}

func newMembershipWithStore(store membershipStore, key, instanceID string, timeout time.Duration) (*membership, error) {
	if store == nil {
		return nil, errors.New("localmq membership store is nil")
	}
	if key == "" {
		return nil, errors.New("localmq membership key is required")
	}
	if instanceID == "" {
		return nil, errors.New("localmq membership instance id is required")
	}
	if timeout <= 0 {
		return nil, errors.New("localmq membership timeout must be greater than zero")
	}
	return &membership{store: store, key: key, instanceID: instanceID, timeout: timeout}, nil
}

func membershipKey(prefix infraredis.KeyPrefix, topic, group string) string {
	return fmt.Sprintf("%s:%s:%s:%s", string(prefix), membershipKeySegment, topic, group)
}

func (m *membership) heartbeat(ctx context.Context) error {
	if m == nil || m.store == nil {
		return errors.New("localmq membership is nil")
	}
	return m.store.heartbeat(ctx, m.key, m.instanceID)
}

func (m *membership) activeMembers(ctx context.Context) ([]string, error) {
	if m == nil || m.store == nil {
		return nil, errors.New("localmq membership is nil")
	}
	return m.store.activeMembers(ctx, m.key, m.timeout)
}

func (m *membership) remove(ctx context.Context) error {
	if m == nil || m.store == nil {
		return errors.New("localmq membership is nil")
	}
	return m.store.remove(ctx, m.key, m.instanceID)
}

func rendezvousOwner(topic, laneID string, members []string) string {
	if len(members) == 0 {
		return ""
	}
	owner := ""
	var ownerScore uint64
	for _, member := range members {
		hash := sha256.Sum256([]byte(topic + "\x00" + laneID + "\x00" + member))
		score := binary.BigEndian.Uint64(hash[:8])
		if owner == "" || score > ownerScore || (score == ownerScore && member < owner) {
			owner = member
			ownerScore = score
		}
	}
	return owner
}
