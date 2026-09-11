package serversend

import (
	"encoding/base64"
	"fmt"

	redisinfra "github.com/NeoJay0705/gaming-core-casino/pkg/infra/redis"
)

// Keyspace centralizes all Redis key and channel names used by Server Send.
// Prefix must come from deployment configuration so independent deployments
// never share presence or broadcast traffic accidentally.
type Keyspace struct{ prefix string }

// NewKeyspace appends the fixed Server Send namespace to the validated Redis
// infrastructure prefix. Prefix validation belongs to the Redis package.
func NewKeyspace(prefix redisinfra.KeyPrefix) (Keyspace, error) {
	if prefix == "" {
		return Keyspace{}, fmt.Errorf("%w: Redis key prefix is required", ErrDestinationInvalid)
	}
	return Keyspace{prefix: string(prefix) + ":server-send"}, nil
}

// Prefix returns the configured immutable namespace prefix.
func (k Keyspace) Prefix() string { return k.prefix }

func (k Keyspace) presence(loginName LoginName) string {
	return k.prefix + ":presence:" + opaqueKeyPart(string(loginName))
}

func (k Keyspace) gateEndpoint(gateID GateID) string {
	return k.prefix + ":gate:" + opaqueKeyPart(string(gateID))
}

// broadcastChannel is shared by every Gate subscriber. Room routing belongs to
// the registered command handler, so transport never creates per-room Redis
// channels.
func (k Keyspace) broadcastChannel() string {
	return k.prefix + ":broadcast"
}

func opaqueKeyPart(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}
