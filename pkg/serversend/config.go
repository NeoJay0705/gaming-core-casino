package serversend

import (
	"fmt"
	"strings"
	"time"
)

const (
	DefaultMaxPayloadBytes    = 1 << 20
	DefaultMaxFanoutEndpoints = 256
)

// Config is the union of the shared Server Send topology and the product-side
// settings. NormalizeForGame and NormalizeForGate deliberately validate only
// the fields owned by that product.
type Config struct {
	RequestTimeout  time.Duration `config:"request_timeout" yaml:"request_timeout"`
	MaxPayloadBytes int           `config:"max_payload_bytes" yaml:"max_payload_bytes"`

	Fanout    FanoutSettings  `config:"fanout" yaml:"fanout"`
	Player    PlayerConfig    `config:"player" yaml:"player"`
	Presence  PresenceConfig  `config:"presence" yaml:"presence"`
	Gate      GateConfig      `config:"gate" yaml:"gate"`
	Broadcast BroadcastConfig `config:"broadcast" yaml:"broadcast"`
}

// FanoutSettings configures the explicit all-Gate gRPC directory used by Game.
// The target must enumerate every Gate instance, such as a headless Service or
// Docker tasks DNS name, rather than a load-balanced virtual IP.
type FanoutSettings struct {
	GRPCTarget   string `config:"grpc_target" yaml:"grpc_target"`
	MaxEndpoints int    `config:"max_endpoints" yaml:"max_endpoints"`
}

// PlayerConfig controls the Game-side fallback for a routed player message.
// Empty and none both mean that no fallback is configured.
type PlayerConfig struct {
	Fallback string `config:"fallback" yaml:"fallback"`
}

// GateConfig configures the Game-to-Gate receiver owned by each Gate process.
// Gate identity and advertised host are runtime/deployment concerns, not
// application YAML fields.
type GateConfig struct {
	ListenAddr      string        `config:"listen_addr" yaml:"listen_addr"`
	EndpointTTL     time.Duration `config:"endpoint_ttl" yaml:"endpoint_ttl"`
	EndpointRefresh time.Duration `config:"endpoint_refresh" yaml:"endpoint_refresh"`
}

// BroadcastConfig selects the shared primary transport and the Game-side
// fallback policy. Primary is read by both Gate and Game; Fallback is read only
// by Game.
type BroadcastConfig struct {
	Primary  string `config:"primary" yaml:"primary"`
	Fallback string `config:"fallback" yaml:"fallback"`
}

// NormalizeForGame validates and canonicalizes values used by Game's sender
// providers. Gate-only presence and listener settings are intentionally not
// required here.
func (c Config) NormalizeForGame() (Config, error) {
	transport, err := (TransportConfig{RequestTimeout: c.RequestTimeout, MaxPayloadBytes: c.MaxPayloadBytes}).normalized()
	if err != nil {
		return Config{}, err
	}
	c.RequestTimeout = transport.RequestTimeout
	c.MaxPayloadBytes = transport.MaxPayloadBytes

	if c.Fanout.MaxEndpoints < 0 {
		return Config{}, fmt.Errorf("%w: max fan-out endpoints cannot be negative", ErrDestinationInvalid)
	}
	if c.Fanout.MaxEndpoints == 0 {
		c.Fanout.MaxEndpoints = DefaultMaxFanoutEndpoints
	}

	primary, err := broadcastPrimary(c.Broadcast.Primary)
	if err != nil {
		return Config{}, err
	}
	c.Broadcast.Primary = primary
	c.Player.Fallback, err = normalizeFallback("player fallback", c.Player.Fallback)
	if err != nil {
		return Config{}, err
	}
	c.Broadcast.Fallback, err = normalizeFallback("broadcast fallback", c.Broadcast.Fallback)
	if err != nil {
		return Config{}, err
	}
	if primary == "grpc" && c.Broadcast.Fallback == "grpc" {
		return Config{}, fmt.Errorf("%w: broadcast gRPC primary cannot fall back to gRPC", ErrDestinationInvalid)
	}

	if c.Fanout.GRPCTarget != strings.TrimSpace(c.Fanout.GRPCTarget) {
		return Config{}, fmt.Errorf("%w: gRPC fan-out target has surrounding whitespace", ErrDestinationInvalid)
	}
	needsFanout := primary == "grpc" || c.Player.Fallback == "grpc" || c.Broadcast.Fallback == "grpc"
	if needsFanout && c.Fanout.GRPCTarget == "" {
		return Config{}, fmt.Errorf("%w: gRPC fan-out target is required when gRPC delivery or fallback is configured", ErrDestinationInvalid)
	}
	if c.Fanout.GRPCTarget != "" {
		if _, err := NewDNSGateDirectory(c.Fanout.GRPCTarget, nil); err != nil {
			return Config{}, err
		}
	}
	return c, nil
}

// ValidateForGame checks the fields Game must provide for Server Send.
func (c Config) ValidateForGame() error {
	_, err := c.NormalizeForGame()
	return err
}

// NormalizeForGate validates and canonicalizes only Gate-owned settings plus
// the shared payload bound and broadcast primary selector. Game-only fallback
// and fan-out settings are deliberately ignored.
func (c Config) NormalizeForGate() (Config, error) {
	var err error
	c.MaxPayloadBytes, err = normalizedPayloadLimit(c.MaxPayloadBytes)
	if err != nil {
		return Config{}, err
	}
	if _, err := c.Presence.normalized(); err != nil {
		return Config{}, err
	}
	if c.Gate.ListenAddr, err = validateListenAddr(c.Gate.ListenAddr); err != nil {
		return Config{}, err
	}
	endpointConfig, err := (EndpointRegistrarConfig{TTL: c.Gate.EndpointTTL, Refresh: c.Gate.EndpointRefresh}).normalized()
	if err != nil {
		return Config{}, err
	}
	c.Gate.EndpointTTL = endpointConfig.TTL
	c.Gate.EndpointRefresh = endpointConfig.Refresh
	if c.Broadcast.Primary, err = broadcastPrimary(c.Broadcast.Primary); err != nil {
		return Config{}, err
	}
	return c, nil
}

// ValidateForGate checks the fields Gate must provide for its Server Send
// receiver and leases.
func (c Config) ValidateForGate() error {
	_, err := c.NormalizeForGate()
	return err
}

func normalizeFallback(field, value string) (string, error) {
	if value != strings.TrimSpace(value) {
		return "", fmt.Errorf("%w: %s has surrounding whitespace", ErrDestinationInvalid, field)
	}
	switch value {
	case "", "none":
		return "none", nil
	case "grpc":
		return "grpc", nil
	default:
		return "", fmt.Errorf("%w: %s %q is invalid", ErrDestinationInvalid, field, value)
	}
}

func broadcastPrimary(value string) (string, error) {
	if value != strings.TrimSpace(value) {
		return "", fmt.Errorf("%w: broadcast primary has surrounding whitespace", ErrDestinationInvalid)
	}
	if value == "" {
		return "redis", nil
	}
	if value != "redis" && value != "grpc" {
		return "", fmt.Errorf("%w: broadcast primary %q is invalid", ErrDestinationInvalid, value)
	}
	return value, nil
}
