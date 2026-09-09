package serversend

import (
	"errors"
	"testing"
	"time"
)

func TestConfigContractValidatesGameOwnedSettings(t *testing.T) {
	valid := Config{Broadcast: BroadcastConfig{Primary: "redis"}}
	normalized, err := valid.NormalizeForGame()
	if err != nil {
		t.Fatalf("validate game config: %v", err)
	}
	if normalized.Broadcast.Primary != "redis" || normalized.Player.Fallback != "none" || normalized.Broadcast.Fallback != "none" || normalized.RequestTimeout != DefaultRequestTimeout || normalized.MaxPayloadBytes != DefaultMaxPayloadBytes || normalized.Fanout.MaxEndpoints != DefaultMaxFanoutEndpoints {
		t.Fatalf("game defaults = %#v", normalized)
	}

	valid = Config{
		Broadcast: BroadcastConfig{Primary: "redis", Fallback: "grpc"},
		Player:    PlayerConfig{Fallback: "grpc"},
		Fanout:    FanoutSettings{GRPCTarget: "dns:///gate-server-send-headless:9091", MaxEndpoints: 12},
	}
	normalized, err = valid.NormalizeForGame()
	if err != nil {
		t.Fatalf("validate game fallback config: %v", err)
	}
	if normalized.Fanout.MaxEndpoints != 12 || normalized.Player.Fallback != "grpc" || normalized.Broadcast.Fallback != "grpc" {
		t.Fatalf("game fallback config = %#v", normalized)
	}
}

func TestConfigContractRejectsInvalidGameFallbackSettings(t *testing.T) {
	cases := []Config{
		{Player: PlayerConfig{Fallback: "http"}},
		{Player: PlayerConfig{Fallback: " grpc"}},
		{Broadcast: BroadcastConfig{Fallback: "http"}},
		{Player: PlayerConfig{Fallback: "grpc"}},
		{Broadcast: BroadcastConfig{Primary: "grpc", Fallback: "grpc"}, Fanout: FanoutSettings{GRPCTarget: "dns:///gate:9091"}},
		{Fanout: FanoutSettings{GRPCTarget: " dns:///gate:9091"}},
		{Fanout: FanoutSettings{MaxEndpoints: -1}},
		{MaxPayloadBytes: -1},
	}
	for i, value := range cases {
		if err := value.ValidateForGame(); !errors.Is(err, ErrDestinationInvalid) {
			t.Fatalf("case %d error = %v, want ErrDestinationInvalid", i, err)
		}
	}
}

func TestConfigContractRequiresFanoutTargetWhenUsed(t *testing.T) {
	for name, value := range map[string]Config{
		"grpc primary":       {Broadcast: BroadcastConfig{Primary: "grpc"}},
		"player fallback":    {Player: PlayerConfig{Fallback: "grpc"}},
		"broadcast fallback": {Broadcast: BroadcastConfig{Fallback: "grpc"}},
	} {
		if err := value.ValidateForGame(); !errors.Is(err, ErrDestinationInvalid) {
			t.Fatalf("%s error = %v, want ErrDestinationInvalid", name, err)
		}
	}
}

func TestConfigContractGateIgnoresGameOnlySettingsAndRequiresPresenceLease(t *testing.T) {
	value := Config{
		RequestTimeout: -time.Second,
		Fanout:         FanoutSettings{GRPCTarget: "not-a-host-port", MaxEndpoints: -1},
		Player:         PlayerConfig{Fallback: "unknown"},
		Broadcast: BroadcastConfig{
			Primary:  "",
			Fallback: "unknown",
		},
		Presence: PresenceConfig{LeaseTTL: time.Minute},
		Gate: GateConfig{
			ListenAddr:  "127.0.0.1:0",
			EndpointTTL: time.Minute,
		},
	}
	normalized, err := value.NormalizeForGate()
	if err != nil {
		t.Fatalf("gate validation rejected Game-only settings: %v", err)
	}
	if normalized.Broadcast.Primary != "redis" || normalized.MaxPayloadBytes != DefaultMaxPayloadBytes || normalized.Gate.EndpointRefresh != 20*time.Second {
		t.Fatalf("gate defaults = %#v", normalized)
	}

	value.Presence.LeaseTTL = 0
	if err := value.ValidateForGate(); !errors.Is(err, ErrDestinationInvalid) {
		t.Fatalf("missing presence lease error = %v, want ErrDestinationInvalid", err)
	}
}
