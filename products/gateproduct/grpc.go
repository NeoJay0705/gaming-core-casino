package gateproduct

import (
	"fmt"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
)

// WebSocketChannel owns player commands received by the Gate WebSocket
// transport. It is separate from Game's direct Gate request channel.
const WebSocketChannel dispatcher.Channel = "gate-websocket"

func newGateGameGRPCClient(cfg gatelink.ClientConfig) (*gatelink.Client, error) {
	return gatelink.NewClient(cfg)
}

func gateGameGRPCConfig(snapshot config.SourceSnapshot) (gatelink.ClientConfig, error) {
	if snapshot == nil {
		return gatelink.ClientConfig{}, fmt.Errorf("gate gRPC: config snapshot is nil")
	}
	if !snapshot.Has("gate_to_game") {
		return gatelink.ClientConfig{}, fmt.Errorf("gate gRPC: gate_to_game is required")
	}
	var cfg gatelink.ClientConfig
	if err := snapshot.Bind("gate_to_game", &cfg, config.Strict()); err != nil {
		return gatelink.ClientConfig{}, fmt.Errorf("gate gRPC: bind config: %w", err)
	}
	return cfg, nil
}
