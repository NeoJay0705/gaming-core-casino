package gameproduct

import (
	"context"
	"fmt"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"go.uber.org/dig"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GateRequestChannel owns Game commands received directly from Gate.
const GateRequestChannel dispatcher.Channel = "gate-request"

type gameGateGRPCInputs struct {
	dig.In

	Config     gatelink.ServerConfig
	Dispatcher *dispatcher.Dispatcher
	Metrics    *gameMetrics
}

func newGameGateGRPCServer(inputs gameGateGRPCInputs) (*gatelink.Server, error) {
	return gatelink.NewServer(inputs.Config, gatelink.RequestHandlerFunc(func(ctx context.Context, request gatelink.Request) error {
		start := time.Now()
		command := gameCommandLabel(inputs.Dispatcher, request.CommandID)
		result := "error"
		if inputs.Metrics != nil {
			inputs.Metrics.gateCommandsInFlight.Inc()
		}
		defer func() {
			if inputs.Metrics != nil {
				inputs.Metrics.gateCommandsInFlight.Dec()
				inputs.Metrics.observeGateCommand(command, result, time.Since(start))
			}
		}()
		handled, err := inputs.Dispatcher.Dispatch(ctx, GateRequestChannel, dispatcher.CommandID(request.CommandID), request.Payload)
		if err != nil {
			return err
		}
		if !handled {
			return status.Errorf(codes.Unimplemented, "gate command %d is not registered", request.CommandID)
		}
		result = "success"
		return nil
	}))
}

func gameGateGRPCConfig(snapshot config.SourceSnapshot) (gatelink.ServerConfig, error) {
	if snapshot == nil {
		return gatelink.ServerConfig{}, fmt.Errorf("game gRPC: config snapshot is nil")
	}
	if !snapshot.Has("gate_to_game") {
		return gatelink.ServerConfig{}, fmt.Errorf("game gRPC: gate_to_game is required")
	}
	var cfg gatelink.ServerConfig
	if err := snapshot.Bind("gate_to_game", &cfg, config.Strict()); err != nil {
		return gatelink.ServerConfig{}, fmt.Errorf("game gRPC: bind config: %w", err)
	}
	return cfg, nil
}

func newGameGateGRPCHook(server *gatelink.Server) framework.Hook {
	return framework.Hook{Name: "game-gate-grpc", Phase: framework.PhaseIngress, OnStart: server.Start, OnStop: server.Stop}
}
