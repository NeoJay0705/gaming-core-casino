// Package gameproduct defines the GameSvr application boundary.
package gameproduct

import (
	"context"
	"fmt"

	"github.com/NeoJay0705/gaming-core-casino/internal/appbootstrap"
	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/grpcserver"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra"
	"github.com/NeoJay0705/gaming-core-casino/pkg/observability"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
)

type AppOptions struct {
	Config                config.ConfigInputs
	EnvPrefix             string
	RequireInputIntegrity bool
}
type App struct{ frameworkApp *framework.App }

func NewApp(ctx context.Context, options AppOptions, productModules ...framework.Module) (*App, error) {
	loaded, err := appbootstrap.LoadWithArtifacts(ctx, options.Config, options.EnvPrefix)
	if err != nil {
		return nil, fmt.Errorf("game app: %w", err)
	}
	if err := config.VerifyFileIntegrity(loaded.Snapshot, loaded.NamedSources, options.RequireInputIntegrity); err != nil {
		return nil, fmt.Errorf("game app: verify file integrity: %w", err)
	}
	modules := append([]framework.Module{moduleWithSnapshot(loaded.Snapshot)}, productModules...)
	frameworkApp, err := framework.New(modules...)
	if err != nil {
		return nil, fmt.Errorf("game app: build framework app: %w", err)
	}
	return &App{frameworkApp: frameworkApp}, nil
}

func (a *App) Run(ctx context.Context) error {
	if a == nil || a.frameworkApp == nil {
		return fmt.Errorf("game app: nil app")
	}
	return a.frameworkApp.Run(ctx)
}

// moduleWithSnapshot wires shared product infrastructure, delivery boundaries,
// and the generic Game gRPC ingress; business handlers remain product modules.
func moduleWithSnapshot(snapshot config.SourceSnapshot) framework.Module {
	return func(r framework.Registry) error {
		if snapshot == nil || framework.IsNilDependency(snapshot) {
			return fmt.Errorf("game config snapshot is nil")
		}
		if err := r.Provide(func() config.SourceSnapshot { return snapshot }); err != nil {
			return err
		}
		if err := r.Provide(func(source config.SourceSnapshot) config.Snapshot { return source }); err != nil {
			return err
		}
		if err := observability.Module(r); err != nil {
			return err
		}
		if err := r.Provide(newGameMetrics); err != nil {
			return err
		}
		grpcConfig, err := gameGRPCServerConfig(snapshot)
		if err != nil {
			return err
		}
		if err := r.Provide(func() grpcserver.Config { return grpcConfig }); err != nil {
			return err
		}
		gateTransportConfig, gateFanout, gateClientEnabled, err := gameGRPCGateClient(snapshot)
		if err != nil {
			return err
		}
		if err := dispatcher.Module(r); err != nil {
			return err
		}
		if err := infra.Module(r); err != nil {
			return err
		}
		broadcastConfig, broadcastConfigured, err := gameServerSendBroadcast(snapshot)
		if err != nil {
			return err
		}
		broadcastEnabled := broadcastConfigured
		if broadcastEnabled && !gateClientEnabled {
			return fmt.Errorf("game broadcast: grpc.clients.gate is required")
		}
		if err := r.Provide(newGameServerSendKeyspace); err != nil {
			return err
		}
		if err := r.Provide(newGameRequestPlayerSender); err != nil {
			return err
		}
		if gateClientEnabled {
			if err := r.Provide(func() serversend.TransportConfig { return gateTransportConfig }); err != nil {
				return err
			}
			if err := r.Provide(func() gameFanoutConfig { return gateFanout }); err != nil {
				return err
			}
			if err := r.ProvideManaged("game-gate-grpc-client", framework.PhaseInfrastructure, newGameServerSendTransport); err != nil {
				return err
			}
			if err := r.Provide(newGameFanoutSender); err != nil {
				return err
			}
			if err := r.Provide(newGamePresenceResolver); err != nil {
				return err
			}
			if err := r.Provide(newGameGateDirectory); err != nil {
				return err
			}
			if err := r.Provide(newGamePlayerSender); err != nil {
				return err
			}
		}
		if broadcastEnabled {
			if err := r.Provide(func() gameServerSendBroadcastConfig { return broadcastConfig }); err != nil {
				return err
			}
			if broadcastConfig.Primary == "redis" {
				if err := r.Provide(newGameRedisBroadcastSender); err != nil {
					return err
				}
			}
			if err := r.Provide(newGameBroadcastSender); err != nil {
				return err
			}
		}
		if err := r.Provide(newGameGateGRPCService); err != nil {
			return err
		}
		if err := r.ProvideManaged("game-grpc-server", framework.PhaseIngress, newGameGRPCServer); err != nil {
			return err
		}
		if err := r.Configure(registerGameGateRequestService); err != nil {
			return err
		}
		return nil
	}
}
