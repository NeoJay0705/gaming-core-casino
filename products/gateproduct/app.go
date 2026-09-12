// Package gateproduct defines the GateSvr application boundary.
package gateproduct

import (
	"context"
	"fmt"

	"github.com/NeoJay0705/gaming-core-casino/internal/appbootstrap"
	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"github.com/NeoJay0705/gaming-core-casino/pkg/grpcserver"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra"
	"github.com/NeoJay0705/gaming-core-casino/pkg/logging"
	"github.com/NeoJay0705/gaming-core-casino/pkg/observability"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
)

type AppOptions struct {
	Config    config.ConfigInputs
	EnvPrefix string
}
type App struct{ frameworkApp *framework.App }

func NewApp(ctx context.Context, options AppOptions, productModules ...framework.Module) (*App, error) {
	snapshot, err := appbootstrap.Load(ctx, options.Config, options.EnvPrefix)
	if err != nil {
		return nil, fmt.Errorf("gate app: %w", err)
	}
	modules := append([]framework.Module{moduleWithSnapshot(snapshot)}, productModules...)
	frameworkApp, err := framework.New(modules...)
	if err != nil {
		return nil, fmt.Errorf("gate app: build framework app: %w", err)
	}
	return &App{frameworkApp: frameworkApp}, nil
}

func (a *App) Run(ctx context.Context) error {
	if a == nil || a.frameworkApp == nil {
		return fmt.Errorf("gate app: nil app")
	}
	return a.frameworkApp.Run(ctx)
}

// moduleWithSnapshot wires shared infrastructure, distributed ownership,
// delivery lifecycles, and the generic Gate gRPC ingress; business handlers
// remain product modules.
func moduleWithSnapshot(snapshot config.SourceSnapshot) framework.Module {
	return func(r framework.Registry) error {
		if snapshot == nil || framework.IsNilDependency(snapshot) {
			return fmt.Errorf("gate config snapshot is nil")
		}
		if err := r.Provide(func() config.SourceSnapshot { return snapshot }); err != nil {
			return err
		}
		if err := r.Provide(func(source config.SourceSnapshot) config.Snapshot { return source }); err != nil {
			return err
		}
		if err := logging.Module(r, "gate"); err != nil {
			return err
		}
		if err := observability.Module(r); err != nil {
			return err
		}
		if err := r.Provide(newGateMetrics); err != nil {
			return err
		}
		serverConfig, err := gateGRPCServerConfig(snapshot)
		if err != nil {
			return err
		}
		if err := r.Provide(func() grpcserver.Config { return serverConfig }); err != nil {
			return err
		}
		clientConfig, err := gateGameGRPCConfig(snapshot)
		if err != nil {
			return err
		}
		if err := r.Provide(func() gatelink.ClientConfig { return clientConfig }); err != nil {
			return err
		}
		gateTransportConfig, gateFanout, gateClientEnabled, err := gateGateGRPCClient(snapshot)
		if err != nil {
			return err
		}
		broadcastConfig, broadcastConfigured, err := gateServerSendBroadcast(snapshot)
		if err != nil {
			return err
		}
		broadcastEnabled := broadcastConfigured
		if broadcastEnabled && !gateClientEnabled {
			return fmt.Errorf("gate broadcast: grpc.clients.gate is required")
		}
		if err := dispatcher.Module(r); err != nil {
			return err
		}
		if err := infra.Module(r); err != nil {
			return err
		}
		ownershipConfig, err := gateSessionOwnershipConfig(snapshot)
		if err != nil {
			return err
		}
		if err := r.Provide(func() serversend.PresenceConfig { return ownershipConfig }); err != nil {
			return err
		}
		if err := r.Provide(newServerSendKeyspace); err != nil {
			return err
		}
		if err := r.Provide(newGateIdentity); err != nil {
			return err
		}
		if err := r.Provide(newGatePresenceRegistry); err != nil {
			return err
		}
		if err := r.ProvideManaged("gate-session-ownership-renewal", framework.PhaseService, newGateSessionPresenceRenewalScheduler); err != nil {
			return err
		}
		if err := r.Provide(newGateSessionRegistry); err != nil {
			return err
		}
		if err := r.Provide(newGateDeliveryService); err != nil {
			return err
		}
		if err := r.Configure(registerGatePlayerDeliveryCommand); err != nil {
			return err
		}
		if err := r.ProvideManaged("gate-grpc-server", framework.PhaseIngress, newGateGRPCServer); err != nil {
			return err
		}
		if err := r.Configure(registerGateDeliveryService); err != nil {
			return err
		}
		endpointConfig, err := gateGRPCServerEndpointRegistration(snapshot)
		if err != nil {
			return err
		}
		if err := r.Provide(func() serversend.EndpointRegistrarConfig { return endpointConfig }); err != nil {
			return err
		}
		if err := r.ProvideManaged("gate-grpc-endpoint-registration", framework.PhaseIngress, newGateGRPCEndpointRegistration); err != nil {
			return err
		}
		if err := r.Configure(func(*gateGRPCEndpointRegistration) error { return nil }); err != nil {
			return err
		}
		if gateClientEnabled {
			if err := r.Provide(func() serversend.TransportConfig { return gateTransportConfig }); err != nil {
				return err
			}
			if err := r.Provide(func() gateFanoutConfig { return gateFanout }); err != nil {
				return err
			}
			if err := r.ProvideManaged("gate-gate-grpc-client", framework.PhaseInfrastructure, newGateServerSendTransport); err != nil {
				return err
			}
			if err := r.Provide(newGateFanoutSender); err != nil {
				return err
			}
		}
		if broadcastEnabled {
			if err := r.Provide(func() gateServerSendBroadcastConfig { return broadcastConfig }); err != nil {
				return err
			}
			if broadcastConfig.Primary == "redis" {
				if err := r.Provide(newGateRedisBroadcastSender); err != nil {
					return err
				}
			}
			if err := r.Provide(newGateBroadcastSender); err != nil {
				return err
			}
			if broadcastConfig.Primary == "redis" {
				if err := r.ProvideManaged("gate-server-send-broadcast", framework.PhaseIngress, newGateServerSendBroadcastRuntimeWithLogger); err != nil {
					return err
				}
				if err := r.Configure(func(*gateServerSendBroadcastRuntime) error { return nil }); err != nil {
					return err
				}
			}
		}
		if err := r.ProvideManaged("gate-game-grpc-client", framework.PhaseInfrastructure, newGateGameGRPCClient); err != nil {
			return err
		}
		if err := r.Provide(newGateWebSocketServer); err != nil {
			return err
		}
		if err := r.AddHook(newGateWebSocketHook); err != nil {
			return err
		}
		return nil
	}
}
