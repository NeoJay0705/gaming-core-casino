// Package gameproduct defines the GameSvr application boundary.
package gameproduct

import (
	"context"
	"fmt"

	"github.com/NeoJay0705/gaming-core-casino/internal/appbootstrap"
	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra"
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

// moduleWithSnapshot registers only the shallow SDK contract; Game business
// resources, handlers, consumers, and transports remain out of this move.
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
		grpcConfig, err := gameGateGRPCConfig(snapshot)
		if err != nil {
			return err
		}
		if err := r.Provide(func() gatelink.ServerConfig { return grpcConfig }); err != nil {
			return err
		}
		if err := dispatcher.Module(r); err != nil {
			return err
		}
		if err := infra.Module(r); err != nil {
			return err
		}
		if err := r.Provide(newGameGateGRPCServer); err != nil {
			return err
		}
		if err := r.AddHook(newGameGateGRPCHook); err != nil {
			return err
		}
		return r.AddHook(noopReadinessHook)
	}
}
func noopReadinessHook() framework.Hook {
	return framework.Hook{Name: "readiness", Phase: framework.PhaseReadiness, OnStart: func(context.Context) error { return nil }, OnStop: func(context.Context) error { return nil }}
}
