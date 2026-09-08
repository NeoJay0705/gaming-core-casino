// Package gmsproduct defines the GMS application boundary.
package gmsproduct

import (
	"context"
	"fmt"

	"github.com/NeoJay0705/gaming-core-casino/internal/appbootstrap"
	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra"
)

type AppOptions struct {
	Config    config.ConfigInputs
	EnvPrefix string
}
type App struct{ frameworkApp *framework.App }

func NewApp(ctx context.Context, options AppOptions, productModules ...framework.Module) (*App, error) {
	snapshot, err := appbootstrap.Load(ctx, options.Config, options.EnvPrefix)
	if err != nil {
		return nil, fmt.Errorf("gms app: %w", err)
	}
	modules := append([]framework.Module{moduleWithSnapshot(snapshot)}, productModules...)
	frameworkApp, err := framework.New(modules...)
	if err != nil {
		return nil, fmt.Errorf("gms app: build framework app: %w", err)
	}
	return &App{frameworkApp: frameworkApp}, nil
}

func (a *App) Run(ctx context.Context) error {
	if a == nil || a.frameworkApp == nil {
		return fmt.Errorf("gms app: nil app")
	}
	return a.frameworkApp.Run(ctx)
}

// moduleWithSnapshot intentionally contains no GMS business graph.
func moduleWithSnapshot(snapshot config.SourceSnapshot) framework.Module {
	return func(r framework.Registry) error {
		if snapshot == nil || framework.IsNilDependency(snapshot) {
			return fmt.Errorf("gms config snapshot is nil")
		}
		if err := r.Provide(func() config.SourceSnapshot { return snapshot }); err != nil {
			return err
		}
		if err := r.Provide(func(source config.SourceSnapshot) config.Snapshot { return source }); err != nil {
			return err
		}
		if err := infra.Module(r); err != nil {
			return err
		}
		return r.AddHook(noopReadinessHook)
	}
}
func noopReadinessHook() framework.Hook {
	return framework.Hook{Name: "readiness", Phase: framework.PhaseReadiness, OnStart: func(context.Context) error { return nil }, OnStop: func(context.Context) error { return nil }}
}
