// Package apiproduct defines the ApiSvr application boundary.
package apiproduct

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/NeoJay0705/gaming-core-casino/internal/appbootstrap"
	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
)

type AppOptions struct {
	Config                config.ConfigInputs
	EnvPrefix             string
	RequireInputIntegrity bool
}
type App struct{ frameworkApp *framework.App }

func NewApp(ctx context.Context, options AppOptions, productModules ...framework.Module) (*App, error) {
	if err := validateYAMLInputs(options.Config); err != nil {
		return nil, fmt.Errorf("api app: %w", err)
	}
	loaded, err := appbootstrap.LoadWithArtifacts(ctx, options.Config, options.EnvPrefix)
	if err != nil {
		return nil, fmt.Errorf("api app: %w", err)
	}
	if err := config.VerifyFileIntegrity(loaded.Snapshot, loaded.NamedSources, options.RequireInputIntegrity); err != nil {
		return nil, fmt.Errorf("api app: verify file integrity: %w", err)
	}
	modules := append([]framework.Module{moduleWithSnapshot(loaded.Snapshot)}, productModules...)
	frameworkApp, err := framework.New(modules...)
	if err != nil {
		return nil, fmt.Errorf("api app: build framework app: %w", err)
	}
	return &App{frameworkApp: frameworkApp}, nil
}

func (a *App) Run(ctx context.Context) error {
	if a == nil || a.frameworkApp == nil {
		return fmt.Errorf("api app: nil app")
	}
	return a.frameworkApp.Run(ctx)
}
func validateYAMLInputs(inputs config.ConfigInputs) error {
	paths := append([]string{}, inputs.MergedPaths...)
	for _, source := range inputs.SourcePaths {
		paths = append(paths, source.Path)
	}
	if len(paths) == 0 {
		return fmt.Errorf("config paths are empty")
	}
	for _, path := range paths {
		ext := strings.ToLower(filepath.Ext(strings.TrimSpace(path)))
		if ext != ".yaml" && ext != ".yml" {
			return fmt.Errorf("config path %q: ApiSvr accepts YAML/YML only", path)
		}
	}
	return nil
}

// moduleWithSnapshot intentionally contains no API business graph.
func moduleWithSnapshot(snapshot config.SourceSnapshot) framework.Module {
	return func(r framework.Registry) error {
		if snapshot == nil || framework.IsNilDependency(snapshot) {
			return fmt.Errorf("api config snapshot is nil")
		}
		if err := r.Provide(func() config.SourceSnapshot { return snapshot }); err != nil {
			return err
		}
		if err := r.Provide(func(source config.SourceSnapshot) config.Snapshot { return source }); err != nil {
			return err
		}
		return r.AddHook(noopReadinessHook)
	}
}
func noopReadinessHook() framework.Hook {
	return framework.Hook{Name: "readiness", Phase: framework.PhaseReadiness, OnStart: func(context.Context) error { return nil }, OnStop: func(context.Context) error { return nil }}
}
