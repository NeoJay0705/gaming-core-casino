package observability

import (
	"context"
	"fmt"

	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
)

// Module 註冊 App-local Prometheus registry、Observability HTTP server 與
// framework 唯一的 readiness hook。
func Module(r framework.Registry) error {
	if r == nil || framework.IsNilDependency(r) {
		return fmt.Errorf("observability registry is nil")
	}
	if err := r.Provide(newConfig); err != nil {
		return err
	}
	if err := r.Provide(newRegistryOwner); err != nil {
		return err
	}
	if err := r.Provide(newRegisterer); err != nil {
		return err
	}
	if err := r.ProvideManaged("observability-http", framework.PhaseInfrastructure, newHTTPServer); err != nil {
		return err
	}
	if err := r.ProvideManaged("observability-pprof", framework.PhaseInfrastructure, newPprofServer); err != nil {
		return err
	}
	return r.AddHook(newReadinessHook)
}

func newReadinessHook(server *httpServer, _ *pprofServer) framework.Hook {
	return framework.Hook{
		Name:  "readiness",
		Phase: framework.PhaseReadiness,
		OnStart: func(context.Context) error {
			server.setReady(true)
			return nil
		},
		OnStop: func(context.Context) error {
			server.setReady(false)
			return nil
		},
	}
}
