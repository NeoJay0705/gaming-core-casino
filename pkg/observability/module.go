package observability

import (
	"context"
	"fmt"

	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/logging"
	"go.uber.org/dig"
)

// httpServerInputs 讓 observability module 可獨立使用；product 有提供
// logging.Factory 時才注入 structured logger。
type httpServerInputs struct {
	dig.In

	Config  Config
	Owner   *registryOwner
	Factory *logging.Factory `optional:"true"`
}

func newHTTPServerFromInputs(inputs httpServerInputs) (*httpServer, error) {
	return newHTTPServerWithLogger(inputs.Config, inputs.Owner, inputs.Factory)
}

// pprofServerInputs 與 HTTP server 使用相同的 optional logger 邊界。
type pprofServerInputs struct {
	dig.In

	Config  Config
	Factory *logging.Factory `optional:"true"`
}

func newPprofServerFromInputs(inputs pprofServerInputs) (*pprofServer, error) {
	return newPprofServerWithLogger(inputs.Config, inputs.Factory)
}

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
	if err := r.ProvideManaged("observability-http", framework.PhaseInfrastructure, newHTTPServerFromInputs); err != nil {
		return err
	}
	if err := r.ProvideManaged("observability-pprof", framework.PhaseInfrastructure, newPprofServerFromInputs); err != nil {
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
