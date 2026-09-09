package dispatcher

import (
	"context"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
)

func TestRegisterModuleConfiguresSharedDispatcherBeforeHooksResolve(t *testing.T) {
	called := false
	_, err := framework.New(
		Module,
		Register(Registration{
			Channel:   "gate-request",
			CommandID: 7,
			Handler: func(context.Context, []byte) error {
				called = true
				return nil
			},
		}),
		func(r framework.Registry) error {
			return r.AddHook(func(value *Dispatcher) (framework.Hook, error) {
				handled, err := value.Dispatch(context.Background(), "gate-request", 7, []byte("opaque"))
				if err != nil {
					return framework.Hook{}, err
				}
				if !handled || !called {
					return framework.Hook{}, ErrRegistrationInvalid
				}
				return framework.Hook{Name: "readiness", Phase: framework.PhaseReadiness, OnStart: func(context.Context) error { return nil }}, nil
			})
		},
	)
	if err != nil {
		t.Fatalf("framework.New() error = %v", err)
	}
}
