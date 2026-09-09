package framework

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type configuredDependency struct{ value string }

func TestConfigureInjectsDependenciesBeforeHooksResolve(t *testing.T) {
	app, err := New(func(r Registry) error {
		if err := r.Provide(func() *configuredDependency { return &configuredDependency{} }); err != nil {
			return err
		}
		if err := r.Configure(func(dependency *configuredDependency) {
			dependency.value = "configured"
		}); err != nil {
			return err
		}
		return r.AddHook(func(dependency *configuredDependency) (Hook, error) {
			if dependency.value != "configured" {
				return Hook{}, errors.New("hook resolved before configuration")
			}
			return Hook{Name: "readiness", Phase: PhaseReadiness, OnStart: func(context.Context) error { return nil }}, nil
		})
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
}

func TestConfigureRejectsInvalidCallbacksAndPropagatesErrors(t *testing.T) {
	var nilCallback func()
	_, err := New(func(r Registry) error { return r.Configure(nilCallback) })
	if err == nil || !strings.Contains(err.Error(), "configure callback must be a non-nil function") {
		t.Fatalf("typed nil Configure error = %v", err)
	}

	_, err = New(func(r Registry) error {
		if err := r.Configure(func() string { return "invalid" }); err != nil {
			return err
		}
		return r.AddHook(func() Hook {
			return Hook{Name: "readiness", Phase: PhaseReadiness, OnStart: func(context.Context) error { return nil }}
		})
	})
	if err == nil || !strings.Contains(err.Error(), "configure callback must return nothing or error") {
		t.Fatalf("invalid Configure output error = %v", err)
	}

	configureErr := errors.New("registration failed")
	_, err = New(func(r Registry) error {
		if err := r.Configure(func() error { return configureErr }); err != nil {
			return err
		}
		return r.AddHook(func() Hook {
			return Hook{Name: "readiness", Phase: PhaseReadiness, OnStart: func(context.Context) error { return nil }}
		})
	})
	if !errors.Is(err, configureErr) {
		t.Fatalf("Configure error = %v, want %v", err, configureErr)
	}
}
