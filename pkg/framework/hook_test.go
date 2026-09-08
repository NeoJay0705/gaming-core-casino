package framework

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestHookPhasesRollbackAndIdempotentStop(t *testing.T) {
	var events []string
	startErr := errors.New("ingress failed")
	stopErr := errors.New("service stop failed")
	app, err := New(func(r Registry) error {
		if err := r.AddHook(func() Hook {
			return Hook{Name: "infra", Phase: PhaseInfrastructure, OnStart: func(context.Context) error { events = append(events, "start:infra"); return nil }, OnStop: func(context.Context) error { events = append(events, "stop:infra"); return nil }}
		}); err != nil {
			return err
		}
		if err := r.AddHook(func() Hook {
			return Hook{Name: "service", Phase: PhaseService, OnStart: func(context.Context) error { events = append(events, "start:service"); return nil }, OnStop: func(context.Context) error { events = append(events, "stop:service"); return stopErr }}
		}); err != nil {
			return err
		}
		if err := r.AddHook(func() Hook {
			return Hook{Name: "ingress", Phase: PhaseIngress, OnStart: func(context.Context) error { events = append(events, "start:ingress"); return startErr }, OnStop: func(context.Context) error { events = append(events, "stop:ingress"); return nil }}
		}); err != nil {
			return err
		}
		return r.AddHook(func() Hook {
			return Hook{Name: "readiness", Phase: PhaseReadiness, OnStart: func(context.Context) error { events = append(events, "start:readiness"); return nil }, OnStop: func(context.Context) error { events = append(events, "stop:readiness"); return nil }}
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); !errors.Is(err, startErr) || !errors.Is(err, stopErr) {
		t.Fatalf("Start() error = %v", err)
	}
	want := []string{"start:infra", "start:service", "start:ingress", "stop:service", "stop:infra"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	if err := app.Stop(context.Background()); !errors.Is(err, stopErr) {
		t.Fatalf("Stop() after rollback = %v", err)
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events after Stop = %v, want unchanged", events)
	}
}

func TestHookValidation(t *testing.T) {
	cases := []struct {
		name   string
		module Module
		want   string
	}{
		{"empty", func(r Registry) error {
			return r.AddHook(func() Hook {
				return Hook{Phase: PhaseInfrastructure, OnStart: func(context.Context) error { return nil }}
			})
		}, "name is empty"},
		{"bad phase", func(r Registry) error {
			return r.AddHook(func() Hook { return Hook{Name: "x", Phase: 1, OnStart: func(context.Context) error { return nil }} })
		}, "unknown phase"},
		{"non readiness phase", func(r Registry) error {
			return r.AddHook(func() Hook {
				return Hook{Name: "x", Phase: PhaseReadiness, OnStart: func(context.Context) error { return nil }}
			})
		}, "only readiness"},
		{"missing readiness", func(r Registry) error {
			return r.AddHook(func() Hook {
				return Hook{Name: "x", Phase: PhaseService, OnStart: func(context.Context) error { return nil }}
			})
		}, "exactly one readiness"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.module); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("New() error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestAddHookRejectsTypedNilConstructor(t *testing.T) {
	var constructor func() Hook

	_, err := New(func(r Registry) error {
		return r.AddHook(constructor)
	})
	if err == nil || !strings.Contains(err.Error(), "non-nil function") {
		t.Fatalf("New() error = %v, want typed-nil constructor validation error", err)
	}
}

func TestStartRollbackUsesIndependentShutdownContext(t *testing.T) {
	startContext, cancelStart := context.WithCancel(context.Background())
	defer cancelStart()
	var rollbackContextErr error
	startErr := errors.New("readiness failed")

	app, err := New(func(r Registry) error {
		if err := r.AddHook(func() Hook {
			return Hook{
				Name:  "infrastructure",
				Phase: PhaseInfrastructure,
				OnStart: func(context.Context) error {
					cancelStart()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					rollbackContextErr = ctx.Err()
					return nil
				},
			}
		}); err != nil {
			return err
		}
		return r.AddHook(func() Hook {
			return Hook{
				Name:    "readiness",
				Phase:   PhaseReadiness,
				OnStart: func(context.Context) error { return startErr },
			}
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := app.Start(startContext); !errors.Is(err, startErr) {
		t.Fatalf("Start() error = %v, want %v", err, startErr)
	}
	if rollbackContextErr != nil {
		t.Fatalf("rollback context error = %v, want nil", rollbackContextErr)
	}
}

func TestAppIsOneShotAfterSuccessAndFailure(t *testing.T) {
	newAppWithHook := func(fail bool) *App {
		app, err := New(func(r Registry) error {
			if err := r.AddHook(func() Hook {
				return Hook{Name: "infra", Phase: PhaseInfrastructure, OnStart: func(context.Context) error {
					if fail {
						return errors.New("boom")
					}
					return nil
				}, OnStop: func(context.Context) error { return nil }}
			}); err != nil {
				return err
			}
			return r.AddHook(func() Hook {
				return Hook{Name: "readiness", Phase: PhaseReadiness, OnStart: func(context.Context) error { return nil }}
			})
		})
		if err != nil {
			t.Fatal(err)
		}
		return app
	}

	started := newAppWithHook(false)
	if err := started.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := started.Start(context.Background()); !errors.Is(err, ErrAppNotStartable) {
		t.Fatalf("duplicate Start = %v", err)
	}
	if err := started.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := started.Start(context.Background()); !errors.Is(err, ErrAppNotStartable) {
		t.Fatalf("restart = %v", err)
	}

	failed := newAppWithHook(true)
	if err := failed.Start(context.Background()); err == nil {
		t.Fatal("failing Start returned nil")
	}
	if err := failed.Start(context.Background()); !errors.Is(err, ErrAppNotStartable) {
		t.Fatalf("retry after failed Start = %v", err)
	}
}
