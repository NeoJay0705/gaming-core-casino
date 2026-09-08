package framework

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type managedTestResource struct {
	name     string
	events   *[]string
	startErr error
	starts   int
	stops    int
}

func (r *managedTestResource) Start(context.Context) error {
	r.starts++
	*r.events = append(*r.events, "start:"+r.name)
	return r.startErr
}

func (r *managedTestResource) Stop(context.Context) error {
	r.stops++
	*r.events = append(*r.events, "stop:"+r.name)
	return nil
}

type redisManagedResource struct{ *managedTestResource }
type databaseManagedResource struct{ *managedTestResource }

func addManagedTestReadiness(r Registry) error {
	return r.AddHook(func() Hook {
		return Hook{Name: "readiness", Phase: PhaseReadiness, OnStart: func(context.Context) error { return nil }, OnStop: func(context.Context) error { return nil }}
	})
}

func TestProvideManagedUnusedResourceDoesNotEnterLifecycle(t *testing.T) {
	var events []string
	created := 0
	resource := &managedTestResource{name: "redis", events: &events}
	app, err := New(func(r Registry) error {
		if err := r.ProvideManaged("redis", PhaseInfrastructure, func() *managedTestResource {
			created++
			return resource
		}); err != nil {
			return err
		}
		return addManagedTestReadiness(r)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := app.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if created != 0 || resource.starts != 0 || resource.stops != 0 || len(events) != 0 {
		t.Fatalf("unused managed resource: created=%d starts=%d stops=%d events=%v", created, resource.starts, resource.stops, events)
	}
}

func TestProvideManagedUsedByHookStartsAndStopsInPhaseOrder(t *testing.T) {
	var events []string
	created := 0
	resource := &managedTestResource{name: "redis", events: &events}
	app, err := New(func(r Registry) error {
		if err := r.ProvideManaged("redis", PhaseInfrastructure, func() *managedTestResource {
			created++
			return resource
		}); err != nil {
			return err
		}
		if err := r.AddHook(func(*managedTestResource) Hook {
			return Hook{Name: "order-service", Phase: PhaseService, OnStart: func(context.Context) error { events = append(events, "start:order-service"); return nil }, OnStop: func(context.Context) error { events = append(events, "stop:order-service"); return nil }}
		}); err != nil {
			return err
		}
		return addManagedTestReadiness(r)
	})
	if err != nil {
		t.Fatal(err)
	}
	if created != 1 {
		t.Fatalf("factory calls after New = %d, want 1", created)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := app.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"start:redis", "start:order-service", "stop:order-service", "stop:redis"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	if resource.starts != 1 || resource.stops != 1 {
		t.Fatalf("managed lifecycle calls: starts=%d stops=%d", resource.starts, resource.stops)
	}
}

func TestProvideManagedSharedByHooksStartsAndStopsOnce(t *testing.T) {
	var events []string
	created := 0
	resource := &managedTestResource{name: "redis", events: &events}
	app, err := New(func(r Registry) error {
		if err := r.ProvideManaged("redis", PhaseInfrastructure, func() *managedTestResource {
			created++
			return resource
		}); err != nil {
			return err
		}
		for _, name := range []string{"service-a", "service-b"} {
			hookName := name
			if err := r.AddHook(func(*managedTestResource) Hook {
				return Hook{Name: hookName, Phase: PhaseService, OnStart: func(context.Context) error { events = append(events, "start:"+hookName); return nil }, OnStop: func(context.Context) error { events = append(events, "stop:"+hookName); return nil }}
			}); err != nil {
				return err
			}
		}
		return addManagedTestReadiness(r)
	})
	if err != nil {
		t.Fatal(err)
	}
	if created != 1 {
		t.Fatalf("factory calls after New = %d, want 1", created)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := app.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"start:redis", "start:service-a", "start:service-b", "stop:service-b", "stop:service-a", "stop:redis"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	if resource.starts != 1 || resource.stops != 1 {
		t.Fatalf("shared managed lifecycle calls: starts=%d stops=%d", resource.starts, resource.stops)
	}
}

func TestProvideManagedSharesRegistrationOrderWithHooks(t *testing.T) {
	var events []string
	redis := &redisManagedResource{&managedTestResource{name: "redis", events: &events}}
	database := &databaseManagedResource{&managedTestResource{name: "database", events: &events}}
	app, err := New(func(r Registry) error {
		if err := r.ProvideManaged("redis", PhaseInfrastructure, func() *redisManagedResource { return redis }); err != nil {
			return err
		}
		if err := r.AddHook(func(*redisManagedResource) Hook {
			return Hook{Name: "infra-hook", Phase: PhaseInfrastructure, OnStart: func(context.Context) error { events = append(events, "start:infra-hook"); return nil }, OnStop: func(context.Context) error { events = append(events, "stop:infra-hook"); return nil }}
		}); err != nil {
			return err
		}
		if err := r.ProvideManaged("database", PhaseInfrastructure, func() *databaseManagedResource { return database }); err != nil {
			return err
		}
		if err := r.AddHook(func(*databaseManagedResource) Hook {
			return Hook{Name: "database-hook", Phase: PhaseInfrastructure, OnStart: func(context.Context) error { events = append(events, "start:database-hook"); return nil }, OnStop: func(context.Context) error { events = append(events, "stop:database-hook"); return nil }}
		}); err != nil {
			return err
		}
		return addManagedTestReadiness(r)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := app.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"start:redis", "start:infra-hook", "start:database", "start:database-hook", "stop:database-hook", "stop:database", "stop:infra-hook", "stop:redis"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestProvideManagedTypedNilResourceFailsDuringHookResolution(t *testing.T) {
	_, err := New(func(r Registry) error {
		if err := r.ProvideManaged("redis", PhaseInfrastructure, func() *managedTestResource { return nil }); err != nil {
			return err
		}
		if err := r.AddHook(func(*managedTestResource) Hook {
			return Hook{Name: "consumer", Phase: PhaseService, OnStart: func(context.Context) error { return nil }}
		}); err != nil {
			return err
		}
		return addManagedTestReadiness(r)
	})
	if err == nil || !strings.Contains(err.Error(), "constructor returned nil") {
		t.Fatalf("New() error = %v, want typed-nil managed resource error", err)
	}
}

func TestProvideDoesNotManageLifecycleResource(t *testing.T) {
	var events []string
	resource := &managedTestResource{name: "redis", events: &events}
	app, err := New(func(r Registry) error {
		if err := r.Provide(func() *managedTestResource { return resource }); err != nil {
			return err
		}
		if err := r.AddHook(func(*managedTestResource) Hook {
			return Hook{Name: "consumer", Phase: PhaseService, OnStart: func(context.Context) error { return nil }, OnStop: func(context.Context) error { return nil }}
		}); err != nil {
			return err
		}
		return addManagedTestReadiness(r)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := app.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if resource.starts != 0 || resource.stops != 0 || len(events) != 0 {
		t.Fatalf("ordinary provider joined lifecycle: starts=%d stops=%d events=%v", resource.starts, resource.stops, events)
	}
}

func TestProvideManagedStartFailureRollsBackEarlierResources(t *testing.T) {
	var events []string
	startErr := errors.New("database unavailable")
	redis := &redisManagedResource{&managedTestResource{name: "redis", events: &events}}
	database := &databaseManagedResource{&managedTestResource{name: "database", events: &events, startErr: startErr}}
	app, err := New(func(r Registry) error {
		if err := r.ProvideManaged("redis", PhaseInfrastructure, func() *redisManagedResource { return redis }); err != nil {
			return err
		}
		if err := r.ProvideManaged("database", PhaseInfrastructure, func() *databaseManagedResource { return database }); err != nil {
			return err
		}
		if err := r.AddHook(func(*redisManagedResource, *databaseManagedResource) Hook {
			return Hook{Name: "service", Phase: PhaseService, OnStart: func(context.Context) error { events = append(events, "start:service"); return nil }, OnStop: func(context.Context) error { events = append(events, "stop:service"); return nil }}
		}); err != nil {
			return err
		}
		return addManagedTestReadiness(r)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); !errors.Is(err, startErr) {
		t.Fatalf("Start() error = %v, want %v", err, startErr)
	}
	want := []string{"start:redis", "start:database", "stop:redis"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	if redis.starts != 1 || redis.stops != 1 || database.starts != 1 || database.stops != 0 {
		t.Fatalf("resource counts redis=%d/%d database=%d/%d", redis.starts, redis.stops, database.starts, database.stops)
	}
}

func TestProvideManagedFactoryErrorAndValidation(t *testing.T) {
	wantErr := errors.New("redis factory failed")
	_, err := New(func(r Registry) error {
		if err := r.ProvideManaged("redis", PhaseInfrastructure, func() (*managedTestResource, error) {
			return nil, wantErr
		}); err != nil {
			return err
		}
		if err := r.AddHook(func(*managedTestResource) Hook {
			return Hook{Name: "consumer", Phase: PhaseService, OnStart: func(context.Context) error { return nil }}
		}); err != nil {
			return err
		}
		return addManagedTestReadiness(r)
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("New() error = %v, want %v", err, wantErr)
	}

	for _, test := range []struct {
		name string
		call func(Registry) error
		want string
	}{
		{name: "invalid name", call: func(r Registry) error {
			return r.ProvideManaged(" redis ", PhaseInfrastructure, func() *managedTestResource { return nil })
		}, want: "name is invalid"},
		{name: "invalid phase", call: func(r Registry) error {
			return r.ProvideManaged("redis", Phase(1), func() *managedTestResource { return nil })
		}, want: "unknown phase"},
		{name: "readiness name", call: func(r Registry) error {
			return r.ProvideManaged("readiness", PhaseInfrastructure, func() *managedTestResource { return nil })
		}, want: "cannot use readiness"},
		{name: "readiness phase", call: func(r Registry) error {
			return r.ProvideManaged("redis", PhaseReadiness, func() *managedTestResource { return nil })
		}, want: "cannot use readiness"},
		{name: "non managed output", call: func(r Registry) error {
			return r.ProvideManaged("redis", PhaseInfrastructure, func() string { return "redis" })
		}, want: "does not implement ManagedResource"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := New(test.call)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("New() error = %v, want %q", err, test.want)
			}
		})
	}
}
