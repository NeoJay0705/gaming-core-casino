package infra

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/database"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/redis"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/rocketmq"
)

func TestModuleContractRegistersAllInfrastructureAsManagedResources(t *testing.T) {
	registry := &recordingRegistry{}
	if err := Module(registry); err != nil {
		t.Fatalf("register infra module: %v", err)
	}
	if got, want := registry.names, []string{"redis", "database", "rocketmq"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("managed names = %v, want %v", got, want)
	}
	if got, want := registry.phases, []framework.Phase{framework.PhaseInfrastructure, framework.PhaseInfrastructure, framework.PhaseInfrastructure}; !reflect.DeepEqual(got, want) {
		t.Fatalf("managed phases = %v, want %v", got, want)
	}
	for i, want := range []reflect.Type{
		reflect.TypeOf((*redis.Client)(nil)),
		reflect.TypeOf((*database.Client)(nil)),
		reflect.TypeOf((*rocketmq.Client)(nil)),
	} {
		got := reflect.TypeOf(registry.constructors[i]).Out(0)
		if got != want {
			t.Fatalf("managed constructor %d result = %v, want %v", i, got, want)
		}
	}
}

func TestExampleConfigBuildsAllInfrastructureResources(t *testing.T) {
	path := filepath.Join("..", "..", "configs", "examples", "infra.yaml")
	snapshot, err := config.LoadInputs(context.Background(), config.ConfigInputs{MergedPaths: []string{path}}, "CORE_CASINO_INFRA_EXAMPLE_TEST__")
	if err != nil {
		t.Fatalf("load example config: %v", err)
	}
	if _, err := redis.New(snapshot); err != nil {
		t.Fatalf("build Redis resource: %v", err)
	}
	if _, err := database.New(snapshot); err != nil {
		t.Fatalf("build database resource: %v", err)
	}
	if _, err := rocketmq.New(snapshot); err != nil {
		t.Fatalf("build RocketMQ resource: %v", err)
	}
}

type recordingRegistry struct {
	names        []string
	phases       []framework.Phase
	constructors []any
}

func (r *recordingRegistry) Provide(any) error { return nil }
func (r *recordingRegistry) ProvideManaged(name string, phase framework.Phase, constructor any) error {
	r.names = append(r.names, name)
	r.phases = append(r.phases, phase)
	r.constructors = append(r.constructors, constructor)
	return nil
}
func (r *recordingRegistry) AddHook(any) error { return nil }
