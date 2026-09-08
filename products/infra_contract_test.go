package products_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/database"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/redis"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/rocketmq"
	"github.com/NeoJay0705/gaming-core-casino/products/apiproduct"
	"github.com/NeoJay0705/gaming-core-casino/products/gameproduct"
	"github.com/NeoJay0705/gaming-core-casino/products/gateproduct"
	"github.com/NeoJay0705/gaming-core-casino/products/gmsproduct"
)

func TestProductAppsProvideManagedInfrastructureToProductModules(t *testing.T) {
	path := filepath.Join(t.TempDir(), "infra.yaml")
	if err := os.WriteFile(path, []byte(infraConfigYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	inputs := config.ConfigInputs{MergedPaths: []string{path}}
	tests := map[string]func() error{
		"api": func() error {
			_, err := apiproduct.NewApp(context.Background(), apiproduct.AppOptions{Config: inputs, EnvPrefix: "CORE_CASINO_API_INFRA_TEST__"}, requireManagedInfrastructure)
			return err
		},
		"game": func() error {
			_, err := gameproduct.NewApp(context.Background(), gameproduct.AppOptions{Config: inputs, EnvPrefix: "CORE_CASINO_GAME_INFRA_TEST__"}, requireManagedInfrastructure)
			return err
		},
		"gate": func() error {
			_, err := gateproduct.NewApp(context.Background(), gateproduct.AppOptions{Config: inputs, EnvPrefix: "CORE_CASINO_GATE_INFRA_TEST__"}, requireManagedInfrastructure)
			return err
		},
		"gms": func() error {
			_, err := gmsproduct.NewApp(context.Background(), gmsproduct.AppOptions{Config: inputs, EnvPrefix: "CORE_CASINO_GMS_INFRA_TEST__"}, requireManagedInfrastructure)
			return err
		},
	}
	for name, build := range tests {
		t.Run(name, func(t *testing.T) {
			if err := build(); err != nil {
				t.Fatalf("build app with managed infrastructure: %v", err)
			}
		})
	}
}

const infraConfigYAML = `redis:
  addr: redis:6379
database:
  dsn: app:password@tcp(tidb:4000)/gaming?parseTime=true
rocketmq:
  endpoint: namesrv:9876
`

func requireManagedInfrastructure(r framework.Registry) error {
	return r.AddHook(func(*redis.Client, *database.Client, *rocketmq.Client) framework.Hook {
		return framework.Hook{Name: "infra-consumer", Phase: framework.PhaseService, OnStart: func(context.Context) error { return nil }}
	})
}
