// Package infra wires framework-owned infrastructure resources.
package infra

import (
	"fmt"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/database"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/redis"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/rocketmq"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/dig"
)

type redisInputs struct {
	dig.In
	Snapshot   config.SourceSnapshot
	Registerer prometheus.Registerer `optional:"true"`
}

type databaseInputs struct {
	dig.In
	Snapshot   config.SourceSnapshot
	Registerer prometheus.Registerer `optional:"true"`
}

func newRedis(inputs redisInputs) (*redis.Client, error) {
	client, err := redis.New(inputs.Snapshot)
	if err != nil {
		return nil, err
	}
	if inputs.Registerer != nil {
		if err := redis.RegisterPoolMetrics(inputs.Registerer, client); err != nil {
			return nil, fmt.Errorf("infra redis metrics: %w", err)
		}
	}
	return client, nil
}

func newDatabase(inputs databaseInputs) (*database.Client, error) {
	client, err := database.New(inputs.Snapshot)
	if err != nil {
		return nil, err
	}
	if inputs.Registerer != nil {
		if err := database.RegisterPoolMetrics(inputs.Registerer, client); err != nil {
			return nil, fmt.Errorf("infra database metrics: %w", err)
		}
	}
	return client, nil
}

// Module registers the common infrastructure resources. Each resource remains
// lazy: it is constructed and enters the lifecycle only if an application hook
// actually depends on its concrete type.
func Module(r framework.Registry) error {
	if r == nil || framework.IsNilDependency(r) {
		return fmt.Errorf("infra registry is nil")
	}
	if err := r.ProvideManaged("redis", framework.PhaseInfrastructure, newRedis); err != nil {
		return err
	}
	if err := r.Provide(redis.KeyPrefixFromClient); err != nil {
		return err
	}
	if err := r.ProvideManaged("database", framework.PhaseInfrastructure, newDatabase); err != nil {
		return err
	}
	return r.ProvideManaged("rocketmq", framework.PhaseInfrastructure, rocketmq.New)
}
