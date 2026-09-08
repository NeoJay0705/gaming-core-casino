// Package infra wires framework-owned infrastructure resources.
package infra

import (
	"fmt"

	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/database"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/redis"
	"github.com/NeoJay0705/gaming-core-casino/pkg/infra/rocketmq"
)

// Module registers the common infrastructure resources. Each resource remains
// lazy: it is constructed and enters the lifecycle only if an application hook
// actually depends on its concrete type.
func Module(r framework.Registry) error {
	if r == nil || framework.IsNilDependency(r) {
		return fmt.Errorf("infra registry is nil")
	}
	if err := r.ProvideManaged("redis", framework.PhaseInfrastructure, redis.New); err != nil {
		return err
	}
	if err := r.ProvideManaged("database", framework.PhaseInfrastructure, database.New); err != nil {
		return err
	}
	return r.ProvideManaged("rocketmq", framework.PhaseInfrastructure, rocketmq.New)
}
