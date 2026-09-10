package observability

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// registryOwner 封裝每個 App 私有的 registry，避免把 concrete registry
// 或 Gatherer 擴大成 product-facing DI contract。
type registryOwner struct {
	registry *prometheus.Registry
}

func newRegistryOwner() (*registryOwner, error) {
	registry := prometheus.NewRegistry()
	// 使用 App-local registry，但仍提供壓測必要的 Go/process 基礎資訊。
	for _, collector := range []prometheus.Collector{
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	} {
		if err := registry.Register(collector); err != nil {
			return nil, fmt.Errorf("observability: register built-in collector: %w", err)
		}
	}
	return &registryOwner{registry: registry}, nil
}

func newRegisterer(owner *registryOwner) prometheus.Registerer {
	if owner == nil || owner.registry == nil {
		return nil
	}
	return owner.registry
}
