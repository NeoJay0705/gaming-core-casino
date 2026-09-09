package observability

import "github.com/prometheus/client_golang/prometheus"

// registryOwner 封裝每個 App 私有的 registry，避免把 concrete registry
// 或 Gatherer 擴大成 product-facing DI contract。
type registryOwner struct {
	registry *prometheus.Registry
}

func newRegistryOwner() *registryOwner {
	return &registryOwner{registry: prometheus.NewRegistry()}
}

func newRegisterer(owner *registryOwner) prometheus.Registerer {
	if owner == nil || owner.registry == nil {
		return nil
	}
	return owner.registry
}
