package serversend

import (
	"context"
	"fmt"
	"net"
	"strings"
)

// HostResolver resolves every address for a fan-out DNS name.
type HostResolver interface {
	LookupHost(context.Context, string) ([]string, error)
}

// DNSGateDirectory resolves every address returned by a deployment-provided
// DNS target to a directly reachable Gate endpoint. The configured target
// must be headless (or otherwise enumerate every Gate); this code cannot
// distinguish a load-balanced VIP from a single-replica headless service.
type DNSGateDirectory struct {
	resolver HostResolver
	host     string
	port     string
}

func NewDNSGateDirectory(target string, resolver HostResolver) (*DNSGateDirectory, error) {
	if target == "" || target != strings.TrimSpace(target) {
		return nil, fmt.Errorf("%w: gRPC fan-out target must not be empty or have surrounding whitespace", ErrDestinationInvalid)
	}
	target = strings.TrimPrefix(target, "dns:///")
	if strings.Contains(target, "://") {
		return nil, fmt.Errorf("%w: unsupported gRPC fan-out target scheme", ErrDestinationInvalid)
	}
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	host, port, err := net.SplitHostPort(target)
	if err != nil || host == "" || port == "" || strings.ContainsAny(host, "/") {
		return nil, fmt.Errorf("%w: gRPC fan-out target %q must be host:port", ErrDestinationInvalid, target)
	}
	return &DNSGateDirectory{resolver: resolver, host: host, port: port}, nil
}

func (d *DNSGateDirectory) List(ctx context.Context) ([]GateEndpoint, error) {
	if d == nil || d.resolver == nil {
		return nil, fmt.Errorf("%w: DNS Gate directory is not configured", ErrRouteStoreUnavailable)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	addresses, err := d.resolver.LookupHost(ctx, d.host)
	if err != nil {
		return nil, routeStoreError(fmt.Sprintf("resolve gRPC fan-out target %q", d.host), err)
	}
	unique := make(map[string]GateEndpoint, len(addresses))
	for _, address := range addresses {
		endpointAddress := net.JoinHostPort(address, d.port)
		unique[endpointAddress] = GateEndpoint{GateID: GateID(endpointAddress), Address: endpointAddress}
	}
	result := make([]GateEndpoint, 0, len(unique))
	for _, endpoint := range unique {
		result = append(result, endpoint)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("%w: gRPC fan-out target %q resolved no Gate endpoints", ErrRouteStoreUnavailable, d.host)
	}
	return result, nil
}

var _ GateEndpointLister = (*DNSGateDirectory)(nil)
