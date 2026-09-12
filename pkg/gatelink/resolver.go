package gatelink

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sort"
	"strings"
	"time"
)

type clientTarget struct {
	dynamic bool
	host    string
	port    string
	address string
}

// hostResolver 刻意維持 package-private，是測試 DNS refresh 所需的最小 seam，
// 不替換 grpc-go 的 global resolver。
type hostResolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

func parseClientTarget(raw string) (clientTarget, error) {
	raw = strings.TrimSpace(raw)
	const dnsPrefix = "dns:///"
	if strings.HasPrefix(raw, dnsPrefix) {
		endpoint := strings.TrimPrefix(raw, dnsPrefix)
		host, port, err := splitTargetEndpoint(endpoint)
		if err != nil {
			return clientTarget{}, fmt.Errorf("gatelink: invalid DNS target %q: %w", raw, err)
		}
		return clientTarget{dynamic: true, host: host, port: port}, nil
	}
	if strings.Contains(raw, "://") {
		return clientTarget{}, fmt.Errorf("gatelink: unsupported target scheme in %q", raw)
	}
	host, port, err := splitTargetEndpoint(raw)
	if err != nil {
		return clientTarget{}, fmt.Errorf("gatelink: invalid target %q: %w", raw, err)
	}
	return clientTarget{host: host, port: port, address: net.JoinHostPort(host, port)}, nil
}

func splitTargetEndpoint(endpoint string) (string, string, error) {
	if endpoint == "" || endpoint != strings.TrimSpace(endpoint) {
		return "", "", errors.New("target must be host:port without surrounding whitespace")
	}
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		return "", "", fmt.Errorf("target must be host:port: %w", err)
	}
	if strings.TrimSpace(host) == "" {
		return "", "", errors.New("target host is required")
	}
	if port == "" {
		return "", "", errors.New("target port is required")
	}
	return host, port, nil
}

func (c *Client) refreshLoop(ctx context.Context, done chan struct{}) {
	defer close(done)
	c.refreshDNS(ctx)
	ticker := time.NewTicker(c.cfg.DNSRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.refreshDNS(ctx)
		}
	}
}

func (c *Client) refreshDNS(parent context.Context) {
	lookupContext, cancel := context.WithTimeout(parent, c.cfg.DNSRefreshInterval)
	addresses, err := c.lookupDNS(lookupContext)
	cancel()
	if err != nil {
		if parent.Err() != nil {
			return
		}
		c.reportRefreshFailure(parent, err)
		return
	}
	result, err := c.reconcile(parent, addresses)
	if err != nil {
		if parent.Err() != nil {
			return
		}
		c.reportRefreshFailure(parent, err)
		return
	}
	c.reportRefreshSuccess(parent, result)
}

func (c *Client) lookupDNS(ctx context.Context) ([]string, error) {
	if c == nil || !c.target.dynamic || c.resolver == nil {
		return nil, errors.New("gatelink: DNS resolver is unavailable")
	}
	addresses, err := c.resolver.LookupNetIP(ctx, "ip", c.target.host)
	if err != nil {
		return nil, fmt.Errorf("gatelink: resolve %q: %w", c.target.host, err)
	}
	canonical := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		canonical[net.JoinHostPort(address.String(), c.target.port)] = struct{}{}
	}
	if len(canonical) == 0 {
		return nil, fmt.Errorf("gatelink: resolve %q returned no endpoints", c.target.host)
	}
	result := make([]string, 0, len(canonical))
	for address := range canonical {
		result = append(result, address)
	}
	sort.Strings(result)
	return result, nil
}
