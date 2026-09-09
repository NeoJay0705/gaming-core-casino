package serversend

import (
	"context"
	"errors"
	"testing"
)

func TestDNSGateDirectoryContractListsEveryResolvedGateOnce(t *testing.T) {
	directory, err := NewDNSGateDirectory("dns:///gate-server-send-headless:9091", hostResolverFunc(func(context.Context, string) ([]string, error) {
		return []string{"10.0.0.2", "10.0.0.1", "10.0.0.2"}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	endpoints, err := directory.List(context.Background())
	if err != nil {
		t.Fatalf("list endpoints: %v", err)
	}
	if len(endpoints) != 2 {
		t.Fatalf("endpoint count = %d, want 2", len(endpoints))
	}
	addresses := map[string]bool{}
	for _, endpoint := range endpoints {
		addresses[endpoint.Address] = true
		if endpoint.GateID != GateID(endpoint.Address) {
			t.Fatalf("endpoint gate id = %q, want address %q", endpoint.GateID, endpoint.Address)
		}
	}
	if !addresses["10.0.0.1:9091"] || !addresses["10.0.0.2:9091"] {
		t.Fatalf("endpoints = %#v, want every resolved address", endpoints)
	}
}

func TestDNSGateDirectoryContractReportsInvalidAndUnavailableTargets(t *testing.T) {
	if _, err := NewDNSGateDirectory("gate-server-send-headless", nil); !errors.Is(err, ErrDestinationInvalid) {
		t.Fatalf("invalid target error = %v, want ErrDestinationInvalid", err)
	}
	for _, target := range []string{" dns:///gate-server-send-headless:9091", "dns:///gate/server:9091"} {
		if _, err := NewDNSGateDirectory(target, nil); !errors.Is(err, ErrDestinationInvalid) {
			t.Fatalf("invalid target %q error = %v, want ErrDestinationInvalid", target, err)
		}
	}
	directory, err := NewDNSGateDirectory("dns:///gate-server-send-headless:9091", hostResolverFunc(func(context.Context, string) ([]string, error) {
		return nil, errors.New("DNS unavailable")
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := directory.List(context.Background()); !errors.Is(err, ErrRouteStoreUnavailable) {
		t.Fatalf("lookup failure error = %v, want ErrRouteStoreUnavailable", err)
	}
}

type hostResolverFunc func(context.Context, string) ([]string, error)

func (f hostResolverFunc) LookupHost(ctx context.Context, host string) ([]string, error) {
	return f(ctx, host)
}
