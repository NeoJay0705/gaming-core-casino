package serversend

import (
	"strings"
	"testing"
	"time"
)

func TestNarrowTransportConfigDefaultsTimeout(t *testing.T) {
	transport, err := NewGRPCTransport(TransportConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if transport.cfg.RequestTimeout != DefaultRequestTimeout {
		t.Fatalf("request timeout = %s, want %s", transport.cfg.RequestTimeout, DefaultRequestTimeout)
	}
}

func TestNarrowTransportConfigRejectsNegativeTimeout(t *testing.T) {
	if _, err := NewGRPCTransport(TransportConfig{RequestTimeout: -time.Second}); err == nil || !strings.Contains(err.Error(), "must not be negative") {
		t.Fatalf("negative timeout error = %v", err)
	}
}

func TestPresenceAndEndpointConfigsValidateOwnedFields(t *testing.T) {
	if _, err := NewPresenceRegistry(nil, Keyspace{}, PresenceConfig{}); err == nil {
		t.Fatal("zero presence config unexpectedly accepted")
	}
	endpoint, err := (EndpointRegistrarConfig{TTL: time.Minute}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	if endpoint.Refresh != 20*time.Second {
		t.Fatalf("endpoint refresh = %s, want 20s", endpoint.Refresh)
	}
	if _, err := (EndpointRegistrarConfig{TTL: time.Second, Refresh: time.Second}).normalized(); err == nil {
		t.Fatal("refresh equal to TTL unexpectedly accepted")
	}
	if _, err := (EndpointRegistrarConfig{TTL: time.Second, Refresh: -time.Second}).normalized(); err == nil {
		t.Fatal("negative refresh unexpectedly accepted")
	}
}
