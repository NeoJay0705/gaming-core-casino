package serversend

import (
	"errors"
	"net"
	"testing"
)

func TestResolveAdvertiseEndpointContractUsesPodIPAndListenerPort(t *testing.T) {
	t.Setenv(podIPEnvironmentVariable, "10.20.30.40")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	endpoint, err := ResolveAdvertiseEndpoint(listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	_, listenerPort, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if host != "10.20.30.40" || port != listenerPort {
		t.Fatalf("advertised endpoint = %q, want host 10.20.30.40 and listener port %q", endpoint, listenerPort)
	}
}

func TestResolveAdvertiseEndpointContractUsesConcreteLocalListener(t *testing.T) {
	t.Setenv(podIPEnvironmentVariable, "")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	endpoint, err := ResolveAdvertiseEndpoint(listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	host, _, err := net.SplitHostPort(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if host != "127.0.0.1" {
		t.Fatalf("local advertised host = %q, want 127.0.0.1", host)
	}
}

func TestResolveAdvertiseEndpointContractUsesPodIPForWildcardListener(t *testing.T) {
	t.Setenv(podIPEnvironmentVariable, "10.20.30.41")
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	endpoint, err := ResolveAdvertiseEndpoint(listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	_, listenerPort, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if host != "10.20.30.41" || port != listenerPort {
		t.Fatalf("wildcard advertised endpoint = %q, want host 10.20.30.41 and listener port %q", endpoint, listenerPort)
	}
}

func TestResolveAdvertiseEndpointContractRejectsInvalidPodIP(t *testing.T) {
	for _, podIP := range []string{"not-an-ip", "127.0.0.1"} {
		t.Run(podIP, func(t *testing.T) {
			t.Setenv(podIPEnvironmentVariable, podIP)
			endpoint, err := ResolveAdvertiseEndpoint(fakeNetAddr("0.0.0.0:9091"))
			if !errors.Is(err, ErrDestinationInvalid) || endpoint != "" {
				t.Fatalf("invalid POD_IP result = %q, %v; want destination error", endpoint, err)
			}
		})
	}
}

type fakeNetAddr string

func (a fakeNetAddr) Network() string { return "tcp" }
func (a fakeNetAddr) String() string  { return string(a) }
