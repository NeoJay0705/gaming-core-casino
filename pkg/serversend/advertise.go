package serversend

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strings"
)

const podIPEnvironmentVariable = "POD_IP"

// RuntimeGateIdentity is the process-wide identity shared by Gate's presence
// owner and endpoint registrar. Its random suffix makes every process restart
// use a distinct identity even when the hostname is reused.
type RuntimeGateIdentity struct {
	GateID GateID
}

// NewRuntimeGateIdentity creates one process-unique Gate identity.
func NewRuntimeGateIdentity() (RuntimeGateIdentity, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return RuntimeGateIdentity{}, fmt.Errorf("%w: resolve Gate hostname: %v", ErrDestinationInvalid, err)
	}
	hostname = strings.TrimSpace(hostname)
	if hostname == "" {
		return RuntimeGateIdentity{}, fmt.Errorf("%w: Gate hostname is empty", ErrDestinationInvalid)
	}
	randomPart := make([]byte, 16)
	if _, err := rand.Read(randomPart); err != nil {
		return RuntimeGateIdentity{}, fmt.Errorf("generate Gate identity: %w", err)
	}
	return RuntimeGateIdentity{GateID: GateID(hostname + "-" + hex.EncodeToString(randomPart))}, nil
}

// ResolveAdvertiseEndpoint combines the actual bound listener port with a
// deployment-resolvable host. POD_IP is preferred, then a concrete listener
// host, then a hostname that resolves to a non-loopback address. A wildcard
// listener without a routable host fails closed; localhost is only accepted
// when the listener itself was explicitly bound to a loopback address.
func ResolveAdvertiseEndpoint(listener net.Addr) (string, error) {
	if listener == nil {
		return "", fmt.Errorf("%w: listener is required", ErrDestinationInvalid)
	}
	listenerHost, port, err := net.SplitHostPort(listener.String())
	if err != nil {
		return "", fmt.Errorf("%w: listener address %q is invalid", ErrDestinationInvalid, listener.String())
	}

	if podIP := strings.TrimSpace(os.Getenv(podIPEnvironmentVariable)); podIP != "" {
		podIP = strings.TrimPrefix(strings.TrimSuffix(podIP, "]"), "[")
		if !isAdvertisableIP(podIP) {
			return "", fmt.Errorf("%w: %s must be a concrete IP address", ErrDestinationInvalid, podIPEnvironmentVariable)
		}
		return net.JoinHostPort(podIP, port), nil
	}
	if !isWildcardHost(listenerHost) {
		return net.JoinHostPort(listenerHost, port), nil
	}

	hostname, _ := os.Hostname()
	hostname = strings.TrimSpace(hostname)
	if hostname != "" && hostnameResolvesToRoutableAddress(hostname) {
		return net.JoinHostPort(hostname, port), nil
	}
	return "", fmt.Errorf("%w: wildcard listener %q has no routable advertise host; set %s", ErrDestinationInvalid, listener.String(), podIPEnvironmentVariable)
}

func isWildcardHost(host string) bool {
	host = strings.TrimSpace(host)
	if host == "" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsUnspecified()
}

func isAdvertisableIP(host string) bool {
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && !ip.IsUnspecified() && !ip.IsLoopback()
}

func hostnameResolvesToRoutableAddress(hostname string) bool {
	addresses, err := net.LookupHost(hostname)
	if err != nil {
		return false
	}
	for _, address := range addresses {
		if ip := net.ParseIP(address); ip != nil && !ip.IsUnspecified() && !ip.IsLoopback() {
			return true
		}
	}
	return false
}
