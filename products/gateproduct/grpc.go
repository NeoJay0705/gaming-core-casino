package gateproduct

import (
	"fmt"
	"strings"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"github.com/NeoJay0705/gaming-core-casino/pkg/grpcserver"
	"github.com/NeoJay0705/gaming-core-casino/pkg/logging"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"google.golang.org/grpc"
)

// WebSocketChannel owns player commands received by the Gate WebSocket
// transport. It is separate from Game's direct Gate request channel.
const WebSocketChannel dispatcher.Channel = "gate-websocket"

type gateGRPCClientConfig struct {
	Target               string        `config:"target" yaml:"target"`
	Timeout              time.Duration `config:"timeout" yaml:"timeout"`
	DNSRefreshInterval   time.Duration `config:"dns_refresh_interval" yaml:"dns_refresh_interval"`
	ConnectionsPerHost   int           `config:"connections_per_host" yaml:"connections_per_host"`
	WriteBufferSizeBytes int           `config:"write_buffer_size_bytes" yaml:"write_buffer_size_bytes"`
}

// gateDeliveryGRPCClientConfig describes Gate-to-Gate fan-out topology. The
// target is a headless DNS name, not a business service key or a VIP.
type gateDeliveryGRPCClientConfig struct {
	Timeout time.Duration    `config:"timeout" yaml:"timeout"`
	Fanout  gateFanoutConfig `config:"fanout" yaml:"fanout"`
}

type gateFanoutConfig struct {
	Target       string `config:"target" yaml:"target"`
	MaxEndpoints int    `config:"max_endpoints" yaml:"max_endpoints"`
}

type gateEndpointRegistrationConfig struct {
	TTL     time.Duration `config:"ttl" yaml:"ttl"`
	Refresh time.Duration `config:"refresh" yaml:"refresh"`
}

func newGateGameGRPCClient(cfg gatelink.ClientConfig, factory *logging.Factory) (*gatelink.Client, error) {
	logger, err := factory.Component("grpc.client.game")
	if err != nil {
		return nil, err
	}
	client, err := gatelink.NewClientWithLogger(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("gate gRPC: validate grpc.clients.game: %w", err)
	}
	return client, nil
}

// newGateGRPCServer creates the single product-level Gate listener. Services
// are registered during composition and do not own a listener themselves.
func newGateGRPCServer(cfg grpcserver.Config, factory *logging.Factory) (*grpcserver.Server, error) {
	logger, err := factory.Component("grpc.server")
	if err != nil {
		return nil, err
	}
	return grpcserver.NewWithLogger(cfg, logger)
}

func newGateDeliveryService(commandDispatcher *dispatcher.Dispatcher) (*serversend.GateDeliveryService, error) {
	return serversend.NewGateDeliveryService(commandDispatcher)
}

func registerGateDeliveryService(server *grpcserver.Server, service *serversend.GateDeliveryService) error {
	if server == nil || service == nil {
		return fmt.Errorf("gate gRPC: server and GateDelivery service are required")
	}
	return server.Register(serversend.GateDelivery_ServiceDesc.ServiceName, func(registrar grpc.ServiceRegistrar) {
		serversend.RegisterGateDeliveryServer(registrar, service)
	})
}

func gateGRPCServerConfig(snapshot config.SourceSnapshot) (grpcserver.Config, error) {
	if snapshot == nil {
		return grpcserver.Config{}, fmt.Errorf("gate gRPC: config snapshot is nil")
	}
	if snapshot.Has("gate_to_game") {
		return grpcserver.Config{}, fmt.Errorf("gate gRPC: legacy gate_to_game key is unsupported; use grpc.server and grpc.clients.game")
	}
	if snapshot.Has("room_broadcast") {
		return grpcserver.Config{}, fmt.Errorf("gate gRPC: legacy room_broadcast key is unsupported; use server_send.broadcast")
	}
	if !snapshot.Has("grpc.server") {
		return grpcserver.Config{}, fmt.Errorf("gate gRPC: grpc.server is required")
	}
	var cfg grpcserver.Config
	if err := snapshot.Bind("grpc.server", &cfg, config.Strict()); err != nil {
		return grpcserver.Config{}, fmt.Errorf("gate gRPC: bind grpc.server: %w", err)
	}
	return cfg, nil
}

func gateGateGRPCClient(snapshot config.SourceSnapshot) (serversend.TransportConfig, gateFanoutConfig, bool, error) {
	if snapshot == nil {
		return serversend.TransportConfig{}, gateFanoutConfig{}, false, fmt.Errorf("gate gRPC: config snapshot is nil")
	}
	if !snapshot.Has("grpc.clients.gate") {
		return serversend.TransportConfig{}, gateFanoutConfig{}, false, nil
	}
	var cfg gateDeliveryGRPCClientConfig
	if err := snapshot.Bind("grpc.clients.gate", &cfg, config.Strict()); err != nil {
		return serversend.TransportConfig{}, gateFanoutConfig{}, false, fmt.Errorf("gate gRPC: bind grpc.clients.gate: %w", err)
	}
	if cfg.Timeout < 0 {
		return serversend.TransportConfig{}, gateFanoutConfig{}, false, fmt.Errorf("gate gRPC: gate client timeout must not be negative")
	}
	cfg.Fanout.Target = strings.TrimSpace(cfg.Fanout.Target)
	if cfg.Fanout.Target == "" {
		return serversend.TransportConfig{}, gateFanoutConfig{}, false, fmt.Errorf("gate gRPC: grpc.clients.gate.fanout.target is required")
	}
	if cfg.Fanout.MaxEndpoints < 0 {
		return serversend.TransportConfig{}, gateFanoutConfig{}, false, fmt.Errorf("gate gRPC: grpc.clients.gate.fanout.max_endpoints cannot be negative")
	}
	if _, err := serversend.NewDNSGateDirectory(cfg.Fanout.Target, nil); err != nil {
		return serversend.TransportConfig{}, gateFanoutConfig{}, false, fmt.Errorf("gate gRPC: validate grpc.clients.gate.fanout.target: %w", err)
	}
	return serversend.TransportConfig{RequestTimeout: cfg.Timeout}, cfg.Fanout, true, nil
}

func gateGameGRPCConfig(snapshot config.SourceSnapshot) (gatelink.ClientConfig, error) {
	if snapshot == nil {
		return gatelink.ClientConfig{}, fmt.Errorf("gate gRPC: config snapshot is nil")
	}
	if !snapshot.Has("grpc.clients.game") {
		return gatelink.ClientConfig{}, fmt.Errorf("gate gRPC: grpc.clients.game is required")
	}
	var cfg gateGRPCClientConfig
	if err := snapshot.Bind("grpc.clients.game", &cfg, config.Strict()); err != nil {
		return gatelink.ClientConfig{}, fmt.Errorf("gate gRPC: bind grpc.clients.game: %w", err)
	}
	cfg.Target = strings.TrimSpace(cfg.Target)
	if cfg.Target == "" {
		return gatelink.ClientConfig{}, fmt.Errorf("gate gRPC: grpc.clients.game.target is required")
	}
	if cfg.Timeout < 0 {
		return gatelink.ClientConfig{}, fmt.Errorf("gate gRPC: grpc.clients.game.timeout must not be negative")
	}
	if cfg.DNSRefreshInterval < 0 {
		return gatelink.ClientConfig{}, fmt.Errorf("gate gRPC: grpc.clients.game.dns_refresh_interval must not be negative")
	}
	if cfg.ConnectionsPerHost < 0 {
		return gatelink.ClientConfig{}, fmt.Errorf("gate gRPC: grpc.clients.game.connections_per_host must not be negative")
	}
	if cfg.WriteBufferSizeBytes < 0 {
		return gatelink.ClientConfig{}, fmt.Errorf("gate gRPC: grpc.clients.game.write_buffer_size_bytes must not be negative")
	}
	return gatelink.ClientConfig{
		Target:               cfg.Target,
		Timeout:              cfg.Timeout,
		DNSRefreshInterval:   cfg.DNSRefreshInterval,
		ConnectionsPerHost:   cfg.ConnectionsPerHost,
		WriteBufferSizeBytes: cfg.WriteBufferSizeBytes,
	}, nil
}

func gateGRPCServerEndpointRegistration(snapshot config.SourceSnapshot) (serversend.EndpointRegistrarConfig, error) {
	if snapshot == nil {
		return serversend.EndpointRegistrarConfig{}, fmt.Errorf("gate gRPC: config snapshot is nil")
	}
	if !snapshot.Has("grpc.endpoint_registration") {
		return serversend.EndpointRegistrarConfig{}, fmt.Errorf("gate gRPC: grpc.endpoint_registration is required")
	}
	var cfg gateEndpointRegistrationConfig
	if err := snapshot.Bind("grpc.endpoint_registration", &cfg, config.Strict()); err != nil {
		return serversend.EndpointRegistrarConfig{}, fmt.Errorf("gate gRPC: bind grpc.endpoint_registration: %w", err)
	}
	if cfg.TTL <= 0 {
		return serversend.EndpointRegistrarConfig{}, fmt.Errorf("gate gRPC: grpc.endpoint_registration.ttl must be positive")
	}
	if cfg.Refresh < 0 {
		return serversend.EndpointRegistrarConfig{}, fmt.Errorf("gate gRPC: grpc.endpoint_registration.refresh must not be negative")
	}
	if cfg.Refresh > 0 && cfg.Refresh >= cfg.TTL {
		return serversend.EndpointRegistrarConfig{}, fmt.Errorf("gate gRPC: grpc.endpoint_registration.refresh must be smaller than ttl")
	}
	return serversend.EndpointRegistrarConfig{TTL: cfg.TTL, Refresh: cfg.Refresh}, nil
}
