package gateproduct

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"github.com/NeoJay0705/gaming-core-casino/pkg/grpcserver"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/alicebob/miniredis/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func TestGateProductUsesOneProductGRPCServerAndGameClient(t *testing.T) {
	path, miniRedis := writeGateGRPCConfig(t, "server_send:\n  broadcast:\n    primary: grpc\n")
	_ = miniRedis
	var server *grpcserver.Server
	var service *serversend.GateDeliveryService
	var broadcastSender serversend.BroadcastSender
	app, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GATE_GRPC_TEST__"}, func(r framework.Registry) error {
		return r.AddHook(func(value *grpcserver.Server, delivery *serversend.GateDeliveryService, sender serversend.BroadcastSender) framework.Hook {
			server, service, broadcastSender = value, delivery, sender
			return framework.Hook{Name: "capture-gate-grpc-boundary", Phase: framework.PhaseService, OnStart: func(context.Context) error { return nil }}
		})
	})
	if err != nil {
		t.Fatalf("new gate app: %v", err)
	}
	if server == nil || service == nil {
		t.Fatal("Gate product did not compose generic server and GateDelivery service")
	}
	if broadcastSender == nil {
		t.Fatal("Gate product did not resolve BroadcastSender")
	}
	if err := app.frameworkApp.Start(context.Background()); err != nil {
		t.Fatalf("start gate app: %v", err)
	}
	t.Cleanup(func() { _ = app.frameworkApp.Stop(context.Background()) })
	if server.Addr() == "" {
		t.Fatal("Gate product gRPC server did not bind")
	}
	conn, err := grpc.NewClient(server.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("new Gate delivery client: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	payload, err := proto.Marshal(&serversend.SendPlayersCommand{Messages: []*serversend.PlayerDelivery{{
		LoginName: "not-connected", ClientCommandId: 1,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := serversend.NewGateDeliveryClient(conn).Forward(context.Background(), &gatelink.GateRequest{CommandId: serversend.PlayerDeliveryCommandID, Payload: payload})
	if err != nil || response == nil {
		t.Fatalf("GateDelivery Forward = response:%#v error:%v, want empty/nil", response, err)
	}
}

func TestGateProductRequiresNewGRPCConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gate.yaml")
	if err := os.WriteFile(path, []byte(observabilityTestYAML+"product: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GATE_GRPC_REQUIRED_CONFIG_TEST__"})
	if err == nil || !strings.Contains(err.Error(), "grpc.server is required") {
		t.Fatalf("new gate app error = %v", err)
	}
}

func TestGateProductRejectsLegacyKeys(t *testing.T) {
	for _, key := range []string{"gate_to_game", "room_broadcast"} {
		t.Run(key, func(t *testing.T) {
			path, miniRedis := writeGateGRPCConfig(t, key+": {}\n")
			_ = miniRedis
			_, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GATE_GRPC_LEGACY_TEST__"})
			if err == nil || !strings.Contains(err.Error(), "legacy "+key+" key is unsupported") {
				t.Fatalf("legacy key error = %v", err)
			}
		})
	}
}

func TestGateProductRejectsLegacyServerSendConfig(t *testing.T) {
	path, miniRedis := writeGateGRPCConfig(t, "server_send:\n  request_timeout: 1s\n")
	_ = miniRedis
	_, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GATE_GRPC_LEGACY_SERVER_SEND_TEST__"})
	if err == nil || !strings.Contains(err.Error(), "server_send.broadcast is required") {
		t.Fatalf("legacy server_send config error = %v", err)
	}
}

func TestGateProductRejectsUnknownServerSendFields(t *testing.T) {
	for _, extra := range []string{
		"server_send:\n  unknown: true\n  broadcast:\n    primary: grpc\n",
		"server_send:\n  broadcast:\n    primary: grpc\n    unknown: true\n",
	} {
		path, miniRedis := writeGateGRPCConfig(t, extra)
		_ = miniRedis
		_, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GATE_GRPC_UNKNOWN_SERVER_SEND_TEST__"})
		if err == nil || !strings.Contains(err.Error(), "unknown config paths") {
			t.Fatalf("unknown server_send config error = %v", err)
		}
	}
}

func TestGateEndpointRegistrationConfigValidatesLeaseWindow(t *testing.T) {
	for _, test := range []struct {
		name string
		cfg  gateEndpointRegistrationConfig
		want string
	}{
		{name: "missing ttl", cfg: gateEndpointRegistrationConfig{}, want: "ttl must be positive"},
		{name: "negative refresh", cfg: gateEndpointRegistrationConfig{TTL: time.Second, Refresh: -time.Second}, want: "refresh must not be negative"},
		{name: "refresh not smaller", cfg: gateEndpointRegistrationConfig{TTL: time.Second, Refresh: time.Second}, want: "refresh must be smaller than ttl"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := gateGRPCServerEndpointRegistration(endpointRegistrationSnapshot{config: test.cfg})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("endpoint registration config error = %v, want %q", err, test.want)
			}
		})
	}
}

func writeGateGRPCConfig(t *testing.T, extra string) (string, *miniredis.Miniredis) {
	t.Helper()
	miniRedis := miniredis.RunT(t)
	path := filepath.Join(t.TempDir(), "gate.yaml")
	contents := observabilityTestYAML + "grpc:\n  server:\n    listen_addr: 127.0.0.1:0\n  clients:\n    game:\n      target: dns:///gameproduct:9090\n    gate:\n      fanout:\n        target: dns:///gate-headless:9091\n  endpoint_registration:\n    ttl: 30s\nsession_ownership:\n  lease_ttl: 30s\nredis:\n  addr: " + miniRedis.Addr() + "\n  key_prefix: core-casino\n" + extra
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path, miniRedis
}

type endpointRegistrationSnapshot struct {
	config gateEndpointRegistrationConfig
}

func (s endpointRegistrationSnapshot) Bind(path string, target any, _ ...config.BindOption) error {
	if path == "grpc.endpoint_registration" {
		*target.(*gateEndpointRegistrationConfig) = s.config
	}
	return nil
}
func (s endpointRegistrationSnapshot) Has(path string) bool {
	return path == "grpc.endpoint_registration"
}
func (endpointRegistrationSnapshot) HasSource(string) bool { return false }
func (endpointRegistrationSnapshot) BindSource(string, string, any, ...config.BindOption) error {
	return nil
}
