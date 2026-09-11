package gameproduct

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"github.com/NeoJay0705/gaming-core-casino/pkg/grpcserver"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGameProductRegistersGateRequestOnProductGRPCServer(t *testing.T) {
	path := writeGameGRPCConfig(t, "")
	requests := make(chan gatelink.Request, 1)
	var server *grpcserver.Server
	app, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GAME_GRPC_TEST__"}, dispatcher.Register(dispatcher.Registration{
		Channel:   GateRequestChannel,
		CommandID: 7,
		Handler: func(ctx context.Context, payload []byte) error {
			requestContext, _ := gatelink.GateRequestContextFrom(ctx)
			if requestContext.Source.ConnectionID != "player-7" {
				return fmt.Errorf("source connection id = %q, want player-7", requestContext.Source.ConnectionID)
			}
			requests <- gatelink.Request{CommandID: 7, Payload: append([]byte(nil), payload...)}
			return nil
		},
	}), func(r framework.Registry) error {
		return r.AddHook(func(value *grpcserver.Server) framework.Hook {
			server = value
			return framework.Hook{Name: "capture-game-grpc-server", Phase: framework.PhaseService, OnStart: func(context.Context) error { return nil }}
		})
	})
	if err != nil {
		t.Fatalf("new game app: %v", err)
	}
	if err := app.frameworkApp.Start(context.Background()); err != nil {
		t.Fatalf("start game app: %v", err)
	}
	t.Cleanup(func() { _ = app.frameworkApp.Stop(context.Background()) })
	if server == nil || server.Addr() == "" {
		t.Fatal("game product did not bind product gRPC server")
	}

	client, err := gatelink.NewClient(gatelink.ClientConfig{Target: server.Addr(), Timeout: time.Second})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Stop(context.Background()) })
	ctx := gatelink.WithGateRequestContext(context.Background(), gatelink.GateRequestContext{Source: gatelink.RequestSource{ConnectionID: "player-7"}})
	if _, err := client.Forward(ctx, gatelink.Request{CommandID: 7, Payload: []byte("opaque")}); err != nil {
		t.Fatalf("forward: %v", err)
	}
	select {
	case request := <-requests:
		if request.CommandID != 7 || string(request.Payload) != "opaque" {
			t.Fatalf("handler request = %#v", request)
		}
	case <-time.After(time.Second):
		t.Fatal("registered handler was not called")
	}
}

func TestGameProductAllowsMissingHandlerUntilDispatcherIsInstalled(t *testing.T) {
	path := writeGameGRPCConfig(t, "")
	var server *grpcserver.Server
	app, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GAME_GRPC_EMPTY_TEST__"}, func(r framework.Registry) error {
		return r.AddHook(func(value *grpcserver.Server) framework.Hook {
			server = value
			return framework.Hook{Name: "capture-empty-game-dispatcher", Phase: framework.PhaseService, OnStart: func(context.Context) error { return nil }}
		})
	})
	if err != nil {
		t.Fatalf("new game app: %v", err)
	}
	if err := app.frameworkApp.Start(context.Background()); err != nil {
		t.Fatalf("start game app: %v", err)
	}
	t.Cleanup(func() { _ = app.frameworkApp.Stop(context.Background()) })
	client, err := gatelink.NewClient(gatelink.ClientConfig{Target: server.Addr(), Timeout: time.Second})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Stop(context.Background()) })
	ctx := gatelink.WithGateRequestContext(context.Background(), gatelink.GateRequestContext{Source: gatelink.RequestSource{ConnectionID: "player-empty"}})
	if _, err := client.Forward(ctx, gatelink.Request{CommandID: 99}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("empty dispatcher status = %s, want %s", status.Code(err), codes.Unimplemented)
	}
}

func TestGameProductRejectsLegacyGateToGameConfig(t *testing.T) {
	path := writeGameGRPCConfig(t, "gate_to_game:\n  listen_addr: 127.0.0.1:0\n")
	_, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GAME_GRPC_LEGACY_TEST__"})
	if err == nil || !strings.Contains(err.Error(), "legacy gate_to_game key is unsupported") {
		t.Fatalf("legacy config error = %v", err)
	}
}

func TestGameProductRejectsLegacyServerSendConfig(t *testing.T) {
	path := writeGameGRPCConfig(t, "server_send:\n  request_timeout: 1s\n")
	_, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GAME_GRPC_LEGACY_SERVER_SEND_TEST__"})
	if err == nil || !strings.Contains(err.Error(), "server_send.broadcast is required") {
		t.Fatalf("legacy server_send config error = %v", err)
	}
}

func TestGameProductRejectsUnknownServerSendFields(t *testing.T) {
	for _, extra := range []string{
		"server_send:\n  unknown: true\n  broadcast:\n    primary: grpc\n",
		"server_send:\n  broadcast:\n    primary: grpc\n    unknown: true\n",
	} {
		path := writeGameGRPCConfig(t, extra)
		_, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GAME_GRPC_UNKNOWN_SERVER_SEND_TEST__"})
		if err == nil || !strings.Contains(err.Error(), "unknown config paths") {
			t.Fatalf("unknown server_send config error = %v", err)
		}
	}
}

func TestGameProductGateClientRejectsTargetField(t *testing.T) {
	snapshot := gameConfigSnapshot{hasGateClient: true, bindGateClient: func(target any) error {
		cfg := target.(*gameGRPCGateClientConfig)
		cfg.Timeout = time.Second
		cfg.Fanout.Target = "dns:///gate-headless:9091"
		return nil
	}}
	if _, _, enabled, err := gameGRPCGateClient(snapshot); err != nil || !enabled {
		t.Fatalf("valid dynamic gate client config: %v", err)
	}
	bad := gameConfigSnapshot{hasGateClient: true, bindGateClient: func(target any) error {
		return fmt.Errorf("unknown field target")
	}}
	if _, _, _, err := gameGRPCGateClient(bad); err == nil {
		t.Fatal("target field unexpectedly accepted")
	}
}

func TestGameServerSendBroadcastConfigValidatesPrimary(t *testing.T) {
	validGRPC := gameBroadcastSnapshot{config: gameServerSendBroadcastConfig{Primary: "grpc"}}
	cfg, enabled, err := gameServerSendBroadcast(validGRPC)
	if err != nil || !enabled || cfg.Primary != "grpc" {
		t.Fatalf("valid gRPC broadcast config = %#v enabled:%t error:%v", cfg, enabled, err)
	}
	for _, test := range []struct {
		name   string
		config gameServerSendBroadcastConfig
		want   string
	}{
		{name: "unsupported primary", config: gameServerSendBroadcastConfig{Primary: "http"}, want: "primary"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := gameServerSendBroadcast(gameBroadcastSnapshot{config: test.config})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("broadcast config error = %v, want %q", err, test.want)
			}
		})
	}
}

func writeGameGRPCConfig(t *testing.T, extra string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "game.yaml")
	contents := observabilityTestYAML + "grpc:\n  server:\n    listen_addr: 127.0.0.1:0\n  clients:\n    gate:\n      timeout: 1s\n      fanout:\n        target: dns:///gate-headless:9091\n" + extra
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

type gameConfigSnapshot struct {
	hasGateClient  bool
	bindGateClient func(any) error
}

type gameBroadcastSnapshot struct{ config gameServerSendBroadcastConfig }

func (s gameBroadcastSnapshot) Bind(path string, target any, _ ...config.BindOption) error {
	if path == "server_send" {
		target.(*gameServerSendConfig).Broadcast = s.config
	}
	return nil
}
func (s gameBroadcastSnapshot) Has(path string) bool {
	return path == "server_send" || path == "server_send.broadcast"
}
func (gameBroadcastSnapshot) HasSource(string) bool                                      { return false }
func (gameBroadcastSnapshot) BindSource(string, string, any, ...config.BindOption) error { return nil }

func (s gameConfigSnapshot) Bind(path string, target any, _ ...config.BindOption) error {
	if path == "grpc.clients.gate" && s.bindGateClient != nil {
		return s.bindGateClient(target)
	}
	return nil
}
func (s gameConfigSnapshot) Has(path string) bool {
	return path == "grpc.clients.gate" && s.hasGateClient
}
func (s gameConfigSnapshot) HasSource(string) bool                                      { return false }
func (s gameConfigSnapshot) BindSource(string, string, any, ...config.BindOption) error { return nil }
