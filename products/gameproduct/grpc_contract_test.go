package gameproduct

import (
	"context"
	"errors"
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
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/alicebob/miniredis/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGameProductContractRegistersGateRequestHandler(t *testing.T) {
	path := filepath.Join(t.TempDir(), "game.yaml")
	if err := os.WriteFile(path, []byte(observabilityTestYAML+"gate_to_game:\n  listen_addr: 127.0.0.1:0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	requests := make(chan gatelink.Request, 1)
	var server *gatelink.Server
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
		return r.AddHook(func(value *gatelink.Server) framework.Hook {
			server = value
			return framework.Hook{Name: "capture-game-gate-grpc", Phase: framework.PhaseService, OnStart: func(context.Context) error { return nil }}
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
		t.Fatal("game product did not register a Gate gRPC server")
	}
	client, err := gatelink.NewClient(gatelink.ClientConfig{Target: server.Addr(), Timeout: time.Second})
	if err != nil {
		t.Fatalf("new gate client: %v", err)
	}
	t.Cleanup(func() { _ = client.Stop(context.Background()) })
	ctx := gatelink.WithGateRequestContext(context.Background(), gatelink.GateRequestContext{Source: gatelink.RequestSource{ConnectionID: "player-7"}})
	if _, err := client.Forward(ctx, gatelink.Request{CommandID: 7, Payload: []byte("opaque")}); err != nil {
		t.Fatalf("forward to game product: %v", err)
	}
	select {
	case request := <-requests:
		if request.CommandID != 7 || string(request.Payload) != "opaque" {
			t.Fatalf("handler request = %#v, want command, payload, and source", request)
		}
	case <-time.After(time.Second):
		t.Fatal("registered handler was not called")
	}
}

func TestGameProductAllowsMissingHandlerUntilDispatcherIsInstalled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "game.yaml")
	if err := os.WriteFile(path, []byte(observabilityTestYAML+"gate_to_game:\n  listen_addr: 127.0.0.1:0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var server *gatelink.Server
	app, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GAME_GRPC_REQUIRED_TEST__"}, func(r framework.Registry) error {
		return r.AddHook(func(value *gatelink.Server) framework.Hook {
			server = value
			return framework.Hook{Name: "capture-empty-game-dispatcher", Phase: framework.PhaseService, OnStart: func(context.Context) error { return nil }}
		})
	})
	if err != nil {
		t.Fatalf("new game app without handler: %v", err)
	}
	if err := app.frameworkApp.Start(context.Background()); err != nil {
		t.Fatalf("start game app without handler: %v", err)
	}
	client, err := gatelink.NewClient(gatelink.ClientConfig{Target: server.Addr(), Timeout: time.Second})
	if err != nil {
		t.Fatalf("new gate client: %v", err)
	}
	t.Cleanup(func() { _ = client.Stop(context.Background()) })
	ctx := gatelink.WithGateRequestContext(context.Background(), gatelink.GateRequestContext{Source: gatelink.RequestSource{ConnectionID: "player-empty"}})
	if _, err := client.Forward(ctx, gatelink.Request{CommandID: 99}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("empty dispatcher status = %s, want %s", status.Code(err), codes.Unimplemented)
	}
	_ = app.frameworkApp.Stop(context.Background())
}

func TestGameProductRequiresGateToGameConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "game.yaml")
	if err := os.WriteFile(path, []byte(observabilityTestYAML+"product: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GAME_GRPC_REQUIRED_CONFIG_TEST__"})
	if err == nil || !strings.Contains(err.Error(), "gate_to_game is required") {
		t.Fatalf("new game app error = %v, want required gate_to_game", err)
	}
}

func TestGameProductContractProvidesServerSendPortsWhenConfigured(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	path := filepath.Join(t.TempDir(), "game.yaml")
	contents := observabilityTestYAML + "redis:\n  addr: " + miniRedis.Addr() + "\n  key_prefix: core-casino\ngate_to_game:\n  listen_addr: 127.0.0.1:0\nserver_send:\n  request_timeout: 1s\n  broadcast:\n    primary: redis\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	var requestSender serversend.RequestPlayerSender
	app, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GAME_SERVER_SEND_TEST__"}, func(r framework.Registry) error {
		return r.AddHook(func(value serversend.RequestPlayerSender) framework.Hook {
			requestSender = value
			return framework.Hook{Name: "capture-game-request-player-sender", Phase: framework.PhaseService, OnStart: func(context.Context) error { return nil }}
		})
	})
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	if requestSender == nil {
		t.Fatal("Game product did not provide RequestPlayerSender")
	}
	if err := app.frameworkApp.Start(context.Background()); err != nil {
		t.Fatalf("start app: %v", err)
	}
	if err := app.frameworkApp.Stop(context.Background()); err != nil {
		t.Fatalf("stop app: %v", err)
	}
}

func TestGameProductContractProvidesAllServerSendPortsWithoutStartingRedis(t *testing.T) {
	path := filepath.Join(t.TempDir(), "game.yaml")
	contents := observabilityTestYAML + "redis:\n  addr: redis:6379\n  key_prefix: core-casino\ngate_to_game:\n  listen_addr: 127.0.0.1:0\nserver_send:\n  broadcast:\n    primary: redis\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	var requestSender serversend.RequestPlayerSender
	var playerSender serversend.PlayerSender
	var broadcastSender serversend.BroadcastSender
	_, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GAME_SERVER_SEND_PORTS_TEST__"}, func(r framework.Registry) error {
		return r.AddHook(func(request serversend.RequestPlayerSender, player serversend.PlayerSender, broadcast serversend.BroadcastSender) framework.Hook {
			requestSender, playerSender, broadcastSender = request, player, broadcast
			return framework.Hook{Name: "capture-game-server-send-ports", Phase: framework.PhaseService, OnStart: func(context.Context) error { return nil }}
		})
	})
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	if requestSender == nil || playerSender == nil || broadcastSender == nil {
		t.Fatalf("server send ports = request:%T player:%T broadcast:%T, want all provided", requestSender, playerSender, broadcastSender)
	}
}

func TestGameProductContractRequiresFanoutTargetAndCanUseGRPCBroadcastWithoutRedis(t *testing.T) {
	missingTargetPath := filepath.Join(t.TempDir(), "missing-target.yaml")
	missingTarget := observabilityTestYAML + "gate_to_game:\n  listen_addr: 127.0.0.1:0\nserver_send:\n  broadcast:\n    primary: grpc\n"
	if err := os.WriteFile(missingTargetPath, []byte(missingTarget), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{missingTargetPath}}, EnvPrefix: "CORE_CASINO_GAME_SERVER_SEND_MISSING_TARGET_TEST__"}); err == nil || !strings.Contains(err.Error(), "gRPC fan-out target") {
		t.Fatalf("missing fan-out target error = %v", err)
	}

	grpcOnlyPath := filepath.Join(t.TempDir(), "grpc-only.yaml")
	grpcOnly := observabilityTestYAML + "gate_to_game:\n  listen_addr: 127.0.0.1:0\nserver_send:\n  fanout:\n    grpc_target: dns:///gate-server-send-headless:9091\n  broadcast:\n    primary: grpc\n"
	if err := os.WriteFile(grpcOnlyPath, []byte(grpcOnly), 0o600); err != nil {
		t.Fatal(err)
	}
	var broadcast serversend.BroadcastSender
	app, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{grpcOnlyPath}}, EnvPrefix: "CORE_CASINO_GAME_SERVER_SEND_GRPC_ONLY_TEST__"}, func(r framework.Registry) error {
		return r.AddHook(func(value serversend.BroadcastSender) framework.Hook {
			broadcast = value
			return framework.Hook{Name: "capture-game-grpc-broadcast", Phase: framework.PhaseService, OnStart: func(context.Context) error { return nil }}
		})
	})
	if err != nil {
		t.Fatalf("new gRPC-only broadcast app: %v", err)
	}
	if broadcast == nil {
		t.Fatal("Game product did not provide gRPC BroadcastSender")
	}
	if err := app.frameworkApp.Start(context.Background()); err != nil {
		t.Fatalf("gRPC-only broadcast unexpectedly required Redis: %v", err)
	}
	if err := app.frameworkApp.Stop(context.Background()); err != nil {
		t.Fatalf("stop gRPC-only broadcast app: %v", err)
	}
}

func TestGameProductContractKeepsPlayerFallbackSeparateFromBroadcastFanout(t *testing.T) {
	miniRedis := miniredis.RunT(t)
	path := filepath.Join(t.TempDir(), "game.yaml")
	contents := observabilityTestYAML + "redis:\n  addr: " + miniRedis.Addr() + "\n  key_prefix: core-casino\ngate_to_game:\n  listen_addr: 127.0.0.1:0\nserver_send:\n  request_timeout: 50ms\n  fanout:\n    grpc_target: dns:///127.0.0.1:1\n  player:\n    fallback: none\n  broadcast:\n    primary: grpc\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	var playerSender serversend.PlayerSender
	app, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GAME_SERVER_SEND_PLAYER_FALLBACK_TEST__"}, func(r framework.Registry) error {
		return r.AddHook(func(value serversend.PlayerSender) framework.Hook {
			playerSender = value
			return framework.Hook{Name: "capture-game-player-sender", Phase: framework.PhaseService, OnStart: func(context.Context) error { return nil }}
		})
	})
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	if err := app.frameworkApp.Start(context.Background()); err != nil {
		t.Fatalf("start app: %v", err)
	}
	defer func() { _ = app.frameworkApp.Stop(context.Background()) }()
	if playerSender == nil {
		t.Fatal("Game product did not provide PlayerSender")
	}
	_, err = playerSender.SendToPlayer(context.Background(), serversend.PlayerMessage{LoginName: "alice", Message: serversend.Message{CommandID: 1}})
	if !errors.Is(err, serversend.ErrPresenceNotFound) {
		t.Fatalf("player fallback=none error = %v, want ErrPresenceNotFound only", err)
	}
	if strings.Contains(err.Error(), "fan out player") {
		t.Fatalf("player fallback=none unexpectedly used broadcast fan-out: %v", err)
	}
}
