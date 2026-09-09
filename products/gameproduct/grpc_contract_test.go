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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGameProductContractRegistersGateRequestHandler(t *testing.T) {
	path := filepath.Join(t.TempDir(), "game.yaml")
	if err := os.WriteFile(path, []byte("gate_to_game:\n  listen_addr: 127.0.0.1:0\n"), 0o600); err != nil {
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
	if err := client.Forward(ctx, gatelink.Request{CommandID: 7, Payload: []byte("opaque")}); err != nil {
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
	if err := os.WriteFile(path, []byte("gate_to_game:\n  listen_addr: 127.0.0.1:0\n"), 0o600); err != nil {
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
	if err := client.Forward(ctx, gatelink.Request{CommandID: 99}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("empty dispatcher status = %s, want %s", status.Code(err), codes.Unimplemented)
	}
	_ = app.frameworkApp.Stop(context.Background())
}

func TestGameProductRequiresGateToGameConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "game.yaml")
	if err := os.WriteFile(path, []byte("product: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := NewApp(context.Background(), AppOptions{Config: config.ConfigInputs{MergedPaths: []string{path}}, EnvPrefix: "CORE_CASINO_GAME_GRPC_REQUIRED_CONFIG_TEST__"})
	if err == nil || !strings.Contains(err.Error(), "gate_to_game is required") {
		t.Fatalf("new game app error = %v, want required gate_to_game", err)
	}
}
