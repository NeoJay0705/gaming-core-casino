package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/profilehttp"
	"github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/workflow"
	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/products/gateproduct"
)

func main() {
	configPath := flag.String("config", "examples/metrics/configs/gate.yaml", "merged YAML configuration path")
	envPrefix := flag.String("env-prefix", "CORE_CASINO_METRICS_GATE__", "environment override prefix")
	pprofAddr := flag.String("pprof-addr", "", "optional loopback pprof listen address")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *configPath, *envPrefix, *pprofAddr); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, configPath, envPrefix, pprofAddr string) error {
	profileServer, err := profilehttp.Start(pprofAddr)
	if err != nil {
		return err
	}
	defer func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if shutdownErr := profileServer.Shutdown(shutdownContext); shutdownErr != nil {
			log.Printf("stop pprof server: %v", shutdownErr)
		}
	}()

	app, err := gateproduct.NewApp(ctx, gateproduct.AppOptions{
		Config:    config.ConfigInputs{MergedPaths: []string{configPath}},
		EnvPrefix: envPrefix,
	}, workflow.GateModule())
	if err != nil {
		return fmt.Errorf("create Gate app: %w", err)
	}
	if err := app.Run(ctx); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}
