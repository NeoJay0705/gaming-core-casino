package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/workflow"
	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/products/gateproduct"
)

func main() {
	configPath := flag.String("config", "examples/metrics/configs/gate.yaml", "merged YAML configuration path")
	envPrefix := flag.String("env-prefix", "CORE_CASINO_METRICS_GATE__", "environment override prefix")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	app, err := gateproduct.NewApp(ctx, gateproduct.AppOptions{
		Config:    config.ConfigInputs{MergedPaths: []string{*configPath}},
		EnvPrefix: *envPrefix,
	}, workflow.GateModule())
	if err != nil {
		log.Fatal(err)
	}
	if err := app.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
