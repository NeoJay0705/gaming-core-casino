package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/products/apiproduct"
)

func main() {
	configPath := flag.String("config", "examples/metrics/configs/api.yaml", "merged YAML configuration path")
	envPrefix := flag.String("env-prefix", "CORE_CASINO_METRICS_API__", "environment override prefix")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	app, err := apiproduct.NewApp(ctx, apiproduct.AppOptions{
		Config:    config.ConfigInputs{MergedPaths: []string{*configPath}},
		EnvPrefix: *envPrefix,
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := app.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
