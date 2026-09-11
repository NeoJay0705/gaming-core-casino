package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/profilehttp"
	"github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/protocol"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"google.golang.org/protobuf/proto"
)

const maxGRPCLoadPayloadBytes = serversend.DefaultMaxPayloadBytes - 32

type grpcLoadConfig struct {
	gameTarget        string
	clientConnections int
	concurrency       int
	duration          time.Duration
	payloadBytes      int
	warmupRequests    int
	requestTimeout    time.Duration
	metricsAddr       string
	pprofAddr         string
}

func main() {
	config := grpcLoadConfig{}
	flag.StringVar(&config.gameTarget, "game-target", "127.0.0.1:19090", "Game GateLink endpoint")
	flag.IntVar(&config.clientConnections, "client-connections", 1, "number of independent gRPC ClientConn instances")
	flag.IntVar(&config.concurrency, "concurrency", 400, "total closed-loop gRPC workers")
	flag.DurationVar(&config.duration, "duration", 30*time.Second, "measured request admission duration")
	flag.IntVar(&config.payloadBytes, "payload-bytes", 32, "Echo payload size")
	flag.IntVar(&config.warmupRequests, "warmup-requests", 1, "warm-up Echo requests per ClientConn")
	flag.DurationVar(&config.requestTimeout, "request-timeout", 10*time.Second, "timeout for one unary request")
	flag.StringVar(&config.metricsAddr, "metrics-addr", "127.0.0.1:22082", "private load metrics listen address")
	flag.StringVar(&config.pprofAddr, "pprof-addr", "", "optional loopback pprof listen address")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, config); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

func validateGRPCLoadConfig(config grpcLoadConfig) error {
	if strings.TrimSpace(config.gameTarget) == "" {
		return errors.New("grpc load: game-target is required")
	}
	if strings.TrimSpace(config.metricsAddr) == "" {
		return errors.New("grpc load: metrics-addr is required")
	}
	if config.clientConnections <= 0 {
		return errors.New("grpc load: client-connections must be positive")
	}
	if config.concurrency <= 0 {
		return errors.New("grpc load: concurrency must be positive")
	}
	if config.clientConnections > config.concurrency {
		return errors.New("grpc load: client-connections cannot exceed concurrency")
	}
	if config.duration <= 0 {
		return errors.New("grpc load: duration must be positive")
	}
	if config.payloadBytes < 0 || config.payloadBytes > maxGRPCLoadPayloadBytes {
		return fmt.Errorf("grpc load: payload-bytes must be between 0 and %d", maxGRPCLoadPayloadBytes)
	}
	if config.warmupRequests < 0 {
		return errors.New("grpc load: warmup-requests must not be negative")
	}
	if config.requestTimeout <= 0 {
		return errors.New("grpc load: request-timeout must be positive")
	}
	return nil
}

func run(ctx context.Context, config grpcLoadConfig) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateGRPCLoadConfig(config); err != nil {
		return err
	}

	profileServer, err := profilehttp.Start(config.pprofAddr)
	if err != nil {
		return err
	}
	defer func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if shutdownErr := profileServer.Shutdown(shutdownContext); shutdownErr != nil {
			log.Printf("stop gRPC load pprof server: %v", shutdownErr)
		}
	}()

	observer, err := newGRPCLoadObserver(config.metricsAddr)
	if err != nil {
		return err
	}
	defer func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if shutdownErr := observer.Shutdown(shutdownContext); shutdownErr != nil {
			log.Printf("stop gRPC load metrics observer: %v", shutdownErr)
		}
	}()

	clients, err := newGRPCLoadClients(config)
	if err != nil {
		return err
	}
	defer closeGRPCLoadClients(clients)

	payload := makeGRPCLoadPayload(config.payloadBytes)
	requestPayload, err := proto.Marshal(&protocol.EchoRequest{Payload: payload})
	if err != nil {
		return fmt.Errorf("encode gRPC load request: %w", err)
	}
	if err := warmupGRPCLoadClients(ctx, clients, requestPayload, payload, config); err != nil {
		return fmt.Errorf("warm up gRPC clients: %w", err)
	}
	return runMeasuredGRPCLoad(ctx, clients, requestPayload, payload, config, observer.metrics)
}

func newGRPCLoadClients(config grpcLoadConfig) ([]*gatelink.Client, error) {
	clients := make([]*gatelink.Client, config.clientConnections)
	for index := range clients {
		client, err := gatelink.NewClient(gatelink.ClientConfig{
			Target:  config.gameTarget,
			Timeout: config.requestTimeout,
		})
		if err != nil {
			closeGRPCLoadClients(clients[:index])
			return nil, fmt.Errorf("create ClientConn %d: %w", index, err)
		}
		if err := client.Start(context.Background()); err != nil {
			_ = client.Stop(context.Background())
			closeGRPCLoadClients(clients[:index])
			return nil, fmt.Errorf("start ClientConn %d: %w", index, err)
		}
		clients[index] = client
	}
	return clients, nil
}

func closeGRPCLoadClients(clients []*gatelink.Client) {
	for index := len(clients) - 1; index >= 0; index-- {
		if clients[index] == nil {
			continue
		}
		if err := clients[index].Stop(context.Background()); err != nil {
			log.Printf("stop gRPC ClientConn %d: %v", index, err)
		}
	}
}

func makeGRPCLoadPayload(size int) []byte {
	payload := make([]byte, size)
	for index := range payload {
		payload[index] = byte('a' + index%26)
	}
	return payload
}

func warmupGRPCLoadClients(ctx context.Context, clients []*gatelink.Client, requestPayload, expectedPayload []byte, config grpcLoadConfig) error {
	if config.warmupRequests == 0 || len(clients) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	childContext, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg       sync.WaitGroup
		once     sync.Once
		errorMu  sync.Mutex
		firstErr error
	)
	for index, client := range clients {
		wg.Add(1)
		go func(index int, client *gatelink.Client) {
			defer wg.Done()
			connectionID := fmt.Sprintf("grpcload-warmup-%d", index)
			for request := 0; request < config.warmupRequests; request++ {
				if err := childContext.Err(); err != nil {
					return
				}
				requestContext, requestCancel := context.WithTimeout(childContext, config.requestTimeout)
				err := grpcEchoRoundTrip(requestContext, client, requestPayload, expectedPayload, connectionID, nil)
				requestCancel()
				if err == nil {
					continue
				}
				once.Do(func() {
					errorMu.Lock()
					firstErr = fmt.Errorf("ClientConn %d warm-up %d: %w", index, request+1, err)
					errorMu.Unlock()
					cancel()
				})
				return
			}
		}(index, client)
	}
	wg.Wait()
	errorMu.Lock()
	err := firstErr
	errorMu.Unlock()
	if err != nil {
		return err
	}
	return ctx.Err()
}

func runMeasuredGRPCLoad(ctx context.Context, clients []*gatelink.Client, requestPayload, expectedPayload []byte, config grpcLoadConfig, metrics *grpcLoadMetrics) error {
	if ctx == nil {
		ctx = context.Background()
	}
	startBarrier := make(chan struct{})
	var (
		wg                 sync.WaitGroup
		workersReady       sync.WaitGroup
		successfulRequests atomic.Uint64
		failedWorkers      atomic.Uint64
		once               sync.Once
		errorMu            sync.Mutex
		firstErr           error
	)
	workersReady.Add(config.concurrency)
	var admissionEnd time.Time
	for workerIndex := 0; workerIndex < config.concurrency; workerIndex++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()
			workersReady.Done()
			<-startBarrier
			successful, err := runMeasuredGRPCWorker(ctx, grpcLoadClientForWorker(clients, workerIndex), requestPayload, expectedPayload, workerIndex, admissionEnd, config, metrics)
			successfulRequests.Add(successful)
			if err == nil || isExpectedGRPCLoadTermination(ctx, err) {
				return
			}
			failedWorkers.Add(1)
			once.Do(func() {
				errorMu.Lock()
				firstErr = fmt.Errorf("worker %d: %w", workerIndex, err)
				errorMu.Unlock()
			})
		}(workerIndex)
	}
	workersReady.Wait()
	measurementStart := time.Now()
	admissionEnd = measurementStart.Add(config.duration)
	close(startBarrier)
	wg.Wait()
	measurementEnd := time.Now()
	result := struct {
		successful uint64
		failed     uint64
	}{successful: successfulRequests.Load(), failed: failedWorkers.Load()}
	measuredDuration := measurementEnd.Sub(measurementStart)
	completedRPS := float64(result.successful) / measuredDuration.Seconds()
	log.Printf("gRPC load complete: client_connections=%d concurrency=%d warmup_requests=%d successful_requests=%d failed_workers=%d measurement_start=%s admission_end=%s measurement_end=%s measured_duration=%s completed_rps=%.3f", config.clientConnections, config.concurrency, config.warmupRequests, result.successful, result.failed, measurementStart.Format(time.RFC3339Nano), admissionEnd.Format(time.RFC3339Nano), measurementEnd.Format(time.RFC3339Nano), measuredDuration.Round(time.Millisecond), completedRPS)
	errorMu.Lock()
	err := firstErr
	errorMu.Unlock()
	if err != nil {
		return err
	}
	return ctx.Err()
}

func runMeasuredGRPCWorker(ctx context.Context, client *gatelink.Client, requestPayload, expectedPayload []byte, workerIndex int, admissionEnd time.Time, config grpcLoadConfig, metrics *grpcLoadMetrics) (uint64, error) {
	var successful uint64
	connectionID := fmt.Sprintf("grpcload-worker-%d", workerIndex)
	for time.Now().Before(admissionEnd) {
		if err := ctx.Err(); err != nil {
			return successful, err
		}
		requestContext, requestCancel := context.WithTimeout(ctx, config.requestTimeout)
		err := grpcEchoRoundTrip(requestContext, client, requestPayload, expectedPayload, connectionID, metrics)
		requestCancel()
		if err != nil {
			return successful, err
		}
		successful++
	}
	return successful, nil
}

func grpcLoadClientForWorker(clients []*gatelink.Client, workerIndex int) *gatelink.Client {
	if len(clients) == 0 || workerIndex < 0 {
		return nil
	}
	return clients[workerIndex%len(clients)]
}

func grpcEchoRoundTrip(ctx context.Context, client *gatelink.Client, requestPayload, expectedPayload []byte, connectionID string, metrics *grpcLoadMetrics) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if client == nil {
		return errors.New("gRPC load: client is nil")
	}
	startedAt := time.Now()
	if metrics != nil {
		metrics.startRoundTrip()
	}
	defer func() {
		if metrics != nil {
			metrics.finishRoundTrip(ctx, err, time.Since(startedAt))
		}
	}()

	requestContext := gatelink.WithGateRequestContext(ctx, gatelink.GateRequestContext{
		Source: gatelink.RequestSource{ConnectionID: connectionID},
	})
	reply, err := client.Forward(requestContext, gatelink.Request{
		CommandID: protocol.EchoRequestCommandID,
		Payload:   requestPayload,
	})
	if err != nil {
		return err
	}
	if reply == nil {
		return errors.New("gRPC load: Forward returned no Echo reply")
	}
	if reply.CommandID != protocol.EchoResponseCommandID {
		return fmt.Errorf("gRPC load: response command id = %#x, want %#x", reply.CommandID, protocol.EchoResponseCommandID)
	}
	response := new(protocol.EchoResponse)
	if err := proto.Unmarshal(reply.Payload, response); err != nil {
		return fmt.Errorf("gRPC load: decode Echo response: %w", err)
	}
	if !bytes.Equal(response.GetPayload(), expectedPayload) {
		return errors.New("gRPC load: Echo response payload does not match request")
	}
	return nil
}
