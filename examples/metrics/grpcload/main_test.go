package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/protocol"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"github.com/NeoJay0705/gaming-core-casino/pkg/grpcserver"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestValidateGRPCLoadConfig(t *testing.T) {
	valid := grpcLoadConfig{
		gameTarget:        "127.0.0.1:19090",
		clientConnections: 1,
		concurrency:       4,
		duration:          time.Second,
		payloadBytes:      32,
		warmupRequests:    1,
		requestTimeout:    time.Second,
		metricsAddr:       "127.0.0.1:22082",
	}
	for _, test := range []struct {
		name   string
		mutate func(*grpcLoadConfig)
		want   string
	}{
		{name: "missing target", mutate: func(config *grpcLoadConfig) { config.gameTarget = " " }, want: "game-target"},
		{name: "missing metrics address", mutate: func(config *grpcLoadConfig) { config.metricsAddr = "" }, want: "metrics-addr"},
		{name: "connections not positive", mutate: func(config *grpcLoadConfig) { config.clientConnections = 0 }, want: "client-connections"},
		{name: "concurrency not positive", mutate: func(config *grpcLoadConfig) { config.concurrency = 0 }, want: "concurrency"},
		{name: "connections exceed concurrency", mutate: func(config *grpcLoadConfig) { config.clientConnections = 5 }, want: "cannot exceed"},
		{name: "duration not positive", mutate: func(config *grpcLoadConfig) { config.duration = 0 }, want: "duration"},
		{name: "payload negative", mutate: func(config *grpcLoadConfig) { config.payloadBytes = -1 }, want: "payload-bytes"},
		{name: "payload too large", mutate: func(config *grpcLoadConfig) { config.payloadBytes = maxGRPCLoadPayloadBytes + 1 }, want: "payload-bytes"},
		{name: "warmup negative", mutate: func(config *grpcLoadConfig) { config.warmupRequests = -1 }, want: "warmup-requests"},
		{name: "request timeout not positive", mutate: func(config *grpcLoadConfig) { config.requestTimeout = 0 }, want: "request-timeout"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := valid
			test.mutate(&config)
			err := validateGRPCLoadConfig(config)
			if err == nil || !contains(err.Error(), test.want) {
				t.Fatalf("validateGRPCLoadConfig() error = %v, want %q", err, test.want)
			}
		})
	}
	if err := validateGRPCLoadConfig(valid); err != nil {
		t.Fatalf("validateGRPCLoadConfig(valid) error = %v", err)
	}
}

func TestGRPCLoadClientSelectionIsDeterministic(t *testing.T) {
	clients := []*gatelink.Client{nil, nil, nil}
	for worker := 0; worker < 9; worker++ {
		if got := grpcLoadClientForWorker(clients, worker); got != clients[worker%len(clients)] {
			t.Fatalf("worker %d selected %p, want %p", worker, got, clients[worker%len(clients)])
		}
	}
	if got := grpcLoadClientForWorker(nil, 0); got != nil {
		t.Fatalf("empty clients selected %p, want nil", got)
	}
	if got := grpcLoadClientForWorker(clients, -1); got != nil {
		t.Fatalf("negative worker selected %p, want nil", got)
	}
}

func TestGRPCEchoRoundTripValidatesMetadataAndPayload(t *testing.T) {
	payload := []byte("direct-gRPC")
	var calls atomic.Uint64
	server, client := newTestGRPCEchoPair(t, func(ctx context.Context, request gatelink.Request) error {
		calls.Add(1)
		requestContext, ok := gatelink.GateRequestContextFrom(ctx)
		if !ok || requestContext.Source.ConnectionID != "connection-7" {
			return errors.New("missing connection metadata")
		}
		requestMessage := new(protocol.EchoRequest)
		if err := proto.Unmarshal(request.Payload, requestMessage); err != nil {
			return err
		}
		responsePayload, err := proto.Marshal(&protocol.EchoResponse{Payload: append([]byte(nil), requestMessage.GetPayload()...)})
		if err != nil {
			return err
		}
		return gatelink.SetForwardReply(ctx, gatelink.Reply{CommandID: protocol.EchoResponseCommandID, Payload: responsePayload})
	})
	defer stopTestGRPCEchoPair(t, server, client)

	requestPayload, err := proto.Marshal(&protocol.EchoRequest{Payload: payload})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	if err := grpcEchoRoundTrip(context.Background(), client, requestPayload, payload, "connection-7", nil); err != nil {
		t.Fatalf("grpcEchoRoundTrip() error = %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("server calls = %d, want 1", got)
	}
}

func TestGRPCEchoRoundTripRejectsMismatchedPayload(t *testing.T) {
	server, client := newTestGRPCEchoPair(t, func(ctx context.Context, _ gatelink.Request) error {
		responsePayload, err := proto.Marshal(&protocol.EchoResponse{Payload: []byte("wrong")})
		if err != nil {
			return err
		}
		return gatelink.SetForwardReply(ctx, gatelink.Reply{CommandID: protocol.EchoResponseCommandID, Payload: responsePayload})
	})
	defer stopTestGRPCEchoPair(t, server, client)

	expectedPayload := []byte("expected")
	requestPayload, err := proto.Marshal(&protocol.EchoRequest{Payload: expectedPayload})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	err = grpcEchoRoundTrip(context.Background(), client, requestPayload, expectedPayload, "connection-8", nil)
	if err == nil || !contains(err.Error(), "does not match") {
		t.Fatalf("grpcEchoRoundTrip() error = %v, want payload mismatch", err)
	}
}

func TestWarmupDoesNotRecordMeasuredMetrics(t *testing.T) {
	server, client := newTestGRPCEchoPair(t, func(ctx context.Context, request gatelink.Request) error {
		requestMessage := new(protocol.EchoRequest)
		if err := proto.Unmarshal(request.Payload, requestMessage); err != nil {
			return err
		}
		responsePayload, err := proto.Marshal(&protocol.EchoResponse{Payload: requestMessage.GetPayload()})
		if err != nil {
			return err
		}
		return gatelink.SetForwardReply(ctx, gatelink.Reply{CommandID: protocol.EchoResponseCommandID, Payload: responsePayload})
	})
	defer stopTestGRPCEchoPair(t, server, client)

	registry := prometheus.NewRegistry()
	metrics, err := newGRPCLoadMetrics(registry)
	if err != nil {
		t.Fatalf("newGRPCLoadMetrics() error = %v", err)
	}
	config := grpcLoadConfig{
		clientConnections: 1,
		concurrency:       1,
		warmupRequests:    1,
		requestTimeout:    time.Second,
	}
	expectedWarmupPayload := []byte("warmup")
	warmupPayload, err := proto.Marshal(&protocol.EchoRequest{Payload: expectedWarmupPayload})
	if err != nil {
		t.Fatalf("marshal warm-up request: %v", err)
	}
	if err := warmupGRPCLoadClients(context.Background(), []*gatelink.Client{client}, warmupPayload, expectedWarmupPayload, config); err != nil {
		t.Fatalf("warmupGRPCLoadClients() error = %v", err)
	}
	if families, err := registry.Gather(); err != nil {
		t.Fatalf("Gather() error = %v", err)
	} else {
		for _, family := range families {
			switch family.GetName() {
			case "gaming_core_example_grpc_load_round_trips_total",
				"gaming_core_example_grpc_load_round_trip_duration_seconds":
				t.Fatalf("warm-up unexpectedly recorded metric family %q", family.GetName())
			case "gaming_core_example_grpc_load_round_trips_in_flight":
				for _, metric := range family.GetMetric() {
					if got := metric.GetGauge().GetValue(); got != 0 {
						t.Fatalf("warm-up left in-flight gauge at %v", got)
					}
				}
			}
		}
	}
	expectedMeasuredPayload := []byte("measured")
	measuredPayload, err := proto.Marshal(&protocol.EchoRequest{Payload: expectedMeasuredPayload})
	if err != nil {
		t.Fatalf("marshal measured request: %v", err)
	}
	if err := grpcEchoRoundTrip(context.Background(), client, measuredPayload, expectedMeasuredPayload, "connection-9", metrics); err != nil {
		t.Fatalf("measured grpcEchoRoundTrip() error = %v", err)
	}
	metric := findMetric(t, registry, "gaming_core_example_grpc_load_round_trips_total", grpcLoadResultSuccess)
	if got := metric.GetCounter().GetValue(); got != 1 {
		t.Fatalf("measured success count = %v, want 1", got)
	}
}

func TestWarmupCoversEveryClientConnection(t *testing.T) {
	var (
		countsMu sync.Mutex
		counts   = make(map[string]int)
	)
	responsePayload, err := proto.Marshal(&protocol.EchoResponse{Payload: []byte("warmup-all")})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	server := newTestGateRequestServer(t, gatelink.RequestHandlerFunc(func(ctx context.Context, _ gatelink.Request) error {
		requestContext, ok := gatelink.GateRequestContextFrom(ctx)
		if !ok {
			return errors.New("missing connection metadata")
		}
		countsMu.Lock()
		counts[requestContext.Source.ConnectionID]++
		countsMu.Unlock()
		return gatelink.SetForwardReply(ctx, gatelink.Reply{CommandID: protocol.EchoResponseCommandID, Payload: responsePayload})
	}))
	clients := make([]*gatelink.Client, 2)
	t.Cleanup(func() {
		for index := len(clients) - 1; index >= 0; index-- {
			if clients[index] != nil {
				_ = clients[index].Stop(context.Background())
			}
		}
		_ = server.Stop(context.Background())
	})
	for index := range clients {
		clients[index], err = gatelink.NewClient(gatelink.ClientConfig{Target: server.Addr(), Timeout: time.Second})
		if err != nil {
			t.Fatalf("gatelink.NewClient(%d) error = %v", index, err)
		}
		if err := clients[index].Start(context.Background()); err != nil {
			t.Fatalf("client %d Start() error = %v", index, err)
		}
	}

	payload := []byte("warmup-all")
	requestPayload, err := proto.Marshal(&protocol.EchoRequest{Payload: payload})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	config := grpcLoadConfig{
		clientConnections: 2,
		warmupRequests:    2,
		requestTimeout:    time.Second,
	}
	if err := warmupGRPCLoadClients(context.Background(), clients, requestPayload, payload, config); err != nil {
		t.Fatalf("warmupGRPCLoadClients() error = %v", err)
	}
	countsMu.Lock()
	defer countsMu.Unlock()
	for index := range clients {
		connectionID := fmt.Sprintf("grpcload-warmup-%d", index)
		if got := counts[connectionID]; got != config.warmupRequests {
			t.Errorf("connection %q warm-up count = %d, want %d", connectionID, got, config.warmupRequests)
		}
	}
}

func TestMeasuredLoadStopsFailedWorkersAndBalancesMetrics(t *testing.T) {
	server, client := newTestGRPCEchoPair(t, func(context.Context, gatelink.Request) error {
		return errors.New("forced gRPC failure")
	})
	defer stopTestGRPCEchoPair(t, server, client)
	registry := prometheus.NewRegistry()
	metrics, err := newGRPCLoadMetrics(registry)
	if err != nil {
		t.Fatalf("newGRPCLoadMetrics() error = %v", err)
	}
	payload := []byte("failed")
	requestPayload, err := proto.Marshal(&protocol.EchoRequest{Payload: payload})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	config := grpcLoadConfig{
		clientConnections: 1,
		concurrency:       2,
		duration:          time.Second,
		requestTimeout:    time.Second,
	}
	if err := runMeasuredGRPCLoad(context.Background(), []*gatelink.Client{client}, requestPayload, payload, config, metrics); err == nil {
		t.Fatal("runMeasuredGRPCLoad() error = nil, want worker failure")
	}
	errorMetric := findMetric(t, registry, "gaming_core_example_grpc_load_round_trips_total", grpcLoadResultError)
	if got := errorMetric.GetCounter().GetValue(); got != float64(config.concurrency) {
		t.Fatalf("error count = %v, want %d", got, config.concurrency)
	}
	inFlightMetric := findMetric(t, registry, "gaming_core_example_grpc_load_round_trips_in_flight", "")
	if got := inFlightMetric.GetGauge().GetValue(); got != 0 {
		t.Fatalf("in-flight = %v, want 0", got)
	}
}

func newTestGRPCEchoPair(t *testing.T, handler func(context.Context, gatelink.Request) error) (*grpcserver.Server, *gatelink.Client) {
	t.Helper()
	server := newTestGateRequestServer(t, gatelink.RequestHandlerFunc(handler))
	client, err := gatelink.NewClient(gatelink.ClientConfig{Target: server.Addr(), Timeout: time.Second})
	if err != nil {
		t.Fatalf("gatelink.NewClient() error = %v", err)
	}
	if err := client.Start(context.Background()); err != nil {
		_ = server.Stop(context.Background())
		t.Fatalf("client.Start() error = %v", err)
	}
	return server, client
}

func stopTestGRPCEchoPair(t *testing.T, server *grpcserver.Server, client *gatelink.Client) {
	t.Helper()
	if err := client.Stop(context.Background()); err != nil {
		t.Fatalf("client.Stop() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Stop(ctx); err != nil {
		t.Fatalf("server.Stop() error = %v", err)
	}
}

func newTestGateRequestServer(t *testing.T, handler gatelink.RequestHandler) *grpcserver.Server {
	t.Helper()
	service, err := gatelink.NewGateRequestService(handler)
	if err != nil {
		t.Fatalf("gatelink.NewGateRequestService() error = %v", err)
	}
	server, err := grpcserver.New(grpcserver.Config{ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("grpcserver.New() error = %v", err)
	}
	if err := server.Register(gatelink.GateRequestService_ServiceDesc.ServiceName, func(registrar grpc.ServiceRegistrar) {
		gatelink.RegisterGateRequestServiceServer(registrar, service)
	}); err != nil {
		t.Fatalf("register GateRequest service: %v", err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("server.Start() error = %v", err)
	}
	return server
}

func contains(value, fragment string) bool {
	return strings.Contains(value, fragment)
}
