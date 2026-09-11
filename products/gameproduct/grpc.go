package gameproduct

import (
	"context"
	"fmt"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"github.com/NeoJay0705/gaming-core-casino/pkg/grpcserver"
	"go.uber.org/dig"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GateRequestChannel owns Game commands received directly from Gate.
const GateRequestChannel dispatcher.Channel = "gate-request"

type gameGRPCServerInputs struct {
	dig.In

	Dispatcher *dispatcher.Dispatcher
	Metrics    *gameMetrics
}

// newGameGateGRPCService creates the GateRequest service without owning a
// listener. The product-level grpcserver is responsible for the listener.
func newGameGateGRPCService(inputs gameGRPCServerInputs) (*gatelink.GateRequestService, error) {
	return gatelink.NewGateRequestService(gatelink.RequestHandlerFunc(func(ctx context.Context, request gatelink.Request) error {
		return dispatchGameGateRequest(ctx, inputs.Dispatcher, inputs.Metrics, request)
	}))
}

func dispatchGameGateRequest(ctx context.Context, commandDispatcher *dispatcher.Dispatcher, metrics *gameMetrics, request gatelink.Request) error {
	if commandDispatcher == nil {
		return fmt.Errorf("game gRPC: dispatcher is nil")
	}
	return dispatchGameGateRequestMeasured(ctx, commandDispatcher, metrics, request)
}

func dispatchGameGateRequestMeasured(ctx context.Context, commandDispatcher *dispatcher.Dispatcher, metrics *gameMetrics, request gatelink.Request) error {
	// Metrics are intentionally kept at the service boundary; the generic
	// server does not know command or product labels.
	start := time.Now()
	command := gameCommandLabel(commandDispatcher, request.CommandID)
	result := "error"
	if metrics != nil {
		metrics.gateCommandsInFlight.Inc()
		defer func() {
			metrics.gateCommandsInFlight.Dec()
			metrics.observeGateCommand(command, result, time.Since(start))
		}()
	}
	handled, err := commandDispatcher.Dispatch(ctx, GateRequestChannel, dispatcher.CommandID(request.CommandID), request.Payload)
	if err != nil {
		return err
	}
	if !handled {
		return status.Errorf(codes.Unimplemented, "gate command %d is not registered", request.CommandID)
	}
	result = "success"
	return nil
}

// newGameGRPCServer creates the one Game product gRPC server. Service
// registration is performed by the composition callback before lifecycle
// hooks are resolved.
func newGameGRPCServer(cfg grpcserver.Config) (*grpcserver.Server, error) {
	return grpcserver.New(cfg)
}

func registerGameGateRequestService(server *grpcserver.Server, service *gatelink.GateRequestService) error {
	if server == nil || service == nil {
		return fmt.Errorf("game gRPC: server and GateRequest service are required")
	}
	return server.Register(gatelink.GateRequestService_ServiceDesc.ServiceName, func(registrar grpc.ServiceRegistrar) {
		gatelink.RegisterGateRequestServiceServer(registrar, service)
	})
}

func gameGRPCServerConfig(snapshot config.SourceSnapshot) (grpcserver.Config, error) {
	if snapshot == nil {
		return grpcserver.Config{}, fmt.Errorf("game gRPC: config snapshot is nil")
	}
	if snapshot.Has("gate_to_game") {
		return grpcserver.Config{}, fmt.Errorf("game gRPC: legacy gate_to_game key is unsupported; use grpc.server")
	}
	if snapshot.Has("room_broadcast") {
		return grpcserver.Config{}, fmt.Errorf("game gRPC: legacy room_broadcast key is unsupported; use server_send.broadcast")
	}
	if !snapshot.Has("grpc.server") {
		return grpcserver.Config{}, fmt.Errorf("game gRPC: grpc.server is required")
	}
	var cfg grpcserver.Config
	if err := snapshot.Bind("grpc.server", &cfg, config.Strict()); err != nil {
		return grpcserver.Config{}, fmt.Errorf("game gRPC: bind grpc.server: %w", err)
	}
	return cfg, nil
}
