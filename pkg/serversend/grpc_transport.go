package serversend

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const DefaultRequestTimeout = 3 * time.Second

// GateEndpoint is one directly reachable Gate delivery listener.
type GateEndpoint struct {
	GateID  GateID
	Address string
}

func (e GateEndpoint) validated() (GateEndpoint, error) {
	e.GateID = GateID(strings.TrimSpace(string(e.GateID)))
	e.Address = strings.TrimSpace(e.Address)
	if e.GateID == "" || e.Address == "" {
		return GateEndpoint{}, fmt.Errorf("%w: gate id and address are required", ErrDestinationInvalid)
	}
	host, port, err := net.SplitHostPort(e.Address)
	if err != nil || host == "" || port == "" {
		return GateEndpoint{}, fmt.Errorf("%w: gate address %q must be host:port", ErrDestinationInvalid, e.Address)
	}
	return e, nil
}

// TransportConfig controls direct Game-to-Gate delivery calls.
type TransportConfig struct {
	RequestTimeout  time.Duration `config:"request_timeout" yaml:"request_timeout"`
	MaxPayloadBytes int           `config:"max_payload_bytes" yaml:"max_payload_bytes"`
}

func (c TransportConfig) normalized() (TransportConfig, error) {
	if c.RequestTimeout < 0 {
		return TransportConfig{}, errors.New("server send: request timeout must not be negative")
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = DefaultRequestTimeout
	}
	maxPayloadBytes, err := normalizedPayloadLimit(c.MaxPayloadBytes)
	if err != nil {
		return TransportConfig{}, err
	}
	c.MaxPayloadBytes = maxPayloadBytes
	return c, nil
}

// GRPCTransport keeps one reusable gRPC channel per Gate endpoint. It is a
// managed resource when composition needs a sender; callers never Dial per
// client message.
type GRPCTransport struct {
	cfg TransportConfig

	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	stopped bool
}

// NewGRPCTransport creates an idle direct Gate transport.
func NewGRPCTransport(cfg TransportConfig) (*GRPCTransport, error) {
	var err error
	if cfg, err = cfg.normalized(); err != nil {
		return nil, err
	}
	return &GRPCTransport{cfg: cfg, conns: make(map[string]*grpc.ClientConn)}, nil
}

// Start validates that the transport has not already stopped. gRPC connects
// lazily on the first RPC.
func (t *GRPCTransport) Start(context.Context) error {
	if t == nil {
		return errors.New("server send: gRPC transport is nil")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return errors.New("server send: gRPC transport is stopped")
	}
	return nil
}

// Stop closes every cached gRPC channel.
func (t *GRPCTransport) Stop(context.Context) error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		return nil
	}
	t.stopped = true
	conns := t.conns
	t.conns = make(map[string]*grpc.ClientConn)
	t.mu.Unlock()
	var errs []error
	for _, conn := range conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// SendToConnection delivers to an exact Gate connection.
func (t *GRPCTransport) SendToConnection(ctx context.Context, endpoint GateEndpoint, message RequestPlayerMessage, connectionID ConnectionID) (DeliveryStatus, error) {
	if connectionID == "" {
		return DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED, fmt.Errorf("%w: connection id is required", ErrDestinationInvalid)
	}
	if err := message.validatePayload(t.cfg.MaxPayloadBytes); err != nil {
		return DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED, err
	}
	message = message.clone()
	response, err := t.call(ctx, endpoint, func(client GateDeliveryClient, ctx context.Context) (*DeliveryResponse, error) {
		return client.SendToConnection(ctx, &SendToConnectionRequest{ConnectionId: string(connectionID), ExpectedLoginName: string(message.ExpectedLoginName), CommandId: message.CommandID, Payload: message.Payload})
	})
	return t.deliveryResult(response, err)
}

// SendToPlayer delivers to the current local session of one player at endpoint.
func (t *GRPCTransport) SendToPlayer(ctx context.Context, endpoint GateEndpoint, message PlayerMessage) (DeliveryStatus, error) {
	if err := message.validatePayload(t.cfg.MaxPayloadBytes); err != nil {
		return DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED, err
	}
	message = message.clone()
	response, err := t.call(ctx, endpoint, func(client GateDeliveryClient, ctx context.Context) (*DeliveryResponse, error) {
		return client.SendToPlayer(ctx, &SendToPlayerRequest{LoginName: string(message.LoginName), CommandId: message.CommandID, Payload: message.Payload})
	})
	return t.deliveryResult(response, err)
}

// BroadcastRoom delivers a room message to the local members at endpoint.
func (t *GRPCTransport) BroadcastRoom(ctx context.Context, endpoint GateEndpoint, message BroadcastMessage) (int, error) {
	if err := message.validatePayload(t.cfg.MaxPayloadBytes); err != nil {
		return 0, err
	}
	message = message.clone()
	response, err := t.call(ctx, endpoint, func(client GateDeliveryClient, ctx context.Context) (*DeliveryResponse, error) {
		return client.BroadcastRoom(ctx, &BroadcastRoomRequest{RoomId: string(message.RoomID), CommandId: message.CommandID, Payload: message.Payload})
	})
	if err != nil {
		return 0, mapTransportError(err)
	}
	if response == nil || response.GetStatus() == DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED {
		return 0, errors.New("server send: Gate returned an invalid delivery response")
	}
	delivered := int(response.GetDeliveredCount())
	return delivered, nil
}

func (t *GRPCTransport) call(ctx context.Context, endpoint GateEndpoint, call func(GateDeliveryClient, context.Context) (*DeliveryResponse, error)) (*DeliveryResponse, error) {
	if t == nil {
		return nil, errors.New("server send: gRPC transport is nil")
	}
	endpoint, err := endpoint.validated()
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := t.connection(endpoint.Address)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, t.cfg.RequestTimeout)
	defer cancel()
	return call(NewGateDeliveryClient(conn), ctx)
}

func (t *GRPCTransport) connection(address string) (*grpc.ClientConn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return nil, errors.New("server send: gRPC transport is stopped")
	}
	if conn := t.conns[address]; conn != nil {
		return conn, nil
	}
	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, request, reply any, connection *grpc.ClientConn, invoker grpc.UnaryInvoker, options ...grpc.CallOption) error {
			return invoker(outgoingRequestContext(ctx), method, request, reply, connection, options...)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("server send: create gRPC client for %q: %w", address, err)
	}
	t.conns[address] = conn
	return conn, nil
}

func (t *GRPCTransport) deliveryResult(response *DeliveryResponse, err error) (DeliveryStatus, error) {
	if err != nil {
		err = mapTransportError(err)
		return DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED, err
	}
	if response == nil || response.GetStatus() == DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED {
		return DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED, errors.New("server send: Gate returned an invalid delivery response")
	}
	if response.GetStatus() == DeliveryStatus_DELIVERY_STATUS_IGNORED {
		return response.GetStatus(), fmt.Errorf("%w", ErrTargetNotConnected)
	}
	return response.GetStatus(), nil
}

func mapTransportError(err error) error {
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("%w: %w", ErrTargetNotConnected, err)
	}
	return err
}

var _ interface {
	Start(context.Context) error
	Stop(context.Context) error
} = (*GRPCTransport)(nil)
