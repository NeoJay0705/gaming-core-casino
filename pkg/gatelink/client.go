package gatelink

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const defaultServiceConfig = `{"loadBalancingConfig":[{"round_robin":{}}]}`

const DefaultTimeout = 10 * time.Second

var errClientUnavailable = errors.New("gatelink: client is unavailable")

// ClientConfig configures the Gate-to-Game client channel.
type ClientConfig struct {
	Target string `config:"target" yaml:"target"`
	// Timeout bounds one Forward call. Zero uses DefaultTimeout.
	Timeout time.Duration `config:"timeout" yaml:"timeout"`
}

// Client forwards opaque Gate packets to Game. gRPC manages the resolved
// backend connections; no application-level connection pool is needed.
type Client struct {
	cfg ClientConfig

	mu      sync.Mutex
	conn    *grpc.ClientConn
	client  GateRequestServiceClient
	stopped bool
}

// NewClient creates a client channel. Target is required; a zero timeout uses
// DefaultTimeout so every forwarded request has a bounded lifetime.
func NewClient(cfg ClientConfig) (*Client, error) {
	cfg.Target = strings.TrimSpace(cfg.Target)
	if cfg.Target == "" {
		return nil, errors.New("gatelink: target is required")
	}
	if cfg.Timeout < 0 {
		return nil, errors.New("gatelink: timeout must not be negative")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = DefaultTimeout
	}
	conn, err := grpc.NewClient(cfg.Target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(defaultServiceConfig),
		grpc.WithChainUnaryInterceptor(outgoingRequestContextInterceptor),
	)
	if err != nil {
		return nil, fmt.Errorf("gatelink: create client for %q: %w", cfg.Target, err)
	}
	return &Client{cfg: cfg, conn: conn, client: NewGateRequestServiceClient(conn)}, nil
}

// Start validates that the client remains usable for its lifecycle. Connections
// are established lazily by gRPC when an RPC needs them.
func (c *Client) Start(context.Context) error {
	if c == nil {
		return errors.New("gatelink: nil client")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return errors.New("gatelink: client is stopped")
	}
	return nil
}

// Stop closes the long-lived gRPC channel.
func (c *Client) Stop(context.Context) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return nil
	}
	c.stopped = true
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// Forward sends an opaque Gate request and returns its optional reply. Its
// unary interceptor propagates the caller's GateRequestContext through gRPC
// metadata.
func (c *Client) Forward(ctx context.Context, request Request) (*Reply, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil {
		return nil, errors.New("gatelink: nil client")
	}
	if request.CommandID == 0 {
		return nil, status.Error(codes.InvalidArgument, "command_id is required")
	}
	c.mu.Lock()
	client := c.client
	stopped := c.stopped
	cfg := c.cfg
	c.mu.Unlock()
	if stopped {
		return nil, errors.New("gatelink: client is stopped")
	}
	if client == nil {
		return nil, errClientUnavailable
	}
	if cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
	}
	response, err := client.Forward(ctx, &GateRequest{
		CommandId: request.CommandID,
		Payload:   append([]byte(nil), request.Payload...),
	})
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, status.Error(codes.Internal, "gatelink: Forward returned nil response")
	}
	forwardReply := response.GetReply()
	if forwardReply == nil {
		return nil, nil
	}
	if forwardReply.GetCommandId() == 0 {
		return nil, status.Error(codes.Internal, "gatelink: Forward reply command_id is required")
	}
	return &Reply{
		CommandID:         forwardReply.GetCommandId(),
		Payload:           append([]byte(nil), forwardReply.GetPayload()...),
		ExpectedLoginName: forwardReply.GetExpectedLoginName(),
	}, nil
}

var _ interface {
	Start(context.Context) error
	Stop(context.Context) error
} = (*Client)(nil)
