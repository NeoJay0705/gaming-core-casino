package gatelink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	// DefaultTimeout 是 ClientConfig.Timeout 為 zero 時單筆 Forward 的上限。
	DefaultTimeout = 10 * time.Second
	// DefaultDNSRefreshInterval 是 dns:/// target 的定時查詢週期。
	DefaultDNSRefreshInterval = 10 * time.Second
	// DefaultConnectionsPerHost 讓每個 resolved endpoint 預設只有一條 HTTP/2
	// transport，除非 operator 明確要求更多。
	DefaultConnectionsPerHost = 1
)

var (
	// ErrAffinityKeyRequired 表示 Forward 缺少本機 routing key。
	ErrAffinityKeyRequired = errors.New("gatelink: affinity key is required")
	// ErrClientNotStarted 表示 client 尚未交由 lifecycle 啟動。
	ErrClientNotStarted = errors.New("gatelink: client is not started")
	// ErrClientStopped 表示 client 已停止且不能重新啟動。
	ErrClientStopped     = errors.New("gatelink: client is stopped")
	errClientUnavailable = errors.New("gatelink: client is unavailable")
)

// ClientConfig 設定 Gate-to-Game client topology。
type ClientConfig struct {
	Target string `config:"target" yaml:"target"`
	// Timeout 限制單筆 Forward；zero 使用 DefaultTimeout。
	Timeout time.Duration `config:"timeout" yaml:"timeout"`
	// DNSRefreshInterval 控制 dns:/// target 的定時 DNS lookup；zero 使用
	// DefaultDNSRefreshInterval。static target 不啟動 refresh，且不可明確設定非 zero。
	DNSRefreshInterval time.Duration `config:"dns_refresh_interval" yaml:"dns_refresh_interval"`
	// ConnectionsPerHost 是每個 resolved Game endpoint 建立的獨立 HTTP/2
	// transport 數量；zero 使用 DefaultConnectionsPerHost。
	ConnectionsPerHost int `config:"connections_per_host" yaml:"connections_per_host"`
	// WriteBufferSizeBytes 為每條 gRPC client transport 的 write buffer size；
	// zero 不傳入 option，保留 grpc-go default。
	WriteBufferSizeBytes int `config:"write_buffer_size_bytes" yaml:"write_buffer_size_bytes"`
}

// Client 將 opaque Gate packet 轉送至 Game，並擁有 resolver lifecycle、immutable
// endpoint snapshot 與每個 endpoint 的 gRPC connection pool。
type Client struct {
	cfg      ClientConfig
	target   clientTarget
	resolver hostResolver
	logger   *logging.Logger

	mu      sync.Mutex
	started bool
	stopped bool
	cancel  context.CancelFunc
	done    chan struct{}

	refreshMu sync.Mutex
	stateMu   sync.Mutex
	failed    bool

	snapshot atomic.Pointer[endpointSnapshot]
}

// NewClient 建立 client topology。只有呼叫 Start 後才啟動網路活動，讓 framework
// lifecycle 擁有所有 resolver goroutine。
func NewClient(cfg ClientConfig) (*Client, error) {
	return newClient(cfg, nil, nil)
}

// NewClientWithLogger 建立帶有 component-scoped logger 的 client，用於低頻 DNS
// topology event；RPC log 仍由共用 interceptor 與 product call site 負責。
func NewClientWithLogger(cfg ClientConfig, logger *logging.Logger) (*Client, error) {
	return newClient(cfg, logger, nil)
}

func newClient(cfg ClientConfig, logger *logging.Logger, resolver hostResolver) (*Client, error) {
	cfg.Target = strings.TrimSpace(cfg.Target)
	if cfg.Target == "" {
		return nil, errors.New("gatelink: target is required")
	}
	if cfg.Timeout < 0 {
		return nil, errors.New("gatelink: timeout must not be negative")
	}
	if cfg.DNSRefreshInterval < 0 {
		return nil, errors.New("gatelink: dns_refresh_interval must not be negative")
	}
	if cfg.ConnectionsPerHost < 0 {
		return nil, errors.New("gatelink: connections_per_host must not be negative")
	}
	if cfg.WriteBufferSizeBytes < 0 {
		return nil, errors.New("gatelink: write_buffer_size_bytes must not be negative")
	}
	target, err := parseClientTarget(cfg.Target)
	if err != nil {
		return nil, err
	}
	if !target.dynamic && cfg.DNSRefreshInterval != 0 {
		return nil, errors.New("gatelink: dns_refresh_interval requires a dns:/// target")
	}
	if cfg.DNSRefreshInterval == 0 {
		cfg.DNSRefreshInterval = DefaultDNSRefreshInterval
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.ConnectionsPerHost == 0 {
		cfg.ConnectionsPerHost = DefaultConnectionsPerHost
	}
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	return &Client{
		cfg:      cfg,
		target:   target,
		resolver: resolver,
		logger:   logger,
	}, nil
}

// newClientWithResolver 是 deterministic resolver contract test 的 package-local
// seam；production caller 使用 NewClient 或 NewClientWithLogger。
func newClientWithResolver(cfg ClientConfig, resolver hostResolver, logger *logging.Logger) (*Client, error) {
	return newClient(cfg, logger, resolver)
}

// Start 啟動 client topology。static target 安裝一個 endpoint pool；DNS target
// 先立即查詢，再開始定時 refresh。
func (c *Client) Start(ctx context.Context) error {
	if c == nil {
		return errors.New("gatelink: nil client")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return ErrClientStopped
	}
	if c.started {
		c.mu.Unlock()
		return errors.New("gatelink: client is already started")
	}
	c.started = true
	if c.target.dynamic {
		lifecycleContext, cancel := context.WithCancel(context.Background())
		c.cancel = cancel
		c.done = make(chan struct{})
		done := c.done
		c.mu.Unlock()
		go c.refreshLoop(lifecycleContext, done)
		return nil
	}
	c.mu.Unlock()
	if _, err := c.reconcile(ctx, []string{c.target.address}); err != nil {
		c.mu.Lock()
		c.stopped = true
		c.mu.Unlock()
		return err
	}
	return nil
}

type reconcileResult struct {
	changed int
	added   int
	removed int
	total   int
}

func (c *Client) reconcile(ctx context.Context, addresses []string) (reconcileResult, error) {
	var result reconcileResult
	if len(addresses) == 0 {
		return result, errors.New("gatelink: no Game endpoints resolved")
	}
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	if c.isStopped() {
		return result, ErrClientStopped
	}
	old := c.snapshot.Load()
	oldByAddress := make(map[string]*endpointPool)
	if old != nil {
		for _, endpoint := range old.endpoints {
			if endpoint != nil {
				oldByAddress[endpoint.address] = endpoint
			}
		}
	}
	newEndpoints := make([]*endpointPool, 0, len(addresses))
	created := make([]*endpointPool, 0)
	for _, address := range addresses {
		if endpoint := oldByAddress[address]; endpoint != nil {
			newEndpoints = append(newEndpoints, endpoint)
			delete(oldByAddress, address)
			continue
		}
		endpoint, err := c.newEndpointPool(address)
		if err != nil {
			for _, partial := range created {
				_ = closeEndpointPool(partial)
			}
			return result, err
		}
		created = append(created, endpoint)
		newEndpoints = append(newEndpoints, endpoint)
	}
	result.added = len(created)
	result.removed = len(oldByAddress)
	result.total = len(newEndpoints)
	if old != nil && len(old.endpoints) == len(newEndpoints) && len(oldByAddress) == 0 && len(created) == 0 {
		return result, nil
	}
	result.changed = 1
	// lookupDNS 已將 address canonicalize、去重並排序；static target 只有一個
	// address，因此所有 target 的 snapshot 都是 deterministic。
	c.snapshot.Store(&endpointSnapshot{endpoints: newEndpoints})
	for _, removed := range oldByAddress {
		if err := closeEndpointPool(removed); err != nil {
			c.logError(ctx, "grpc.client.game.endpoint_close", "removed Game endpoint close failed", err)
		}
	}
	return result, nil
}

func (c *Client) newEndpointPool(address string) (*endpointPool, error) {
	endpoint := &endpointPool{address: address, conns: make([]*clientConnection, 0, c.cfg.ConnectionsPerHost)}
	for index := 0; index < c.cfg.ConnectionsPerHost; index++ {
		dialOptions := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithChainUnaryInterceptor(logging.UnaryClientInterceptor(), outgoingRequestContextInterceptor),
		}
		if c.cfg.WriteBufferSizeBytes > 0 {
			dialOptions = append(dialOptions, grpc.WithWriteBufferSize(c.cfg.WriteBufferSizeBytes))
		}
		conn, err := grpc.NewClient("passthrough:///"+address, dialOptions...)
		if err != nil {
			_ = closeEndpointPool(endpoint)
			return nil, fmt.Errorf("gatelink: create connection for endpoint %q: %w", address, err)
		}
		conn.Connect()
		endpoint.conns = append(endpoint.conns, &clientConnection{conn: conn, client: NewGateRequestServiceClient(conn)})
	}
	return endpoint, nil
}

// Stop 停止 refresh，再關閉目前發布的所有 connection pool。仍使用舊 snapshot
// 的 in-flight call 可能收到一般 transport error；Forward 不重試可能非 idempotent
// 的 command。
func (c *Client) Stop(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil
	}
	c.stopped = true
	cancel := c.cancel
	done := c.done
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	var waitErr error
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			waitErr = ctx.Err()
		}
	}
	c.refreshMu.Lock()
	snapshot := c.snapshot.Swap(nil)
	c.refreshMu.Unlock()
	var closeErr error
	if snapshot != nil {
		for _, endpoint := range snapshot.endpoints {
			closeErr = errors.Join(closeErr, closeEndpointPool(endpoint))
		}
	}
	return errors.Join(waitErr, closeErr)
}

// Forward 傳送 opaque Gate request 並回傳 optional reply。實際網路呼叫前必須
// 存在 request-local affinity key。
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
	started := c.started
	stopped := c.stopped
	cfg := c.cfg
	c.mu.Unlock()
	if stopped {
		return nil, ErrClientStopped
	}
	if !started {
		return nil, ErrClientNotStarted
	}
	affinityKey, ok := AffinityKeyFromContext(ctx)
	if !ok {
		return nil, ErrAffinityKeyRequired
	}
	snapshot := c.snapshot.Load()
	if snapshot == nil {
		return nil, status.Error(codes.Unavailable, errClientUnavailable.Error()+": no resolved Game endpoint")
	}
	_, pooledConnection := pickEndpoint(snapshot, affinityKey)
	if pooledConnection == nil || pooledConnection.client == nil {
		return nil, status.Error(codes.Unavailable, errClientUnavailable.Error()+": no usable Game connection")
	}
	return c.forwardWithClient(ctx, pooledConnection.client, cfg, request)
}

func (c *Client) forwardWithClient(ctx context.Context, client GateRequestServiceClient, cfg ClientConfig, request Request) (*Reply, error) {
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

func (c *Client) isStopped() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopped
}

func (c *Client) reportRefreshFailure(ctx context.Context, err error) {
	c.stateMu.Lock()
	first := !c.failed
	c.failed = true
	c.stateMu.Unlock()
	if first {
		c.logWarn(ctx, "grpc.client.game.dns_refresh", "Game DNS refresh failed; retaining last-known-good endpoints", slog.String("error", err.Error()))
	}
}

func (c *Client) reportRefreshSuccess(ctx context.Context, result reconcileResult) {
	c.stateMu.Lock()
	recovered := c.failed
	c.failed = false
	c.stateMu.Unlock()
	if recovered {
		c.logInfo(ctx, "grpc.client.game.dns_refresh_recovered", "Game DNS refresh recovered")
	}
	if result.changed != 0 {
		c.logInfo(ctx, "grpc.client.game.topology_changed", "Game endpoint topology changed",
			slog.Int("added", result.added), slog.Int("removed", result.removed), slog.Int("total", result.total))
	}
}

func (c *Client) logInfo(ctx context.Context, operation, message string, attrs ...slog.Attr) {
	if c != nil && c.logger != nil {
		c.logger.Info(ctx, operation, message, attrs...)
	}
}

func (c *Client) logWarn(ctx context.Context, operation, message string, attrs ...slog.Attr) {
	if c != nil && c.logger != nil {
		c.logger.Warn(ctx, operation, message, attrs...)
	}
}

func (c *Client) logError(ctx context.Context, operation, message string, err error, attrs ...slog.Attr) {
	if c != nil && c.logger != nil {
		c.logger.Error(ctx, operation, message, err, attrs...)
	}
}

func closeEndpointPool(endpoint *endpointPool) error {
	if endpoint == nil {
		return nil
	}
	var err error
	for _, connection := range endpoint.conns {
		if connection == nil || connection.conn == nil {
			continue
		}
		err = errors.Join(err, connection.conn.Close())
	}
	return err
}

var _ interface {
	Start(context.Context) error
	Stop(context.Context) error
} = (*Client)(nil)
