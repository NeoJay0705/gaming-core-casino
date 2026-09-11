package serversend

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
)

const defaultFanoutConcurrency = 16

// DefaultMaxFanoutEndpoints 限制明確選用的 broadcast gRPC directory，在
// 任何 RPC 前先拒絕超過上限的 endpoint 數量。
const DefaultMaxFanoutEndpoints = 256

// GateResolver 將一個受信任的 Gate instance 解析為 delivery endpoint。它用
// 於 player exact primary path；request-player reply 沿原 unary call 返回，
// 不使用此 resolver。
type GateResolver interface {
	Resolve(context.Context, GateID) (GateEndpoint, error)
}

// GateEndpointLister 列舉明確選用的 gRPC broadcast／player fallback 所需的
// 全部受信任 Gate endpoint。
type GateEndpointLister interface {
	List(context.Context) ([]GateEndpoint, error)
}

// DirectRequestPlayerSender 接受原始 request connection 的單一回覆。gatelink
// server 會透過原 unary response 返回回覆；此 sender 不進行網路 I/O。
type DirectRequestPlayerSender struct {
}

func NewDirectRequestPlayerSender() (*DirectRequestPlayerSender, error) {
	return &DirectRequestPlayerSender{}, nil
}

func (s *DirectRequestPlayerSender) SendToRequestPlayer(ctx context.Context, message RequestPlayerMessage) (Receipt, error) {
	if s == nil {
		return Receipt{}, errors.New("server send: direct request sender is not configured")
	}
	if err := message.validatePayload(DefaultMaxPayloadBytes); err != nil {
		return Receipt{}, err
	}
	err := gatelink.SetForwardReply(ctx, gatelink.Reply{
		CommandID:         message.CommandID,
		Payload:           message.Payload,
		ExpectedLoginName: string(message.ExpectedLoginName),
	})
	if err != nil {
		if errors.Is(err, gatelink.ErrForwardReplyUnavailable) {
			return Receipt{}, ErrRequestRouteUnavailable
		}
		if errors.Is(err, gatelink.ErrForwardReplyAlreadySet) {
			return Receipt{}, ErrRequestReplyAlreadySet
		}
		return Receipt{}, err
	}
	return newReceipt(), nil
}

// RoutedPlayerSender 先解析 player presence，再送至擁有該 player 的 exact
// Gate endpoint。驗證過的 primary route 失敗時才使用設定的 fan-out sender。
type RoutedPlayerSender struct {
	presence  PresenceResolver
	directory GateResolver
	transport *GRPCTransport
	fallback  *FanoutSender
}

func NewRoutedPlayerSender(
	presence PresenceResolver,
	directory GateResolver,
	transport *GRPCTransport,
	fallback *FanoutSender,
) (*RoutedPlayerSender, error) {
	if presence == nil || directory == nil || transport == nil {
		return nil, errors.New("server send: routed player sender dependencies are required")
	}
	return &RoutedPlayerSender{presence: presence, directory: directory, transport: transport, fallback: fallback}, nil
}

func (s *RoutedPlayerSender) SendToPlayer(ctx context.Context, message PlayerMessage) (Receipt, error) {
	if s == nil || s.presence == nil || s.directory == nil || s.transport == nil {
		return Receipt{}, errors.New("server send: routed player sender is not configured")
	}
	if err := message.validatePayload(DefaultMaxPayloadBytes); err != nil {
		return Receipt{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	receipt, err := s.sendToPlayerPrimary(ctx, message)
	if err == nil || s.fallback == nil {
		return receipt, err
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return Receipt{}, contextErr
	}
	fallbackReceipt, fallbackErr := s.fallback.SendToPlayer(ctx, message)
	if fallbackErr != nil {
		return fallbackReceipt, errors.Join(err, fallbackErr)
	}
	return fallbackReceipt, nil
}

func (s *RoutedPlayerSender) sendToPlayerPrimary(ctx context.Context, message PlayerMessage) (Receipt, error) {
	presence, err := s.presence.Resolve(ctx, message.LoginName)
	if err != nil {
		return Receipt{}, err
	}
	endpoint, err := s.directory.Resolve(ctx, presence.GateID)
	if err != nil {
		return Receipt{}, err
	}
	if _, err := s.transport.SendToPlayer(ctx, endpoint, message); err != nil {
		return Receipt{}, err
	}
	return newReceipt(), nil
}

// FanoutSender 是明確選用的 gRPC all-Gate transport，會以 bounded concurrency
// 對 directory 回傳的每個 endpoint 各送一筆訊息。
type FanoutSender struct {
	directory    GateEndpointLister
	transport    *GRPCTransport
	maxEndpoints int
}

// FanoutConfig 設定 all-Gate fan-out 的 endpoint 上限；0 使用預設值。
type FanoutConfig struct{ MaxEndpoints int }

func NewFanoutSender(directory GateEndpointLister, transport *GRPCTransport, configs ...FanoutConfig) (*FanoutSender, error) {
	if directory == nil || transport == nil {
		return nil, errors.New("server send: fan-out sender dependencies are required")
	}
	maxEndpoints := DefaultMaxFanoutEndpoints
	if len(configs) > 1 {
		return nil, fmt.Errorf("%w: only one fan-out config is supported", ErrDestinationInvalid)
	}
	if len(configs) == 1 {
		if configs[0].MaxEndpoints < 0 {
			return nil, fmt.Errorf("%w: max fan-out endpoints cannot be negative", ErrDestinationInvalid)
		}
		if configs[0].MaxEndpoints > 0 {
			maxEndpoints = configs[0].MaxEndpoints
		}
	}
	return &FanoutSender{directory: directory, transport: transport, maxEndpoints: maxEndpoints}, nil
}

func (s *FanoutSender) Broadcast(ctx context.Context, message Message) (Receipt, error) {
	if err := message.validatePayload(DefaultMaxPayloadBytes); err != nil {
		return Receipt{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	endpoints, err := s.list(ctx)
	if err != nil {
		return Receipt{}, err
	}
	results := s.fanout(ctx, endpoints, func(endpoint GateEndpoint) (DeliveryStatus, error) {
		err := s.transport.Forward(ctx, endpoint, message)
		if err != nil {
			return DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED, fmt.Errorf("fan out command %d to gate %q endpoint %q: %w", message.CommandID, endpoint.GateID, endpoint.Address, err)
		}
		return DeliveryStatus_DELIVERY_STATUS_DELIVERED, nil
	})
	errs := fanoutErrors(results)
	if ctx != nil && ctx.Err() != nil {
		return Receipt{}, errors.Join(append(errs, ctx.Err())...)
	}
	if len(errs) == len(endpoints) {
		return Receipt{}, errors.Join(errs...)
	}
	return newReceipt(), errors.Join(errs...)
}

// SendToPlayer 嘗試每個 Gate 的本機 session registry。IGNORED 表示該 Gate
// 不擁有 player，對這輪 fan-out 不算錯誤。
func (s *FanoutSender) SendToPlayer(ctx context.Context, message PlayerMessage) (Receipt, error) {
	if err := message.validatePayload(DefaultMaxPayloadBytes); err != nil {
		return Receipt{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	endpoints, err := s.list(ctx)
	if err != nil {
		return Receipt{}, err
	}
	results := s.fanout(ctx, endpoints, func(endpoint GateEndpoint) (DeliveryStatus, error) {
		statusValue, err := s.transport.SendToPlayer(ctx, endpoint, message)
		if err != nil {
			if errors.Is(err, ErrTargetNotConnected) {
				return DeliveryStatus_DELIVERY_STATUS_IGNORED, nil
			}
			return DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED, fmt.Errorf("fan out player %q to gate %q endpoint %q: %w", message.LoginName, endpoint.GateID, endpoint.Address, err)
		}
		return statusValue, nil
	})

	delivered := 0
	var errs []error
	for _, result := range results {
		if result.err != nil {
			errs = append(errs, result.err)
			continue
		}
		switch result.status {
		case DeliveryStatus_DELIVERY_STATUS_DELIVERED:
			delivered++
		case DeliveryStatus_DELIVERY_STATUS_IGNORED:
			// 該 Gate 不擁有 player，繼續檢查其他 Gate。
		default:
			errs = append(errs, fmt.Errorf("fan out player %q to gate %q endpoint %q: invalid delivery status %s", message.LoginName, result.endpoint.GateID, result.endpoint.Address, result.status))
		}
	}
	if ctx != nil && ctx.Err() != nil {
		errs = append(errs, ctx.Err())
	}
	if delivered > 0 {
		return newReceipt(), errors.Join(errs...)
	}
	if len(errs) > 0 {
		return Receipt{}, errors.Join(errs...)
	}
	return Receipt{}, fmt.Errorf("%w: player %q was ignored by every Gate", ErrTargetNotConnected, message.LoginName)
}

func (s *FanoutSender) list(ctx context.Context) ([]GateEndpoint, error) {
	if s == nil || s.directory == nil || s.transport == nil {
		return nil, errors.New("server send: fan-out sender is not configured")
	}
	endpoints, err := s.directory.List(ctx)
	if err != nil {
		return nil, err
	}
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("%w: no Gate endpoints available for fan-out", ErrRouteStoreUnavailable)
	}
	unique := make(map[string]GateEndpoint, len(endpoints))
	for _, endpoint := range endpoints {
		endpoint, err = endpoint.validated()
		if err != nil {
			return nil, err
		}
		unique[endpoint.Address] = endpoint
		if s.maxEndpoints > 0 && len(unique) > s.maxEndpoints {
			return nil, fmt.Errorf("%w: %d endpoints exceeds %d", ErrFanoutLimitExceeded, len(unique), s.maxEndpoints)
		}
	}
	result := make([]GateEndpoint, 0, len(unique))
	for _, endpoint := range unique {
		result = append(result, endpoint)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Address < result[j].Address
	})
	return result, nil
}

type fanoutResult struct {
	endpoint GateEndpoint
	status   DeliveryStatus
	err      error
}

// fanout 限制並行 RPC，避免單一不可用 Gate 讓一次明確 delivery 變成
// endpoint 數量倍增的 timeout；每個 endpoint 仍會嘗試一次，結果如何聚合
// 由呼叫端決定。
func (s *FanoutSender) fanout(ctx context.Context, endpoints []GateEndpoint, deliver func(GateEndpoint) (DeliveryStatus, error)) []fanoutResult {
	if ctx == nil {
		ctx = context.Background()
	}
	workerCount := min(defaultFanoutConcurrency, len(endpoints))
	jobs := make(chan GateEndpoint)
	results := make(chan fanoutResult, len(endpoints))
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for endpoint := range jobs {
				statusValue, err := deliver(endpoint)
				results <- fanoutResult{endpoint: endpoint, status: statusValue, err: err}
			}
		}()
	}
schedule:
	for _, endpoint := range endpoints {
		select {
		case jobs <- endpoint:
		case <-ctx.Done():
			break schedule
		}
	}
	close(jobs)
	workers.Wait()
	close(results)
	result := make([]fanoutResult, 0, len(endpoints))
	for item := range results {
		result = append(result, item)
	}
	return result
}

func fanoutErrors(results []fanoutResult) []error {
	errs := make([]error, 0, len(results))
	for _, result := range results {
		if result.err != nil {
			errs = append(errs, result.err)
		}
	}
	return errs
}

// FallbackBroadcastSender 在 Redis primary error 後執行一次 gRPC fallback。
// primary error 可能是結果不明，因此此 best-effort policy 可能產生重複 frame。
type FallbackBroadcastSender struct {
	primary  BroadcastSender
	fallback BroadcastSender
}

func NewFallbackBroadcastSender(primary, fallback BroadcastSender) (*FallbackBroadcastSender, error) {
	if primary == nil || fallback == nil {
		return nil, errors.New("server send: primary and fallback broadcast senders are required")
	}
	return &FallbackBroadcastSender{primary: primary, fallback: fallback}, nil
}

func (s *FallbackBroadcastSender) Broadcast(ctx context.Context, message Message) (Receipt, error) {
	if s == nil || s.primary == nil || s.fallback == nil {
		return Receipt{}, errors.New("server send: fallback broadcast sender is not configured")
	}
	if err := message.validatePayload(DefaultMaxPayloadBytes); err != nil {
		return Receipt{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	primaryReceipt, primaryErr := s.primary.Broadcast(ctx, message)
	if primaryErr == nil {
		return primaryReceipt, nil
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	fallbackReceipt, fallbackErr := s.fallback.Broadcast(ctx, message)
	if fallbackErr == nil {
		return fallbackReceipt, nil
	}
	return fallbackReceipt, errors.Join(primaryErr, fallbackErr)
}

var _ RequestPlayerSender = (*DirectRequestPlayerSender)(nil)
var _ PlayerSender = (*RoutedPlayerSender)(nil)
var _ PlayerSender = (*FanoutSender)(nil)
var _ BroadcastSender = (*FanoutSender)(nil)
var _ BroadcastSender = (*FallbackBroadcastSender)(nil)
