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

// GateResolver resolves one trusted Gate instance to its delivery endpoint.
// It is used by direct request replies and the routed player primary path.
type GateResolver interface {
	Resolve(context.Context, GateID) (GateEndpoint, error)
}

// GateEndpointLister enumerates every trusted Gate endpoint for the explicitly
// selected gRPC room-broadcast and fallback transports.
type GateEndpointLister interface {
	List(context.Context) ([]GateEndpoint, error)
}

// DirectRequestPlayerSender resolves the trusted source Gate from the inbound
// request context and sends to the exact connection that originated it. The
// request context carries identity, not a caller-controlled network endpoint.
type DirectRequestPlayerSender struct {
	directory GateResolver
	transport *GRPCTransport
}

func NewDirectRequestPlayerSender(directory GateResolver, transport *GRPCTransport) (*DirectRequestPlayerSender, error) {
	if directory == nil || transport == nil {
		return nil, errors.New("server send: direct request directory and transport are required")
	}
	return &DirectRequestPlayerSender{directory: directory, transport: transport}, nil
}

func (s *DirectRequestPlayerSender) SendToRequestPlayer(ctx context.Context, message RequestPlayerMessage) (Receipt, error) {
	if s == nil || s.directory == nil || s.transport == nil {
		return Receipt{}, errors.New("server send: direct request sender is not configured")
	}
	if err := message.validatePayload(s.transport.cfg.MaxPayloadBytes); err != nil {
		return Receipt{}, err
	}
	requestContext, ok := gatelink.GateRequestContextFrom(ctx)
	if !ok {
		return Receipt{}, ErrRequestRouteUnavailable
	}
	source := requestContext.Source
	if source.GateID == "" {
		return Receipt{}, fmt.Errorf("%w: gate id is required", ErrRequestRouteInvalid)
	}
	if source.ConnectionID == "" {
		return Receipt{}, fmt.Errorf("%w: connection id is required", ErrRequestRouteInvalid)
	}
	endpoint, err := s.directory.Resolve(ctx, GateID(source.GateID))
	if err != nil {
		return Receipt{}, err
	}
	if endpoint.GateID != GateID(source.GateID) {
		return Receipt{}, fmt.Errorf("%w: resolved Gate id %q does not match source Gate id %q", ErrRequestRouteInvalid, endpoint.GateID, source.GateID)
	}
	if _, err := s.transport.SendToConnection(ctx, endpoint, message, ConnectionID(source.ConnectionID)); err != nil {
		return Receipt{}, err
	}
	return newReceipt(), nil
}

// RoutedPlayerSender resolves player presence first. Once the validated
// primary route fails, the explicitly configured fan-out sender is attempted;
// it is never used for an invalid message.
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
	if err := message.validatePayload(s.transport.cfg.MaxPayloadBytes); err != nil {
		return Receipt{}, err
	}
	receipt, err := s.sendToPlayerPrimary(ctx, message)
	if err == nil || s.fallback == nil {
		return receipt, err
	}
	fallbackReceipt, fallbackErr := s.fallback.sendToPlayerValidated(ctx, message)
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

// FanoutSender is the explicitly selected gRPC all-Gate transport. It sends
// one message to every endpoint returned by its directory with bounded
// concurrency.
type FanoutSender struct {
	directory    GateEndpointLister
	transport    *GRPCTransport
	maxEndpoints int
}

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

func (s *FanoutSender) Broadcast(ctx context.Context, message BroadcastMessage) (Receipt, error) {
	if err := message.validatePayload(s.transportPayloadLimit()); err != nil {
		return Receipt{}, err
	}
	return s.broadcastValidated(ctx, message)
}

func (s *FanoutSender) broadcastValidated(ctx context.Context, message BroadcastMessage) (Receipt, error) {
	endpoints, err := s.list(ctx)
	if err != nil {
		return Receipt{}, err
	}
	results := s.fanout(ctx, endpoints, func(endpoint GateEndpoint) (DeliveryStatus, error) {
		delivered, err := s.transport.BroadcastRoom(ctx, endpoint, message)
		if err != nil {
			return DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED, fmt.Errorf("fan out room %q to gate %q endpoint %q: %w", message.RoomID, endpoint.GateID, endpoint.Address, err)
		}
		if delivered == 0 {
			return DeliveryStatus_DELIVERY_STATUS_IGNORED, nil
		}
		return DeliveryStatus_DELIVERY_STATUS_DELIVERED, nil
	})
	errs := fanoutErrors(results)
	if len(errs) == len(endpoints) {
		return Receipt{}, errors.Join(errs...)
	}
	return newReceipt(), errors.Join(errs...)
}

// SendToPlayer tries the local session registry of every Gate. IGNORED means
// that Gate does not own the player and is not an error for this fan-out.
func (s *FanoutSender) SendToPlayer(ctx context.Context, message PlayerMessage) (Receipt, error) {
	if err := message.validatePayload(s.transportPayloadLimit()); err != nil {
		return Receipt{}, err
	}
	return s.sendToPlayerValidated(ctx, message)
}

func (s *FanoutSender) sendToPlayerValidated(ctx context.Context, message PlayerMessage) (Receipt, error) {
	endpoints, err := s.list(ctx)
	if err != nil {
		return Receipt{}, err
	}
	results := s.fanout(ctx, endpoints, func(endpoint GateEndpoint) (DeliveryStatus, error) {
		statusValue, err := s.transport.SendToPlayer(ctx, endpoint, message)
		if err != nil {
			if statusValue == DeliveryStatus_DELIVERY_STATUS_IGNORED || errors.Is(err, ErrTargetNotConnected) {
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
			// This Gate does not own the player; continue checking others.
		default:
			errs = append(errs, fmt.Errorf("fan out player %q to gate %q endpoint %q: invalid delivery status %s", message.LoginName, result.endpoint.GateID, result.endpoint.Address, result.status))
		}
	}
	if delivered > 0 {
		return newReceipt(), errors.Join(errs...)
	}
	if len(errs) > 0 {
		return Receipt{}, errors.Join(errs...)
	}
	return Receipt{}, fmt.Errorf("%w: player %q was ignored by every Gate", ErrTargetNotConnected, message.LoginName)
}

func (s *FanoutSender) transportPayloadLimit() int {
	if s == nil || s.transport == nil {
		return DefaultMaxPayloadBytes
	}
	return s.transport.cfg.MaxPayloadBytes
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

// fanout bounds concurrent RPCs so one unavailable Gate cannot turn an
// explicitly selected delivery into endpoint-count multiplied timeouts.
// Every endpoint still receives one attempt; callers decide how to aggregate
// statuses.
func (s *FanoutSender) fanout(ctx context.Context, endpoints []GateEndpoint, deliver func(GateEndpoint) (DeliveryStatus, error)) []fanoutResult {
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
	for _, endpoint := range endpoints {
		jobs <- endpoint
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

// FallbackBroadcastSender runs one explicitly configured fallback after any
// primary error. It deliberately does not classify errors: a primary error may
// be ambiguous and this best-effort policy can produce a duplicate frame.
type FallbackBroadcastSender struct {
	primary         BroadcastSender
	fallback        BroadcastSender
	maxPayloadBytes int
}

type BroadcastFallbackConfig struct{ MaxPayloadBytes int }

func NewFallbackBroadcastSender(primary, fallback BroadcastSender, configs ...BroadcastFallbackConfig) (*FallbackBroadcastSender, error) {
	if primary == nil || fallback == nil {
		return nil, errors.New("server send: primary and fallback broadcast senders are required")
	}
	if len(configs) > 1 {
		return nil, fmt.Errorf("%w: only one broadcast fallback config is supported", ErrDestinationInvalid)
	}
	maxPayloadBytes := 0
	if len(configs) == 1 {
		maxPayloadBytes = configs[0].MaxPayloadBytes
	}
	maxPayloadBytes, err := normalizedPayloadLimit(maxPayloadBytes)
	if err != nil {
		return nil, err
	}
	return &FallbackBroadcastSender{primary: primary, fallback: fallback, maxPayloadBytes: maxPayloadBytes}, nil
}

func (s *FallbackBroadcastSender) Broadcast(ctx context.Context, message BroadcastMessage) (Receipt, error) {
	if s == nil || s.primary == nil || s.fallback == nil {
		return Receipt{}, errors.New("server send: fallback broadcast sender is not configured")
	}
	if err := message.validatePayload(s.maxPayloadBytes); err != nil {
		return Receipt{}, err
	}
	primaryReceipt, primaryErr := callValidatedBroadcast(s.primary, ctx, message)
	if primaryErr == nil {
		return primaryReceipt, nil
	}
	fallbackReceipt, fallbackErr := callValidatedBroadcast(s.fallback, ctx, message)
	if fallbackErr == nil {
		return fallbackReceipt, nil
	}
	return fallbackReceipt, errors.Join(primaryErr, fallbackErr)
}

type validatedBroadcastSender interface {
	broadcastValidated(context.Context, BroadcastMessage) (Receipt, error)
}

func callValidatedBroadcast(sender BroadcastSender, ctx context.Context, message BroadcastMessage) (Receipt, error) {
	if validated, ok := sender.(validatedBroadcastSender); ok {
		return validated.broadcastValidated(ctx, message)
	}
	return sender.Broadcast(ctx, message)
}

var _ RequestPlayerSender = (*DirectRequestPlayerSender)(nil)
var _ PlayerSender = (*RoutedPlayerSender)(nil)
var _ PlayerSender = (*FanoutSender)(nil)
var _ BroadcastSender = (*FanoutSender)(nil)
var _ BroadcastSender = (*FallbackBroadcastSender)(nil)
