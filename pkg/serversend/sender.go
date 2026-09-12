package serversend

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const defaultFanoutConcurrency = 16

// DefaultMaxFanoutEndpoints 限制明確選用的 DNS fan-out directory，在任何 RPC
// 開始前先拒絕超過上限的 endpoint。
const DefaultMaxFanoutEndpoints = 256

// GateResolver 解析一個受信任的 Gate instance，保留給其他單 key routing
// contract 使用。
type GateResolver interface {
	Resolve(context.Context, GateID) (GateEndpoint, error)
}

// GateEndpointLister 列舉 DNS fallback 或 generic broadcast 使用的受信任 Gate
// endpoints。
type GateEndpointLister interface {
	List(context.Context) ([]GateEndpoint, error)
}

// PresenceBatchResolver 在單一 routing phase 解析所有指定玩家。
type PresenceBatchResolver interface {
	ResolveMany(context.Context, []LoginName) (map[LoginName]Presence, error)
}

// GateEndpointBatchResolver 在單一 routing phase 解析所有不重複的 Gate ID。
type GateEndpointBatchResolver interface {
	ResolveMany(context.Context, []GateID) (map[GateID]GateEndpoint, error)
}

// DirectRequestPlayerSender 接受原始 request connection 的單一回覆。回覆透過
// 原始 unary call 返回；此 sender 不進行 network I/O。
type DirectRequestPlayerSender struct{}

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

// BatchPlayerSender 解析玩家批次，依 exact Gate endpoint 分組，並對每個 endpoint
// 傳送一個或多個 generic Forward command。只有 Redis routing operation 在任何
// exact RPC 開始前失敗時才使用 fallback。
type BatchPlayerSender struct {
	presence     PresenceBatchResolver
	directory    GateEndpointBatchResolver
	fallback     GateEndpointLister
	transport    *GRPCTransport
	maxEndpoints int
}

// NewBatchPlayerSender 建立同步的 batch sender。fallback directory 必須獨立於
// Redis（通常是既有 DNS directory）。
func NewBatchPlayerSender(
	presence PresenceBatchResolver,
	directory GateEndpointBatchResolver,
	fallback GateEndpointLister,
	transport *GRPCTransport,
	configs ...FanoutConfig,
) (*BatchPlayerSender, error) {
	if presence == nil || directory == nil || fallback == nil || transport == nil {
		return nil, errors.New("server send: batch player sender dependencies are required")
	}
	maxEndpoints, err := fanoutMaxEndpoints(configs)
	if err != nil {
		return nil, err
	}
	return &BatchPlayerSender{
		presence:     presence,
		directory:    directory,
		fallback:     fallback,
		transport:    transport,
		maxEndpoints: maxEndpoints,
	}, nil
}

// SendToPlayers 接受非空批次。Redis route failure 只切換一次至 all-Gate DNS
// fan-out；gRPC 與 Gate delivery error 不會觸發 route switch。
func (s *BatchPlayerSender) SendToPlayers(ctx context.Context, messages []PlayerMessage) (Receipt, error) {
	if s == nil || s.presence == nil || s.directory == nil || s.fallback == nil || s.transport == nil {
		return Receipt{}, errors.New("server send: batch player sender is not configured")
	}
	if err := validatePlayerMessages(messages); err != nil {
		return Receipt{}, err
	}
	if err := validatePlayerEnvelopeSizes(messages); err != nil {
		return Receipt{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	messages = clonePlayerMessages(messages)

	loginNames := make([]LoginName, 0, len(messages))
	for _, message := range messages {
		loginNames = append(loginNames, message.LoginName)
	}
	presenceByLogin, presenceErr := s.presence.ResolveMany(ctx, loginNames)
	if shouldPlayerFallback(ctx, presenceErr) {
		return s.fallbackAll(ctx, messages, presenceErr)
	}
	if err := contextError(ctx, presenceErr); err != nil {
		return Receipt{}, err
	}
	var routeErrs []error
	if presenceErr != nil {
		routeErrs = append(routeErrs, presenceErr)
	} else {
		// 防禦不符合 contract、回傳 partial map + nil 的 custom resolver。
		for _, message := range messages {
			if _, exists := presenceByLogin[message.LoginName]; !exists {
				routeErrs = append(routeErrs, fmt.Errorf("%w: %q", ErrPresenceNotFound, message.LoginName))
			}
		}
	}

	gateIDs := make([]GateID, 0, len(presenceByLogin))
	for _, presence := range presenceByLogin {
		gateIDs = append(gateIDs, presence.GateID)
	}
	gateIDs = uniqueGateIDs(gateIDs)
	sort.Slice(gateIDs, func(i, j int) bool { return gateIDs[i] < gateIDs[j] })
	if len(gateIDs) == 0 {
		return Receipt{}, errors.Join(routeErrs...)
	}
	endpointByGate, endpointErr := s.directory.ResolveMany(ctx, gateIDs)
	if shouldPlayerFallback(ctx, endpointErr) {
		return s.fallbackAll(ctx, messages, endpointErr)
	}
	if err := contextError(ctx, endpointErr); err != nil {
		return Receipt{}, err
	}
	if endpointErr != nil {
		routeErrs = append(routeErrs, endpointErr)
	} else {
		// 防禦不符合 contract、回傳 partial map + nil 的 custom resolver。
		for _, presence := range presenceByLogin {
			if _, exists := endpointByGate[presence.GateID]; !exists {
				routeErrs = append(routeErrs, fmt.Errorf("%w: %q", ErrGateEndpointNotFound, presence.GateID))
			}
		}
	}

	plans, err := playerPlans(messages, presenceByLogin, endpointByGate)
	if err != nil {
		return Receipt{}, errors.Join(append(routeErrs, err)...)
	}
	accepted, sendErr := s.sendPlans(ctx, plans)
	if err := contextError(ctx, sendErr); err != nil {
		sendErr = errors.Join(sendErr, err)
	}
	allErrs := append(routeErrs, sendErr)
	if accepted {
		return newReceipt(), errors.Join(allErrs...)
	}
	return Receipt{}, errors.Join(allErrs...)
}

func clonePlayerMessages(messages []PlayerMessage) []PlayerMessage {
	cloned := make([]PlayerMessage, len(messages))
	for index, message := range messages {
		cloned[index] = message.clone()
	}
	return cloned
}

// validatePlayerEnvelopeSizes 在 routing 前檢查每個 protobuf envelope 的 wire
// size，避免 oversized item 先觸發 Redis 或 DNS I/O。
func validatePlayerEnvelopeSizes(messages []PlayerMessage) error {
	for index, message := range messages {
		size := playerDeliveryWireSize(message)
		if size > DefaultMaxPayloadBytes {
			return fmt.Errorf("player message %d: %w: encoded envelope is %d bytes", index, ErrPayloadTooLarge, size)
		}
	}
	return nil
}

func shouldPlayerFallback(ctx context.Context, err error) bool {
	return err != nil && ctx != nil && contextError(ctx, err) == nil && errors.Is(err, ErrRouteStoreUnavailable)
}

func contextError(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return nil
}

type playerEndpointPlan struct {
	endpoint GateEndpoint
	chunks   []Message
}

func playerPlans(messages []PlayerMessage, presenceByLogin map[LoginName]Presence, endpointByGate map[GateID]GateEndpoint) ([]playerEndpointPlan, error) {
	groupedMessages := make(map[string][]PlayerMessage)
	endpointByAddress := make(map[string]GateEndpoint)
	order := make([]string, 0)
	for _, message := range messages {
		presence, exists := presenceByLogin[message.LoginName]
		if !exists {
			continue
		}
		endpoint, exists := endpointByGate[presence.GateID]
		if !exists {
			continue
		}
		if _, exists := endpointByAddress[endpoint.Address]; !exists {
			endpointByAddress[endpoint.Address] = endpoint
			order = append(order, endpoint.Address)
		}
		groupedMessages[endpoint.Address] = append(groupedMessages[endpoint.Address], message)
	}
	plans := make([]playerEndpointPlan, 0, len(order))
	for _, address := range order {
		chunks, err := splitPlayerMessages(groupedMessages[address])
		if err != nil {
			return nil, fmt.Errorf("prepare player delivery for endpoint %q: %w", address, err)
		}
		plans = append(plans, playerEndpointPlan{endpoint: endpointByAddress[address], chunks: chunks})
	}
	return plans, nil
}

func splitPlayerMessages(messages []PlayerMessage) ([]Message, error) {
	if len(messages) == 0 {
		return nil, nil
	}
	chunks := make([]Message, 0, 1)
	current := make([]PlayerMessage, 0)
	currentSize := 0
	for _, message := range messages {
		itemSize := playerDeliveryWireSize(message)
		if itemSize > DefaultMaxPayloadBytes {
			return nil, fmt.Errorf("%w: player %q envelope is %d bytes", ErrPayloadTooLarge, message.LoginName, itemSize)
		}
		if len(current) > 0 && currentSize+itemSize > DefaultMaxPayloadBytes {
			encoded, err := marshalPlayerBatch(current)
			if err != nil {
				return nil, err
			}
			chunks = append(chunks, Message{CommandID: PlayerDeliveryCommandID, Payload: encoded})
			current = current[:0]
			currentSize = 0
		}
		current = append(current, message)
		currentSize += itemSize
	}
	if len(current) > 0 {
		encoded, err := marshalPlayerBatch(current)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, Message{CommandID: PlayerDeliveryCommandID, Payload: encoded})
	}
	return chunks, nil
}

func playerDeliveryWireSize(message PlayerMessage) int {
	item := &PlayerDelivery{
		LoginName:       string(message.LoginName),
		ClientCommandId: message.CommandID,
		ClientPayload:   message.Payload,
	}
	itemSize := proto.Size(item)
	// SendPlayersCommand.messages is field 1, so its key is one byte.
	return 1 + protowire.SizeVarint(uint64(itemSize)) + itemSize
}

func marshalPlayerBatch(messages []PlayerMessage) ([]byte, error) {
	command := &SendPlayersCommand{Messages: make([]*PlayerDelivery, 0, len(messages))}
	for _, message := range messages {
		command.Messages = append(command.Messages, &PlayerDelivery{
			LoginName:       string(message.LoginName),
			ClientCommandId: message.CommandID,
			ClientPayload:   append([]byte(nil), message.Payload...),
		})
	}
	encoded, err := proto.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("server send: encode player delivery batch: %w", err)
	}
	if len(encoded) > DefaultMaxPayloadBytes || proto.Size(command) != len(encoded) {
		return nil, fmt.Errorf("%w: player delivery batch is %d bytes", ErrPayloadTooLarge, len(encoded))
	}
	return encoded, nil
}

type planResult struct {
	accepted bool
	err      error
}

func (s *BatchPlayerSender) sendPlans(ctx context.Context, plans []playerEndpointPlan) (bool, error) {
	if len(plans) == 0 {
		return false, nil
	}
	results := make(chan planResult, len(plans))
	jobs := make(chan playerEndpointPlan)
	workerCount := min(defaultFanoutConcurrency, len(plans))
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for plan := range jobs {
				accepted, err := s.sendPlan(ctx, plan)
				results <- planResult{accepted: accepted, err: err}
			}
		}()
	}
scheduling:
	for _, plan := range plans {
		select {
		case jobs <- plan:
		case <-ctx.Done():
			break scheduling
		}
	}
	close(jobs)
	workers.Wait()
	close(results)
	accepted := false
	var errs []error
	for result := range results {
		accepted = accepted || result.accepted
		if result.err != nil {
			errs = append(errs, result.err)
		}
	}
	return accepted, errors.Join(errs...)
}

func (s *BatchPlayerSender) sendPlan(ctx context.Context, plan playerEndpointPlan) (bool, error) {
	accepted := false
	var errs []error
	for index, chunk := range plan.chunks {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if err := s.transport.Forward(ctx, plan.endpoint, chunk); err != nil {
			errs = append(errs, fmt.Errorf("send player batch to Gate %q endpoint %q chunk %d: %w", plan.endpoint.GateID, plan.endpoint.Address, index, err))
			continue
		}
		accepted = true
	}
	return accepted, errors.Join(errs...)
}

func (s *BatchPlayerSender) fallbackAll(ctx context.Context, messages []PlayerMessage, routeErr error) (Receipt, error) {
	endpoints, err := listGateEndpoints(ctx, s.fallback, s.maxEndpoints)
	if err != nil {
		return Receipt{}, errors.Join(routeErr, err)
	}
	chunks, err := splitPlayerMessages(messages)
	if err != nil {
		return Receipt{}, errors.Join(routeErr, err)
	}
	plans := make([]playerEndpointPlan, 0, len(endpoints))
	for _, endpoint := range endpoints {
		plans = append(plans, playerEndpointPlan{endpoint: endpoint, chunks: chunks})
	}
	accepted, sendErr := s.sendPlans(ctx, plans)
	if accepted && sendErr == nil {
		return newReceipt(), nil
	}
	if accepted {
		return newReceipt(), errors.Join(routeErr, sendErr)
	}
	return Receipt{}, errors.Join(routeErr, sendErr)
}

// FanoutSender 是 room broadcast 使用的 generic all-Gate command transport。
// Player fallback 共用 endpoint listing 與 Forward transport，但保留自己的
// batch routing policy。
type FanoutSender struct {
	directory    GateEndpointLister
	transport    *GRPCTransport
	maxEndpoints int
}

// FanoutConfig 限制 all-Gate endpoint 列舉數；0 使用預設值。
type FanoutConfig struct{ MaxEndpoints int }

func fanoutMaxEndpoints(configs []FanoutConfig) (int, error) {
	if len(configs) > 1 {
		return 0, fmt.Errorf("%w: only one fan-out config is supported", ErrDestinationInvalid)
	}
	maxEndpoints := DefaultMaxFanoutEndpoints
	if len(configs) == 1 {
		if configs[0].MaxEndpoints < 0 {
			return 0, fmt.Errorf("%w: max fan-out endpoints cannot be negative", ErrDestinationInvalid)
		}
		if configs[0].MaxEndpoints > 0 {
			maxEndpoints = configs[0].MaxEndpoints
		}
	}
	return maxEndpoints, nil
}

func NewFanoutSender(directory GateEndpointLister, transport *GRPCTransport, configs ...FanoutConfig) (*FanoutSender, error) {
	if directory == nil || transport == nil {
		return nil, errors.New("server send: fan-out sender dependencies are required")
	}
	maxEndpoints, err := fanoutMaxEndpoints(configs)
	if err != nil {
		return nil, err
	}
	return &FanoutSender{directory: directory, transport: transport, maxEndpoints: maxEndpoints}, nil
}

func (s *FanoutSender) Broadcast(ctx context.Context, message Message) (Receipt, error) {
	if s == nil || s.directory == nil || s.transport == nil {
		return Receipt{}, errors.New("server send: fan-out sender is not configured")
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
	message = message.clone()
	endpoints, err := listGateEndpoints(ctx, s.directory, s.maxEndpoints)
	if err != nil {
		return Receipt{}, err
	}
	results := fanoutEndpoints(ctx, endpoints, func(endpoint GateEndpoint) error {
		if err := s.transport.Forward(ctx, endpoint, message); err != nil {
			return fmt.Errorf("fan out command %d to Gate %q endpoint %q: %w", message.CommandID, endpoint.GateID, endpoint.Address, err)
		}
		return nil
	})
	var errs []error
	accepted := 0
	for _, result := range results {
		if result.err != nil {
			errs = append(errs, result.err)
		} else {
			accepted++
		}
	}
	if ctx.Err() != nil {
		errs = append(errs, ctx.Err())
	}
	if accepted == 0 {
		return Receipt{}, errors.Join(errs...)
	}
	return newReceipt(), errors.Join(errs...)
}

func listGateEndpoints(ctx context.Context, directory GateEndpointLister, maxEndpoints int) ([]GateEndpoint, error) {
	if directory == nil {
		return nil, errors.New("server send: Gate endpoint directory is not configured")
	}
	endpoints, err := directory.List(ctx)
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
		if maxEndpoints > 0 && len(unique) > maxEndpoints {
			return nil, fmt.Errorf("%w: %d endpoints exceeds %d", ErrFanoutLimitExceeded, len(unique), maxEndpoints)
		}
	}
	result := make([]GateEndpoint, 0, len(unique))
	for _, endpoint := range unique {
		result = append(result, endpoint)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Address < result[j].Address })
	return result, nil
}

type fanoutResult struct {
	endpoint GateEndpoint
	err      error
}

func fanoutEndpoints(ctx context.Context, endpoints []GateEndpoint, deliver func(GateEndpoint) error) []fanoutResult {
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
				results <- fanoutResult{endpoint: endpoint, err: deliver(endpoint)}
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

// FallbackBroadcastSender 在 Redis primary 失敗後執行一次 gRPC broadcast。
// Redis 結果不明時可能造成 duplicate frame。
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
var _ PlayerSender = (*BatchPlayerSender)(nil)
var _ BroadcastSender = (*FanoutSender)(nil)
var _ BroadcastSender = (*FallbackBroadcastSender)(nil)
