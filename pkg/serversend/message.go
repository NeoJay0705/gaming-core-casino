package serversend

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// DefaultMaxPayloadBytes is the fixed logical payload contract shared by all
// delivery boundaries. Transport-specific wire limits remain grpc-go's
// responsibility and are intentionally separate.
const DefaultMaxPayloadBytes = 1 << 20

// PlayerDeliveryCommandID is reserved by the framework on RemoteCommandChannel
// for the batch player-delivery envelope. Products must not register a
// different handler for this ID.
const PlayerDeliveryCommandID uint32 = 0xC00010

var (
	// ErrMessageInvalid indicates a malformed client-facing message.
	ErrMessageInvalid = errors.New("server send: message is invalid")
	// ErrDestinationInvalid indicates an incomplete or malformed destination.
	ErrDestinationInvalid = errors.New("server send: destination is invalid")
	// ErrRequestRouteUnavailable indicates that the inbound Gate request has no
	// active unary reply slot.
	ErrRequestRouteUnavailable = errors.New("server send: request route is unavailable")
	// ErrRequestRouteInvalid is retained for compatibility with callers that
	// classify the former direct-route error.
	ErrRequestRouteInvalid = errors.New("server send: request route is invalid")
	// ErrRequestReplyAlreadySet indicates that the current Gate request already
	// accepted its one request-player reply.
	ErrRequestReplyAlreadySet = errors.New("server send: request reply is already set")
	// ErrPresenceNotFound indicates that no current Gate owner exists for a
	// login name.
	ErrPresenceNotFound = errors.New("server send: player presence is not found")
	// ErrPresenceNotOwner indicates a stale renew or release attempt.
	ErrPresenceNotOwner = errors.New("server send: presence lease is not owned")
	// ErrRouteStoreUnavailable indicates that Redis or another route store is
	// unavailable. Callers must decide explicitly whether a retry is safe.
	ErrRouteStoreUnavailable = errors.New("server send: route store is unavailable")
	// ErrTargetNotConnected indicates that a Gate does not currently own the
	// requested local player session.
	ErrTargetNotConnected = errors.New("server send: target is not connected")
	// ErrPayloadTooLarge indicates that a delivery payload exceeds the shared
	// transport limit.
	ErrPayloadTooLarge = errors.New("server send: payload is too large")
	// ErrFanoutLimitExceeded indicates that a gRPC fan-out would exceed its
	// configured endpoint bound.
	ErrFanoutLimitExceeded = errors.New("server send: fan-out endpoint limit exceeded")
)

// LoginName is a caller-authenticated canonical player identity. It is opaque:
// this package does not trim, case-fold, or otherwise normalize it.
type LoginName string

// GateID identifies one running Gate instance.
type GateID string

// ConnectionID identifies one accepted player connection at a Gate.
type ConnectionID string

// Message 是 opaque command。Gate product handler 可將 business payload 編成
// WebSocket packet；此 transport 不解讀任一欄位。
type Message struct {
	CommandID uint32
	Payload   []byte
}

// Validate checks the minimum client delivery contract.
func (m Message) Validate() error {
	if m.CommandID == 0 {
		return fmt.Errorf("%w: command id is required", ErrMessageInvalid)
	}
	return nil
}

func (m Message) validatePayload(maxBytes int) error {
	if err := m.Validate(); err != nil {
		return err
	}
	if maxBytes > 0 && len(m.Payload) > maxBytes {
		return fmt.Errorf("%w: %d bytes exceeds %d", ErrPayloadTooLarge, len(m.Payload), maxBytes)
	}
	return nil
}

func (m Message) clone() Message {
	m.Payload = append([]byte(nil), m.Payload...)
	return m
}

// PlayerMessage targets the current authoritative session of LoginName. The
// command payload remains opaque to this package and to the Gate transport.
type PlayerMessage struct {
	LoginName LoginName
	Message
}

// Validate checks a player delivery target and its message.
func (m PlayerMessage) Validate() error {
	if m.LoginName == "" {
		return fmt.Errorf("%w: login name is required", ErrDestinationInvalid)
	}
	return m.Message.Validate()
}

func (m PlayerMessage) validatePayload(maxBytes int) error {
	if err := m.Validate(); err != nil {
		return err
	}
	return m.Message.validatePayload(maxBytes)
}

func (m PlayerMessage) clone() PlayerMessage {
	m.Message = m.Message.clone()
	return m
}

func validatePlayerMessages(messages []PlayerMessage) error {
	if len(messages) == 0 {
		return fmt.Errorf("%w: at least one player message is required", ErrMessageInvalid)
	}
	for index, message := range messages {
		if err := message.validatePayload(DefaultMaxPayloadBytes); err != nil {
			return fmt.Errorf("player message %d: %w", index, err)
		}
	}
	return nil
}

// RequestPlayerMessage targets the exact connection that originated a request.
// ExpectedLoginName is optional for pre-login replies. When present, Gate must
// verify that the connection is still authenticated as that identity.
type RequestPlayerMessage struct {
	ExpectedLoginName LoginName
	Message
}

// Validate checks the client message. RequestPlayerSender obtains its
// destination from the active unary reply slot in ctx.
func (m RequestPlayerMessage) Validate() error { return m.Message.Validate() }

func (m RequestPlayerMessage) validatePayload(maxBytes int) error {
	return m.Message.validatePayload(maxBytes)
}

func (m RequestPlayerMessage) clone() RequestPlayerMessage {
	m.Message = m.Message.clone()
	return m
}

// Receipt records that the sender accepted a message for its documented
// delivery path. For RequestPlayerSender, it means the message was accepted
// into the current Gate-to-Game unary response; it does not mean Gate queued or
// a browser received the WebSocket frame.
type Receipt struct{ AcceptedAt time.Time }

func newReceipt() Receipt { return Receipt{AcceptedAt: time.Now()} }

// RequestPlayerSender accepts at most one reply for the current Gate request.
// The framework returns that reply through the original Gate-to-Game unary
// response; it must not use player presence, endpoint routing, or fan-out.
type RequestPlayerSender interface {
	SendToRequestPlayer(context.Context, RequestPlayerMessage) (Receipt, error)
}

// PlayerSender sends a batch to the Gates that currently own the players. The
// implementation may group messages by endpoint, but it never turns a
// private message into a room broadcast.
type PlayerSender interface {
	SendToPlayers(context.Context, []PlayerMessage) (Receipt, error)
}

// BroadcastSender 透過明確選用的 transport 將一筆 opaque command 傳給所有
// Gate。command 的業務語意與 target 由已註冊的 Gate handler 擁有，而非本 package。
type BroadcastSender interface {
	Broadcast(context.Context, Message) (Receipt, error)
}
