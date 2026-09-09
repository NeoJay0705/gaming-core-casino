package serversend

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrMessageInvalid indicates a malformed client-facing message.
	ErrMessageInvalid = errors.New("server send: message is invalid")
	// ErrDestinationInvalid indicates an incomplete or malformed destination.
	ErrDestinationInvalid = errors.New("server send: destination is invalid")
	// ErrRequestRouteUnavailable indicates that an inbound Gate request did not
	// carry a route back to its source Gate connection.
	ErrRequestRouteUnavailable = errors.New("server send: request route is unavailable")
	// ErrRequestRouteInvalid indicates a request route that cannot be used for
	// direct delivery.
	ErrRequestRouteInvalid = errors.New("server send: request route is invalid")
	// ErrPresenceNotFound indicates that no current Gate owner exists for a
	// login name.
	ErrPresenceNotFound = errors.New("server send: player presence is not found")
	// ErrPresenceNotOwner indicates a stale renew or release attempt.
	ErrPresenceNotOwner = errors.New("server send: presence lease is not owned")
	// ErrRouteStoreUnavailable indicates that Redis or another route store is
	// unavailable. Callers must decide explicitly whether a retry is safe.
	ErrRouteStoreUnavailable = errors.New("server send: route store is unavailable")
	// ErrTargetNotConnected indicates that a Gate does not currently own the
	// requested local connection, player, or room.
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

// RoomID identifies a caller-authorized room audience. It is opaque for the
// same reason as LoginName.
type RoomID string

// GateID identifies one running Gate instance.
type GateID string

// ConnectionID identifies one accepted player connection at a Gate.
type ConnectionID string

// Message is a client-facing command. Gate owns framing this command as a
// WebSocket packet; callers only provide the opaque business payload.
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

func normalizedPayloadLimit(value int) (int, error) {
	if value < 0 {
		return 0, fmt.Errorf("%w: max payload bytes cannot be negative", ErrDestinationInvalid)
	}
	if value == 0 {
		return DefaultMaxPayloadBytes, nil
	}
	return value, nil
}

func (m Message) clone() Message {
	m.Payload = append([]byte(nil), m.Payload...)
	return m
}

// PlayerMessage targets the current authoritative session of LoginName.
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

// RequestPlayerMessage targets the exact connection that originated a request.
// ExpectedLoginName is optional for pre-login replies. When present, Gate must
// verify that the connection is still authenticated as that identity.
type RequestPlayerMessage struct {
	ExpectedLoginName LoginName
	Message
}

// Validate checks the client message. The route itself is validated from ctx.
func (m RequestPlayerMessage) Validate() error { return m.Message.Validate() }

func (m RequestPlayerMessage) validatePayload(maxBytes int) error {
	return m.Message.validatePayload(maxBytes)
}

func (m RequestPlayerMessage) clone() RequestPlayerMessage {
	m.Message = m.Message.clone()
	return m
}

// BroadcastMessage targets the local members of one room at every Gate.
type BroadcastMessage struct {
	RoomID RoomID
	Message
}

// Validate checks a room broadcast target and its message.
func (m BroadcastMessage) Validate() error {
	if m.RoomID == "" {
		return fmt.Errorf("%w: room id is required", ErrDestinationInvalid)
	}
	return m.Message.Validate()
}

func (m BroadcastMessage) validatePayload(maxBytes int) error {
	if err := m.Validate(); err != nil {
		return err
	}
	return m.Message.validatePayload(maxBytes)
}

func (m BroadcastMessage) clone() BroadcastMessage {
	m.Message = m.Message.clone()
	return m
}

// Receipt records that the sender accepted a message for its documented
// delivery path. It does not mean a browser has received the WebSocket frame.
type Receipt struct{ AcceptedAt time.Time }

func newReceipt() Receipt { return Receipt{AcceptedAt: time.Now()} }

// RequestPlayerSender sends to the Gate connection described by the current
// request context. It must not use player presence or broadcast fan-out.
type RequestPlayerSender interface {
	SendToRequestPlayer(context.Context, RequestPlayerMessage) (Receipt, error)
}

// PlayerSender sends to the Gate that currently owns a player. Route lookup
// failures are returned; this API never turns a private message into a
// broadcast.
type PlayerSender interface {
	SendToPlayer(context.Context, PlayerMessage) (Receipt, error)
}

// BroadcastSender sends a room message to every Gate through one explicitly
// selected transport.
type BroadcastSender interface {
	Broadcast(context.Context, BroadcastMessage) (Receipt, error)
}
