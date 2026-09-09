package gateproduct

import (
	"context"
	"errors"
	"fmt"

	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
)

// gateServerSendReceiver adapts Game-to-Gate delivery to the local WebSocket
// registry. It is intentionally the only layer that turns a server-send
// command into the player-facing WebSocket wire packet.
type gateServerSendReceiver struct{ sessions *SessionRegistry }

func newGateServerSendReceiver(sessions *SessionRegistry) (*gateServerSendReceiver, error) {
	if sessions == nil {
		return nil, errors.New("gate server send: session registry is nil")
	}
	return &gateServerSendReceiver{sessions: sessions}, nil
}

func (r *gateServerSendReceiver) SendToConnection(_ context.Context, connectionID serversend.ConnectionID, expectedLoginName serversend.LoginName, message serversend.Message) (serversend.DeliveryStatus, error) {
	if r == nil || r.sessions == nil {
		return serversend.DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED, errors.New("gate server send: receiver is not configured")
	}
	if err := r.sessions.SendToConnection(WebSocketConnectionID(connectionID), LoginName(expectedLoginName), encodeServerSendPacket(message)); err != nil {
		if errors.Is(err, serversend.ErrTargetNotConnected) {
			return serversend.DeliveryStatus_DELIVERY_STATUS_IGNORED, nil
		}
		return serversend.DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED, fmt.Errorf("deliver to connection %q: %w", connectionID, err)
	}
	return serversend.DeliveryStatus_DELIVERY_STATUS_DELIVERED, nil
}

func (r *gateServerSendReceiver) SendToPlayer(_ context.Context, message serversend.PlayerMessage) (serversend.DeliveryStatus, error) {
	if r == nil || r.sessions == nil {
		return serversend.DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED, errors.New("gate server send: receiver is not configured")
	}
	if err := r.sessions.SendToLoginName(LoginName(message.LoginName), encodeServerSendPacket(message.Message)); err != nil {
		if errors.Is(err, ErrLoginSessionNotFound) {
			return serversend.DeliveryStatus_DELIVERY_STATUS_IGNORED, nil
		}
		return serversend.DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED, fmt.Errorf("deliver to login %q: %w", message.LoginName, err)
	}
	return serversend.DeliveryStatus_DELIVERY_STATUS_DELIVERED, nil
}

func (r *gateServerSendReceiver) BroadcastRoom(_ context.Context, message serversend.BroadcastMessage) (int, error) {
	if r == nil || r.sessions == nil {
		return 0, errors.New("gate server send: receiver is not configured")
	}
	delivered, err := r.sessions.BroadcastRoom(RoomID(message.RoomID), encodeServerSendPacket(message.Message))
	if err != nil {
		return delivered, fmt.Errorf("broadcast room %q: %w", message.RoomID, err)
	}
	return delivered, nil
}

func encodeServerSendPacket(message serversend.Message) []byte {
	return encodeWebSocketPacket(WebSocketPacket{CommandID: message.CommandID, Payload: message.Payload})
}

var _ serversend.LocalReceiver = (*gateServerSendReceiver)(nil)
