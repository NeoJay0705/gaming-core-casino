package gateproduct

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
)

// gateServerSendReceiver adapts Game-to-Gate delivery to the local WebSocket
// registry. It is intentionally the only layer that turns a server-send
// command into the player-facing WebSocket wire packet.
type gateServerSendReceiver struct {
	sessions *SessionRegistry
	metrics  *gateMetrics
}

func newGateServerSendReceiver(sessions *SessionRegistry, metrics ...*gateMetrics) (*gateServerSendReceiver, error) {
	if sessions == nil {
		return nil, errors.New("gate server send: session registry is nil")
	}
	var observed *gateMetrics
	if len(metrics) != 0 {
		observed = metrics[0]
	}
	return &gateServerSendReceiver{sessions: sessions, metrics: observed}, nil
}

func (r *gateServerSendReceiver) SendToConnection(_ context.Context, connectionID serversend.ConnectionID, expectedLoginName serversend.LoginName, message serversend.Message) (serversend.DeliveryStatus, error) {
	if r == nil || r.sessions == nil {
		return serversend.DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED, errors.New("gate server send: receiver is not configured")
	}
	receivedAt := time.Now()
	if err := r.sessions.sendToConnectionAt(WebSocketConnectionID(connectionID), LoginName(expectedLoginName), encodeServerSendPacket(message), receivedAt, serverSendTargetConnection); err != nil {
		if errors.Is(err, serversend.ErrTargetNotConnected) {
			r.metrics.observeServerSendRequest(string(serverSendTargetConnection), "ignored")
			return serversend.DeliveryStatus_DELIVERY_STATUS_IGNORED, nil
		}
		r.metrics.observeServerSendRequest(string(serverSendTargetConnection), "error")
		return serversend.DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED, fmt.Errorf("deliver to connection %q: %w", connectionID, err)
	}
	r.metrics.observeServerSendRequest(string(serverSendTargetConnection), "queued")
	return serversend.DeliveryStatus_DELIVERY_STATUS_DELIVERED, nil
}

func (r *gateServerSendReceiver) SendToPlayer(_ context.Context, message serversend.PlayerMessage) (serversend.DeliveryStatus, error) {
	if r == nil || r.sessions == nil {
		return serversend.DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED, errors.New("gate server send: receiver is not configured")
	}
	receivedAt := time.Now()
	if err := r.sessions.sendToLoginNameAt(LoginName(message.LoginName), encodeServerSendPacket(message.Message), receivedAt, serverSendTargetPlayer); err != nil {
		if errors.Is(err, ErrLoginSessionNotFound) {
			r.metrics.observeServerSendRequest(string(serverSendTargetPlayer), "ignored")
			return serversend.DeliveryStatus_DELIVERY_STATUS_IGNORED, nil
		}
		r.metrics.observeServerSendRequest(string(serverSendTargetPlayer), "error")
		return serversend.DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED, fmt.Errorf("deliver to login %q: %w", message.LoginName, err)
	}
	r.metrics.observeServerSendRequest(string(serverSendTargetPlayer), "queued")
	return serversend.DeliveryStatus_DELIVERY_STATUS_DELIVERED, nil
}

func (r *gateServerSendReceiver) BroadcastRoom(_ context.Context, message serversend.BroadcastMessage) (int, error) {
	if r == nil || r.sessions == nil {
		return 0, errors.New("gate server send: receiver is not configured")
	}
	receivedAt := time.Now()
	delivered, err := r.sessions.broadcastRoomAt(RoomID(message.RoomID), encodeServerSendPacket(message.Message), receivedAt, serverSendTargetRoom)
	if err != nil {
		result := "error"
		if delivered == 0 && errors.Is(err, ErrRoomIDInvalid) {
			result = "ignored"
		}
		r.metrics.observeServerSendRequest(string(serverSendTargetRoom), result)
		return delivered, fmt.Errorf("broadcast room %q: %w", message.RoomID, err)
	}
	result := "queued"
	if delivered == 0 {
		result = "ignored"
	}
	r.metrics.observeServerSendRequest(string(serverSendTargetRoom), result)
	return delivered, nil
}

func encodeServerSendPacket(message serversend.Message) []byte {
	return encodeWebSocketPacket(WebSocketPacket{CommandID: message.CommandID, Payload: message.Payload})
}

var _ serversend.LocalReceiver = (*gateServerSendReceiver)(nil)
