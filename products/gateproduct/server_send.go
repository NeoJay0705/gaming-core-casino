package gateproduct

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
)

// gateDeliveryReceiver adapts exact Game-to-Gate player delivery to the local
// WebSocket registry. Generic remote commands are handled by product modules.
type gateDeliveryReceiver struct {
	sessions *SessionRegistry
	metrics  *gateMetrics
}

func newGateDeliveryReceiver(sessions *SessionRegistry, metrics ...*gateMetrics) (*gateDeliveryReceiver, error) {
	if sessions == nil {
		return nil, errors.New("gate delivery: session registry is nil")
	}
	var observed *gateMetrics
	if len(metrics) != 0 {
		observed = metrics[0]
	}
	return &gateDeliveryReceiver{sessions: sessions, metrics: observed}, nil
}

func (r *gateDeliveryReceiver) SendToPlayer(_ context.Context, message serversend.PlayerMessage) (serversend.DeliveryStatus, error) {
	if r == nil || r.sessions == nil {
		return serversend.DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED, errors.New("gate delivery: receiver is not configured")
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

func encodeServerSendPacket(message serversend.Message) []byte {
	return encodeWebSocketPacket(WebSocketPacket{CommandID: message.CommandID, Payload: message.Payload})
}

var _ serversend.LocalReceiver = (*gateDeliveryReceiver)(nil)
