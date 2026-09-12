package gateproduct

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"google.golang.org/protobuf/proto"
)

// registerGatePlayerDeliveryCommand 在 Gate Forward 與 Redis Pub/Sub ingress
// 共用的 dispatcher namespace 註冊 framework-owned player batch command。
func registerGatePlayerDeliveryCommand(registry *SessionRegistry, metrics *gateMetrics, commandDispatcher *dispatcher.Dispatcher) error {
	if registry == nil {
		return errors.New("gate player delivery: session registry is nil")
	}
	if commandDispatcher == nil {
		return errors.New("gate player delivery: dispatcher is nil")
	}
	return commandDispatcher.Register(serversend.RemoteCommandChannel, dispatcher.CommandID(serversend.PlayerDeliveryCommandID), func(ctx context.Context, payload []byte) error {
		return handlePlayerDeliveryCommand(registry, metrics, ctx, payload)
	})
}

// handlePlayerDeliveryCommand 只解碼 framework routing envelope。每個
// client_payload 保持 opaque，完成 login-name lookup 後複製到既有 WebSocket
// packet format。
func handlePlayerDeliveryCommand(registry *SessionRegistry, metrics *gateMetrics, _ context.Context, payload []byte) error {
	if registry == nil {
		return errors.New("gate player delivery: session registry is nil")
	}
	if len(payload) > serversend.DefaultMaxPayloadBytes {
		return fmt.Errorf("%w: player delivery batch is %d bytes", serversend.ErrPayloadTooLarge, len(payload))
	}
	command := new(serversend.SendPlayersCommand)
	if err := proto.Unmarshal(payload, command); err != nil {
		return fmt.Errorf("%w: decode player delivery command: %v", serversend.ErrMessageInvalid, err)
	}
	if len(command.GetMessages()) == 0 {
		return fmt.Errorf("%w: player delivery batch is empty", serversend.ErrMessageInvalid)
	}

	receivedAt := time.Now()
	var errs []error
	for index, delivery := range command.GetMessages() {
		if delivery == nil {
			errs = append(errs, fmt.Errorf("%w: player delivery item %d is nil", serversend.ErrMessageInvalid, index))
			continue
		}
		message := serversend.PlayerMessage{
			LoginName: serversend.LoginName(delivery.GetLoginName()),
			Message: serversend.Message{
				CommandID: delivery.GetClientCommandId(),
				Payload:   append([]byte(nil), delivery.GetClientPayload()...),
			},
		}
		if err := message.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("player delivery item %d: %w", index, err))
			continue
		}
		if len(message.Payload) > serversend.DefaultMaxPayloadBytes {
			errs = append(errs, fmt.Errorf("player delivery item %d: %w", index, serversend.ErrPayloadTooLarge))
			continue
		}
		packet := encodeServerSendPacket(message.Message)
		if err := registry.sendToLoginNameAt(LoginName(message.LoginName), packet, receivedAt, serverSendTargetPlayer); err != nil {
			if errors.Is(err, ErrLoginSessionNotFound) {
				if metrics != nil {
					metrics.observeServerSendRequest(string(serverSendTargetPlayer), "ignored")
				}
				continue
			}
			if metrics != nil {
				metrics.observeServerSendRequest(string(serverSendTargetPlayer), "error")
			}
			errs = append(errs, fmt.Errorf("deliver player %q: %w", message.LoginName, err))
			continue
		}
		if metrics != nil {
			metrics.observeServerSendRequest(string(serverSendTargetPlayer), "queued")
		}
	}
	return errors.Join(errs...)
}

func encodeServerSendPacket(message serversend.Message) []byte {
	return encodeWebSocketPacket(WebSocketPacket{CommandID: message.CommandID, Payload: message.Payload})
}
