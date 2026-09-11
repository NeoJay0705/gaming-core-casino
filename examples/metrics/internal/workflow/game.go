package workflow

import (
	"context"
	"fmt"

	"github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/protocol"
	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/NeoJay0705/gaming-core-casino/products/gameproduct"
	"google.golang.org/protobuf/proto"
)

// GameModule 在 Game GateRequestChannel 註冊 Echo 與 room broadcast producer。
// Echo 透過 direct request-player contract 回傳；broadcast 則透過
// BroadcastSender 發送至 Gate。
func GameModule() framework.Module {
	return func(r framework.Registry) error {
		return r.Configure(func(commandDispatcher *dispatcher.Dispatcher, sender serversend.RequestPlayerSender, broadcastSender serversend.BroadcastSender) error {
			if broadcastSender == nil {
				return fmt.Errorf("broadcast sender is required")
			}
			if err := commandDispatcher.Register(gameproduct.GateRequestChannel, dispatcher.CommandID(protocol.EchoRequestCommandID), func(ctx context.Context, payload []byte) error {
				request := new(protocol.EchoRequest)
				if err := proto.Unmarshal(payload, request); err != nil {
					return fmt.Errorf("decode echo request: %w", err)
				}
				responsePayload, err := proto.Marshal(&protocol.EchoResponse{Payload: append([]byte(nil), request.GetPayload()...)})
				if err != nil {
					return fmt.Errorf("encode echo response: %w", err)
				}
				if _, err := sender.SendToRequestPlayer(ctx, serversend.RequestPlayerMessage{Message: serversend.Message{
					CommandID: protocol.EchoResponseCommandID,
					Payload:   responsePayload,
				}}); err != nil {
					return fmt.Errorf("send echo response: %w", err)
				}
				return nil
			}); err != nil {
				return err
			}
			return commandDispatcher.Register(gameproduct.GateRequestChannel, dispatcher.CommandID(protocol.BroadcastRoomCommandID), func(ctx context.Context, payload []byte) error {
				command := new(protocol.BroadcastRoomCommand)
				if err := proto.Unmarshal(payload, command); err != nil {
					return fmt.Errorf("decode broadcast room command: %w", err)
				}
				if _, err := BroadcastRoom(ctx, broadcastSender, command.GetRoomId(), command.GetClientCommandId(), command.GetClientPayload()); err != nil {
					return fmt.Errorf("broadcast room: %w", err)
				}
				return nil
			})
		})
	}
}

// BroadcastRoom 封裝範例 Game producer 的最小 room command。outer command
// 由 Gate handler 解碼；clientPayload 在 transport 與 Gate handler 內保持
// opaque bytes。
func BroadcastRoom(ctx context.Context, sender serversend.BroadcastSender, roomID string, clientCommandID uint32, clientPayload []byte) (serversend.Receipt, error) {
	if sender == nil {
		return serversend.Receipt{}, fmt.Errorf("broadcast sender is required")
	}
	if roomID == "" {
		return serversend.Receipt{}, fmt.Errorf("room id is required")
	}
	if clientCommandID == 0 {
		return serversend.Receipt{}, fmt.Errorf("client command id is required")
	}
	payload, err := proto.Marshal(&protocol.BroadcastRoomCommand{
		RoomId:          roomID,
		ClientCommandId: clientCommandID,
		ClientPayload:   append([]byte(nil), clientPayload...),
	})
	if err != nil {
		return serversend.Receipt{}, fmt.Errorf("encode broadcast room command: %w", err)
	}
	return sender.Broadcast(ctx, serversend.Message{CommandID: protocol.BroadcastRoomCommandID, Payload: payload})
}
