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

// GameModule 在 Game GateRequestChannel 註冊 Echo，並透過 direct
// request-player server-send contract 回傳結果。
func GameModule() framework.Module {
	return func(r framework.Registry) error {
		return r.Configure(func(commandDispatcher *dispatcher.Dispatcher, sender serversend.RequestPlayerSender) error {
			return commandDispatcher.Register(gameproduct.GateRequestChannel, dispatcher.CommandID(protocol.EchoRequestCommandID), func(ctx context.Context, payload []byte) error {
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
			})
		})
	}
}
