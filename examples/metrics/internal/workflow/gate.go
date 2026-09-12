package workflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/protocol"
	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gateproto"
	"github.com/NeoJay0705/gaming-core-casino/pkg/serversend"
	"github.com/NeoJay0705/gaming-core-casino/products/gateproduct"
	"google.golang.org/protobuf/proto"
)

// GateModule 註冊可啟動 metrics 範本使用的最小登入與進房 workflow。
// Authentication 刻意採 in-memory，不代表 production identity policy。
func GateModule() framework.Module {
	return func(r framework.Registry) error {
		return r.Configure(func(registry *gateproduct.SessionRegistry, commandDispatcher *dispatcher.Dispatcher) error {
			if err := commandDispatcher.Register(gateproduct.WebSocketChannel, dispatcher.CommandID(gateproto.LoginRequestCommandID), func(ctx context.Context, payload []byte) error {
				return loginHandler(registry, ctx, payload)
			}); err != nil {
				return err
			}
			if err := commandDispatcher.Register(gateproduct.WebSocketChannel, dispatcher.CommandID(protocol.EnterRoomRequestCommandID), func(ctx context.Context, payload []byte) error {
				return enterRoomHandler(registry, ctx, payload)
			}); err != nil {
				return err
			}
			if err := commandDispatcher.Register(serversend.RemoteCommandChannel, dispatcher.CommandID(protocol.BroadcastRoomCommandID), func(ctx context.Context, payload []byte) error {
				return broadcastRoomHandler(registry, ctx, payload)
			}); err != nil {
				return err
			}
			return commandDispatcher.Register(gateproduct.WebSocketChannel, dispatcher.CommandID(protocol.LocalEchoRequestCommandID), func(ctx context.Context, payload []byte) error {
				return localEchoHandler(registry, ctx, payload)
			})
		})
	}
}

func broadcastRoomHandler(registry *gateproduct.SessionRegistry, ctx context.Context, payload []byte) error {
	command := new(protocol.BroadcastRoomCommand)
	if err := proto.Unmarshal(payload, command); err != nil {
		return fmt.Errorf("decode broadcast room command: %w", err)
	}
	roomID := strings.TrimSpace(command.GetRoomId())
	if roomID == "" {
		return gateproduct.ErrRoomIDInvalid
	}
	if command.GetClientCommandId() == 0 {
		return fmt.Errorf("client_command_id is required")
	}
	if registry == nil {
		return fmt.Errorf("session registry is required")
	}
	packet := gateproduct.EncodeWebSocketPacket(gateproduct.WebSocketPacket{
		CommandID: command.GetClientCommandId(),
		Payload:   append([]byte(nil), command.GetClientPayload()...),
	})
	_, err := registry.BroadcastRoom(ctx, gateproduct.RoomID(roomID), packet)
	if err != nil {
		return fmt.Errorf("broadcast room %q: %w", roomID, err)
	}
	return nil
}

func loginHandler(registry *gateproduct.SessionRegistry, ctx context.Context, payload []byte) error {
	request := new(gateproto.LoginRequest)
	if err := proto.Unmarshal(payload, request); err != nil {
		return fmt.Errorf("decode login request: %w", err)
	}
	loginName := strings.TrimSpace(request.GetLoginName())
	if loginName == "" {
		return fmt.Errorf("login_name is required")
	}
	requestContext, ok := gateproduct.WebSocketRequestContextFrom(ctx)
	if !ok {
		return fmt.Errorf("websocket request context is required")
	}
	session, ok := requestContext.Session.(gateproduct.ClosableWebSocketSession)
	if !ok {
		return fmt.Errorf("closable websocket session is required")
	}
	// SessionRegistry 是本範本的唯一登入狀態來源；token 目前不宣稱具備
	// authentication 意義，正式服務應在自己的 module 替換此 policy。
	if registry == nil {
		return fmt.Errorf("session registry is required")
	}
	if err := registry.Register(session, gateproduct.LoginName(loginName)); err != nil {
		return err
	}
	response := &gateproto.LoginResponse{ServerTime: uint64(time.Now().Unix())}
	return sendResponse(ctx, requestContext, gateproto.LoginResponseCommandID, response)
}

func enterRoomHandler(registry *gateproduct.SessionRegistry, ctx context.Context, payload []byte) error {
	request := new(protocol.EnterRoomRequest)
	if err := proto.Unmarshal(payload, request); err != nil {
		return fmt.Errorf("decode enter-room request: %w", err)
	}
	roomID := strings.TrimSpace(request.GetRoomId())
	if roomID == "" {
		return gateproduct.ErrRoomIDInvalid
	}
	requestContext, ok := gateproduct.WebSocketRequestContextFrom(ctx)
	if !ok {
		return fmt.Errorf("websocket request context is required")
	}
	if registry == nil {
		return fmt.Errorf("session registry is required")
	}
	state, exists := registry.State(requestContext.Session.ID())
	if !exists || state.LoginName == "" {
		return gateproduct.ErrLoginRequired
	}
	if err := registry.EnterRoom(state.LoginName, gateproduct.RoomID(roomID)); err != nil {
		if errors.Is(err, gateproduct.ErrLoginSessionNotFound) {
			return gateproduct.ErrLoginRequired
		}
		return err
	}
	return sendResponse(ctx, requestContext, protocol.EnterRoomResponseCommandID, &protocol.EnterRoomResponse{})
}

func localEchoHandler(registry *gateproduct.SessionRegistry, ctx context.Context, payload []byte) error {
	request := new(protocol.EchoRequest)
	if err := proto.Unmarshal(payload, request); err != nil {
		return fmt.Errorf("decode local echo request: %w", err)
	}
	requestContext, ok := gateproduct.WebSocketRequestContextFrom(ctx)
	if !ok {
		return fmt.Errorf("websocket request context is required")
	}
	if registry == nil {
		return fmt.Errorf("session registry is required")
	}
	state, exists := registry.State(requestContext.Session.ID())
	if !exists || state.LoginName == "" {
		return gateproduct.ErrLoginRequired
	}
	if state.RoomID == "" {
		return gateproduct.ErrRoomRequired
	}
	response := &protocol.EchoResponse{Payload: append([]byte(nil), request.GetPayload()...)}
	return sendResponse(ctx, requestContext, protocol.LocalEchoResponseCommandID, response)
}

func sendResponse(ctx context.Context, request gateproduct.WebSocketRequestContext, commandID uint32, message proto.Message) error {
	payload, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	return request.Session.SendBinary(ctx, gateproduct.EncodeWebSocketPacket(gateproduct.WebSocketPacket{
		CommandID: commandID,
		Sequence:  request.Packet.Sequence,
		Session:   request.Packet.Session,
		Version:   request.Packet.Version,
		Payload:   payload,
	}))
}
