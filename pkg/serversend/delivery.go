package serversend

import (
	"context"
	"errors"
	"fmt"

	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
)

// RemoteCommandChannel 是 Gate gRPC Forward 與 Redis Pub/Sub ingress 共用的
// dispatcher namespace。它不描述 room、kick 或其他業務語意。
const RemoteCommandChannel dispatcher.Channel = "gate-remote-command"

var (
	ErrCommandNotRegistered  = errors.New("server send: command is not registered")
	ErrDispatcherUnavailable = errors.New("server send: remote command dispatcher is unavailable")
)

// dispatchRemoteCommand 將 generic command 交給產品註冊的 handler。transport
// 只驗證 command envelope，不解碼 payload，也不建立本機 delivery result slot。
func dispatchRemoteCommand(ctx context.Context, commandDispatcher *dispatcher.Dispatcher, message Message) (bool, error) {
	if err := message.validatePayload(DefaultMaxPayloadBytes); err != nil {
		return false, err
	}
	if commandDispatcher == nil {
		return false, ErrDispatcherUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	handled, err := commandDispatcher.Dispatch(
		ctx,
		RemoteCommandChannel,
		dispatcher.CommandID(message.CommandID),
		append([]byte(nil), message.Payload...),
	)
	if err != nil {
		return handled, err
	}
	if !handled {
		return false, fmt.Errorf("%w: command %d", ErrCommandNotRegistered, message.CommandID)
	}
	return true, nil
}
