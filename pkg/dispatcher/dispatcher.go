// Package dispatcher provides transport-neutral command routing.
package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var ErrRegistrationInvalid = errors.New("dispatcher: invalid handler registration")

// Channel separates command spaces owned by different ingress paths.
type Channel string

// CommandID identifies one command within a Channel. Its underlying type
// matches the Gate binary protocol command field.
type CommandID uint32

// Handler receives the caller context and opaque command bytes.
type Handler func(context.Context, []byte) error

// Registration declares one command handler.
type Registration struct {
	Channel   Channel
	CommandID CommandID
	Handler   Handler
}

// Dispatcher routes an opaque command by channel and command ID. It is safe
// for concurrent registration and dispatch.
type Dispatcher struct {
	mu       sync.RWMutex
	handlers map[Channel]map[CommandID]Handler
}

func New() *Dispatcher { return &Dispatcher{handlers: make(map[Channel]map[CommandID]Handler)} }

func normalizeChannel(channel Channel) Channel {
	return Channel(strings.TrimSpace(string(channel)))
}

// Register adds one handler. Duplicate channel and command ID pairs are
// rejected so application wiring cannot silently override an existing route.
func (d *Dispatcher) Register(channel Channel, commandID CommandID, handler Handler) error {
	if d == nil {
		return fmt.Errorf("%w: dispatcher is nil", ErrRegistrationInvalid)
	}
	channel = normalizeChannel(channel)
	if channel == "" {
		return fmt.Errorf("%w: channel is required", ErrRegistrationInvalid)
	}
	if commandID == 0 {
		return fmt.Errorf("%w: command id is required", ErrRegistrationInvalid)
	}
	if handler == nil {
		return fmt.Errorf("%w: handler for %q command %d is nil", ErrRegistrationInvalid, channel, commandID)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.handlers == nil {
		d.handlers = make(map[Channel]map[CommandID]Handler)
	}
	byCommand := d.handlers[channel]
	if byCommand == nil {
		byCommand = make(map[CommandID]Handler)
		d.handlers[channel] = byCommand
	}
	if _, exists := byCommand[commandID]; exists {
		return fmt.Errorf("%w: handler for %q command %d is already registered", ErrRegistrationInvalid, channel, commandID)
	}
	byCommand[commandID] = handler
	return nil
}

// Dispatch invokes the matching handler. handled is false without an error
// when the channel has no matching command; each ingress owns that policy.
func (d *Dispatcher) Dispatch(ctx context.Context, channel Channel, commandID CommandID, payload []byte) (handled bool, err error) {
	if d == nil {
		return false, fmt.Errorf("%w: dispatcher is nil", ErrRegistrationInvalid)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	d.mu.RLock()
	handler := d.handlers[normalizeChannel(channel)][commandID]
	d.mu.RUnlock()
	if handler == nil {
		return false, nil
	}
	return true, handler(ctx, payload)
}

// RegisteredCommandIDs returns a sorted copy of the command IDs registered
// for channel. An empty dispatcher therefore remains observable and usable.
func (d *Dispatcher) RegisteredCommandIDs(channel Channel) []CommandID {
	if d == nil {
		return nil
	}
	d.mu.RLock()
	byCommand := d.handlers[normalizeChannel(channel)]
	ids := make([]CommandID, 0, len(byCommand))
	for commandID := range byCommand {
		ids = append(ids, commandID)
	}
	d.mu.RUnlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// IsRegistered reports whether a command is registered in channel. The
// lookup uses the same channel normalization as Register and Dispatch, so
// callers can use it as the trusted source for bounded command labels.
func (d *Dispatcher) IsRegistered(channel Channel, commandID CommandID) bool {
	if d == nil {
		return false
	}
	d.mu.RLock()
	_, ok := d.handlers[normalizeChannel(channel)][commandID]
	d.mu.RUnlock()
	return ok
}
