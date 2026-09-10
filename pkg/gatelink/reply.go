package gatelink

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrForwardReplyUnavailable 表示目前 context 沒有可寫入的 Forward reply
	// slot，或該 slot 已隨 handler lifecycle 結束。
	ErrForwardReplyUnavailable = errors.New("gatelink: forward reply is unavailable")
	// ErrForwardReplyAlreadySet 表示同一筆 Forward 已經設定過 reply。
	ErrForwardReplyAlreadySet = errors.New("gatelink: forward reply already set")
	// ErrForwardReplyInvalid 表示 reply 不符合最小 transport contract。
	ErrForwardReplyInvalid = errors.New("gatelink: forward reply is invalid")
)

type forwardReplySlotKey struct{}

// Reply 是 Gate-to-Game unary request 的 optional、單一回覆。
// ExpectedLoginName 非空時由 Gate 在 enqueue 前驗證目前 connection 身分。
type Reply struct {
	CommandID         uint32
	Payload           []byte
	ExpectedLoginName string
}

func (r Reply) clone() Reply {
	r.Payload = append([]byte(nil), r.Payload...)
	return r
}

// forwardReplySlot 只存在於一筆 gRPC handler lifecycle。其 mutex 同時保護
// reply pointer 與 closed 狀態，避免 handler 內意外 concurrent 呼叫造成兩筆回覆。
type forwardReplySlot struct {
	mu     sync.Mutex
	reply  *Reply
	closed bool
}

func newForwardReplySlot() *forwardReplySlot {
	return &forwardReplySlot{}
}

func withForwardReplySlot(ctx context.Context, slot *forwardReplySlot) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, forwardReplySlotKey{}, slot)
}

func forwardReplySlotFrom(ctx context.Context) (*forwardReplySlot, bool) {
	if ctx == nil {
		return nil, false
	}
	slot, ok := ctx.Value(forwardReplySlotKey{}).(*forwardReplySlot)
	return slot, ok && slot != nil
}

// SetForwardReply 是 serversend.RequestPlayerSender 使用的 transport bridge。
// 業務 handler 不應直接呼叫它；正常入口是 RequestPlayerSender interface。
func SetForwardReply(ctx context.Context, reply Reply) error {
	if reply.CommandID == 0 {
		return ErrForwardReplyInvalid
	}
	slot, ok := forwardReplySlotFrom(ctx)
	if !ok {
		return ErrForwardReplyUnavailable
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	reply = reply.clone()
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.closed {
		return ErrForwardReplyUnavailable
	}
	if slot.reply != nil {
		return ErrForwardReplyAlreadySet
	}
	slot.reply = &reply
	return nil
}

// finish 關閉 slot 並取出 reply。handler error 時 discard 必須為 true，
// 以確保失敗的 handler 不會把部分結果送回 Gate。
func (s *forwardReplySlot) finish(keep bool) *Reply {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if !keep || s.reply == nil {
		s.reply = nil
		return nil
	}
	reply := s.reply
	s.reply = nil
	return reply
}

func (s *forwardReplySlot) close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.closed = true
	s.reply = nil
	s.mu.Unlock()
}
