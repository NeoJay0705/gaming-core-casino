package localmq

import (
	"context"
	"time"
)

// Message 是 producer 要寫入 WAL 的 opaque event。
type Message struct {
	Topic         string
	MessageID     []byte
	EventType     string
	SchemaVersion uint32
	Payload       []byte
}

// Receipt 指出成功寫入的 lane 與 sequence。
type Receipt struct {
	Topic    string
	LaneID   string
	Sequence uint64
}

// Subscription 指定一個 consumer 要處理的 durable Topic/Group。
type Subscription struct {
	Topic string
	Group string
}

// GroupType 決定 Group 是否參與 retention barrier。
type GroupType string

const (
	GroupTypeProtected  GroupType = "PROTECTED"
	GroupTypeBestEffort GroupType = "BEST_EFFORT"
)

// InitialPosition 只在 CreateGroup 時使用。
type InitialPosition string

const (
	InitialEarliest InitialPosition = "EARLIEST"
	InitialLatest   InitialPosition = "LATEST"
)

// GroupState 是 durable group lifecycle。
type GroupState string

const (
	GroupInitializing GroupState = "INITIALIZING"
	GroupActive       GroupState = "ACTIVE"
	GroupDeleting     GroupState = "DELETING"
	GroupDeleted      GroupState = "DELETED"
)

// Publisher 是 producer hot path 使用的最小介面。
type Publisher interface {
	Publish(context.Context, Message) (Receipt, error)
}

// DeliveredMessage 是 consumer handler 收到的 immutable event view。
type DeliveredMessage struct {
	Sequence      uint64
	AppendTime    time.Time
	MessageID     []byte
	EventType     string
	SchemaVersion uint32
	Payload       []byte
}

// Batch 只包含同一 topic/lane 內連續 sequence 的 messages。
type Batch struct {
	Topic    string
	LaneID   string
	Messages []DeliveredMessage
}

// Handler 是 product-owned downstream adapter。Handler 必須遵守 context，
// 並能安全處理同一 message 的 sequential/concurrent duplicate。
type Handler interface {
	Handle(context.Context, Batch) error
}

// HandlerFunc 讓簡單的 consumer 可以使用 function 實作 Handler。
type HandlerFunc func(context.Context, Batch) error

func (f HandlerFunc) Handle(ctx context.Context, batch Batch) error {
	if f == nil {
		return nil
	}
	return f(ctx, batch)
}
