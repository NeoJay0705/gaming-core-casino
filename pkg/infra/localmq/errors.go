package localmq

import (
	"errors"
	"fmt"
)

// PublishErrorKind 是 Publish 對呼叫端的可判斷結果分類。
type PublishErrorKind uint8

const (
	// PublishRejectedBeforeWrite 表示任何 record bytes 寫入前被拒絕。
	PublishRejectedBeforeWrite PublishErrorKind = iota + 1
	// PublishDurabilityUnknown 表示 write/sync 已開始，但結果無法確認。
	PublishDurabilityUnknown
)

// PublishError 保留 Publish 結果分類，避免呼叫端依 error string 判斷是否重試。
type PublishError struct {
	Kind  PublishErrorKind
	Cause error
}

func (e *PublishError) Error() string {
	if e == nil {
		return "localmq publish error"
	}
	if e.Cause == nil {
		return "localmq publish error"
	}
	return fmt.Sprintf("localmq publish: %v", e.Cause)
}

func (e *PublishError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// IsRejectedBeforeWrite 判斷是否可以用相同 message_id 安全重試。
func IsRejectedBeforeWrite(err error) bool {
	var pe *PublishError
	return errors.As(err, &pe) && pe.Kind == PublishRejectedBeforeWrite
}

// IsDurabilityUnknown 判斷 message 是否可能已寫入 WAL。
func IsDurabilityUnknown(err error) bool {
	var pe *PublishError
	return errors.As(err, &pe) && pe.Kind == PublishDurabilityUnknown
}

// PermanentError 將 handler error 分類為 poison/permanent error。
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string {
	if e == nil || e.Err == nil {
		return "localmq permanent handler error"
	}
	return fmt.Sprintf("localmq permanent handler error: %v", e.Err)
}

func (e *PermanentError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Permanent 將 error 標記為不可透過 retry 解決的 handler error。
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

// IsPermanent 判斷 handler 是否明確將錯誤分類為 poison/permanent。
func IsPermanent(err error) bool {
	var permanent *PermanentError
	return errors.As(err, &permanent)
}
