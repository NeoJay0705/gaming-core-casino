package logging

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
)

const traceParentHeader = "traceparent"

type requestContextKey struct{}

// RequestContext 是唯一由 logging package 放入 context 的 request-scoped
// trace state。欄位刻意不公開，讓未來可替換為 OpenTelemetry SpanContext，
// 而不讓 business code 依賴 ID 儲存方式。
type RequestContext struct {
	traceID [16]byte
	spanID  [8]byte
	flags   byte
}

// NewRoot 為一個獨立 request 建立新的 W3C trace root。ctx 中既有的
// RequestContext 會被覆蓋；其他 context values 保留。
func NewRoot(ctx context.Context) (context.Context, error) {
	return newRoot(ctx, rand.Reader)
}

// newRoot 接受 private entropy dependency，讓 failure contract 可測試而不
// 讓 production hot path 透過可變的 package-global reader 或 mutex 序列化。
func newRoot(ctx context.Context, reader io.Reader) (context.Context, error) {
	var traceID [16]byte
	err := fillRandom(reader, traceID[:])
	if err != nil {
		return nil, fmt.Errorf("logging: generate trace id: %w", err)
	}
	var spanID [8]byte
	err = fillRandom(reader, spanID[:])
	if err != nil {
		return nil, fmt.Errorf("logging: generate span id: %w", err)
	}
	return withRequestContext(ctx, RequestContext{traceID: traceID, spanID: spanID}), nil
}

// ContinueOrNew 驗證 remote traceparent 並建立本地 span。缺少或不合法的
// parent 不會拒絕 business request，而是建立新的 root trace。
func ContinueOrNew(ctx context.Context, traceparent string) (context.Context, error) {
	parent, ok := parseTraceParent(traceparent)
	if !ok {
		return NewRoot(ctx)
	}
	var spanID [8]byte
	err := fillRandom(rand.Reader, spanID[:])
	if err != nil {
		return nil, fmt.Errorf("logging: generate span id: %w", err)
	}
	parent.spanID = spanID
	return withRequestContext(ctx, parent), nil
}

// Child 建立目前 trace 的下一個 local span。ctx 沒有 trace 時直接建立
// root context，讓 outbound call 仍能帶有可用的 traceparent。
func Child(ctx context.Context) (context.Context, error) {
	current, ok := requestContextFrom(ctx)
	if !ok {
		return NewRoot(ctx)
	}
	var spanID [8]byte
	err := fillRandom(rand.Reader, spanID[:])
	if err != nil {
		return nil, fmt.Errorf("logging: generate child span id: %w", err)
	}
	current.spanID = spanID
	return withRequestContext(ctx, current), nil
}

// Detach 只保留 logging RequestContext，移除 cancellation、deadline 與
// 其他 request graph，供 writer／subscriber 等 async boundary 使用。
func Detach(ctx context.Context) context.Context {
	value, ok := requestContextFrom(ctx)
	if !ok {
		return context.Background()
	}
	return context.WithValue(context.Background(), requestContextKey{}, value)
}

// TraceParentFromContext 回傳 canonical W3C version-00 traceparent。
func TraceParentFromContext(ctx context.Context) (string, bool) {
	value, ok := requestContextFrom(ctx)
	if !ok {
		return "", false
	}
	return formatTraceParent(value), true
}

// IDsFromContext 回傳 lowercase hexadecimal trace/span IDs，供 structured
// logger 與測試使用。
func IDsFromContext(ctx context.Context) (traceID, spanID string, ok bool) {
	value, ok := requestContextFrom(ctx)
	if !ok {
		return "", "", false
	}
	return hex.EncodeToString(value.traceID[:]), hex.EncodeToString(value.spanID[:]), true
}

func requestContextFrom(ctx context.Context) (RequestContext, bool) {
	if ctx == nil {
		return RequestContext{}, false
	}
	value, ok := ctx.Value(requestContextKey{}).(RequestContext)
	if !ok || isZeroRequestContext(value) {
		return RequestContext{}, false
	}
	return value, true
}

func withRequestContext(ctx context.Context, value RequestContext) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requestContextKey{}, value)
}

func isZeroRequestContext(value RequestContext) bool {
	return value.traceID == [16]byte{} || value.spanID == [8]byte{}
}

func parseTraceParent(value string) (RequestContext, bool) {
	// version 00 的 canonical traceparent 固定為 55 個字元，且 W3C
	// HEXDIGLC 僅允許 lowercase hex；不替輸入值 trim 或正規化。
	if len(value) != 55 || value != strings.ToLower(value) {
		return RequestContext{}, false
	}
	parts := strings.Split(value, "-")
	if len(parts) != 4 || parts[0] != "00" || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return RequestContext{}, false
	}
	var result RequestContext
	if _, err := hex.Decode(result.traceID[:], []byte(parts[1])); err != nil {
		return RequestContext{}, false
	}
	if _, err := hex.Decode(result.spanID[:], []byte(parts[2])); err != nil {
		return RequestContext{}, false
	}
	flags, err := hex.DecodeString(parts[3])
	if err != nil || len(flags) != 1 || isZeroBytes(result.traceID[:]) || isZeroBytes(result.spanID[:]) {
		return RequestContext{}, false
	}
	result.flags = flags[0]
	return result, true
}

func formatTraceParent(value RequestContext) string {
	return "00-" + hex.EncodeToString(value.traceID[:]) + "-" + hex.EncodeToString(value.spanID[:]) + "-" + fmt.Sprintf("%02x", value.flags)
}

func isZeroBytes(value []byte) bool {
	for _, item := range value {
		if item != 0 {
			return false
		}
	}
	return true
}

func fillRandom(reader io.Reader, result []byte) error {
	if reader == nil {
		return errors.New("entropy reader is nil")
	}
	if _, err := io.ReadFull(reader, result[:]); err != nil {
		return err
	}
	if isZeroBytes(result) {
		return errors.New("random value is zero")
	}
	return nil
}
