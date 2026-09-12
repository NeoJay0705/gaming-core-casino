package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

const (
	fieldTimestamp = "timestamp"
	fieldLevel     = "level"
	fieldMessage   = "message"
	fieldService   = "service"
	fieldComponent = "component"
	fieldOperation = "operation"
	fieldTraceID   = "trace_id"
	fieldSpanID    = "span_id"
	fieldError     = "error"
)

// Factory 建立同一個 process 內的 component-scoped Logger。Factory 使用
// immutable slog handler，不透過 package global logger，方便多個 App 在同一
// 個 test process 中隔離使用。
type Factory struct {
	base *slog.Logger
}

// Logger 是帶有固定 component 欄位的 structured logger。
type Logger struct {
	logger *slog.Logger
}

func newFactory(service string, cfg Config, writer io.Writer) (*Factory, error) {
	service = strings.TrimSpace(service)
	if service == "" {
		return nil, fmt.Errorf("logging service is required")
	}
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	level, err := levelFor(cfg.Level)
	if err != nil {
		return nil, err
	}
	if writer == nil {
		writer = os.Stdout
	}
	handler := slog.NewJSONHandler(writer, &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: replaceJSONAttr,
	})
	return &Factory{base: slog.New(handler).With(fieldService, service)}, nil
}

func replaceJSONAttr(groups []string, attr slog.Attr) slog.Attr {
	if len(groups) != 0 {
		return attr
	}
	switch attr.Key {
	case slog.TimeKey:
		attr.Key = fieldTimestamp
		if attr.Value.Kind() == slog.KindTime {
			// Production logs use one UTC representation regardless of the
			// process host timezone.
			attr.Value = slog.TimeValue(attr.Value.Time().UTC())
		}
	case slog.LevelKey:
		attr.Key = fieldLevel
		if attr.Value.Kind() == slog.KindAny {
			if level, ok := attr.Value.Any().(slog.Level); ok {
				attr.Value = slog.StringValue(strings.ToLower(level.String()))
			}
		}
	case slog.MessageKey:
		attr.Key = fieldMessage
	}
	return attr
}

// Component 建立固定 component 欄位的 logger。component 必須是穩定的
// module 名稱，不應放入 room、player 或 request-specific value。
func (f *Factory) Component(component string) (*Logger, error) {
	if f == nil || f.base == nil {
		return nil, fmt.Errorf("logging factory is nil")
	}
	component = strings.TrimSpace(component)
	if component == "" {
		return nil, fmt.Errorf("logging component is required")
	}
	return &Logger{logger: f.base.With(fieldComponent, component)}, nil
}

// Debug 記錄 debug-level event。
func (l *Logger) Debug(ctx context.Context, operation, message string, attrs ...slog.Attr) {
	l.log(ctx, slog.LevelDebug, operation, message, attrs...)
}

// Info 記錄 info-level event。
func (l *Logger) Info(ctx context.Context, operation, message string, attrs ...slog.Attr) {
	l.log(ctx, slog.LevelInfo, operation, message, attrs...)
}

// Warn 記錄 warn-level event。
func (l *Logger) Warn(ctx context.Context, operation, message string, attrs ...slog.Attr) {
	l.log(ctx, slog.LevelWarn, operation, message, attrs...)
}

// Error 記錄 error-level event。err 非 nil 時以穩定的 error 字串欄位輸出。
func (l *Logger) Error(ctx context.Context, operation, message string, err error, attrs ...slog.Attr) {
	if err != nil {
		attrs = append(attrs, slog.String(fieldError, err.Error()))
	}
	l.log(ctx, slog.LevelError, operation, message, attrs...)
}

func (l *Logger) log(ctx context.Context, level slog.Level, operation, message string, attrs ...slog.Attr) {
	if l == nil || l.logger == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	operation = strings.TrimSpace(operation)
	if operation == "" {
		// 維持固定 schema；呼叫端仍應提供明確 operation。
		operation = "unknown"
	}
	fixed := []slog.Attr{slog.String(fieldOperation, operation)}
	if traceID, spanID, ok := IDsFromContext(ctx); ok {
		fixed = append(fixed, slog.String(fieldTraceID, traceID), slog.String(fieldSpanID, spanID))
	}
	fixed = append(fixed, attrs...)
	l.logger.LogAttrs(ctx, level, message, fixed...)
}
