package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"log/slog"
)

func TestLoggerContractEmitsOperationalJSON(t *testing.T) {
	var output bytes.Buffer
	factory, err := newFactory("gate", Config{Level: "info"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	logger, err := factory.Component("websocket")
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := ContinueOrNew(context.Background(), "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if err != nil {
		t.Fatal(err)
	}
	logger.Error(ctx, "forward_game", "forward to Game failed", errors.New("deadline exceeded"), slog.String("command_id", "1003"))

	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &record); err != nil {
		t.Fatalf("log is not one JSON record: %v; output=%q", err, output.String())
	}
	for _, key := range []string{"timestamp", "level", "message", "service", "component", "operation", "trace_id", "span_id", "error", "command_id"} {
		if _, ok := record[key]; !ok {
			t.Fatalf("log field %q is missing: %s", key, output.String())
		}
	}
	if got, want := record["level"], "error"; got != want {
		t.Fatalf("level = %v, want %v", got, want)
	}
	if got, want := record["message"], "forward to Game failed"; got != want {
		t.Fatalf("message = %v, want %v", got, want)
	}
	if got, want := record["service"], "gate"; got != want {
		t.Fatalf("service = %v, want %v", got, want)
	}
	if got, want := record["component"], "websocket"; got != want {
		t.Fatalf("component = %v, want %v", got, want)
	}
	if got, want := record["operation"], "forward_game"; got != want {
		t.Fatalf("operation = %v, want %v", got, want)
	}
	if got, want := record["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736"; got != want {
		t.Fatalf("trace_id = %v, want %v", got, want)
	}
	if got, want := record["error"], "deadline exceeded"; got != want {
		t.Fatalf("error = %v, want %v", got, want)
	}
	if !strings.HasSuffix(output.String(), "\n") {
		t.Fatal("structured log does not end with one record newline")
	}
}

func TestLoggerContractHonorsLevelAndOperationFallback(t *testing.T) {
	var output bytes.Buffer
	factory, err := newFactory("game", Config{Level: "warn"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	logger, err := factory.Component("grpc.server")
	if err != nil {
		t.Fatal(err)
	}
	logger.Info(context.Background(), "ignored", "not emitted")
	logger.Warn(nil, "", "warning emitted")
	if strings.Contains(output.String(), "not emitted") {
		t.Fatal("info event passed warn level filter")
	}
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &record); err != nil {
		t.Fatalf("warning is not JSON: %v", err)
	}
	if got, want := record["operation"], "unknown"; got != want {
		t.Fatalf("empty operation = %v, want %v", got, want)
	}
	if _, ok := record["trace_id"]; ok {
		t.Fatal("lifecycle log unexpectedly contains trace_id")
	}
}

func TestFactoryContractRejectsInvalidIdentityAndLevel(t *testing.T) {
	if _, err := newFactory(" ", Config{Level: "info"}, nil); err == nil {
		t.Fatal("empty service was accepted")
	}
	if _, err := newFactory("gate", Config{Level: "verbose"}, nil); err == nil {
		t.Fatal("invalid level was accepted")
	}
	factory, err := newFactory("gate", Config{Level: "info"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := factory.Component(" "); err == nil {
		t.Fatal("empty component was accepted")
	}
}
