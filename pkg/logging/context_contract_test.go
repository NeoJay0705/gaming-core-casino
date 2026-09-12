package logging

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
)

const contractTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func TestTraceContextContractContinuesW3CParentAndCreatesChild(t *testing.T) {
	ctx, err := ContinueOrNew(context.Background(), contractTraceparent)
	if err != nil {
		t.Fatal(err)
	}
	traceID, spanID, ok := IDsFromContext(ctx)
	if !ok {
		t.Fatal("continued context has no IDs")
	}
	if traceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("trace ID = %q", traceID)
	}
	parent, ok := TraceParentFromContext(ctx)
	if !ok || !strings.HasPrefix(parent, "00-4bf92f3577b34da6a3ce929d0e0e4736-") || !strings.HasSuffix(parent, "-01") {
		t.Fatalf("continued traceparent = %q", parent)
	}

	child, err := Child(ctx)
	if err != nil {
		t.Fatal(err)
	}
	childTraceID, childSpanID, ok := IDsFromContext(child)
	if !ok || childTraceID != traceID || childSpanID == spanID {
		t.Fatalf("child IDs = %q/%q, parent = %q/%q", childTraceID, childSpanID, traceID, spanID)
	}
	if got, ok := TraceParentFromContext(child); !ok || !strings.HasPrefix(got, "00-"+traceID+"-"+childSpanID+"-") {
		t.Fatalf("child traceparent = %q", got)
	}
}

func TestTraceContextContractMalformedParentStartsNewRoot(t *testing.T) {
	for _, parent := range []string{
		"",
		"not-a-traceparent",
		" 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00 ",
		"00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-00",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00F067AA0BA902B7-00",
		"00-00000000000000000000000000000000-00f067aa0ba902b7-00",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-00",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00-extra",
		"01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00",
	} {
		ctx, err := ContinueOrNew(context.Background(), parent)
		if err != nil {
			t.Fatalf("parent %q: %v", parent, err)
		}
		traceID, spanID, ok := IDsFromContext(ctx)
		if !ok || len(traceID) != 32 || len(spanID) != 16 || traceID == strings.Repeat("0", 32) || spanID == strings.Repeat("0", 16) {
			t.Fatalf("parent %q produced invalid IDs: %q/%q/%t", parent, traceID, spanID, ok)
		}
	}
}

func TestTraceContextContractNewRootReplacesExistingTraceButKeepsOtherValues(t *testing.T) {
	base := context.WithValue(context.Background(), "caller-value", "kept")
	first, err := ContinueOrNew(base, contractTraceparent)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRoot(first)
	if err != nil {
		t.Fatal(err)
	}
	firstTrace, _, _ := IDsFromContext(first)
	secondTrace, _, _ := IDsFromContext(second)
	if firstTrace == secondTrace {
		t.Fatal("NewRoot did not replace existing trace")
	}
	if got := second.Value("caller-value"); got != "kept" {
		t.Fatalf("other context value = %v, want kept", got)
	}
}

func TestTraceContextContractDetachDropsCancellationAndUnrelatedValues(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.WithValue(context.Background(), "unrelated", "drop"))
	ctx, err := ContinueOrNew(cancelled, contractTraceparent)
	if err != nil {
		t.Fatal(err)
	}
	detached := Detach(ctx)
	cancel()
	if detached.Err() != nil {
		t.Fatalf("detached context has error: %v", detached.Err())
	}
	if detached.Value("unrelated") != nil {
		t.Fatal("detached context retained unrelated value")
	}
	if _, _, ok := IDsFromContext(detached); !ok {
		t.Fatal("detached context lost trace IDs")
	}
}

func TestTraceContextContractEntropyFailureIsReturned(t *testing.T) {
	if _, err := newRoot(context.Background(), bytes.NewReader(nil)); err == nil {
		t.Fatal("entropy failure was ignored")
	}
}

func TestTraceContextContractZeroEntropyIsRejected(t *testing.T) {
	if _, err := newRoot(context.Background(), bytes.NewReader(make([]byte, 24))); err == nil {
		t.Fatal("zero entropy value was accepted")
	}
}

func TestTraceContextContractConcurrentGeneration(t *testing.T) {
	const (
		workers    = 32
		iterations = 64
	)
	ids := make(chan string, workers*iterations*2)
	errorsCh := make(chan error, workers*iterations*2)
	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			for range iterations {
				root, err := NewRoot(context.Background())
				if err != nil {
					errorsCh <- err
					continue
				}
				traceID, spanID, ok := IDsFromContext(root)
				if !ok {
					errorsCh <- context.Canceled
					continue
				}
				ids <- traceID + "/" + spanID

				child, err := Child(root)
				if err != nil {
					errorsCh <- err
					continue
				}
				childTraceID, childSpanID, ok := IDsFromContext(child)
				if !ok || childTraceID != traceID {
					errorsCh <- context.Canceled
					continue
				}
				ids <- childTraceID + "/" + childSpanID
			}
		}()
	}
	wait.Wait()
	close(ids)
	close(errorsCh)
	for err := range errorsCh {
		t.Fatalf("concurrent trace generation failed: %v", err)
	}
	seen := make(map[string]struct{}, workers*iterations*2)
	for id := range ids {
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate trace/span ID: %s", id)
		}
		seen[id] = struct{}{}
	}
	if got, want := len(seen), workers*iterations*2; got != want {
		t.Fatalf("generated IDs = %d, want %d", got, want)
	}
}
