package localmq

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestRendezvousOwnerDoesNotDependOnRedisOrdering(t *testing.T) {
	members := []string{"01993e11-8d3a-7c8f-a7ce-2ab21df8d410", "01993e11-8d3b-7c8f-a7ce-2ab21df8d410"}
	forward := rendezvousOwner("events", "01993e11-8d3c-7c8f-a7ce-2ab21df8d410", members)
	reversed := rendezvousOwner("events", "01993e11-8d3c-7c8f-a7ce-2ab21df8d410", []string{members[1], members[0]})
	if forward == "" || forward != reversed {
		t.Fatalf("owner changed with member order: %q vs %q", forward, reversed)
	}
}

func TestPublishRequestBoundaryClassifiesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	request := &publishRequest{ctx: ctx}
	cancel()
	if !request.cancelBeforeWrite() {
		t.Fatal("cancelled request crossed write boundary")
	}
	if request.markWriteStarted() {
		t.Fatal("cancelled request became writable")
	}

	request = &publishRequest{ctx: context.Background()}
	if !request.markWriteStarted() || request.cancelBeforeWrite() {
		t.Fatal("write boundary state is not monotonic")
	}
}

func TestPublishErrorClassificationSurvivesWrapping(t *testing.T) {
	before := &PublishError{Kind: PublishRejectedBeforeWrite, Cause: errors.New("capacity")}
	unknown := &PublishError{Kind: PublishDurabilityUnknown, Cause: errors.New("sync")}
	if !IsRejectedBeforeWrite(errors.Join(errors.New("wrapped"), before)) || !IsDurabilityUnknown(unknown) {
		t.Fatal("typed publish error classification was lost")
	}
	if IsRejectedBeforeWrite(unknown) || IsDurabilityUnknown(before) {
		t.Fatal("publish error classifications overlap")
	}
}

func TestLaneAndRequestNamesCannotEscapeStorageLayout(t *testing.T) {
	if err := validateLaneID("../lane"); err == nil {
		t.Fatal("path traversal lane id was accepted")
	}
	if err := validateRequestID("../request"); err == nil {
		t.Fatal("path traversal request id was accepted")
	}
}

func FuzzDecodeRecordIsBounded(f *testing.F) {
	seed, err := encodeRecord(walRecord{Sequence: 1, MessageID: []byte("id"), EventType: "event", Payload: []byte("payload")}, 1024)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = decodeRecord(data, 1024)
	})
}

func TestDefaultConfigIsStable(t *testing.T) {
	first := DefaultConfig()
	second := DefaultConfig()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("DefaultConfig changed between calls: %+v vs %+v", first, second)
	}
}
