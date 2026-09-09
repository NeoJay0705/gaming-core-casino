package dispatcher

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestContractRoutesByChannelAndCommandID(t *testing.T) {
	dispatcher := New()
	var calls []string
	for _, registration := range []Registration{
		{Channel: "gate-request", CommandID: 7, Handler: func(context.Context, []byte) error { calls = append(calls, "gate"); return nil }},
		{Channel: "other-request", CommandID: 7, Handler: func(context.Context, []byte) error { calls = append(calls, "other"); return nil }},
	} {
		if err := dispatcher.Register(registration.Channel, registration.CommandID, registration.Handler); err != nil {
			t.Fatalf("register: %v", err)
		}
	}
	for _, channel := range []Channel{"gate-request", "other-request"} {
		handled, err := dispatcher.Dispatch(context.Background(), channel, 7, []byte("binary"))
		if err != nil || !handled {
			t.Fatalf("dispatch %q = handled:%t err:%v", channel, handled, err)
		}
	}
	if !reflect.DeepEqual(calls, []string{"gate", "other"}) {
		t.Fatalf("calls = %v", calls)
	}
}

func TestContractEmptyDispatcherReportsUnhandled(t *testing.T) {
	handled, err := New().Dispatch(context.Background(), "gate-request", 7, nil)
	if handled || err != nil {
		t.Fatalf("empty dispatch = handled:%t err:%v, want false nil", handled, err)
	}
}

func TestContractRejectsDuplicateOrInvalidRegistrations(t *testing.T) {
	dispatcher := New()
	handler := func(context.Context, []byte) error { return nil }
	if err := dispatcher.Register("gate-request", 7, handler); err != nil {
		t.Fatalf("register: %v", err)
	}
	for _, registration := range []Registration{
		{Channel: "", CommandID: 7, Handler: handler},
		{Channel: "gate-request", CommandID: 0, Handler: handler},
		{Channel: "gate-request", CommandID: 8},
		{Channel: "gate-request", CommandID: 7, Handler: handler},
	} {
		if err := dispatcher.Register(registration.Channel, registration.CommandID, registration.Handler); !errors.Is(err, ErrRegistrationInvalid) {
			t.Fatalf("register %#v error = %v, want ErrRegistrationInvalid", registration, err)
		}
	}
}

func TestContractNormalizesChannelAcrossDispatcherOperations(t *testing.T) {
	dispatcher := &Dispatcher{}
	if err := dispatcher.Register(" gate-request ", 7, func(context.Context, []byte) error { return nil }); err != nil {
		t.Fatalf("register: %v", err)
	}
	if got := dispatcher.RegisteredCommandIDs(" gate-request "); !reflect.DeepEqual(got, []CommandID{7}) {
		t.Fatalf("registered command ids = %v, want [7]", got)
	}
	handled, err := dispatcher.Dispatch(context.Background(), "gate-request", 7, nil)
	if err != nil || !handled {
		t.Fatalf("dispatch = handled:%t err:%v", handled, err)
	}
	if err := dispatcher.Register("gate-request", 7, func(context.Context, []byte) error { return nil }); err == nil || !strings.Contains(err.Error(), "command 7") {
		t.Fatalf("duplicate registration error = %v, want numeric command id", err)
	}
}
