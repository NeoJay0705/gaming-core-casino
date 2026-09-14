package protocol

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestExampleCommandContractIDsAndDescriptors(t *testing.T) {
	ids := []struct {
		name string
		got  uint32
		want uint32
	}{
		{name: "EnterRoomRequest", got: EnterRoomRequestCommandID, want: 0xF1000001},
		{name: "EnterRoomResponse", got: EnterRoomResponseCommandID, want: 0xF1000002},
		{name: "EchoRequest", got: EchoRequestCommandID, want: 0xF1000011},
		{name: "EchoResponse", got: EchoResponseCommandID, want: 0xF1000012},
		{name: "LocalEchoRequest", got: LocalEchoRequestCommandID, want: 0xF1000021},
		{name: "LocalEchoResponse", got: LocalEchoResponseCommandID, want: 0xF1000022},
		{name: "BroadcastRoom", got: BroadcastRoomCommandID, want: 0xF1000031},
		{name: "StartPushRequest", got: StartPushRequestCommandID, want: 0xF1000041},
		{name: "StartPushResponse", got: StartPushResponseCommandID, want: 0xF1000042},
		{name: "PushMessage", got: PushMessageCommandID, want: 0xF1000043},
	}
	seen := make(map[uint32]struct{}, len(ids))
	for _, item := range ids {
		t.Run(item.name, func(t *testing.T) {
			if item.got != item.want {
				t.Fatalf("command ID = %#x, want %#x", item.got, item.want)
			}
			if item.got == 0 {
				t.Fatal("command ID must be non-zero")
			}
			if _, exists := seen[item.got]; exists {
				t.Fatalf("duplicate command ID %#x", item.got)
			}
			seen[item.got] = struct{}{}
		})
	}
	for _, item := range []struct {
		name      string
		message   protoreflect.Message
		fullName  protoreflect.FullName
		fieldName protoreflect.Name
		fieldKind protoreflect.Kind
	}{
		{name: "EnterRoomRequest", message: (&EnterRoomRequest{}).ProtoReflect(), fullName: "metrics.example.v1.EnterRoomRequest", fieldName: "room_id", fieldKind: protoreflect.StringKind},
		{name: "EnterRoomResponse", message: (&EnterRoomResponse{}).ProtoReflect(), fullName: "metrics.example.v1.EnterRoomResponse", fieldName: "code", fieldKind: protoreflect.Uint32Kind},
		{name: "EchoRequest", message: (&EchoRequest{}).ProtoReflect(), fullName: "metrics.example.v1.EchoRequest", fieldName: "payload", fieldKind: protoreflect.BytesKind},
		{name: "EchoResponse", message: (&EchoResponse{}).ProtoReflect(), fullName: "metrics.example.v1.EchoResponse", fieldName: "payload", fieldKind: protoreflect.BytesKind},
		{name: "BroadcastRoomCommand", message: (&BroadcastRoomCommand{}).ProtoReflect(), fullName: "metrics.example.v1.BroadcastRoomCommand", fieldName: "room_id", fieldKind: protoreflect.StringKind},
	} {
		t.Run(item.name, func(t *testing.T) {
			descriptor := item.message.Descriptor()
			if got := descriptor.FullName(); got != item.fullName {
				t.Fatalf("full name = %q, want %q", got, item.fullName)
			}
			field := descriptor.Fields().ByNumber(1)
			if field == nil {
				t.Fatal("field 1 is missing")
			}
			if field.Name() != item.fieldName || field.Kind() != item.fieldKind {
				t.Fatalf("field 1 = name:%q kind:%v, want name:%q kind:%v", field.Name(), field.Kind(), item.fieldName, item.fieldKind)
			}
		})
	}
	if fields := (&BroadcastRoomCommand{}).ProtoReflect().Descriptor().Fields(); fields.Len() != 2 {
		t.Fatalf("BroadcastRoomCommand field count = %d, want 2", fields.Len())
	}
	for name, number := range map[protoreflect.Name]protoreflect.FieldNumber{
		"room_id":  1,
		"messages": 2,
	} {
		field := (&BroadcastRoomCommand{}).ProtoReflect().Descriptor().Fields().ByName(name)
		if field == nil || field.Number() != number {
			t.Fatalf("BroadcastRoomCommand.%s = %v, want field number %d", name, field, number)
		}
	}
	for name, number := range map[protoreflect.Name]protoreflect.FieldNumber{
		"client_command_id": 1,
		"client_payload":    2,
	} {
		field := (&BroadcastClientMessage{}).ProtoReflect().Descriptor().Fields().ByName(name)
		if field == nil || field.Number() != number {
			t.Fatalf("BroadcastClientMessage.%s = %v, want field number %d", name, field, number)
		}
	}
	for name, number := range map[protoreflect.Name]protoreflect.FieldNumber{
		"run_id":          1,
		"mode":            2,
		"room_id":         3,
		"login_names":     4,
		"interval_millis": 5,
		"duration_millis": 6,
		"payload_bytes":   7,
	} {
		field := (&StartPushRequest{}).ProtoReflect().Descriptor().Fields().ByName(name)
		if field == nil || field.Number() != number {
			t.Fatalf("StartPushRequest.%s = %v, want field number %d", name, field, number)
		}
	}
	for name, number := range map[protoreflect.Name]protoreflect.FieldNumber{
		"run_id":        1,
		"planned_ticks": 2,
	} {
		field := (&StartPushResponse{}).ProtoReflect().Descriptor().Fields().ByName(name)
		if field == nil || field.Number() != number {
			t.Fatalf("StartPushResponse.%s = %v, want field number %d", name, field, number)
		}
	}
	for name, number := range map[protoreflect.Name]protoreflect.FieldNumber{
		"run_id":         1,
		"sequence":       2,
		"sent_unix_nano": 3,
		"payload":        4,
	} {
		field := (&PushMessage{}).ProtoReflect().Descriptor().Fields().ByName(name)
		if field == nil || field.Number() != number {
			t.Fatalf("PushMessage.%s = %v, want field number %d", name, field, number)
		}
	}
	if got := (&StartPushRequest{}).ProtoReflect().Descriptor().FullName(); got != "metrics.example.v1.StartPushRequest" {
		t.Fatalf("StartPushRequest full name = %q", got)
	}
	if got := (&StartPushResponse{}).ProtoReflect().Descriptor().FullName(); got != "metrics.example.v1.StartPushResponse" {
		t.Fatalf("StartPushResponse full name = %q", got)
	}
	if got := (&PushMessage{}).ProtoReflect().Descriptor().FullName(); got != "metrics.example.v1.PushMessage" {
		t.Fatalf("PushMessage full name = %q", got)
	}
	pushModes := (&StartPushRequest{}).ProtoReflect().Descriptor().Fields().ByName("mode").Enum().Values()
	for name, want := range map[protoreflect.Name]protoreflect.EnumNumber{
		"PUSH_MODE_UNSPECIFIED": 0,
		"PUSH_MODE_BROADCAST":   1,
		"PUSH_MODE_PLAYER":      2,
	} {
		value := pushModes.ByName(name)
		if value == nil || value.Number() != want {
			t.Fatalf("PushMode.%s = %v, want %d", name, value, want)
		}
	}
}
