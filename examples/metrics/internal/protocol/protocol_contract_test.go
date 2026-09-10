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
}
