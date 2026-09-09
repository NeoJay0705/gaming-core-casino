package gateproto

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestLoginSchemaContract(t *testing.T) {
	if LoginRequestCommandID != 0xC00002 {
		t.Fatalf("login request command id = %#x, want %#x", LoginRequestCommandID, uint32(0xC00002))
	}
	if LoginResponseCommandID != 0xC00003 {
		t.Fatalf("login response command id = %#x, want %#x", LoginResponseCommandID, uint32(0xC00003))
	}
	assertLoginMessageSchema(t, "gateproto.v1.LoginRequest", (&LoginRequest{}).ProtoReflect().Descriptor(), []loginSchemaField{
		{1, "login_name", protoreflect.StringKind, protoreflect.Optional},
		{2, "token", protoreflect.StringKind, protoreflect.Optional},
		{3, "device_type", protoreflect.Uint32Kind, protoreflect.Optional},
		{4, "app_version", protoreflect.StringKind, protoreflect.Optional},
		{5, "phone_type", protoreflect.StringKind, protoreflect.Optional},
		{6, "phone_name", protoreflect.StringKind, protoreflect.Optional},
		{7, "phone_os", protoreflect.StringKind, protoreflect.Optional},
		{8, "browser", protoreflect.StringKind, protoreflect.Optional},
		{9, "net_type", protoreflect.StringKind, protoreflect.Optional},
		{10, "screen_resolution", protoreflect.StringKind, protoreflect.Optional},
		{11, "width_height", protoreflect.StringKind, protoreflect.Optional},
		{12, "language", protoreflect.StringKind, protoreflect.Optional},
		{13, "vid", protoreflect.StringKind, protoreflect.Optional},
		{14, "web_channel", protoreflect.StringKind, protoreflect.Optional},
		{15, "web_site", protoreflect.StringKind, protoreflect.Optional},
	})
	assertLoginMessageSchema(t, "gateproto.v1.LoginResponse", (&LoginResponse{}).ProtoReflect().Descriptor(), []loginSchemaField{
		{1, "code", protoreflect.Uint32Kind, protoreflect.Optional},
		{2, "server_time", protoreflect.Uint64Kind, protoreflect.Optional},
		{3, "user_flag", protoreflect.Uint32Kind, protoreflect.Optional},
		{4, "account", protoreflect.DoubleKind, protoreflect.Optional},
		{5, "currency", protoreflect.StringKind, protoreflect.Optional},
		{6, "nickname", protoreflect.StringKind, protoreflect.Optional},
		{7, "gender", protoreflect.Uint32Kind, protoreflect.Optional},
		{8, "region", protoreflect.StringKind, protoreflect.Optional},
		{9, "last_customer_number", protoreflect.StringKind, protoreflect.Optional},
		{10, "session", protoreflect.BytesKind, protoreflect.Optional},
		{11, "confirm", protoreflect.Uint32Kind, protoreflect.Optional},
		{12, "head_icon", protoreflect.StringKind, protoreflect.Optional},
		{13, "vip", protoreflect.Uint32Kind, protoreflect.Optional},
	})
}

type loginSchemaField struct {
	number      protoreflect.FieldNumber
	name        protoreflect.Name
	kind        protoreflect.Kind
	cardinality protoreflect.Cardinality
}

func assertLoginMessageSchema(t *testing.T, wantFullName protoreflect.FullName, message protoreflect.MessageDescriptor, want []loginSchemaField) {
	t.Helper()
	if message.FullName() != wantFullName {
		t.Fatalf("message full name = %q, want %q", message.FullName(), wantFullName)
	}
	fields := message.Fields()
	if fields.Len() != len(want) {
		t.Fatalf("%s field count = %d, want %d", message.FullName(), fields.Len(), len(want))
	}
	for _, expected := range want {
		field := fields.ByNumber(expected.number)
		if field == nil {
			t.Fatalf("%s field %d is missing", message.FullName(), expected.number)
		}
		if field.Name() != expected.name || field.Kind() != expected.kind || field.Cardinality() != expected.cardinality {
			t.Fatalf("%s field %d = name:%q kind:%s cardinality:%s, want name:%q kind:%s cardinality:%s", message.FullName(), expected.number, field.Name(), field.Kind(), field.Cardinality(), expected.name, expected.kind, expected.cardinality)
		}
	}
}
