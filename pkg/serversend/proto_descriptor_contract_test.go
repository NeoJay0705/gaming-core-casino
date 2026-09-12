package serversend

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestServerSendProtoDescriptorContractKeepsWireSchema(t *testing.T) {
	file := File_pkg_serversend_serversend_proto
	if file == nil {
		t.Fatal("serversend protobuf descriptor is nil")
	}
	if got, want := file.Package(), protoreflect.FullName("serversend.v1"); got != want {
		t.Fatalf("protobuf package = %q, want %q", got, want)
	}

	service := file.Services().ByName("GateDelivery")
	if service == nil {
		t.Fatal("GateDelivery service is missing")
	}
	if got := service.Methods().Len(); got != 1 {
		t.Fatalf("GateDelivery method count = %d, want 1", got)
	}
	for _, methodName := range []protoreflect.Name{"Forward"} {
		if service.Methods().ByName(methodName) == nil {
			t.Fatalf("GateDelivery method %q is missing", methodName)
		}
	}
	if method := service.Methods().ByName("Forward"); method.Input().FullName() != "gatelink.v1.GateRequest" {
		t.Fatalf("Forward input = %q, want gatelink.v1.GateRequest", method.Input().FullName())
	}
	if method := service.Methods().ByName("Forward"); method.Output().FullName() != "google.protobuf.Empty" {
		t.Fatalf("Forward output = %q, want google.protobuf.Empty", method.Output().FullName())
	}
	assertDescriptorFieldNumbers(t, file.Messages().ByName("SendPlayersCommand"), map[protoreflect.Name]protoreflect.FieldNumber{
		"messages": 1,
	})
	assertDescriptorFieldNumbers(t, file.Messages().ByName("PlayerDelivery"), map[protoreflect.Name]protoreflect.FieldNumber{
		"login_name":        1,
		"client_command_id": 2,
		"client_payload":    3,
	})
	if file.Messages().ByName("BroadcastRoomRequest") != nil || file.Messages().ByName("RedisBroadcastEnvelope") != nil {
		t.Fatal("room-specific broadcast messages must not be part of the transport schema")
	}
	for _, name := range []protoreflect.Name{"SendToPlayerRequest", "DeliveryResponse"} {
		if file.Messages().ByName(name) != nil {
			t.Fatalf("obsolete transport message %q is present", name)
		}
	}
	if file.Enums().ByName("DeliveryStatus") != nil {
		t.Fatal("obsolete DeliveryStatus enum is present")
	}
}

func assertDescriptorFieldNumbers(t *testing.T, message protoreflect.MessageDescriptor, want map[protoreflect.Name]protoreflect.FieldNumber) {
	t.Helper()
	if message == nil {
		t.Fatal("protobuf message descriptor is missing")
	}
	if got := message.Fields().Len(); got != len(want) {
		t.Fatalf("%s field count = %d, want %d", message.FullName(), got, len(want))
	}
	for name, number := range want {
		field := message.Fields().ByName(name)
		if field == nil || field.Number() != number {
			t.Fatalf("%s.%s = %v, want field number %d", message.FullName(), name, field, number)
		}
	}
}
