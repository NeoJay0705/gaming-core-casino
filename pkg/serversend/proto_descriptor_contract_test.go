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
	for _, methodName := range []protoreflect.Name{"SendToConnection", "SendToPlayer", "BroadcastRoom"} {
		if service.Methods().ByName(methodName) == nil {
			t.Fatalf("GateDelivery method %q is missing", methodName)
		}
	}

	assertDescriptorFieldNumbers(t, file.Messages().ByName("SendToConnectionRequest"), map[protoreflect.Name]protoreflect.FieldNumber{
		"connection_id":       1,
		"expected_login_name": 2,
		"command_id":          3,
		"payload":             4,
	})
	assertDescriptorFieldNumbers(t, file.Messages().ByName("SendToPlayerRequest"), map[protoreflect.Name]protoreflect.FieldNumber{
		"login_name": 1,
		"command_id": 2,
		"payload":    3,
	})
	assertDescriptorFieldNumbers(t, file.Messages().ByName("BroadcastRoomRequest"), map[protoreflect.Name]protoreflect.FieldNumber{
		"room_id":    1,
		"command_id": 2,
		"payload":    3,
	})
	assertDescriptorFieldNumbers(t, file.Messages().ByName("DeliveryResponse"), map[protoreflect.Name]protoreflect.FieldNumber{
		"status":          1,
		"delivered_count": 2,
	})

	statusDescriptor := file.Enums().ByName("DeliveryStatus")
	if statusDescriptor == nil {
		t.Fatal("DeliveryStatus enum is missing")
	}
	for name, want := range map[protoreflect.Name]protoreflect.EnumNumber{
		"DELIVERY_STATUS_UNSPECIFIED": 0,
		"DELIVERY_STATUS_DELIVERED":   1,
		"DELIVERY_STATUS_IGNORED":     2,
	} {
		value := statusDescriptor.Values().ByName(name)
		if value == nil || value.Number() != want {
			t.Fatalf("DeliveryStatus.%s = %v, want %d", name, value, want)
		}
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
