package localmq

import (
	"testing"
	"time"
)

func TestMetadataEnvelopeRoundTripAndChecksumGuard(t *testing.T) {
	input := storageControl{
		FormatVersion:         formatVersion,
		StorageID:             "01993e11-8d3a-7c8f-a7ce-2ab21df8d410",
		ProducerStopFreeBytes: 1024,
		RecoveryReserveBytes:  512,
		MaxClockSkewNanos:     int64(time.Second),
		UpdatedAtUnixNano:     123,
	}
	data, err := encodeMetadata(metadataKindStorageControl, input)
	if err != nil {
		t.Fatal(err)
	}
	var output storageControl
	if err := decodeMetadata(data, metadataKindStorageControl, &output); err != nil {
		t.Fatal(err)
	}
	if output != input {
		t.Fatalf("metadata round trip = %+v, want %+v", output, input)
	}
	data[len(data)-1]++
	if err := decodeMetadata(data, metadataKindStorageControl, &output); err == nil {
		t.Fatal("checksum-corrupted metadata decoded successfully")
	}
}

func TestRecordAndFooterRoundTrip(t *testing.T) {
	input := walRecord{
		Sequence:      7,
		AppendTime:    time.Unix(100, 20).UTC(),
		MessageID:     []byte("message-7"),
		EventType:     "log.created",
		SchemaVersion: 3,
		Payload:       []byte(`{"ok":true}`),
	}
	data, err := encodeRecord(input, 1024)
	if err != nil {
		t.Fatal(err)
	}
	output, consumed, err := decodeRecord(data, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if consumed != len(data) || output.Sequence != input.Sequence || output.EventType != input.EventType || string(output.Payload) != string(input.Payload) {
		t.Fatalf("record round trip = %+v, consumed=%d", output, consumed)
	}
	footer := encodeSegmentFooter(segmentFooter{
		FileLength:     uint64(len(data) + segmentFooterBytes),
		NextSequence:   8,
		RecordCount:    1,
		MinAppendNanos: input.AppendTime.UnixNano(),
		MaxAppendNanos: input.AppendTime.UnixNano(),
		DataChecksum:   crc32c(data),
	})
	decodedFooter, err := decodeSegmentFooter(footer)
	if err != nil || decodedFooter.NextSequence != 8 {
		t.Fatalf("footer round trip = %+v, err=%v", decodedFooter, err)
	}
}

func TestDecoderRejectsOversizedAndTruncatedRecords(t *testing.T) {
	if _, _, err := decodeRecord([]byte{0, 0, 0, 255}, 64); err == nil {
		t.Fatal("oversized record decoded successfully")
	}
	if _, _, err := decodeRecord([]byte{0, 0}, 64); err == nil {
		t.Fatal("truncated record decoded successfully")
	}
}
