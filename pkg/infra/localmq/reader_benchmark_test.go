package localmq

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

type countingFileHandle struct {
	fileHandle
	bytesRead *atomic.Int64
}

func (f *countingFileHandle) ReadAt(data []byte, offset int64) (int, error) {
	read, err := f.fileHandle.ReadAt(data, offset)
	f.bytesRead.Add(int64(read))
	return read, err
}

// BenchmarkRequiredCursorReadAmplification 確認多個 immutable segments 的連續
// batch 不會形成 segments*batches 的 boundary 掃描。
func BenchmarkRequiredCursorReadAmplification(b *testing.B) {
	s, topic, laneID := newRequiredTestStorage(b)
	const segmentCount = 20
	const recordsPerSegment = 100
	const batchSize = 97
	var dataEnd int64
	for segmentIndex := 0; segmentIndex < segmentCount; segmentIndex++ {
		base := uint64(segmentIndex * recordsPerSegment)
		records := requiredTestRecordsWithPayload(base, recordsPerSegment, 64)
		dataEnd = writeRequiredSealedSegment(b, s, topic, laneID, base, records)
	}
	totalRecords := uint64(segmentCount * recordsPerSegment)
	writeRequiredFrontier(b, s, topic, laneID, totalRecords-recordsPerSegment, totalRecords, dataEnd)

	var bytesRead atomic.Int64
	var storageOperations atomic.Int64
	var segmentOpens atomic.Int64
	baseOps := newStorageOps()
	ops := newStorageOps()
	ops.open = func(path string) (fileHandle, error) {
		storageOperations.Add(1)
		if filepath.Ext(path) == ".wal" {
			segmentOpens.Add(1)
		}
		file, err := baseOps.open(path)
		if err != nil {
			return nil, err
		}
		return &countingFileHandle{fileHandle: file, bytesRead: &bytesRead}, nil
	}
	ops.openFile = func(path string, flag int, perm os.FileMode) (fileHandle, error) {
		storageOperations.Add(1)
		file, err := baseOps.openFile(path, flag, perm)
		if err != nil {
			return nil, err
		}
		return &countingFileHandle{fileHandle: file, bytesRead: &bytesRead}, nil
	}
	ops.stat = func(path string) (os.FileInfo, error) {
		storageOperations.Add(1)
		return baseOps.stat(path)
	}
	ops.readFile = func(path string) ([]byte, error) {
		storageOperations.Add(1)
		return baseOps.readFile(path)
	}
	ops.readDir = func(path string) ([]os.DirEntry, error) {
		storageOperations.Add(1)
		return baseOps.readDir(path)
	}
	s.ops = ops

	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		reader := newLaneReader(s, topic, laneID)
		from := uint64(0)
		for from < totalRecords {
			result, err := s.readLaneBatchWithCursor(reader, from, batchSize, 1<<20)
			if err != nil {
				b.Fatal(err)
			}
			if len(result.Batch.Messages) == 0 || result.NextSequence <= from {
				b.Fatalf("cursor stopped at sequence %d", from)
			}
			expected := from
			for _, message := range result.Batch.Messages {
				if message.Sequence != expected {
					b.Fatalf("cursor returned sequence %d, want %d", message.Sequence, expected)
				}
				expected++
			}
			if result.NextSequence != expected {
				b.Fatalf("cursor next sequence = %d, want %d", result.NextSequence, expected)
			}
			from = result.NextSequence
		}
	}
	b.StopTimer()
	iterations := float64(b.N)
	messages := float64(totalRecords) * iterations
	maximumSegmentOpens := int64(b.N * (2*segmentCount + int(totalRecords)/batchSize + 2))
	if got := segmentOpens.Load(); got > maximumSegmentOpens {
		b.Fatalf("segment opens = %d, want <= %d (segments+batches)", got, maximumSegmentOpens)
	}
	b.ReportMetric(float64(bytesRead.Load())/iterations, "bytes_read/op")
	b.ReportMetric(float64(bytesRead.Load())/messages, "bytes_read/message")
	b.ReportMetric(float64(segmentOpens.Load())/iterations, "segment_opens/op")
	b.ReportMetric(float64(storageOperations.Load())/messages, "storage_ops/message")
}

func requiredTestRecordsWithPayload(base uint64, count, payloadSize int) []walRecord {
	records := make([]walRecord, count)
	for index := range records {
		sequence := base + uint64(index)
		records[index] = walRecord{
			Sequence:   sequence,
			AppendTime: requiredTestTopicTime(int(sequence)),
			MessageID:  []byte{byte(index), byte(index >> 8)},
			EventType:  "event",
			Payload:    make([]byte, payloadSize),
		}
	}
	return records
}

func requiredTestTopicTime(sequence int) time.Time {
	return time.Unix(1000+int64(sequence), 0).UTC()
}
