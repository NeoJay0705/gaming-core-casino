package gatelink

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc/metadata"
)

// BenchmarkIncomingMetadataExtraction 比較原本完整 MD copy 與 production
// bounded lookup；legacy path 僅存在於 benchmark，不會進入 production code。
func BenchmarkIncomingMetadataExtraction(b *testing.B) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"traceparent", "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
		"user-agent", "benchmark",
		"x-extra-a", "a",
		"x-extra-b", "b",
		connectionIDMetadataKey, "connection-1",
		gateIDMetadataKey, "gate-a",
	))
	b.Run("legacy_full_md_copy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			values, ok := metadata.FromIncomingContext(ctx)
			if !ok {
				b.Fatal("incoming metadata is missing")
			}
			connectionID, err := legacySingleMetadataValue(values, connectionIDMetadataKey, true)
			if err != nil {
				b.Fatal(err)
			}
			gateID, err := legacySingleMetadataValue(values, gateIDMetadataKey, false)
			if err != nil {
				b.Fatal(err)
			}
			_ = WithGateRequestContext(ctx, GateRequestContext{Source: RequestSource{
				ConnectionID: connectionID,
				GateID:       gateID,
			}})
		}
	})
	b.Run("bounded_value_lookup", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := withIncomingRequestContext(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// legacySingleMetadataValue 重現修改前的完整 MD copy 後取值路徑；只供
// benchmark reference 使用，避免 production binary 保留慢路徑。
func legacySingleMetadataValue(values metadata.MD, key string, required bool) (string, error) {
	entries := values.Get(key)
	if len(entries) == 0 {
		if required {
			return "", errors.New("gatelink: request source connection_id is required")
		}
		return "", nil
	}
	if len(entries) != 1 {
		return "", errors.New("gatelink: request metadata contains duplicate " + key)
	}
	value := strings.TrimSpace(entries[0])
	if required && value == "" {
		return "", errors.New("gatelink: request source connection_id is required")
	}
	return value, nil
}
