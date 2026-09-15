package logging

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

// BenchmarkIncomingMetadataTraceparent 比較 traceparent 讀取所需的完整
// metadata copy 與 bounded lookup；只測量 extraction，不改變 trace contract。
func BenchmarkIncomingMetadataTraceparent(b *testing.B) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		traceParentHeader, "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
		"user-agent", "benchmark",
		"x-extra-a", "a",
		"x-extra-b", "b",
		"x-extra-c", "c",
	))
	b.Run("legacy_full_md_copy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			values, ok := metadata.FromIncomingContext(ctx)
			if !ok || len(values.Get(traceParentHeader)) != 1 {
				b.Fatal("traceparent is missing")
			}
		}
	})
	b.Run("bounded_value_lookup", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if len(metadata.ValueFromIncomingContext(ctx, traceParentHeader)) != 1 {
				b.Fatal("traceparent is missing")
			}
		}
	})
}
