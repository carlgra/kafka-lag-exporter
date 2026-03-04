package sink

import (
	"context"
	"log/slog"
	"testing"

	"github.com/seglo/kafka-lag-exporter/internal/metrics"
)

func BenchmarkGraphiteSink_BuildPath(b *testing.B) {
	sink := NewGraphiteSink("localhost", 2003, "kafka", false, nil, slog.Default())
	mv := metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "my-cluster", "topic": "my.topic", "partition": "0"},
		Value:      12345,
	}

	b.ResetTimer()
	for range b.N {
		sink.buildPath(mv)
	}
}

func BenchmarkGraphiteSink_Report(b *testing.B) {
	sink := NewGraphiteSink("localhost", 2003, "kafka", false, nil, slog.Default())
	// Set a large buffer so we don't hit the limit during the benchmark.
	sink.maxBufSize = 100 * 1024 * 1024
	mv := metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "my-cluster", "topic": "my-topic", "partition": "0"},
		Value:      12345,
	}
	ctx := context.Background()

	b.ResetTimer()
	for range b.N {
		sink.Report(ctx, mv)
	}
}
