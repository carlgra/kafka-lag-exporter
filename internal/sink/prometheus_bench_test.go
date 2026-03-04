package sink

import (
	"context"
	"log/slog"
	"testing"

	"github.com/seglo/kafka-lag-exporter/internal/metrics"
)

func BenchmarkPrometheusSink_Report(b *testing.B) {
	port := getFreePort()
	filter, _ := NewMetricFilter([]string{".*"})
	sink, err := NewPrometheusSink(port, "", filter, slog.Default())
	if err != nil {
		b.Fatal(err)
	}
	defer sink.Stop()

	// Disable cardinality limit for benchmarking.
	sink.maxTimeSeries = 0

	mv := metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "bench-cluster", "topic": "bench-topic", "partition": "0"},
		Value:      12345,
	}
	ctx := context.Background()

	b.ResetTimer()
	for range b.N {
		sink.Report(ctx, mv)
	}
}
