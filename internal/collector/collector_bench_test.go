package collector

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/seglo/kafka-lag-exporter/internal/config"
	"github.com/seglo/kafka-lag-exporter/internal/domain"
	"github.com/seglo/kafka-lag-exporter/internal/lookup"
	"github.com/seglo/kafka-lag-exporter/internal/metrics"
	"github.com/seglo/kafka-lag-exporter/internal/sink"
)

func BenchmarkComputeMetrics(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("gtps_%d", n), func(b *testing.B) {
			benchComputeMetrics(b, n)
		})
	}
}

// benchNoopSink is a minimal sink for benchmarks (avoids using recordingSink which allocates).
type benchNoopSink struct{}

func (benchNoopSink) Report(_ context.Context, _ metrics.MetricValue)  {}
func (benchNoopSink) Remove(_ context.Context, _ metrics.RemoveMetric) {}
func (benchNoopSink) Stop()                                            {}

func benchComputeMetrics(b *testing.B, numGTPs int) {
	b.Helper()

	client := &mockClient{}
	c, err := NewCollector(
		config.ClusterConfig{Name: "bench-cluster"},
		client,
		[]sink.Sink{benchNoopSink{}},
		func() lookup.LookupTable { return lookup.NewMemoryTable(60) },
		30*time.Second,
		slog.Default(),
	)
	if err != nil {
		b.Fatal(err)
	}

	// Pre-populate lookup tables.
	snapshot := buildBenchSnapshot(numGTPs)
	for tp, offset := range snapshot.LatestOffsets {
		table := c.lookupFactory()
		c.lookupTables[tp] = table
		for j := range 10 {
			table.AddPoint(lookup.Point{
				Offset: offset.Offset - int64((10-j)*100),
				Time:   offset.Timestamp - int64((10-j)*1000),
			})
		}
	}

	b.ResetTimer()
	for range b.N {
		c.computeMetrics(snapshot)
	}
}

func buildBenchSnapshot(numGTPs int) *OffsetsSnapshot {
	now := int64(1700000000000)
	groupOffsets := make(domain.GroupOffsets)
	latestOffsets := make(domain.PartitionOffsets)
	earliestOffsets := make(domain.PartitionOffsets)

	for i := range numGTPs {
		topic := fmt.Sprintf("topic-%d", i%10)
		partition := int32(i % 5)
		tp := domain.TopicPartition{Topic: topic, Partition: partition}

		latestOffsets[tp] = domain.PartitionOffset{Offset: int64(10000 + i*100), Timestamp: now}
		earliestOffsets[tp] = domain.PartitionOffset{Offset: 0, Timestamp: now - 3600000}

		gtp := domain.GroupTopicPartition{
			Group:     fmt.Sprintf("group-%d", i%3),
			Topic:     topic,
			Partition: partition,
		}
		groupOffsets[gtp] = domain.PartitionOffset{
			Offset:    int64(9000 + i*50),
			Timestamp: now,
		}
	}

	return &OffsetsSnapshot{
		Timestamp:       now,
		GroupOffsets:    groupOffsets,
		EarliestOffsets: earliestOffsets,
		LatestOffsets:   latestOffsets,
	}
}
