package sink

import (
	"bufio"
	"context"
	"log/slog"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/seglo/kafka-lag-exporter/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraphiteSink_BuildPath(t *testing.T) {
	s := &GraphiteSink{prefix: "kafka"}

	path := s.buildPath(metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "my-cluster", "topic": "my-topic", "partition": "0"},
	})
	assert.Equal(t, "kafka.my-cluster.my-topic.0.kafka_partition_latest_offset", path)
}

func TestGraphiteSink_BuildPath_NoPrefix(t *testing.T) {
	s := &GraphiteSink{}

	path := s.buildPath(metrics.MetricValue{
		Definition: metrics.GroupLag,
		Labels:     map[string]string{"cluster_name": "cluster", "group": "group", "topic": "topic", "partition": "0", "member_host": "host", "consumer_id": "consumer-1", "client_id": "client-1"},
	})
	assert.Equal(t, "cluster.group.topic.0.host.consumer-1.client-1.kafka_consumergroup_group_lag", path)
}

func TestGraphiteSink_BuildPath_SanitizesDots(t *testing.T) {
	s := &GraphiteSink{prefix: "prod.kafka"}

	path := s.buildPath(metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "my.cluster", "topic": "my.topic", "partition": "0"},
	})
	assert.Equal(t, "prod_kafka.my_cluster.my_topic.0.kafka_partition_latest_offset", path)
}

func TestGraphiteSink_Report_And_Flush(t *testing.T) {
	// Start a TCP listener.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	port := ln.Addr().(*net.TCPAddr).Port

	received := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		scanner := bufio.NewScanner(conn)
		if scanner.Scan() {
			received <- scanner.Text()
		}
	}()

	filter, _ := NewMetricFilter([]string{".*"})
	s := NewGraphiteSink("127.0.0.1", port, "test", false, filter, slog.Default())
	ctx := context.Background()

	s.Report(ctx, metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "cluster", "topic": "topic", "partition": "0"},
		Value:      42,
	})

	// Flush to send the buffered data.
	s.Flush()

	select {
	case line := <-received:
		assert.True(t, strings.HasPrefix(line, "test.cluster.topic.0.kafka_partition_latest_offset 42 "))
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for graphite data")
	}

	s.Stop()
}

func TestGraphiteSink_Filter(t *testing.T) {
	// Start a TCP listener.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	port := ln.Addr().(*net.TCPAddr).Port

	filter, _ := NewMetricFilter([]string{"kafka_consumergroup.*"})
	s := NewGraphiteSink("127.0.0.1", port, "", false, filter, slog.Default())

	// This should be filtered out — no data buffered.
	s.Report(context.Background(), metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "cluster", "topic": "topic", "partition": "0"},
		Value:      100,
	})

	// Buffer should be empty since the metric was filtered.
	s.mu.Lock()
	assert.Equal(t, 0, s.buf.Len())
	s.mu.Unlock()
}

func TestGraphiteSink_NaN_Ignored(t *testing.T) {
	filter, _ := NewMetricFilter([]string{".*"})
	s := NewGraphiteSink("127.0.0.1", 1, "test", false, filter, slog.Default())

	s.Report(context.Background(), metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "cluster", "topic": "topic", "partition": "0"},
		Value:      math.NaN(),
	})

	s.mu.Lock()
	assert.Equal(t, 0, s.buf.Len())
	s.mu.Unlock()
}

func TestGraphiteSink_Inf_Ignored(t *testing.T) {
	filter, _ := NewMetricFilter([]string{".*"})
	s := NewGraphiteSink("127.0.0.1", 1, "test", false, filter, slog.Default())

	s.Report(context.Background(), metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "cluster", "topic": "topic", "partition": "0"},
		Value:      math.Inf(1),
	})

	s.mu.Lock()
	assert.Equal(t, 0, s.buf.Len())
	s.mu.Unlock()
}

func TestGraphiteSink_ConnectionError(t *testing.T) {
	// Connect to a port where nothing is listening.
	filter, _ := NewMetricFilter([]string{".*"})
	s := NewGraphiteSink("127.0.0.1", 1, "test", false, filter, slog.Default())

	s.Report(context.Background(), metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "cluster", "topic": "topic", "partition": "0"},
		Value:      42,
	})

	// Flush should not panic on connection error.
	s.Flush()
}

func TestGraphiteSink_Remove_Noop(t *testing.T) {
	s := &GraphiteSink{}
	// Should not panic — Graphite doesn't support removal.
	s.Remove(context.Background(), metrics.RemoveMetric{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "cluster", "topic": "topic", "partition": "0"},
	})
}

func TestGraphiteSink_Stop_Noop(t *testing.T) {
	s := &GraphiteSink{}
	s.Stop() // Should not panic.
}

func TestGraphiteSink_BufferOverflow_DropsOldest(t *testing.T) {
	filter, _ := NewMetricFilter([]string{".*"})
	s := NewGraphiteSink("127.0.0.1", 1, "test", false, filter, slog.Default())
	s.maxBufSize = 100 // Very small buffer.

	ctx := context.Background()
	// Fill the buffer with metrics.
	for i := 0; i < 20; i++ {
		s.Report(ctx, metrics.MetricValue{
			Definition: metrics.PartitionLatestOffset,
			Labels:     map[string]string{"cluster_name": "cluster", "topic": "topic", "partition": "0"},
			Value:      float64(i),
		})
	}

	s.mu.Lock()
	bufLen := s.buf.Len()
	bufContent := s.buf.String()
	s.mu.Unlock()

	// Buffer should be capped near maxBufSize.
	assert.LessOrEqual(t, bufLen, s.maxBufSize)
	// Should contain the latest metrics, not the oldest.
	assert.Contains(t, bufContent, "19")
	// Should NOT contain the earliest metric.
	assert.NotContains(t, bufContent, " 0 ")
}
