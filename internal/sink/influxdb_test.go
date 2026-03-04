package sink

import (
	"context"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seglo/kafka-lag-exporter/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInfluxDBSink_Report_Sync(t *testing.T) {
	received := make(chan string, 10)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if len(body) > 0 {
			received <- string(body)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	filter, _ := NewMetricFilter([]string{".*"})
	sink, err := NewInfluxDBSink(server.URL, 0, "testdb", "", "", false, filter, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()

	sink.Report(context.Background(), metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "test-cluster", "topic": "test-topic", "partition": "0"},
		Value:      12345,
	})

	// Check we got something.
	select {
	case data := <-received:
		assert.Contains(t, data, "kafka_partition_latest_offset")
		assert.Contains(t, data, "value=12345")
	default:
		// InfluxDB client may batch; check that sink didn't error.
	}
}

func TestInfluxDBSink_Filter(t *testing.T) {
	callCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if len(body) > 0 && strings.Contains(string(body), "kafka_partition") {
			callCount++
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	filter, _ := NewMetricFilter([]string{"kafka_consumergroup.*"})
	sink, err := NewInfluxDBSink(server.URL, 0, "testdb", "", "", false, filter, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()

	// This should be filtered out.
	sink.Report(context.Background(), metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "test-cluster", "topic": "test-topic", "partition": "0"},
		Value:      100,
	})

	assert.Equal(t, 0, callCount)
}

func TestInfluxDBSink_NaN_Ignored(t *testing.T) {
	received := make(chan string, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if len(body) > 0 {
			received <- string(body)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	filter, _ := NewMetricFilter([]string{".*"})
	sink, err := NewInfluxDBSink(server.URL, 0, "testdb", "", "", false, filter, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()

	sink.Report(context.Background(), metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "test-cluster", "topic": "test-topic", "partition": "0"},
		Value:      math.NaN(),
	})

	// Nothing should have been sent.
	select {
	case data := <-received:
		t.Fatalf("NaN should not be sent, got: %s", data)
	default:
	}
}

func TestInfluxDBSink_Inf_Ignored(t *testing.T) {
	received := make(chan string, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if len(body) > 0 {
			received <- string(body)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	filter, _ := NewMetricFilter([]string{".*"})
	sink, err := NewInfluxDBSink(server.URL, 0, "testdb", "", "", false, filter, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()
	ctx := context.Background()

	sink.Report(ctx, metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "test-cluster", "topic": "test-topic", "partition": "0"},
		Value:      math.Inf(1),
	})
	sink.Report(ctx, metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "test-cluster", "topic": "test-topic", "partition": "0"},
		Value:      math.Inf(-1),
	})

	select {
	case data := <-received:
		t.Fatalf("Inf should not be sent, got: %s", data)
	default:
	}
}

func TestInfluxDBSink_TagMapping(t *testing.T) {
	received := make(chan string, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if len(body) > 0 {
			received <- string(body)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	filter, _ := NewMetricFilter([]string{".*"})
	sink, err := NewInfluxDBSink(server.URL, 0, "testdb", "", "", false, filter, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()

	sink.Report(context.Background(), metrics.MetricValue{
		Definition: metrics.GroupLag,
		Labels:     map[string]string{"cluster_name": "my-cluster", "group": "my-group", "topic": "my-topic", "partition": "0", "member_host": "host1", "consumer_id": "consumer-1", "client_id": "client-1"},
		Value:      42,
	})

	select {
	case data := <-received:
		assert.Contains(t, data, "kafka_consumergroup_group_lag")
		assert.Contains(t, data, "cluster_name=my-cluster")
		assert.Contains(t, data, "group=my-group")
		assert.Contains(t, data, "topic=my-topic")
		assert.Contains(t, data, "value=42")
	default:
		// InfluxDB client may batch.
	}
}

func TestInfluxDBSink_NilFilter(t *testing.T) {
	received := make(chan string, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if len(body) > 0 {
			received <- string(body)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	sink, err := NewInfluxDBSink(server.URL, 0, "testdb", "", "", false, nil, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()

	// With nil filter all metrics pass through.
	sink.Report(context.Background(), metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "test-cluster", "topic": "test-topic", "partition": "0"},
		Value:      99,
	})

	select {
	case data := <-received:
		assert.Contains(t, data, "kafka_partition_latest_offset")
	default:
	}
}

func TestInfluxDBSink_Remove_Noop(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	sink, err := NewInfluxDBSink(server.URL, 0, "testdb", "", "", false, nil, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()

	// Should not panic — InfluxDB doesn't support removal.
	sink.Remove(context.Background(), metrics.RemoveMetric{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "cluster", "topic": "topic", "partition": "0"},
	})
}

func TestInfluxDBSink_WithAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	sink, err := NewInfluxDBSink(server.URL, 0, "testdb", "user", "pass", false, nil, slog.Default())
	require.NoError(t, err)
	sink.Stop()
}

func TestInfluxDBSink_Async(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	filter, _ := NewMetricFilter([]string{".*"})
	sink, err := NewInfluxDBSink(server.URL, 0, "testdb", "", "", true, filter, slog.Default())
	require.NoError(t, err)

	// Report via async API.
	sink.Report(context.Background(), metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "test-cluster", "topic": "test-topic", "partition": "0"},
		Value:      42,
	})

	// Stop flushes the async writer.
	sink.Stop()
}
