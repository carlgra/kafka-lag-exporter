package sink

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/seglo/kafka-lag-exporter/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getFreePort() int {
	// Use port 0 to get a free port, but for simplicity in tests,
	// we'll use a high port range.
	return 19090 + int(time.Now().UnixNano()%1000)
}

func TestPrometheusSink_ReportAndScrape(t *testing.T) {
	port := getFreePort()
	logger := slog.Default()
	ctx := context.Background()

	filter, err := NewMetricFilter([]string{".*"})
	require.NoError(t, err)

	sink, err := NewPrometheusSink(port, "", 100000, filter, logger)
	require.NoError(t, err)
	defer sink.Stop()

	// Give server time to start.
	time.Sleep(100 * time.Millisecond)

	// Report a metric.
	sink.Report(ctx, metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "test-cluster", "topic": "test-topic", "partition": "0"},
		Value:      12345,
	})

	// Scrape.
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", port))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	bodyStr := string(body)
	assert.Contains(t, bodyStr, "kafka_partition_latest_offset")
	assert.Contains(t, bodyStr, `cluster_name="test-cluster"`)
	assert.Contains(t, bodyStr, `topic="test-topic"`)
	assert.Contains(t, bodyStr, `partition="0"`)
	assert.Contains(t, bodyStr, "12345")
}

func TestPrometheusSink_Remove(t *testing.T) {
	port := getFreePort()
	logger := slog.Default()
	ctx := context.Background()

	filter, err := NewMetricFilter([]string{".*"})
	require.NoError(t, err)

	sink, err := NewPrometheusSink(port, "", 100000, filter, logger)
	require.NoError(t, err)
	defer sink.Stop()

	time.Sleep(100 * time.Millisecond)

	// Report then remove.
	sink.Report(ctx, metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "test-cluster", "topic": "test-topic", "partition": "0"},
		Value:      100,
	})
	sink.Remove(ctx, metrics.RemoveMetric{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "test-cluster", "topic": "test-topic", "partition": "0"},
	})

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", port))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// The metric series should be gone.
	assert.False(t, strings.Contains(string(body), `kafka_partition_latest_offset{`))
}

func TestPrometheusSink_Filter(t *testing.T) {
	port := getFreePort()
	logger := slog.Default()
	ctx := context.Background()

	filter, err := NewMetricFilter([]string{"kafka_consumergroup.*"})
	require.NoError(t, err)

	sink, err := NewPrometheusSink(port, "", 100000, filter, logger)
	require.NoError(t, err)
	defer sink.Stop()

	time.Sleep(100 * time.Millisecond)

	// This metric should be filtered out.
	sink.Report(ctx, metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "test-cluster", "topic": "test-topic", "partition": "0"},
		Value:      100,
	})

	// This metric should pass the filter.
	sink.Report(ctx, metrics.MetricValue{
		Definition: metrics.GroupLag,
		Labels:     map[string]string{"cluster_name": "test-cluster", "group": "test-group", "topic": "test-topic", "partition": "0", "member_host": "", "consumer_id": "", "client_id": ""},
		Value:      50,
	})

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", port))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	bodyStr := string(body)

	assert.False(t, strings.Contains(bodyStr, `kafka_partition_latest_offset{`))
	assert.Contains(t, bodyStr, "kafka_consumergroup_group_lag")
}

func TestPrometheusSink_NaN_Ignored(t *testing.T) {
	port := getFreePort()
	filter, _ := NewMetricFilter([]string{".*"})
	sink, err := NewPrometheusSink(port, "", 100000, filter, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()
	time.Sleep(100 * time.Millisecond)

	sink.Report(context.Background(), metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "test-cluster", "topic": "nan-topic", "partition": "0"},
		Value:      math.NaN(),
	})

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", port))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	assert.False(t, strings.Contains(string(body), `topic="nan-topic"`))
}

func TestPrometheusSink_Inf_Ignored(t *testing.T) {
	port := getFreePort()
	filter, _ := NewMetricFilter([]string{".*"})
	sink, err := NewPrometheusSink(port, "", 100000, filter, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()
	time.Sleep(100 * time.Millisecond)
	ctx := context.Background()

	sink.Report(ctx, metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "test-cluster", "topic": "inf-topic", "partition": "0"},
		Value:      math.Inf(1),
	})
	sink.Report(ctx, metrics.MetricValue{
		Definition: metrics.PartitionEarliestOffset,
		Labels:     map[string]string{"cluster_name": "test-cluster", "topic": "inf-topic", "partition": "0"},
		Value:      math.Inf(-1),
	})

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", port))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	assert.False(t, strings.Contains(string(body), `topic="inf-topic"`))
}

func TestPrometheusSink_HealthEndpoint(t *testing.T) {
	port := getFreePort()
	filter, _ := NewMetricFilter([]string{".*"})
	sink, err := NewPrometheusSink(port, "", 100000, filter, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/health", port))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestPrometheusSink_NilFilter(t *testing.T) {
	port := getFreePort()
	sink, err := NewPrometheusSink(port, "", 100000, nil, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()
	time.Sleep(100 * time.Millisecond)

	// With nil filter, all metrics should be reported.
	sink.Report(context.Background(), metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "test-cluster", "topic": "test-topic", "partition": "0"},
		Value:      42,
	})

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", port))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "kafka_partition_latest_offset")
}

func TestPrometheusSink_UnknownMetric(t *testing.T) {
	port := getFreePort()
	filter, _ := NewMetricFilter([]string{".*"})
	sink, err := NewPrometheusSink(port, "", 100000, filter, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()
	ctx := context.Background()

	// Should not panic when reporting an unknown metric name.
	sink.Report(ctx, metrics.MetricValue{
		Definition: metrics.GaugeDefinition{Name: "unknown_metric", Labels: []string{"a"}},
		Labels:     map[string]string{"a": "val"},
		Value:      1,
	})

	// Should not panic when removing an unknown metric name.
	sink.Remove(ctx, metrics.RemoveMetric{
		Definition: metrics.GaugeDefinition{Name: "unknown_metric", Labels: []string{"a"}},
		Labels:     map[string]string{"a": "val"},
	})
}

func TestPrometheusSink_ReportPollMetrics(t *testing.T) {
	port := getFreePort()
	filter, _ := NewMetricFilter([]string{".*"})
	sink, err := NewPrometheusSink(port, "", 100000, filter, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()
	time.Sleep(100 * time.Millisecond)

	sink.ReportPollMetrics(2*time.Second, true)
	sink.ReportPollMetrics(3*time.Second, false)

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", port))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	assert.Contains(t, bodyStr, "kafka_lag_exporter_polls_total 2")
	assert.Contains(t, bodyStr, "kafka_lag_exporter_poll_errors_total 1")
	assert.Contains(t, bodyStr, "kafka_lag_exporter_poll_duration_seconds 3")
}

func TestPrometheusSink_ReportLookupTableSize(t *testing.T) {
	port := getFreePort()
	filter, _ := NewMetricFilter([]string{".*"})
	sink, err := NewPrometheusSink(port, "", 100000, filter, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()
	time.Sleep(100 * time.Millisecond)

	sink.ReportLookupTableSize("my-cluster", "my-topic", "0", 42)

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", port))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	assert.Contains(t, bodyStr, "kafka_lag_exporter_lookup_table_entries")
	assert.Contains(t, bodyStr, `cluster_name="my-cluster"`)
	assert.Contains(t, bodyStr, "42")
}

func TestPrometheusSink_ReportClientMetrics(t *testing.T) {
	port := getFreePort()
	filter, _ := NewMetricFilter([]string{".*"})
	sink, err := NewPrometheusSink(port, "", 100000, filter, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()
	time.Sleep(100 * time.Millisecond)

	sink.ReportClientMetrics("test-cluster", 10, 3, 2, 1)

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", port))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	assert.Contains(t, bodyStr, "kafka_lag_exporter_client_connects_total")
	assert.Contains(t, bodyStr, "kafka_lag_exporter_client_disconnects_total")
	assert.Contains(t, bodyStr, "kafka_lag_exporter_client_write_errors_total")
	assert.Contains(t, bodyStr, "kafka_lag_exporter_client_read_errors_total")
	assert.Contains(t, bodyStr, `cluster_name="test-cluster"`)
	assert.Contains(t, bodyStr, "10")
}

func TestPrometheusSink_CardinalityLimit(t *testing.T) {
	port := getFreePort()
	filter, _ := NewMetricFilter([]string{".*"})
	sink, err := NewPrometheusSink(port, "", 5, filter, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()

	// Fill up to the limit with 5 unique series.
	for i := range 5 {
		sink.Report(ctx, metrics.MetricValue{
			Definition: metrics.PartitionLatestOffset,
			Labels:     map[string]string{"cluster_name": "c", "topic": "t", "partition": fmt.Sprintf("%d", i)},
			Value:      float64(i * 10),
		})
	}

	// This 6th unique series should be dropped.
	sink.Report(ctx, metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "c", "topic": "t", "partition": "99"},
		Value:      100,
	})

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", port))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	// The 6th series should have been dropped.
	assert.False(t, strings.Contains(bodyStr, `partition="99"`))
	// The dropped counter should be incremented.
	assert.Contains(t, bodyStr, "kafka_lag_exporter_dropped_series_total 1")
}

func TestPrometheusSink_SeriesCountTracksUniqueSeries(t *testing.T) {
	port := getFreePort()
	filter, _ := NewMetricFilter([]string{".*"})
	sink, err := NewPrometheusSink(port, "", 0, filter, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()

	ctx := context.Background()

	// Report 3 unique series for 100 cycles each.
	for cycle := range 100 {
		for i := range 3 {
			sink.Report(ctx, metrics.MetricValue{
				Definition: metrics.PartitionLatestOffset,
				Labels:     map[string]string{"cluster_name": "c", "topic": "t", "partition": fmt.Sprintf("%d", i)},
				Value:      float64(cycle*10 + i),
			})
		}
	}

	// seriesCount should be 3, not 300.
	assert.Equal(t, int64(3), sink.seriesCount.Load())
}

func TestPrometheusSink_MaxTimeSeriesLimitsUniqueSeries(t *testing.T) {
	port := getFreePort()
	filter, _ := NewMetricFilter([]string{".*"})
	sink, err := NewPrometheusSink(port, "", 3, filter, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()

	// Report 3 unique series (fills the limit).
	for i := range 3 {
		sink.Report(ctx, metrics.MetricValue{
			Definition: metrics.PartitionLatestOffset,
			Labels:     map[string]string{"cluster_name": "c", "topic": "t", "partition": fmt.Sprintf("%d", i)},
			Value:      float64(i),
		})
	}

	// Updating existing series should still work.
	sink.Report(ctx, metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "c", "topic": "t", "partition": "0"},
		Value:      999,
	})

	// New (4th) series should be dropped.
	sink.Report(ctx, metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "c", "topic": "t", "partition": "new"},
		Value:      42,
	})

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", port))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	// Existing series updated.
	assert.Contains(t, bodyStr, "999")
	// New series dropped.
	assert.False(t, strings.Contains(bodyStr, `partition="new"`))
	// Series count is still 3.
	assert.Equal(t, int64(3), sink.seriesCount.Load())
}

func TestPrometheusSink_ExistingSeriesContinueAfterLimit(t *testing.T) {
	port := getFreePort()
	filter, _ := NewMetricFilter([]string{".*"})
	sink, err := NewPrometheusSink(port, "", 2, filter, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()

	// Establish 2 series.
	sink.Report(ctx, metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "c", "topic": "t", "partition": "0"},
		Value:      10,
	})
	sink.Report(ctx, metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "c", "topic": "t", "partition": "1"},
		Value:      20,
	})

	// Simulate many poll cycles updating the same 2 series.
	for cycle := 1; cycle <= 50; cycle++ {
		sink.Report(ctx, metrics.MetricValue{
			Definition: metrics.PartitionLatestOffset,
			Labels:     map[string]string{"cluster_name": "c", "topic": "t", "partition": "0"},
			Value:      float64(10 + cycle),
		})
		sink.Report(ctx, metrics.MetricValue{
			Definition: metrics.PartitionLatestOffset,
			Labels:     map[string]string{"cluster_name": "c", "topic": "t", "partition": "1"},
			Value:      float64(20 + cycle),
		})
	}

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", port))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	// Both series should have the latest values (not frozen).
	assert.Contains(t, bodyStr, "60") // 10 + 50
	assert.Contains(t, bodyStr, "70") // 20 + 50
	assert.Equal(t, int64(2), sink.seriesCount.Load())
}

func TestPrometheusSink_ReadyEndpoint_NoCheck(t *testing.T) {
	port := getFreePort()
	filter, _ := NewMetricFilter([]string{".*"})
	sink, err := NewPrometheusSink(port, "", 100000, filter, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()
	time.Sleep(100 * time.Millisecond)

	// No readiness check set — should return 200.
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/ready", port))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestPrometheusSink_ReadyEndpoint_Failing(t *testing.T) {
	port := getFreePort()
	filter, _ := NewMetricFilter([]string{".*"})
	sink, err := NewPrometheusSink(port, "", 100000, filter, slog.Default())
	require.NoError(t, err)
	defer sink.Stop()
	time.Sleep(100 * time.Millisecond)

	sink.SetReadinessCheck(func() error {
		return fmt.Errorf("no collectors have polled")
	})

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/ready", port))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}
