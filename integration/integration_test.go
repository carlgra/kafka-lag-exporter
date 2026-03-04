//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seglo/kafka-lag-exporter/internal/collector"
	"github.com/seglo/kafka-lag-exporter/internal/config"
	"github.com/seglo/kafka-lag-exporter/internal/domain"
	"github.com/seglo/kafka-lag-exporter/internal/kafka"
	"github.com/seglo/kafka-lag-exporter/internal/lookup"
	"github.com/seglo/kafka-lag-exporter/internal/metrics"
	"github.com/seglo/kafka-lag-exporter/internal/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ---------------------------------------------------------------------------
// Kafka / Redpanda end-to-end test
// ---------------------------------------------------------------------------

func TestKafkaEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start a Redpanda container.
	rpContainer, err := redpanda.Run(ctx, "docker.redpanda.com/redpandadata/redpanda:v24.2.18")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rpContainer.Terminate(ctx) })

	brokers, err := rpContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	// Create a test topic and produce messages.
	const topic = "integration-test-topic"
	const numMessages = 50

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	require.NoError(t, err)
	defer client.Close()

	// Produce messages.
	for i := 0; i < numMessages; i++ {
		record := &kgo.Record{
			Topic: topic,
			Value: []byte(fmt.Sprintf("message-%d", i)),
		}
		results := client.ProduceSync(ctx, record)
		require.NoError(t, results.FirstErr())
	}

	// Create a consumer group and consume some messages.
	const groupID = "integration-test-group"
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.FetchMaxWait(time.Second),
	)
	require.NoError(t, err)

	// Consume 30 of 50 messages.
	consumed := 0
	for consumed < 30 {
		fetches := consumer.PollFetches(ctx)
		fetches.EachRecord(func(r *kgo.Record) {
			consumed++
		})
	}
	consumer.CommitUncommittedOffsets(ctx)
	consumer.Close()

	// Now use kafka-lag-exporter's FranzClient to verify it can read offsets.
	globalCfg := &config.Config{
		PollIntervalSeconds:       5,
		KafkaClientTimeoutSeconds: 10,
		Lookup:                    config.LookupConfig{Memory: config.MemoryLookupConfig{Size: 60}},
	}
	clusterCfg := config.ClusterConfig{
		Name:             "integration-test",
		BootstrapBrokers: brokers,
	}

	kafkaClient, err := kafka.NewFranzClient(clusterCfg, globalCfg, slog.Default())
	require.NoError(t, err)
	defer kafkaClient.Close()

	// Verify GetGroups.
	groups, gtps, err := kafkaClient.GetGroups(ctx)
	require.NoError(t, err)
	assert.Contains(t, groups, groupID)

	// Filter for our test group.
	var testGTPs []domain.GroupTopicPartition
	for _, gtp := range gtps {
		if gtp.Group == groupID {
			testGTPs = append(testGTPs, gtp)
		}
	}

	// Verify GetGroupOffsets.
	groupOffsets, err := kafkaClient.GetGroupOffsets(ctx, time.Now().UnixMilli(), groups, testGTPs)
	require.NoError(t, err)
	// Offsets should reflect that ~30 messages were consumed.
	var totalConsumed int64
	for _, offset := range groupOffsets {
		totalConsumed += offset.Offset
	}
	assert.GreaterOrEqual(t, totalConsumed, int64(30))

	// Verify GetLatestOffsets.
	tps := []domain.TopicPartition{{Topic: topic, Partition: 0}}
	latestOffsets, err := kafkaClient.GetLatestOffsets(ctx, time.Now().UnixMilli(), tps)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, latestOffsets[domain.TopicPartition{Topic: topic, Partition: 0}].Offset, int64(numMessages))

	// Verify GetEarliestOffsets.
	earliestOffsets, err := kafkaClient.GetEarliestOffsets(ctx, time.Now().UnixMilli(), tps)
	require.NoError(t, err)
	assert.Equal(t, int64(0), earliestOffsets[domain.TopicPartition{Topic: topic, Partition: 0}].Offset)

	// Full collector test: run one poll cycle and verify metrics.
	rec := &recordingSink{}
	port := 19190 + int(time.Now().UnixNano()%100)
	filter, _ := sink.NewMetricFilter([]string{".*"})
	promSink, err := sink.NewPrometheusSink(port, "", filter, slog.Default())
	require.NoError(t, err)
	defer promSink.Stop()

	coll, err := collector.NewCollector(
		clusterCfg, kafkaClient,
		[]sink.Sink{promSink, rec},
		func() lookup.LookupTable { return lookup.NewMemoryTable(60) },
		5*time.Second, slog.Default(),
	)
	require.NoError(t, err)

	// Run one poll cycle.
	pollCtx, pollCancel := context.WithCancel(ctx)
	go func() {
		time.Sleep(2 * time.Second)
		pollCancel()
	}()
	coll.Run(pollCtx)

	// Verify lag metrics were reported.
	lagMetric := rec.findMetric("kafka_consumergroup_group_lag")
	require.NotNil(t, lagMetric, "expected group_lag metric to be reported")
	assert.Greater(t, lagMetric.Value, float64(0), "should have non-zero lag")

	// Verify Prometheus endpoint has metrics.
	time.Sleep(100 * time.Millisecond)
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", port))
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	assert.Contains(t, bodyStr, "kafka_partition_latest_offset")
	assert.Contains(t, bodyStr, "kafka_consumergroup_group_lag")
	assert.Contains(t, bodyStr, topic)
}

// ---------------------------------------------------------------------------
// Redis lookup table integration test
// ---------------------------------------------------------------------------

func TestRedisLookupTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start a Redis container.
	redisContainer, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() { _ = redisContainer.Terminate(ctx) })

	endpoint, err := redisContainer.Endpoint(ctx, "")
	require.NoError(t, err)

	// Parse host:port.
	parts := strings.SplitN(endpoint, ":", 2)
	require.Len(t, parts, 2)

	host := parts[0]
	port := 6379 // default Redis port
	fmt.Sscanf(parts[1], "%d", &port)

	cfg := lookup.RedisTableConfig{
		Host:       host,
		Port:       port,
		Database:   0,
		Timeout:    5 * time.Second,
		Prefix:     "test",
		Separator:  ":",
		Retention:  time.Hour,
		Expiration: time.Hour,
		MaxSize:    100,
	}

	table := lookup.NewRedisTable(cfg, "cluster1", "topic1", 0, slog.Default())

	// Test full AddPoint/Lookup cycle.
	assert.Equal(t, lookup.Inserted, table.AddPoint(lookup.Point{Offset: 100, Time: 1000}))
	assert.Equal(t, lookup.Inserted, table.AddPoint(lookup.Point{Offset: 200, Time: 2000}))
	assert.Equal(t, lookup.Inserted, table.AddPoint(lookup.Point{Offset: 300, Time: 3000}))
	assert.Equal(t, int64(3), table.Length())

	// Interpolation.
	result := table.Lookup(150)
	require.True(t, result.Found)
	assert.Equal(t, int64(1500), result.Time)

	// At latest.
	result = table.Lookup(300)
	require.True(t, result.Found)
	assert.Equal(t, int64(3000), result.Time)

	// Beyond latest.
	result = table.Lookup(500)
	require.True(t, result.Found)
	assert.Equal(t, int64(3000), result.Time)

	// Extrapolation.
	result = table.Lookup(50)
	require.True(t, result.Found)
	assert.Equal(t, int64(500), result.Time)

	// Flat line.
	assert.Equal(t, lookup.Updated, table.AddPoint(lookup.Point{Offset: 300, Time: 3500}))
	assert.Equal(t, int64(3), table.Length())

	// Non-monotonic.
	assert.Equal(t, lookup.NonMonotonic, table.AddPoint(lookup.Point{Offset: 250, Time: 4000}))

	// Out of order.
	assert.Equal(t, lookup.OutOfOrder, table.AddPoint(lookup.Point{Offset: 400, Time: 2000}))

	err = table.Close()
	assert.NoError(t, err)
}

// ---------------------------------------------------------------------------
// InfluxDB sink integration test
// ---------------------------------------------------------------------------

func TestInfluxDBSink(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Use a simple HTTP server to simulate InfluxDB for now.
	// A full InfluxDB testcontainer would add significant startup time.
	received := make(chan string, 100)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if len(body) > 0 {
			received <- string(body)
		}
		w.WriteHeader(http.StatusNoContent)
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := &http.Server{Handler: mux}
	go server.Serve(ln)
	defer server.Shutdown(ctx)

	addr := ln.Addr().String()
	endpoint := fmt.Sprintf("http://%s", addr)

	filter, _ := sink.NewMetricFilter([]string{".*"})
	influx, err := sink.NewInfluxDBSink(endpoint, 0, "testdb", "", "", false, filter, slog.Default())
	require.NoError(t, err)
	defer influx.Stop()

	// Report some metrics.
	influx.Report(ctx, metrics.MetricValue{
		Definition: metrics.PartitionLatestOffset,
		Labels:     map[string]string{"cluster_name": "test-cluster", "topic": "test-topic", "partition": "0"},
		Value:      12345,
	})
	influx.Report(ctx, metrics.MetricValue{
		Definition: metrics.GroupLag,
		Labels:     map[string]string{"cluster_name": "test-cluster", "group": "test-group", "topic": "test-topic", "partition": "0", "member_host": "", "consumer_id": "", "client_id": ""},
		Value:      42,
	})

	// Check that data was received.
	var bodies []string
	timeout := time.After(5 * time.Second)
loop:
	for len(bodies) < 2 {
		select {
		case body := <-received:
			bodies = append(bodies, body)
		case <-timeout:
			break loop
		}
	}

	allData := strings.Join(bodies, "\n")
	assert.Contains(t, allData, "kafka_partition_latest_offset")
	assert.Contains(t, allData, "value=12345")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type recordingSink struct {
	mu       sync.Mutex
	reported []metrics.MetricValue
	removed  []metrics.RemoveMetric
}

func (r *recordingSink) Report(_ context.Context, m metrics.MetricValue) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reported = append(r.reported, m)
}

func (r *recordingSink) Remove(_ context.Context, m metrics.RemoveMetric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removed = append(r.removed, m)
}

func (r *recordingSink) Stop() {}

func (r *recordingSink) findMetric(name string) *metrics.MetricValue {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, m := range r.reported {
		if m.Definition.Name == name {
			return &r.reported[i]
		}
	}
	return nil
}
