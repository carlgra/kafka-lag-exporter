package collector

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/seglo/kafka-lag-exporter/internal/config"
	"github.com/seglo/kafka-lag-exporter/internal/domain"
	"github.com/seglo/kafka-lag-exporter/internal/lookup"
	"github.com/seglo/kafka-lag-exporter/internal/metrics"
	"github.com/seglo/kafka-lag-exporter/internal/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockClient implements kafka.Client for testing.
type mockClient struct {
	groups      []string
	gtps        []domain.GroupTopicPartition
	groupOff    domain.GroupOffsets
	earliestOff domain.PartitionOffsets
	latestOff   domain.PartitionOffsets
	err         error
}

func (m *mockClient) GetGroups(ctx context.Context) ([]string, []domain.GroupTopicPartition, error) {
	return m.groups, m.gtps, m.err
}

func (m *mockClient) GetGroupOffsets(ctx context.Context, now int64, groups []string, gtps []domain.GroupTopicPartition) (domain.GroupOffsets, error) {
	return m.groupOff, m.err
}

func (m *mockClient) GetEarliestOffsets(ctx context.Context, now int64, tps []domain.TopicPartition) (domain.PartitionOffsets, error) {
	return m.earliestOff, m.err
}

func (m *mockClient) GetLatestOffsets(ctx context.Context, now int64, tps []domain.TopicPartition) (domain.PartitionOffsets, error) {
	return m.latestOff, m.err
}

func (m *mockClient) Close() {}

// recordingSink records all reported and removed metrics.
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

func (r *recordingSink) findMetricWithLabels(name string, labels map[string]string) *metrics.MetricValue {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, m := range r.reported {
		if m.Definition.Name == name && labelsEqual(m.Labels, labels) {
			return &r.reported[i]
		}
	}
	return nil
}

func labelsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestCollector_SinglePollCycle(t *testing.T) {
	tp := domain.TopicPartition{Topic: "test-topic", Partition: 0}
	gtp := domain.GroupTopicPartition{
		Group: "test-group", Topic: "test-topic", Partition: 0,
		MemberHost: "host1", ConsumerID: "consumer-1", ClientID: "client-1",
	}

	mock := &mockClient{
		groups: []string{"test-group"},
		gtps:   []domain.GroupTopicPartition{gtp},
		groupOff: domain.GroupOffsets{
			gtp: {Offset: 90, Timestamp: 1000},
		},
		earliestOff: domain.PartitionOffsets{
			tp: {Offset: 0, Timestamp: 1000},
		},
		latestOff: domain.PartitionOffsets{
			tp: {Offset: 100, Timestamp: 1000},
		},
	}

	rec := &recordingSink{}

	clusterCfg := config.ClusterConfig{Name: "test-cluster"}
	c, err := NewCollector(
		clusterCfg,
		mock,
		[]sink.Sink{rec},
		func() lookup.LookupTable { return lookup.NewMemoryTable(60) },
		10*time.Second,
		slog.Default(),
	)
	require.NoError(t, err)

	// Run one poll cycle.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	c.Run(ctx)

	// Verify metrics were reported.
	latestOffsetMetric := rec.findMetricWithLabels("kafka_partition_latest_offset", map[string]string{"cluster_name": "test-cluster", "topic": "test-topic", "partition": "0"})
	require.NotNil(t, latestOffsetMetric)
	assert.Equal(t, float64(100), latestOffsetMetric.Value)

	earliestOffsetMetric := rec.findMetricWithLabels("kafka_partition_earliest_offset", map[string]string{"cluster_name": "test-cluster", "topic": "test-topic", "partition": "0"})
	require.NotNil(t, earliestOffsetMetric)
	assert.Equal(t, float64(0), earliestOffsetMetric.Value)

	groupOffsetMetric := rec.findMetric("kafka_consumergroup_group_offset")
	require.NotNil(t, groupOffsetMetric)
	assert.Equal(t, float64(90), groupOffsetMetric.Value)

	lagMetric := rec.findMetric("kafka_consumergroup_group_lag")
	require.NotNil(t, lagMetric)
	assert.Equal(t, float64(10), lagMetric.Value)

	maxLagMetric := rec.findMetric("kafka_consumergroup_group_max_lag")
	require.NotNil(t, maxLagMetric)
	assert.Equal(t, float64(10), maxLagMetric.Value)

	sumLagMetric := rec.findMetric("kafka_consumergroup_group_sum_lag")
	require.NotNil(t, sumLagMetric)
	assert.Equal(t, float64(10), sumLagMetric.Value)

	topicSumLag := rec.findMetric("kafka_consumergroup_group_topic_sum_lag")
	require.NotNil(t, topicSumLag)
	assert.Equal(t, float64(10), topicSumLag.Value)

	pollTime := rec.findMetric("kafka_consumergroup_poll_time_ms")
	require.NotNil(t, pollTime)
}

func TestCollector_GroupFiltering(t *testing.T) {
	clusterCfg := config.ClusterConfig{
		Name:           "test-cluster",
		GroupAllowlist: []string{"^allowed.*"},
		GroupDenylist:  []string{"^denied.*"},
	}

	mock := &mockClient{
		groups: []string{"allowed-group", "denied-group", "other-group"},
		gtps: []domain.GroupTopicPartition{
			{Group: "allowed-group", Topic: "t", Partition: 0},
			{Group: "denied-group", Topic: "t", Partition: 0},
			{Group: "other-group", Topic: "t", Partition: 0},
		},
		groupOff:    domain.GroupOffsets{},
		earliestOff: domain.PartitionOffsets{},
		latestOff:   domain.PartitionOffsets{},
	}

	rec := &recordingSink{}

	c, err := NewCollector(
		clusterCfg, mock, []sink.Sink{rec},
		func() lookup.LookupTable { return lookup.NewMemoryTable(60) },
		10*time.Second, slog.Default(),
	)
	require.NoError(t, err)

	// Test filtering.
	filtered := c.filterGroups(mock.groups)
	assert.Equal(t, []string{"allowed-group"}, filtered)
}

func TestSnapshot_RemovedGTPs(t *testing.T) {
	gtp1 := domain.GroupTopicPartition{Group: "g1", Topic: "t1", Partition: 0}
	gtp2 := domain.GroupTopicPartition{Group: "g1", Topic: "t1", Partition: 1}

	prev := &OffsetsSnapshot{
		GroupOffsets: domain.GroupOffsets{
			gtp1: {Offset: 10},
			gtp2: {Offset: 20},
		},
	}
	current := &OffsetsSnapshot{
		GroupOffsets: domain.GroupOffsets{
			gtp1: {Offset: 15},
		},
	}

	removed := RemovedGroupTopicPartitions(prev, current)
	require.Len(t, removed, 1)
	assert.Equal(t, gtp2, removed[0])
}

func TestSnapshot_RemovedTPs(t *testing.T) {
	tp1 := domain.TopicPartition{Topic: "t1", Partition: 0}
	tp2 := domain.TopicPartition{Topic: "t1", Partition: 1}

	prev := &OffsetsSnapshot{
		LatestOffsets: domain.PartitionOffsets{
			tp1: {Offset: 100},
			tp2: {Offset: 200},
		},
	}
	current := &OffsetsSnapshot{
		LatestOffsets: domain.PartitionOffsets{
			tp1: {Offset: 150},
		},
	}

	removed := RemovedTopicPartitions(prev, current)
	require.Len(t, removed, 1)
	assert.Equal(t, tp2, removed[0])
}

// --- Eviction tests ---------------------------------------------------------

func TestCollector_EvictRemovedGTPs(t *testing.T) {
	tp := domain.TopicPartition{Topic: "t1", Partition: 0}
	gtp1 := domain.GroupTopicPartition{Group: "g1", Topic: "t1", Partition: 0, MemberHost: "h1", ConsumerID: "c1", ClientID: "cl1"}
	gtp2 := domain.GroupTopicPartition{Group: "g2", Topic: "t1", Partition: 0, MemberHost: "h2", ConsumerID: "c2", ClientID: "cl2"}

	// First poll: both GTPs present.
	firstPollClient := &mockClient{
		groups:      []string{"g1", "g2"},
		gtps:        []domain.GroupTopicPartition{gtp1, gtp2},
		groupOff:    domain.GroupOffsets{gtp1: {Offset: 10}, gtp2: {Offset: 20}},
		earliestOff: domain.PartitionOffsets{tp: {Offset: 0}},
		latestOff:   domain.PartitionOffsets{tp: {Offset: 100}},
	}

	rec := &recordingSink{}
	c, err := NewCollector(
		config.ClusterConfig{Name: "test"},
		firstPollClient,
		[]sink.Sink{rec},
		func() lookup.LookupTable { return lookup.NewMemoryTable(60) },
		10*time.Second, slog.Default(),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	c.Run(ctx)

	// Second poll: g2 removed.
	rec2 := &recordingSink{}
	secondPollClient := &mockClient{
		groups:      []string{"g1"},
		gtps:        []domain.GroupTopicPartition{gtp1},
		groupOff:    domain.GroupOffsets{gtp1: {Offset: 15}},
		earliestOff: domain.PartitionOffsets{tp: {Offset: 0}},
		latestOff:   domain.PartitionOffsets{tp: {Offset: 110}},
	}

	c.client = secondPollClient
	c.sinks = []sink.Sink{rec2}
	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel2()
	}()
	c.Run(ctx2)

	// Verify g2's metrics were evicted.
	var removedNames []string
	for _, r := range rec2.removed {
		removedNames = append(removedNames, r.Definition.Name)
	}
	assert.Contains(t, removedNames, "kafka_consumergroup_group_offset")
	assert.Contains(t, removedNames, "kafka_consumergroup_group_lag")
	assert.Contains(t, removedNames, "kafka_consumergroup_group_lag_seconds")
	assert.Contains(t, removedNames, "kafka_consumergroup_group_max_lag")
	assert.Contains(t, removedNames, "kafka_consumergroup_group_max_lag_seconds")
	assert.Contains(t, removedNames, "kafka_consumergroup_group_sum_lag")
}

func TestCollector_EvictRemovedTopicPartitions(t *testing.T) {
	tp1 := domain.TopicPartition{Topic: "t1", Partition: 0}
	tp2 := domain.TopicPartition{Topic: "t1", Partition: 1}
	gtp1 := domain.GroupTopicPartition{Group: "g1", Topic: "t1", Partition: 0}
	gtp2 := domain.GroupTopicPartition{Group: "g1", Topic: "t1", Partition: 1}

	// First poll: both TPs present.
	firstPollClient := &mockClient{
		groups:      []string{"g1"},
		gtps:        []domain.GroupTopicPartition{gtp1, gtp2},
		groupOff:    domain.GroupOffsets{gtp1: {Offset: 10}, gtp2: {Offset: 20}},
		earliestOff: domain.PartitionOffsets{tp1: {Offset: 0}, tp2: {Offset: 0}},
		latestOff:   domain.PartitionOffsets{tp1: {Offset: 100}, tp2: {Offset: 200}},
	}

	rec := &recordingSink{}
	c, err := NewCollector(
		config.ClusterConfig{Name: "test"},
		firstPollClient,
		[]sink.Sink{rec},
		func() lookup.LookupTable { return lookup.NewMemoryTable(60) },
		10*time.Second, slog.Default(),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	c.Run(ctx)

	// Verify lookup table created for both TPs.
	assert.Contains(t, c.lookupTables, tp1)
	assert.Contains(t, c.lookupTables, tp2)

	// Second poll: tp2 gone.
	rec2 := &recordingSink{}
	c.client = &mockClient{
		groups:      []string{"g1"},
		gtps:        []domain.GroupTopicPartition{gtp1},
		groupOff:    domain.GroupOffsets{gtp1: {Offset: 15}},
		earliestOff: domain.PartitionOffsets{tp1: {Offset: 0}},
		latestOff:   domain.PartitionOffsets{tp1: {Offset: 110}},
	}
	c.sinks = []sink.Sink{rec2}

	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel2()
	}()
	c.Run(ctx2)

	// Verify tp2 evicted from lookup tables.
	assert.NotContains(t, c.lookupTables, tp2)
	assert.Contains(t, c.lookupTables, tp1)

	// Verify partition metrics removed.
	var removedNames []string
	for _, r := range rec2.removed {
		removedNames = append(removedNames, r.Definition.Name)
	}
	assert.Contains(t, removedNames, "kafka_partition_latest_offset")
	assert.Contains(t, removedNames, "kafka_partition_earliest_offset")
}

func TestCollector_EvictRemovedGroupAggregates(t *testing.T) {
	tp := domain.TopicPartition{Topic: "t1", Partition: 0}
	gtp1 := domain.GroupTopicPartition{Group: "g1", Topic: "t1", Partition: 0}
	gtp2 := domain.GroupTopicPartition{Group: "g2", Topic: "t1", Partition: 0}

	// First poll: both groups present.
	rec := &recordingSink{}
	c, err := NewCollector(
		config.ClusterConfig{Name: "test"},
		&mockClient{
			groups:      []string{"g1", "g2"},
			gtps:        []domain.GroupTopicPartition{gtp1, gtp2},
			groupOff:    domain.GroupOffsets{gtp1: {Offset: 10}, gtp2: {Offset: 20}},
			earliestOff: domain.PartitionOffsets{tp: {Offset: 0}},
			latestOff:   domain.PartitionOffsets{tp: {Offset: 100}},
		},
		[]sink.Sink{rec},
		func() lookup.LookupTable { return lookup.NewMemoryTable(60) },
		10*time.Second, slog.Default(),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()
	c.Run(ctx)

	// Second poll: g2 removed entirely (no topic-partitions for it either).
	rec2 := &recordingSink{}
	c.client = &mockClient{
		groups:      []string{"g1"},
		gtps:        []domain.GroupTopicPartition{gtp1},
		groupOff:    domain.GroupOffsets{gtp1: {Offset: 15}},
		earliestOff: domain.PartitionOffsets{tp: {Offset: 0}},
		latestOff:   domain.PartitionOffsets{tp: {Offset: 110}},
	}
	c.sinks = []sink.Sink{rec2}

	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() { time.Sleep(200 * time.Millisecond); cancel2() }()
	c.Run(ctx2)

	// Verify group aggregate metrics removed for g2.
	removedMap := make(map[string]bool)
	for _, r := range rec2.removed {
		removedMap[r.Definition.Name] = true
	}
	assert.True(t, removedMap["kafka_consumergroup_group_max_lag"])
	assert.True(t, removedMap["kafka_consumergroup_group_max_lag_seconds"])
	assert.True(t, removedMap["kafka_consumergroup_group_sum_lag"])
	assert.True(t, removedMap["kafka_consumergroup_group_topic_sum_lag"])
}

// --- Filtering tests --------------------------------------------------------

func TestCollector_TopicFiltering(t *testing.T) {
	clusterCfg := config.ClusterConfig{
		Name:           "test-cluster",
		TopicAllowlist: []string{"^allowed-.*"},
		TopicDenylist:  []string{"^allowed-internal.*"},
	}

	mock := &mockClient{
		groups: []string{"g1"},
		gtps: []domain.GroupTopicPartition{
			{Group: "g1", Topic: "allowed-events", Partition: 0},
			{Group: "g1", Topic: "allowed-internal-metrics", Partition: 0},
			{Group: "g1", Topic: "other-topic", Partition: 0},
		},
		groupOff:    domain.GroupOffsets{},
		earliestOff: domain.PartitionOffsets{},
		latestOff:   domain.PartitionOffsets{},
	}

	c, err := NewCollector(
		clusterCfg, mock, []sink.Sink{&recordingSink{}},
		func() lookup.LookupTable { return lookup.NewMemoryTable(60) },
		10*time.Second, slog.Default(),
	)
	require.NoError(t, err)

	// Filter topic-partitions.
	tps := []domain.TopicPartition{
		{Topic: "allowed-events", Partition: 0},
		{Topic: "allowed-internal-metrics", Partition: 0},
		{Topic: "other-topic", Partition: 0},
	}
	filtered := c.filterTopicPartitions(tps)
	require.Len(t, filtered, 1)
	assert.Equal(t, "allowed-events", filtered[0].Topic)
}

func TestCollector_CombinedGroupAndTopicFiltering(t *testing.T) {
	clusterCfg := config.ClusterConfig{
		Name:           "test-cluster",
		GroupAllowlist: []string{"^app-.*"},
		TopicAllowlist: []string{"^events-.*"},
	}

	mock := &mockClient{
		groups: []string{"app-service", "infra-monitor"},
		gtps: []domain.GroupTopicPartition{
			{Group: "app-service", Topic: "events-orders", Partition: 0},
			{Group: "app-service", Topic: "logs-access", Partition: 0},
			{Group: "infra-monitor", Topic: "events-orders", Partition: 0},
		},
		groupOff:    domain.GroupOffsets{},
		earliestOff: domain.PartitionOffsets{},
		latestOff:   domain.PartitionOffsets{},
	}

	c, err := NewCollector(
		clusterCfg, mock, []sink.Sink{&recordingSink{}},
		func() lookup.LookupTable { return lookup.NewMemoryTable(60) },
		10*time.Second, slog.Default(),
	)
	require.NoError(t, err)

	// Filter groups.
	groups := c.filterGroups(mock.groups)
	assert.Equal(t, []string{"app-service"}, groups)

	// Filter GTPs (combined group + topic filter).
	filteredGTPs := c.filterGTPs(mock.gtps)
	require.Len(t, filteredGTPs, 1)
	assert.Equal(t, "app-service", filteredGTPs[0].Group)
	assert.Equal(t, "events-orders", filteredGTPs[0].Topic)
}

func TestCollector_DenylistOnly(t *testing.T) {
	clusterCfg := config.ClusterConfig{
		Name:          "test",
		GroupDenylist: []string{"^__.*"}, // filter internal groups
	}

	c, err := NewCollector(
		clusterCfg, &mockClient{}, []sink.Sink{&recordingSink{}},
		func() lookup.LookupTable { return lookup.NewMemoryTable(60) },
		10*time.Second, slog.Default(),
	)
	require.NoError(t, err)

	groups := c.filterGroups([]string{"my-app", "__consumer_offsets", "__redpanda"})
	assert.Equal(t, []string{"my-app"}, groups)
}

func TestCollector_NoFilters(t *testing.T) {
	clusterCfg := config.ClusterConfig{Name: "test"}

	c, err := NewCollector(
		clusterCfg, &mockClient{}, []sink.Sink{&recordingSink{}},
		func() lookup.LookupTable { return lookup.NewMemoryTable(60) },
		10*time.Second, slog.Default(),
	)
	require.NoError(t, err)

	groups := []string{"a", "b", "c"}
	assert.Equal(t, groups, c.filterGroups(groups))

	gtps := []domain.GroupTopicPartition{
		{Group: "a", Topic: "t1", Partition: 0},
	}
	assert.Equal(t, gtps, c.filterGTPs(gtps))
}

func TestCollector_InvalidFilterPattern(t *testing.T) {
	clusterCfg := config.ClusterConfig{
		Name:           "test",
		GroupAllowlist: []string{"[invalid"},
	}

	_, err := NewCollector(
		clusterCfg, &mockClient{}, nil,
		func() lookup.LookupTable { return lookup.NewMemoryTable(60) },
		10*time.Second, slog.Default(),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "compiling group allowlist")
}

// --- Poll error tests -------------------------------------------------------

func TestCollector_PollError_ContinuesLoop(t *testing.T) {
	failClient := &mockClient{err: fmt.Errorf("kafka unavailable")}
	rec := &recordingSink{}

	c, err := NewCollector(
		config.ClusterConfig{Name: "test"},
		failClient,
		[]sink.Sink{rec},
		func() lookup.LookupTable { return lookup.NewMemoryTable(60) },
		10*time.Second, slog.Default(),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	c.Run(ctx)

	// No metrics should be reported on error.
	rec.mu.Lock()
	assert.Empty(t, rec.reported)
	rec.mu.Unlock()
}

// --- Stop test --------------------------------------------------------------

func TestCollector_Stop(t *testing.T) {
	closed := false
	closingClient := &mockClosableClient{onClose: func() { closed = true }}

	c, err := NewCollector(
		config.ClusterConfig{Name: "test"},
		closingClient,
		nil,
		func() lookup.LookupTable { return lookup.NewMemoryTable(60) },
		10*time.Second, slog.Default(),
	)
	require.NoError(t, err)

	c.Stop()
	assert.True(t, closed)
}

type mockClosableClient struct {
	mockClient
	onClose func()
}

func (m *mockClosableClient) Close() {
	if m.onClose != nil {
		m.onClose()
	}
}

// --- Snapshot helper tests --------------------------------------------------

func TestSnapshot_AllTopicPartitions(t *testing.T) {
	tp1 := domain.TopicPartition{Topic: "t1", Partition: 0}
	tp2 := domain.TopicPartition{Topic: "t2", Partition: 1}

	s := &OffsetsSnapshot{
		LatestOffsets: domain.PartitionOffsets{
			tp1: {Offset: 100},
			tp2: {Offset: 200},
		},
	}

	tps := s.AllTopicPartitions()
	assert.Len(t, tps, 2)
}

func TestSnapshot_AllGroupTopicPartitions(t *testing.T) {
	gtp1 := domain.GroupTopicPartition{Group: "g1", Topic: "t1", Partition: 0}
	gtp2 := domain.GroupTopicPartition{Group: "g1", Topic: "t1", Partition: 1}

	s := &OffsetsSnapshot{
		GroupOffsets: domain.GroupOffsets{
			gtp1: {Offset: 10},
			gtp2: {Offset: 20},
		},
	}

	gtps := s.AllGroupTopicPartitions()
	assert.Len(t, gtps, 2)
}

func TestSnapshot_RemovedGTPs_NilPrev(t *testing.T) {
	current := &OffsetsSnapshot{GroupOffsets: domain.GroupOffsets{}}
	assert.Nil(t, RemovedGroupTopicPartitions(nil, current))
}

func TestSnapshot_RemovedTPs_NilPrev(t *testing.T) {
	current := &OffsetsSnapshot{LatestOffsets: domain.PartitionOffsets{}}
	assert.Nil(t, RemovedTopicPartitions(nil, current))
}

func TestSnapshot_RemovedGTPs_NilCurrent(t *testing.T) {
	prev := &OffsetsSnapshot{GroupOffsets: domain.GroupOffsets{
		{Group: "g1", Topic: "t1", Partition: 0}: {Offset: 10},
	}}
	assert.Nil(t, RemovedGroupTopicPartitions(prev, nil))
}

func TestSnapshot_RemovedTPs_NilCurrent(t *testing.T) {
	prev := &OffsetsSnapshot{LatestOffsets: domain.PartitionOffsets{
		{Topic: "t1", Partition: 0}: {Offset: 100},
	}}
	assert.Nil(t, RemovedTopicPartitions(prev, nil))
}

// --- Client metrics reporting ------------------------------------------------

type mockClientWithMetrics struct {
	mockClient
}

func (m *mockClientWithMetrics) ClientMetrics() (connects, disconnects, writeErrors, readErrors int64) {
	return 5, 2, 1, 3
}

type clientMetricsRecorder struct {
	recordingSink
	clusterName                                    string
	connects, disconnects, writeErrors, readErrors int64
}

func (r *clientMetricsRecorder) ReportClientMetrics(cluster string, connects, disconnects, writeErrors, readErrors int64) {
	r.clusterName = cluster
	r.connects = connects
	r.disconnects = disconnects
	r.writeErrors = writeErrors
	r.readErrors = readErrors
}

func TestCollector_ReportClientMetrics(t *testing.T) {
	tp := domain.TopicPartition{Topic: "t1", Partition: 0}
	gtp := domain.GroupTopicPartition{Group: "g1", Topic: "t1", Partition: 0}

	mock := &mockClientWithMetrics{mockClient: mockClient{
		groups:      []string{"g1"},
		gtps:        []domain.GroupTopicPartition{gtp},
		groupOff:    domain.GroupOffsets{gtp: {Offset: 10}},
		earliestOff: domain.PartitionOffsets{tp: {Offset: 0}},
		latestOff:   domain.PartitionOffsets{tp: {Offset: 100}},
	}}

	rec := &clientMetricsRecorder{}
	c, err := NewCollector(
		config.ClusterConfig{Name: "metrics-test"},
		mock, []sink.Sink{rec},
		func() lookup.LookupTable { return lookup.NewMemoryTable(60) },
		10*time.Second, slog.Default(),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()
	c.Run(ctx)

	assert.Equal(t, "metrics-test", rec.clusterName)
	assert.Equal(t, int64(5), rec.connects)
	assert.Equal(t, int64(2), rec.disconnects)
	assert.Equal(t, int64(1), rec.writeErrors)
	assert.Equal(t, int64(3), rec.readErrors)
}

func TestCollector_ReportClientMetrics_NoProvider(t *testing.T) {
	// Regular mockClient doesn't implement ClientMetrics — should not panic.
	tp := domain.TopicPartition{Topic: "t1", Partition: 0}
	gtp := domain.GroupTopicPartition{Group: "g1", Topic: "t1", Partition: 0}

	mock := &mockClient{
		groups:      []string{"g1"},
		gtps:        []domain.GroupTopicPartition{gtp},
		groupOff:    domain.GroupOffsets{gtp: {Offset: 10}},
		earliestOff: domain.PartitionOffsets{tp: {Offset: 0}},
		latestOff:   domain.PartitionOffsets{tp: {Offset: 100}},
	}

	rec := &recordingSink{}
	c, err := NewCollector(
		config.ClusterConfig{Name: "test"},
		mock, []sink.Sink{rec},
		func() lookup.LookupTable { return lookup.NewMemoryTable(60) },
		10*time.Second, slog.Default(),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()
	c.Run(ctx) // Should not panic.
}

// --- Negative lag clamping --------------------------------------------------

// --- TooFewPoints / NaN lag seconds tests -----------------------------------

func TestCollector_TooFewPoints_LagSecondsIsNaN(t *testing.T) {
	// On the first poll cycle the lookup table has only one point, so Lookup
	// returns Found=false (TooFewPoints). The collector should report NaN for
	// lag_seconds so the Prometheus sink's NaN filter skips it.
	tp := domain.TopicPartition{Topic: "t1", Partition: 0}
	gtp := domain.GroupTopicPartition{
		Group: "g1", Topic: "t1", Partition: 0,
		MemberHost: "h1", ConsumerID: "c1", ClientID: "cl1",
	}

	mock := &mockClient{
		groups: []string{"g1"},
		gtps:   []domain.GroupTopicPartition{gtp},
		groupOff: domain.GroupOffsets{
			gtp: {Offset: 90, Timestamp: 1000},
		},
		earliestOff: domain.PartitionOffsets{tp: {Offset: 0, Timestamp: 1000}},
		latestOff:   domain.PartitionOffsets{tp: {Offset: 100, Timestamp: 1000}},
	}

	rec := &recordingSink{}
	c, err := NewCollector(
		config.ClusterConfig{Name: "test"},
		mock, []sink.Sink{rec},
		func() lookup.LookupTable { return lookup.NewMemoryTable(60) },
		10*time.Second, slog.Default(),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()
	c.Run(ctx)

	// lag_seconds should be NaN (TooFewPoints on first cycle).
	lagSec := rec.findMetric("kafka_consumergroup_group_lag_seconds")
	require.NotNil(t, lagSec)
	assert.True(t, math.IsNaN(lagSec.Value), "expected NaN for lag_seconds on first poll cycle, got %v", lagSec.Value)

	// max_lag_seconds should NOT be reported when all partitions return NaN.
	maxLagSec := rec.findMetric("kafka_consumergroup_group_max_lag_seconds")
	assert.Nil(t, maxLagSec, "max_lag_seconds should not be reported when all partition lag_seconds are NaN")
}

func TestCollector_NegativeLagClampedToZero(t *testing.T) {
	tp := domain.TopicPartition{Topic: "t1", Partition: 0}
	gtp := domain.GroupTopicPartition{Group: "g1", Topic: "t1", Partition: 0}

	mock := &mockClient{
		groups: []string{"g1"},
		gtps:   []domain.GroupTopicPartition{gtp},
		groupOff: domain.GroupOffsets{
			gtp: {Offset: 200, Timestamp: 1000}, // Ahead of latest
		},
		earliestOff: domain.PartitionOffsets{tp: {Offset: 0, Timestamp: 1000}},
		latestOff:   domain.PartitionOffsets{tp: {Offset: 100, Timestamp: 1000}},
	}

	rec := &recordingSink{}
	c, err := NewCollector(
		config.ClusterConfig{Name: "test"},
		mock, []sink.Sink{rec},
		func() lookup.LookupTable { return lookup.NewMemoryTable(60) },
		10*time.Second, slog.Default(),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()
	c.Run(ctx)

	lag := rec.findMetric("kafka_consumergroup_group_lag")
	require.NotNil(t, lag)
	assert.Equal(t, float64(0), lag.Value) // Clamped to 0, not -100.
}
