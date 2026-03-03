package manager

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seglo/kafka-lag-exporter/internal/config"
	"github.com/seglo/kafka-lag-exporter/internal/domain"
	"github.com/seglo/kafka-lag-exporter/internal/kafka"
	"github.com/seglo/kafka-lag-exporter/internal/lookup"
	"github.com/seglo/kafka-lag-exporter/internal/metrics"
	"github.com/seglo/kafka-lag-exporter/internal/sink"
	"github.com/seglo/kafka-lag-exporter/internal/watcher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- helpers ----------------------------------------------------------------

type noopSink struct {
	mu       sync.Mutex
	reported []metrics.MetricValue
}

func (s *noopSink) Report(_ context.Context, m metrics.MetricValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reported = append(s.reported, m)
}
func (s *noopSink) Remove(_ context.Context, _ metrics.RemoveMetric) {}
func (s *noopSink) Stop()                                            {}

// mockKafkaClient is a minimal kafka.Client for manager tests.
type mockKafkaClient struct {
	closed atomic.Bool
}

func (m *mockKafkaClient) GetGroups(ctx context.Context) ([]string, []domain.GroupTopicPartition, error) {
	return nil, nil, nil
}
func (m *mockKafkaClient) GetGroupOffsets(ctx context.Context, now int64, groups []string, gtps []domain.GroupTopicPartition) (domain.GroupOffsets, error) {
	return domain.GroupOffsets{}, nil
}
func (m *mockKafkaClient) GetEarliestOffsets(ctx context.Context, now int64, tps []domain.TopicPartition) (domain.PartitionOffsets, error) {
	return domain.PartitionOffsets{}, nil
}
func (m *mockKafkaClient) GetLatestOffsets(ctx context.Context, now int64, tps []domain.TopicPartition) (domain.PartitionOffsets, error) {
	return domain.PartitionOffsets{}, nil
}
func (m *mockKafkaClient) Close() { m.closed.Store(true) }

type mockWatcher struct {
	events chan watcher.ClusterEvent
}

func (w *mockWatcher) Events() <-chan watcher.ClusterEvent { return w.events }
func (w *mockWatcher) Stop()                               { close(w.events) }

func testConfig() *config.Config {
	return &config.Config{
		PollIntervalSeconds:       30,
		KafkaClientTimeoutSeconds: 10,
		Lookup:                    config.LookupConfig{Memory: config.MemoryLookupConfig{Size: 60}},
	}
}

func mockFactory(clients map[string]*mockKafkaClient) ClientFactory {
	return func(clusterCfg config.ClusterConfig, _ *config.Config, _ *slog.Logger) (kafka.Client, error) {
		c := &mockKafkaClient{}
		if clients != nil {
			clients[clusterCfg.Name] = c
		}
		return c, nil
	}
}

func failingFactory(err error) ClientFactory {
	return func(_ config.ClusterConfig, _ *config.Config, _ *slog.Logger) (kafka.Client, error) {
		return nil, err
	}
}

// --- tests ------------------------------------------------------------------

func TestManager_AddCluster(t *testing.T) {
	clients := make(map[string]*mockKafkaClient)
	mgr := New(testConfig(), []sink.Sink{&noopSink{}}, func() lookup.LookupTable {
		return lookup.NewMemoryTable(60)
	}, slog.Default(), WithClientFactory(mockFactory(clients)))

	ctx := context.Background()
	err := mgr.AddCluster(ctx, config.ClusterConfig{Name: "c1", BootstrapBrokers: "b:9092"})
	require.NoError(t, err)

	mgr.mu.Lock()
	assert.Len(t, mgr.collectors, 1)
	assert.Contains(t, mgr.collectors, "c1")
	mgr.mu.Unlock()

	assert.NotNil(t, clients["c1"])
	mgr.Stop()
}

func TestManager_AddCluster_ReplacesExisting(t *testing.T) {
	clients := make(map[string]*mockKafkaClient)
	mgr := New(testConfig(), []sink.Sink{&noopSink{}}, func() lookup.LookupTable {
		return lookup.NewMemoryTable(60)
	}, slog.Default(), WithClientFactory(mockFactory(clients)))

	ctx := context.Background()
	err := mgr.AddCluster(ctx, config.ClusterConfig{Name: "c1", BootstrapBrokers: "b:9092"})
	require.NoError(t, err)
	firstClient := clients["c1"]

	// Add same cluster again — should replace.
	err = mgr.AddCluster(ctx, config.ClusterConfig{Name: "c1", BootstrapBrokers: "b:9093"})
	require.NoError(t, err)

	assert.True(t, firstClient.closed.Load(), "first client should have been closed")

	mgr.mu.Lock()
	assert.Len(t, mgr.collectors, 1)
	mgr.mu.Unlock()

	mgr.Stop()
}

func TestManager_AddCluster_FactoryError(t *testing.T) {
	mgr := New(testConfig(), []sink.Sink{&noopSink{}}, func() lookup.LookupTable {
		return lookup.NewMemoryTable(60)
	}, slog.Default(), WithClientFactory(failingFactory(fmt.Errorf("connection refused"))))

	err := mgr.AddCluster(context.Background(), config.ClusterConfig{Name: "bad", BootstrapBrokers: "b:9092"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")

	mgr.mu.Lock()
	assert.Len(t, mgr.collectors, 0)
	mgr.mu.Unlock()
}

func TestManager_RemoveCluster(t *testing.T) {
	clients := make(map[string]*mockKafkaClient)
	mgr := New(testConfig(), []sink.Sink{&noopSink{}}, func() lookup.LookupTable {
		return lookup.NewMemoryTable(60)
	}, slog.Default(), WithClientFactory(mockFactory(clients)))

	ctx := context.Background()
	err := mgr.AddCluster(ctx, config.ClusterConfig{Name: "c1", BootstrapBrokers: "b:9092"})
	require.NoError(t, err)

	mgr.RemoveCluster("c1")

	mgr.mu.Lock()
	assert.Len(t, mgr.collectors, 0)
	mgr.mu.Unlock()
	assert.True(t, clients["c1"].closed.Load())
}

func TestManager_RemoveCluster_Unknown(t *testing.T) {
	mgr := New(testConfig(), []sink.Sink{&noopSink{}}, func() lookup.LookupTable {
		return lookup.NewMemoryTable(60)
	}, slog.Default(), WithClientFactory(mockFactory(nil)))

	// Should not panic.
	mgr.RemoveCluster("nonexistent")

	mgr.mu.Lock()
	assert.Len(t, mgr.collectors, 0)
	mgr.mu.Unlock()
}

func TestManager_Stop(t *testing.T) {
	clients := make(map[string]*mockKafkaClient)
	s := &noopSink{}
	mgr := New(testConfig(), []sink.Sink{s}, func() lookup.LookupTable {
		return lookup.NewMemoryTable(60)
	}, slog.Default(), WithClientFactory(mockFactory(clients)))

	ctx := context.Background()
	_ = mgr.AddCluster(ctx, config.ClusterConfig{Name: "c1", BootstrapBrokers: "b:9092"})
	_ = mgr.AddCluster(ctx, config.ClusterConfig{Name: "c2", BootstrapBrokers: "b:9092"})

	mgr.Stop()

	assert.True(t, clients["c1"].closed.Load())
	assert.True(t, clients["c2"].closed.Load())
}

func TestManager_Start_StaticClusters(t *testing.T) {
	cfg := testConfig()
	cfg.Clusters = []config.ClusterConfig{
		{Name: "static-1", BootstrapBrokers: "b:9092"},
		{Name: "static-2", BootstrapBrokers: "b:9093"},
	}

	clients := make(map[string]*mockKafkaClient)
	mgr := New(cfg, []sink.Sink{&noopSink{}}, func() lookup.LookupTable {
		return lookup.NewMemoryTable(60)
	}, slog.Default(), WithClientFactory(mockFactory(clients)))

	err := mgr.Start(context.Background())
	require.NoError(t, err)

	mgr.mu.Lock()
	assert.Len(t, mgr.collectors, 2)
	mgr.mu.Unlock()

	mgr.Stop()
}

func TestManager_Start_FactoryError(t *testing.T) {
	cfg := testConfig()
	cfg.Clusters = []config.ClusterConfig{
		{Name: "fail", BootstrapBrokers: "b:9092"},
	}

	mgr := New(cfg, []sink.Sink{&noopSink{}}, func() lookup.LookupTable {
		return lookup.NewMemoryTable(60)
	}, slog.Default(), WithClientFactory(failingFactory(fmt.Errorf("boom"))))

	err := mgr.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

func TestManager_WatcherEvents(t *testing.T) {
	clients := make(map[string]*mockKafkaClient)
	mgr := New(testConfig(), []sink.Sink{&noopSink{}}, func() lookup.LookupTable {
		return lookup.NewMemoryTable(60)
	}, slog.Default(), WithClientFactory(mockFactory(clients)))

	events := make(chan watcher.ClusterEvent, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mw := &mockWatcher{events: events}
	go mgr.consumeWatcherEvents(ctx, mw)

	// Add a cluster via watcher event.
	events <- watcher.ClusterEvent{
		Type:    watcher.ClusterAdded,
		Cluster: config.ClusterConfig{Name: "watched-1", BootstrapBrokers: "b:9092"},
	}

	// Wait for it to be added.
	require.Eventually(t, func() bool {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		return len(mgr.collectors) == 1
	}, 2*time.Second, 10*time.Millisecond)

	// Remove it.
	events <- watcher.ClusterEvent{
		Type:    watcher.ClusterRemoved,
		Cluster: config.ClusterConfig{Name: "watched-1"},
	}

	require.Eventually(t, func() bool {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		return len(mgr.collectors) == 0
	}, 2*time.Second, 10*time.Millisecond)

	assert.True(t, clients["watched-1"].closed.Load())

	// Remove unknown — no panic.
	events <- watcher.ClusterEvent{
		Type:    watcher.ClusterRemoved,
		Cluster: config.ClusterConfig{Name: "nonexistent"},
	}

	mgr.Stop()
}

func TestManager_WatcherEvents_ContextCancellation(t *testing.T) {
	mgr := New(testConfig(), []sink.Sink{&noopSink{}}, func() lookup.LookupTable {
		return lookup.NewMemoryTable(60)
	}, slog.Default(), WithClientFactory(mockFactory(nil)))

	events := make(chan watcher.ClusterEvent, 1)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	mw := &mockWatcher{events: events}
	go func() {
		mgr.consumeWatcherEvents(ctx, mw)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("consumeWatcherEvents did not return after context cancel")
	}
}

func TestManager_WatcherEvents_ChannelClose(t *testing.T) {
	mgr := New(testConfig(), []sink.Sink{&noopSink{}}, func() lookup.LookupTable {
		return lookup.NewMemoryTable(60)
	}, slog.Default(), WithClientFactory(mockFactory(nil)))

	events := make(chan watcher.ClusterEvent, 1)
	ctx := context.Background()

	done := make(chan struct{})
	mw := &mockWatcher{events: events}
	go func() {
		mgr.consumeWatcherEvents(ctx, mw)
		close(done)
	}()

	close(events)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("consumeWatcherEvents did not return after channel close")
	}
}

func TestManager_Start_WithStrimziWatcher(t *testing.T) {
	cfg := testConfig()
	cfg.Watchers.Strimzi = true

	events := make(chan watcher.ClusterEvent, 4)
	mw := &mockWatcher{events: events}

	mgr := New(cfg, []sink.Sink{&noopSink{}}, func() lookup.LookupTable {
		return lookup.NewMemoryTable(60)
	}, slog.Default(),
		WithClientFactory(mockFactory(nil)),
		WithWatcherFactory(func(logger *slog.Logger) (watcher.Watcher, error) {
			return mw, nil
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := mgr.Start(ctx)
	require.NoError(t, err)

	assert.Len(t, mgr.watchers, 1)

	mgr.Stop()
}

func TestManager_Start_WatcherFactoryError(t *testing.T) {
	cfg := testConfig()
	cfg.Watchers.Strimzi = true

	mgr := New(cfg, []sink.Sink{&noopSink{}}, func() lookup.LookupTable {
		return lookup.NewMemoryTable(60)
	}, slog.Default(),
		WithClientFactory(mockFactory(nil)),
		WithWatcherFactory(func(logger *slog.Logger) (watcher.Watcher, error) {
			return nil, fmt.Errorf("no in-cluster config")
		}),
	)

	err := mgr.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no in-cluster config")
}

func TestManager_Stop_WithWatchersAndSinks(t *testing.T) {
	cfg := testConfig()

	events := make(chan watcher.ClusterEvent, 1)
	mw := &mockWatcher{events: events}
	stopped := false
	s := &trackingStopSink{onStop: func() { stopped = true }}

	mgr := New(cfg, []sink.Sink{s}, func() lookup.LookupTable {
		return lookup.NewMemoryTable(60)
	}, slog.Default(), WithClientFactory(mockFactory(nil)))

	mgr.watchers = append(mgr.watchers, mw)

	mgr.Stop()
	assert.True(t, stopped)
}

type trackingStopSink struct {
	noopSink
	onStop func()
}

func (s *trackingStopSink) Stop() {
	if s.onStop != nil {
		s.onStop()
	}
}

func TestManager_WatcherEvents_AddClusterError(t *testing.T) {
	mgr := New(testConfig(), []sink.Sink{&noopSink{}}, func() lookup.LookupTable {
		return lookup.NewMemoryTable(60)
	}, slog.Default(), WithClientFactory(failingFactory(fmt.Errorf("conn refused"))))

	events := make(chan watcher.ClusterEvent, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mw := &mockWatcher{events: events}
	go mgr.consumeWatcherEvents(ctx, mw)

	// Send an add event that will fail — manager should log error but not crash.
	events <- watcher.ClusterEvent{
		Type:    watcher.ClusterAdded,
		Cluster: config.ClusterConfig{Name: "fail-cluster", BootstrapBrokers: "b:9092"},
	}

	// Give time for the event to be processed.
	time.Sleep(100 * time.Millisecond)

	mgr.mu.Lock()
	assert.Len(t, mgr.collectors, 0)
	mgr.mu.Unlock()
}
