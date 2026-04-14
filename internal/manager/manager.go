// Package manager orchestrates collectors, watchers, and sinks.
package manager

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/seglo/kafka-lag-exporter/internal/collector"
	"github.com/seglo/kafka-lag-exporter/internal/config"
	"github.com/seglo/kafka-lag-exporter/internal/kafka"
	"github.com/seglo/kafka-lag-exporter/internal/lookup"
	"github.com/seglo/kafka-lag-exporter/internal/sink"
	"github.com/seglo/kafka-lag-exporter/internal/watcher"
)

type collectorEntry struct {
	collector *collector.Collector
	cancel    context.CancelFunc
}

// ClientFactory creates a kafka.Client for the given cluster config.
type ClientFactory func(clusterCfg config.ClusterConfig, globalCfg *config.Config, logger *slog.Logger) (kafka.Client, error)

// WatcherFactory creates a Watcher instance.
type WatcherFactory func(logger *slog.Logger) (watcher.Watcher, error)

// Manager coordinates collectors for multiple Kafka clusters.
type Manager struct {
	cfg            *config.Config
	sinks          []sink.Sink
	lookupFactory  func() lookup.LookupTable
	clientFactory  ClientFactory
	watcherFactory WatcherFactory
	collectors     map[string]collectorEntry
	watchers       []watcher.Watcher
	mu             sync.Mutex
	wg             sync.WaitGroup
	logger         *slog.Logger
}

// New creates a new Manager.
func New(cfg *config.Config, sinks []sink.Sink, lookupFactory func() lookup.LookupTable, logger *slog.Logger, opts ...Option) *Manager {
	m := &Manager{
		cfg:            cfg,
		sinks:          sinks,
		lookupFactory:  lookupFactory,
		clientFactory:  defaultClientFactory,
		watcherFactory: defaultWatcherFactoryFor(cfg),
		collectors:     make(map[string]collectorEntry),
		logger:         logger,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Option configures a Manager.
type Option func(*Manager)

// WithClientFactory sets a custom ClientFactory.
func WithClientFactory(f ClientFactory) Option {
	return func(m *Manager) {
		m.clientFactory = f
	}
}

// WithWatcherFactory sets a custom WatcherFactory.
func WithWatcherFactory(f WatcherFactory) Option {
	return func(m *Manager) {
		m.watcherFactory = f
	}
}

func defaultClientFactory(clusterCfg config.ClusterConfig, globalCfg *config.Config, logger *slog.Logger) (kafka.Client, error) {
	return kafka.NewFranzClient(clusterCfg, globalCfg, logger)
}

// defaultWatcherFactoryFor returns a WatcherFactory that creates a
// StrimziWatcher pinned to the namespace configured in cfg. The closure keeps
// the WatcherFactory signature (logger-only) stable so tests overriding it via
// WithWatcherFactory don't need to thread config through.
func defaultWatcherFactoryFor(cfg *config.Config) WatcherFactory {
	return func(logger *slog.Logger) (watcher.Watcher, error) {
		return watcher.NewStrimziWatcher(cfg.Watchers.StrimziWatchNamespace, logger)
	}
}

// Start launches collectors for all configured clusters and starts watchers.
func (m *Manager) Start(ctx context.Context) error {
	// Start collectors for statically configured clusters.
	for _, clusterCfg := range m.cfg.Clusters {
		if err := m.AddCluster(ctx, clusterCfg); err != nil {
			return fmt.Errorf("adding cluster %s: %w", clusterCfg.Name, err)
		}
	}

	// Start watchers if configured.
	if m.cfg.Watchers.Strimzi {
		w, err := m.watcherFactory(m.logger.With("component", "strimzi-watcher"))
		if err != nil {
			return fmt.Errorf("creating strimzi watcher: %w", err)
		}
		m.watchers = append(m.watchers, w)
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					m.logger.Error("panic recovered in watcher event consumer", "panic", r, "stack", string(debug.Stack()))
				}
			}()
			m.consumeWatcherEvents(ctx, w)
		}()
		m.logger.Info("strimzi watcher started")
	}

	return nil
}

// AddCluster starts a collector for the given cluster.
func (m *Manager) AddCluster(ctx context.Context, clusterCfg config.ClusterConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Stop existing collector if present (for updates).
	if entry, ok := m.collectors[clusterCfg.Name]; ok {
		entry.cancel()
		entry.collector.Stop()
		delete(m.collectors, clusterCfg.Name)
		m.logger.Info("stopped existing collector for cluster update", "cluster", clusterCfg.Name)
	}

	client, err := m.clientFactory(clusterCfg, m.cfg, m.logger)
	if err != nil {
		return fmt.Errorf("creating kafka client: %w", err)
	}

	var collectorOpts []collector.CollectorOption
	if m.cfg.KafkaRetries > 0 {
		collectorOpts = append(collectorOpts, collector.WithKafkaRetries(m.cfg.KafkaRetries))
	}

	c, err := collector.NewCollector(
		clusterCfg,
		client,
		m.sinks,
		m.lookupFactory,
		m.cfg.PollInterval(),
		m.logger.With("component", "collector"),
		collectorOpts...,
	)
	if err != nil {
		client.Close()
		return fmt.Errorf("creating collector: %w", err)
	}

	collectorCtx, cancel := context.WithCancel(ctx)
	m.collectors[clusterCfg.Name] = collectorEntry{collector: c, cancel: cancel}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				m.logger.Error("panic recovered in collector", "cluster", clusterCfg.Name, "panic", r, "stack", string(debug.Stack()))
			}
		}()
		c.Run(collectorCtx)
	}()
	m.logger.Info("collector started", "cluster", clusterCfg.Name)

	return nil
}

// RemoveCluster stops the collector for the given cluster.
func (m *Manager) RemoveCluster(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.collectors[name]
	if !ok {
		m.logger.Warn("attempted to remove unknown cluster", "cluster", name)
		return
	}

	entry.cancel()
	entry.collector.Stop()
	delete(m.collectors, name)
	m.logger.Info("collector removed", "cluster", name)
}

// HealthCheck returns nil if all collectors are healthy, or an error describing the problem.
// A collector is considered unhealthy if it hasn't successfully polled within 2x its poll interval.
func (m *Manager) HealthCheck() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.collectors) == 0 {
		return nil
	}

	now := time.Now().UnixMilli()
	maxStale := m.cfg.PollInterval().Milliseconds() * 2

	for name, entry := range m.collectors {
		lastPoll := entry.collector.LastPollTime()
		if lastPoll == 0 {
			// Collector hasn't polled yet — may still be starting.
			continue
		}
		if now-lastPoll > maxStale {
			return fmt.Errorf("collector %s last polled %dms ago (threshold: %dms)", name, now-lastPoll, maxStale)
		}
		if !entry.collector.LastPollSuccess() {
			return fmt.Errorf("collector %s last poll failed", name)
		}
	}

	return nil
}

// Stop gracefully shuts down all collectors, watchers, and sinks.
// It waits up to 10 seconds for goroutines to finish.
func (m *Manager) Stop() {
	m.mu.Lock()

	for name, entry := range m.collectors {
		entry.cancel()
		entry.collector.Stop()
		m.logger.Info("collector stopped", "cluster", name)
	}

	for _, w := range m.watchers {
		w.Stop()
	}

	m.mu.Unlock()

	// Wait for goroutines with a timeout.
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		m.logger.Debug("all goroutines stopped")
	case <-time.After(10 * time.Second):
		m.logger.Warn("timed out waiting for goroutines to stop")
	}

	for _, s := range m.sinks {
		s.Stop()
	}

	m.logger.Info("manager stopped")
}

// ReadinessCheck returns nil if the manager has at least one collector that has
// successfully polled recently. Unlike HealthCheck which verifies all collectors,
// ReadinessCheck confirms the service is ready to serve traffic.
func (m *Manager) ReadinessCheck() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.collectors) == 0 {
		return fmt.Errorf("no collectors configured")
	}

	now := time.Now().UnixMilli()
	maxStale := m.cfg.PollInterval().Milliseconds() * 2

	for _, entry := range m.collectors {
		lastPoll := entry.collector.LastPollTime()
		if lastPoll == 0 {
			continue
		}
		if now-lastPoll <= maxStale && entry.collector.LastPollSuccess() {
			return nil
		}
	}

	return fmt.Errorf("no collectors have polled successfully within threshold")
}

func (m *Manager) consumeWatcherEvents(ctx context.Context, w watcher.Watcher) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-w.Events():
			if !ok {
				return
			}
			switch event.Type {
			case watcher.ClusterAdded:
				if err := m.AddCluster(ctx, event.Cluster); err != nil {
					m.logger.Error("failed to add cluster from watcher", "cluster", event.Cluster.Name, "error", err)
				}
			case watcher.ClusterRemoved:
				m.RemoveCluster(event.Cluster.Name)
			}
		}
	}
}
