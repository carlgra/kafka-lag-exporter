// Package collector implements per-cluster Kafka offset polling and metric computation.
package collector

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/seglo/kafka-lag-exporter/internal/config"
	"github.com/seglo/kafka-lag-exporter/internal/domain"
	"github.com/seglo/kafka-lag-exporter/internal/kafka"
	"github.com/seglo/kafka-lag-exporter/internal/lookup"
	"github.com/seglo/kafka-lag-exporter/internal/metrics"
	"github.com/seglo/kafka-lag-exporter/internal/sink"
	"golang.org/x/sync/errgroup"
)

// Collector polls a single Kafka cluster and computes lag metrics.
type Collector struct {
	clusterName    string
	clusterLabels  map[string]string
	client         kafka.Client
	sinks          []sink.Sink
	lookupTables   map[domain.TopicPartition]lookup.LookupTable
	lookupFactory  func() lookup.LookupTable
	pollInterval   time.Duration
	kafkaRetries   int
	groupWhitelist []*regexp.Regexp
	groupBlacklist []*regexp.Regexp
	topicWhitelist []*regexp.Regexp
	topicBlacklist []*regexp.Regexp
	lastSnapshot   *OffsetsSnapshot
	logger         *slog.Logger

	// Health tracking.
	lastPollTime    atomic.Int64
	lastPollSuccess atomic.Bool

	// Pre-allocated aggregation maps, reused across poll cycles to reduce GC pressure.
	maxLag        map[groupKey]float64
	maxLagSeconds map[groupKey]float64
	sumLag        map[groupKey]float64
	topicSumLag   map[groupTopicKey]float64
}

// NewCollector creates a collector for the given cluster.
func NewCollector(
	clusterCfg config.ClusterConfig,
	client kafka.Client,
	sinks []sink.Sink,
	lookupFactory func() lookup.LookupTable,
	pollInterval time.Duration,
	logger *slog.Logger,
	opts ...CollectorOption,
) (*Collector, error) {
	c := &Collector{
		clusterName:   clusterCfg.Name,
		clusterLabels: clusterCfg.Labels,
		client:        client,
		sinks:         sinks,
		lookupTables:  make(map[domain.TopicPartition]lookup.LookupTable),
		lookupFactory: lookupFactory,
		pollInterval:  pollInterval,
		logger:        logger.With("cluster", clusterCfg.Name),
		maxLag:        make(map[groupKey]float64),
		maxLagSeconds: make(map[groupKey]float64),
		sumLag:        make(map[groupKey]float64),
		topicSumLag:   make(map[groupTopicKey]float64),
	}

	for _, opt := range opts {
		opt(c)
	}

	var err error
	if c.groupWhitelist, err = compilePatterns(clusterCfg.GroupWhitelist); err != nil {
		return nil, fmt.Errorf("compiling group whitelist: %w", err)
	}
	if c.groupBlacklist, err = compilePatterns(clusterCfg.GroupBlacklist); err != nil {
		return nil, fmt.Errorf("compiling group blacklist: %w", err)
	}
	if c.topicWhitelist, err = compilePatterns(clusterCfg.TopicWhitelist); err != nil {
		return nil, fmt.Errorf("compiling topic whitelist: %w", err)
	}
	if c.topicBlacklist, err = compilePatterns(clusterCfg.TopicBlacklist); err != nil {
		return nil, fmt.Errorf("compiling topic blacklist: %w", err)
	}

	return c, nil
}

// CollectorOption configures a Collector.
type CollectorOption func(*Collector)

// WithKafkaRetries sets the number of Kafka retries on failure.
func WithKafkaRetries(n int) CollectorOption {
	return func(c *Collector) {
		c.kafkaRetries = n
	}
}

// Run starts the polling loop. It blocks until ctx is cancelled.
func (c *Collector) Run(ctx context.Context) {
	c.logger.Info("collector started", "pollInterval", c.pollInterval)

	// Poll immediately on start.
	c.poll(ctx)

	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("collector stopped")
			return
		case <-ticker.C:
			c.poll(ctx)
		}
	}
}

// LastPollTime returns the Unix millisecond timestamp of the last poll attempt.
func (c *Collector) LastPollTime() int64 {
	return c.lastPollTime.Load()
}

// LastPollSuccess returns whether the last poll completed successfully.
func (c *Collector) LastPollSuccess() bool {
	return c.lastPollSuccess.Load()
}

func (c *Collector) poll(ctx context.Context) {
	start := time.Now()
	now := start.UnixMilli()

	c.logger.Debug("starting poll cycle")
	c.lastPollTime.Store(now)

	snapshot, err := c.collectOffsets(ctx, now)
	if err != nil {
		c.logger.Warn("poll cycle failed", "error", err)
		c.lastPollSuccess.Store(false)
		c.reportPollInstrumentation(time.Since(start), false)
		return
	}

	c.computeAndReport(ctx, snapshot)

	// Evict removed metrics.
	c.evictRemoved(ctx, snapshot)

	c.lastSnapshot = snapshot
	c.lastPollSuccess.Store(true)

	elapsed := time.Since(start)
	c.logger.Debug("poll cycle complete", "duration", elapsed)

	// Report poll time.
	c.reportToSinks(ctx, metrics.MetricValue{
		Definition: metrics.PollTimeMs,
		Labels:     c.makeLabels("cluster_name", c.clusterName),
		Value:      float64(elapsed.Milliseconds()),
	})

	// Report self-instrumentation and lookup table sizes.
	c.reportPollInstrumentation(elapsed, true)
	c.reportLookupTableSizes()
}

// reportPollInstrumentation sends poll metrics to any PrometheusSink.
func (c *Collector) reportPollInstrumentation(duration time.Duration, success bool) {
	for _, s := range c.sinks {
		if ps, ok := s.(interface {
			ReportPollMetrics(time.Duration, bool)
		}); ok {
			ps.ReportPollMetrics(duration, success)
		}
	}
}

// reportLookupTableSizes sends lookup table size metrics to any PrometheusSink.
func (c *Collector) reportLookupTableSizes() {
	for _, s := range c.sinks {
		ps, ok := s.(interface {
			ReportLookupTableSize(string, string, string, int64)
		})
		if !ok {
			continue
		}
		for tp, table := range c.lookupTables {
			ps.ReportLookupTableSize(c.clusterName, tp.Topic, fmt.Sprintf("%d", tp.Partition), table.Length())
		}
	}
}

func (c *Collector) collectOffsets(ctx context.Context, now int64) (*OffsetsSnapshot, error) {
	var lastErr error
	maxAttempts := 1 + c.kafkaRetries

	const maxBackoff = 30 * time.Second
	for attempt := range maxAttempts {
		snapshot, err := c.tryCollectOffsets(ctx, now)
		if err == nil {
			return snapshot, nil
		}
		lastErr = err
		if attempt < maxAttempts-1 {
			backoff := time.Duration(1<<uint(attempt)) * time.Second
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			c.logger.Warn("offset collection failed, retrying", "attempt", attempt+1, "backoff", backoff, "error", err)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return nil, lastErr
}

func (c *Collector) tryCollectOffsets(ctx context.Context, now int64) (*OffsetsSnapshot, error) {
	groups, gtps, err := c.client.GetGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting groups: %w", err)
	}

	// Filter groups.
	groups = c.filterGroups(groups)
	gtps = c.filterGTPs(gtps)

	// Collect all unique topic-partitions from GTPs.
	tpSet := make(map[domain.TopicPartition]bool)
	for _, gtp := range gtps {
		tpSet[gtp.TopicPartition()] = true
	}
	var tps []domain.TopicPartition
	for tp := range tpSet {
		tps = append(tps, tp)
	}

	// Filter topics.
	tps = c.filterTopicPartitions(tps)

	// Fetch offsets concurrently.
	var (
		groupOffsets    domain.GroupOffsets
		earliestOffsets domain.PartitionOffsets
		latestOffsets   domain.PartitionOffsets
	)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		var err error
		groupOffsets, err = c.client.GetGroupOffsets(gctx, now, groups, gtps)
		return err
	})

	g.Go(func() error {
		var err error
		earliestOffsets, err = c.client.GetEarliestOffsets(gctx, now, tps)
		return err
	})

	g.Go(func() error {
		var err error
		latestOffsets, err = c.client.GetLatestOffsets(gctx, now, tps)
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("fetching offsets: %w", err)
	}

	return &OffsetsSnapshot{
		Timestamp:       now,
		GroupOffsets:    groupOffsets,
		EarliestOffsets: earliestOffsets,
		LatestOffsets:   latestOffsets,
	}, nil
}

// makeLabels creates a label map from key-value pairs and merges in cluster labels.
func (c *Collector) makeLabels(pairs ...string) map[string]string {
	labels := make(map[string]string, len(pairs)/2+len(c.clusterLabels))
	for i := 0; i < len(pairs)-1; i += 2 {
		labels[pairs[i]] = pairs[i+1]
	}
	for k, v := range c.clusterLabels {
		labels[k] = v
	}
	return labels
}

// reportToSinks sends a metric value to all sinks.
func (c *Collector) reportToSinks(ctx context.Context, mv metrics.MetricValue) {
	for _, s := range c.sinks {
		s.Report(ctx, mv)
	}
}

// removeFromSinks requests metric removal from all sinks.
func (c *Collector) removeFromSinks(ctx context.Context, rm metrics.RemoveMetric) {
	for _, s := range c.sinks {
		s.Remove(ctx, rm)
	}
}

// computeMetrics computes all metrics from a snapshot (pure computation, no side effects).
func (c *Collector) computeMetrics(snapshot *OffsetsSnapshot) []metrics.MetricValue {
	var result []metrics.MetricValue

	// Update lookup tables with latest offsets.
	for tp, offset := range snapshot.LatestOffsets {
		table, ok := c.lookupTables[tp]
		if !ok {
			table = c.lookupFactory()
			c.lookupTables[tp] = table
		}
		table.AddPoint(lookup.Point{Offset: offset.Offset, Time: offset.Timestamp})
	}

	// Partition-level metrics.
	for tp, latest := range snapshot.LatestOffsets {
		partStr := strconv.FormatInt(int64(tp.Partition), 10)
		partLabels := c.makeLabels("cluster_name", c.clusterName, "topic", tp.Topic, "partition", partStr)
		result = append(result, metrics.MetricValue{
			Definition: metrics.PartitionLatestOffset,
			Labels:     partLabels,
			Value:      float64(latest.Offset),
		})
		if earliest, ok := snapshot.EarliestOffsets[tp]; ok {
			result = append(result, metrics.MetricValue{
				Definition: metrics.PartitionEarliestOffset,
				Labels:     partLabels,
				Value:      float64(earliest.Offset),
			})
		}
	}

	// Aggregate lag per group and per group-topic.
	// Clear and reuse pre-allocated maps to reduce GC pressure.
	for k := range c.maxLag {
		delete(c.maxLag, k)
	}
	for k := range c.maxLagSeconds {
		delete(c.maxLagSeconds, k)
	}
	for k := range c.sumLag {
		delete(c.sumLag, k)
	}
	for k := range c.topicSumLag {
		delete(c.topicSumLag, k)
	}

	for gtp, groupOffset := range snapshot.GroupOffsets {
		tp := gtp.TopicPartition()
		latestOffset, ok := snapshot.LatestOffsets[tp]
		if !ok {
			continue
		}

		partStr := strconv.FormatInt(int64(gtp.Partition), 10)
		gtpLabels := c.makeLabels(
			"cluster_name", c.clusterName,
			"group", gtp.Group,
			"topic", gtp.Topic,
			"partition", partStr,
			"member_host", gtp.MemberHost,
			"consumer_id", gtp.ConsumerID,
			"client_id", gtp.ClientID,
		)

		// Group offset.
		result = append(result, metrics.MetricValue{
			Definition: metrics.GroupOffset,
			Labels:     gtpLabels,
			Value:      float64(groupOffset.Offset),
		})

		// Compute offset lag.
		lag := latestOffset.Offset - groupOffset.Offset
		if lag < 0 {
			lag = 0
		}
		lagF := float64(lag)

		result = append(result, metrics.MetricValue{
			Definition: metrics.GroupLag,
			Labels:     gtpLabels,
			Value:      lagF,
		})

		// Compute time lag via lookup table.
		var lagSeconds float64
		if table, ok := c.lookupTables[tp]; ok {
			lookupResult := table.Lookup(groupOffset.Offset)
			if lookupResult.Found {
				lagSeconds = float64(snapshot.Timestamp-lookupResult.Time) / 1000.0
				if lagSeconds < 0 {
					// Negative lag means the lookup returned a time in the future
					// (race between offset/time collection). Report NaN so sinks
					// can filter it rather than reporting a misleading value.
					lagSeconds = math.NaN()
				}
			}
		}

		result = append(result, metrics.MetricValue{
			Definition: metrics.GroupLagSeconds,
			Labels:     gtpLabels,
			Value:      lagSeconds,
		})

		// Aggregate.
		gk := groupKey{group: gtp.Group}
		gtk := groupTopicKey{group: gtp.Group, topic: gtp.Topic}

		c.sumLag[gk] += lagF
		c.topicSumLag[gtk] += lagF

		if lagF > c.maxLag[gk] {
			c.maxLag[gk] = lagF
		}
		if !math.IsNaN(lagSeconds) && lagSeconds > c.maxLagSeconds[gk] {
			c.maxLagSeconds[gk] = lagSeconds
		}
	}

	// Aggregate metrics.
	for gk, v := range c.maxLag {
		result = append(result, metrics.MetricValue{
			Definition: metrics.GroupMaxLag,
			Labels:     c.makeLabels("cluster_name", c.clusterName, "group", gk.group),
			Value:      v,
		})
	}
	for gk, v := range c.maxLagSeconds {
		result = append(result, metrics.MetricValue{
			Definition: metrics.GroupMaxLagSeconds,
			Labels:     c.makeLabels("cluster_name", c.clusterName, "group", gk.group),
			Value:      v,
		})
	}
	for gk, v := range c.sumLag {
		result = append(result, metrics.MetricValue{
			Definition: metrics.GroupSumLag,
			Labels:     c.makeLabels("cluster_name", c.clusterName, "group", gk.group),
			Value:      v,
		})
	}
	for gtk, v := range c.topicSumLag {
		result = append(result, metrics.MetricValue{
			Definition: metrics.GroupTopicSumLag,
			Labels:     c.makeLabels("cluster_name", c.clusterName, "group", gtk.group, "topic", gtk.topic),
			Value:      v,
		})
	}

	return result
}

func (c *Collector) computeAndReport(ctx context.Context, snapshot *OffsetsSnapshot) {
	for _, mv := range c.computeMetrics(snapshot) {
		c.reportToSinks(ctx, mv)
	}
}

func (c *Collector) evictRemoved(ctx context.Context, current *OffsetsSnapshot) {
	// Evict removed group-topic-partitions.
	for _, gtp := range RemovedGroupTopicPartitions(c.lastSnapshot, current) {
		partStr := strconv.FormatInt(int64(gtp.Partition), 10)
		labels := c.makeLabels(
			"cluster_name", c.clusterName,
			"group", gtp.Group,
			"topic", gtp.Topic,
			"partition", partStr,
			"member_host", gtp.MemberHost,
			"consumer_id", gtp.ConsumerID,
			"client_id", gtp.ClientID,
		)
		c.removeFromSinks(ctx, metrics.RemoveMetric{Definition: metrics.GroupOffset, Labels: labels})
		c.removeFromSinks(ctx, metrics.RemoveMetric{Definition: metrics.GroupLag, Labels: labels})
		c.removeFromSinks(ctx, metrics.RemoveMetric{Definition: metrics.GroupLagSeconds, Labels: labels})
		c.logger.Debug("evicted group-topic-partition metrics", "gtp", gtp.String())
	}

	// Evict removed topic-partitions.
	for _, tp := range RemovedTopicPartitions(c.lastSnapshot, current) {
		partStr := strconv.FormatInt(int64(tp.Partition), 10)
		labels := c.makeLabels("cluster_name", c.clusterName, "topic", tp.Topic, "partition", partStr)
		c.removeFromSinks(ctx, metrics.RemoveMetric{Definition: metrics.PartitionLatestOffset, Labels: labels})
		c.removeFromSinks(ctx, metrics.RemoveMetric{Definition: metrics.PartitionEarliestOffset, Labels: labels})
		delete(c.lookupTables, tp)
		c.logger.Debug("evicted topic-partition metrics", "tp", tp.String())
	}

	// Evict removed group aggregates.
	if c.lastSnapshot != nil {
		prevGroups := collectGroups(c.lastSnapshot)
		currGroups := collectGroups(current)
		for group := range prevGroups {
			if !currGroups[group] {
				labels := c.makeLabels("cluster_name", c.clusterName, "group", group)
				c.removeFromSinks(ctx, metrics.RemoveMetric{Definition: metrics.GroupMaxLag, Labels: labels})
				c.removeFromSinks(ctx, metrics.RemoveMetric{Definition: metrics.GroupMaxLagSeconds, Labels: labels})
				c.removeFromSinks(ctx, metrics.RemoveMetric{Definition: metrics.GroupSumLag, Labels: labels})
			}
		}

		prevGroupTopics := collectGroupTopics(c.lastSnapshot)
		currGroupTopics := collectGroupTopics(current)
		for gtk := range prevGroupTopics {
			if !currGroupTopics[gtk] {
				c.removeFromSinks(ctx, metrics.RemoveMetric{Definition: metrics.GroupTopicSumLag, Labels: c.makeLabels("cluster_name", c.clusterName, "group", gtk.group, "topic", gtk.topic)})
			}
		}
	}
}

// groupKey identifies a consumer group for aggregation maps.
type groupKey struct{ group string }

func collectGroups(s *OffsetsSnapshot) map[string]bool {
	groups := make(map[string]bool)
	for gtp := range s.GroupOffsets {
		groups[gtp.Group] = true
	}
	return groups
}

type groupTopicKey struct{ group, topic string }

func collectGroupTopics(s *OffsetsSnapshot) map[groupTopicKey]bool {
	gts := make(map[groupTopicKey]bool)
	for gtp := range s.GroupOffsets {
		gts[groupTopicKey{group: gtp.Group, topic: gtp.Topic}] = true
	}
	return gts
}

// Stop cleans up the collector's Kafka client.
func (c *Collector) Stop() {
	c.client.Close()
	c.logger.Info("collector stopped, kafka client closed")
}

// Filtering helpers.

// compilePatterns compiles user-supplied regex patterns.
// Go's regexp package uses the RE2 engine which guarantees linear-time matching,
// so user-supplied patterns cannot cause catastrophic backtracking (ReDoS).
func compilePatterns(patterns []string) ([]*regexp.Regexp, error) {
	compiled := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("compiling pattern %q: %w", p, err)
		}
		compiled = append(compiled, re)
	}
	return compiled, nil
}

func matchesAny(s string, patterns []*regexp.Regexp) bool {
	for _, re := range patterns {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}

func (c *Collector) filterGroups(groups []string) []string {
	if len(c.groupWhitelist) == 0 && len(c.groupBlacklist) == 0 {
		return groups
	}
	var filtered []string
	for _, g := range groups {
		if len(c.groupWhitelist) > 0 && !matchesAny(g, c.groupWhitelist) {
			continue
		}
		if len(c.groupBlacklist) > 0 && matchesAny(g, c.groupBlacklist) {
			continue
		}
		filtered = append(filtered, g)
	}
	return filtered
}

func (c *Collector) filterGTPs(gtps []domain.GroupTopicPartition) []domain.GroupTopicPartition {
	if len(c.groupWhitelist) == 0 && len(c.groupBlacklist) == 0 &&
		len(c.topicWhitelist) == 0 && len(c.topicBlacklist) == 0 {
		return gtps
	}
	var filtered []domain.GroupTopicPartition
	for _, gtp := range gtps {
		if len(c.groupWhitelist) > 0 && !matchesAny(gtp.Group, c.groupWhitelist) {
			continue
		}
		if len(c.groupBlacklist) > 0 && matchesAny(gtp.Group, c.groupBlacklist) {
			continue
		}
		if len(c.topicWhitelist) > 0 && !matchesAny(gtp.Topic, c.topicWhitelist) {
			continue
		}
		if len(c.topicBlacklist) > 0 && matchesAny(gtp.Topic, c.topicBlacklist) {
			continue
		}
		filtered = append(filtered, gtp)
	}
	return filtered
}

func (c *Collector) filterTopicPartitions(tps []domain.TopicPartition) []domain.TopicPartition {
	if len(c.topicWhitelist) == 0 && len(c.topicBlacklist) == 0 {
		return tps
	}
	var filtered []domain.TopicPartition
	for _, tp := range tps {
		if len(c.topicWhitelist) > 0 && !matchesAny(tp.Topic, c.topicWhitelist) {
			continue
		}
		if len(c.topicBlacklist) > 0 && matchesAny(tp.Topic, c.topicBlacklist) {
			continue
		}
		filtered = append(filtered, tp)
	}
	return filtered
}
