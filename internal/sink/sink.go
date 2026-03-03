// Package sink defines metric output destinations.
package sink

import (
	"context"
	"time"

	"github.com/seglo/kafka-lag-exporter/internal/metrics"
)

// Default timeout constants used across sinks.
const (
	DefaultDialTimeout     = 5 * time.Second
	DefaultWriteTimeout    = 5 * time.Second
	DefaultShutdownTimeout = 5 * time.Second
)

// Sink is the interface for metric reporting destinations.
type Sink interface {
	// Report sends a metric value to the sink.
	Report(ctx context.Context, m metrics.MetricValue)
	// Remove removes a previously reported metric from the sink.
	Remove(ctx context.Context, m metrics.RemoveMetric)
	// Stop gracefully shuts down the sink.
	Stop()
}
