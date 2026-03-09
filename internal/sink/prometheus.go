package sink

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/seglo/kafka-lag-exporter/internal/metrics"
)

// version holds the build version for the build_info metric. Set once via SetVersion.
var (
	version     = "dev"
	versionOnce sync.Once
)

// SetVersion sets the build version (can only be called once; subsequent calls are no-ops).
func SetVersion(v string) {
	versionOnce.Do(func() {
		version = v
	})
}

// GetVersion returns the current build version.
func GetVersion() string {
	return version
}

// ReadinessChecker is called by the /ready endpoint to check if the service is ready.
type ReadinessChecker func() error

// PrometheusSink exposes metrics via an HTTP /metrics endpoint.
type PrometheusSink struct {
	registry       *prometheus.Registry
	gauges         map[string]*prometheus.GaugeVec
	filter         *MetricFilter
	server         *http.Server
	logger         *slog.Logger
	readinessCheck ReadinessChecker

	// Cardinality protection.
	maxTimeSeries     int
	seriesCount       atomic.Int64
	droppedSeriesOnce sync.Once
	droppedSeries     prometheus.Counter

	// Self-instrumentation.
	pollsTotal       prometheus.Counter
	pollErrors       prometheus.Counter
	pollDuration     prometheus.Gauge
	lookupEntries    *prometheus.GaugeVec
	clientConnects   *prometheus.GaugeVec
	clientDisconnects *prometheus.GaugeVec
	clientWriteErrors *prometheus.GaugeVec
	clientReadErrors  *prometheus.GaugeVec
}

// NewPrometheusSink creates and starts a Prometheus HTTP endpoint on the given port.
// If bindAddress is empty, it binds to all interfaces (0.0.0.0).
func NewPrometheusSink(port int, bindAddress string, maxTimeSeries int, filter *MetricFilter, logger *slog.Logger) (*PrometheusSink, error) {
	reg := prometheus.NewRegistry()

	gauges := make(map[string]*prometheus.GaugeVec)
	for _, def := range metrics.AllDefinitions() {
		gv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: def.Name,
			Help: def.Help,
		}, def.Labels)

		if err := reg.Register(gv); err != nil {
			return nil, fmt.Errorf("registering metric %s: %w", def.Name, err)
		}
		gauges[def.Name] = gv
	}

	// Self-instrumentation metrics.
	pollsTotal := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_lag_exporter_polls_total",
		Help: "Total number of poll cycles",
	})
	pollErrors := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_lag_exporter_poll_errors_total",
		Help: "Total number of failed poll cycles",
	})
	pollDuration := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kafka_lag_exporter_poll_duration_seconds",
		Help: "Duration of the last poll cycle in seconds",
	})
	lookupEntries := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_lag_exporter_lookup_table_entries",
		Help: "Number of entries in the lookup table",
	}, []string{"cluster_name", "topic", "partition"})
	droppedSeries := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_lag_exporter_dropped_series_total",
		Help: "Total number of time series dropped due to cardinality limit",
	})

	// Client connection metrics.
	clientConnects := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_lag_exporter_client_connects_total",
		Help: "Total number of successful broker connections",
	}, []string{"cluster_name"})
	clientDisconnects := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_lag_exporter_client_disconnects_total",
		Help: "Total number of broker disconnections",
	}, []string{"cluster_name"})
	clientWriteErrors := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_lag_exporter_client_write_errors_total",
		Help: "Total number of write errors to brokers",
	}, []string{"cluster_name"})
	clientReadErrors := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_lag_exporter_client_read_errors_total",
		Help: "Total number of read errors from brokers",
	}, []string{"cluster_name"})

	// Build info metric.
	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_lag_exporter_build_info",
		Help: "Build information",
	}, []string{"version", "goversion"})

	for _, c := range []prometheus.Collector{pollsTotal, pollErrors, pollDuration, lookupEntries, droppedSeries, clientConnects, clientDisconnects, clientWriteErrors, clientReadErrors, buildInfo} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("registering self-instrumentation metric: %w", err)
		}
	}

	// Set build info to 1.
	buildInfo.WithLabelValues(version, runtime.Version()).Set(1)

	sink := &PrometheusSink{
		registry:      reg,
		gauges:        gauges,
		filter:        filter,
		logger:        logger,
		maxTimeSeries:     maxTimeSeries,
		pollsTotal:        pollsTotal,
		pollErrors:        pollErrors,
		pollDuration:      pollDuration,
		lookupEntries:     lookupEntries,
		droppedSeries:     droppedSeries,
		clientConnects:    clientConnects,
		clientDisconnects: clientDisconnects,
		clientWriteErrors: clientWriteErrors,
		clientReadErrors:  clientReadErrors,
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	// Liveness probe — always 200 if HTTP server is running.
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	// Readiness probe — checks if collectors have polled successfully.
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if sink.readinessCheck != nil {
			if err := sink.readinessCheck(); err != nil {
				// Log the detailed error internally but return a generic message
				// to avoid leaking cluster names and internal state.
				logger.Debug("readiness check failed", "error", err)
				http.Error(w, "service not ready", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	})

	listenAddr := fmt.Sprintf("%s:%d", bindAddress, port)
	server := &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 16, // 64KB
	}
	sink.server = server

	// Use a listener so we can detect bind failures immediately.
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return nil, fmt.Errorf("prometheus bind failed on %s: %w", server.Addr, err)
	}

	go func() {
		logger.Info("starting prometheus endpoint", "port", port)
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Error("prometheus server error", "error", err)
		}
	}()

	return sink, nil
}

// SetReadinessCheck sets the readiness checker function.
func (s *PrometheusSink) SetReadinessCheck(check ReadinessChecker) {
	s.readinessCheck = check
}

// ReportPollMetrics records self-instrumentation data from a poll cycle.
func (s *PrometheusSink) ReportPollMetrics(duration time.Duration, success bool) {
	s.pollsTotal.Inc()
	s.pollDuration.Set(duration.Seconds())
	if !success {
		s.pollErrors.Inc()
	}
}

// ReportLookupTableSize records the size of a lookup table.
func (s *PrometheusSink) ReportLookupTableSize(cluster, topic, partition string, size int64) {
	s.lookupEntries.WithLabelValues(cluster, topic, partition).Set(float64(size))
}

// ReportClientMetrics records Kafka client connection statistics.
func (s *PrometheusSink) ReportClientMetrics(clusterName string, connects, disconnects, writeErrors, readErrors int64) {
	s.clientConnects.WithLabelValues(clusterName).Set(float64(connects))
	s.clientDisconnects.WithLabelValues(clusterName).Set(float64(disconnects))
	s.clientWriteErrors.WithLabelValues(clusterName).Set(float64(writeErrors))
	s.clientReadErrors.WithLabelValues(clusterName).Set(float64(readErrors))
}

func (s *PrometheusSink) Report(_ context.Context, m metrics.MetricValue) {
	if s.filter != nil && !s.filter.Matches(m.Definition.Name) {
		return
	}
	if math.IsNaN(m.Value) || math.IsInf(m.Value, 0) {
		return
	}

	gv, ok := s.gauges[m.Definition.Name]
	if !ok {
		return
	}

	// Cardinality protection: skip new series if limit exceeded.
	if s.maxTimeSeries > 0 && s.seriesCount.Load() >= int64(s.maxTimeSeries) {
		s.droppedSeries.Inc()
		s.droppedSeriesOnce.Do(func() {
			s.logger.Warn("cardinality limit reached, dropping new series", "limit", s.maxTimeSeries)
		})
		return
	}

	labelValues := labelsToValues(m.Definition.Labels, m.Labels)
	gauge, err := gv.GetMetricWithLabelValues(labelValues...)
	if err != nil {
		s.logger.Warn("failed to get metric", "metric", m.Definition.Name, "error", err)
		return
	}
	gauge.Set(m.Value)
	s.seriesCount.Add(1)
}

func (s *PrometheusSink) Remove(_ context.Context, m metrics.RemoveMetric) {
	gv, ok := s.gauges[m.Definition.Name]
	if !ok {
		return
	}
	labelValues := labelsToValues(m.Definition.Labels, m.Labels)
	gv.DeleteLabelValues(labelValues...)
}

// labelsToValues converts a label map to an ordered slice matching the definition's label order.
func labelsToValues(names []string, labels map[string]string) []string {
	values := make([]string, len(names))
	for i, name := range names {
		values[i] = labels[name]
	}
	return values
}

func (s *PrometheusSink) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultShutdownTimeout)
	defer cancel()
	if err := s.server.Shutdown(ctx); err != nil {
		s.logger.Error("prometheus server shutdown error", "error", err)
	}
}
