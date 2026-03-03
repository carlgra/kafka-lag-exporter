package sink

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	ihttp "github.com/influxdata/influxdb-client-go/v2/api/http"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/seglo/kafka-lag-exporter/internal/metrics"
)

// InfluxDB async write defaults for backpressure.
const (
	influxBatchSize     = 1000
	influxFlushInterval = 5000 // milliseconds
)

// InfluxDBSink pushes metrics to an InfluxDB instance.
type InfluxDBSink struct {
	client   influxdb2.Client
	writeAPI api.WriteAPIBlocking
	asyncAPI api.WriteAPI
	async    bool
	database string
	filter   *MetricFilter
	logger   *slog.Logger
}

// NewInfluxDBSink creates a new InfluxDB sink.
func NewInfluxDBSink(endpoint string, port int, database, username, password string, async bool, filter *MetricFilter, logger *slog.Logger) (*InfluxDBSink, error) {
	url := fmt.Sprintf("%s:%d", endpoint, port)

	var token string
	if username != "" && password != "" {
		token = fmt.Sprintf("%s:%s", username, password)
		if strings.HasPrefix(strings.ToLower(url), "http://") {
			logger.Warn("influxdb credentials are being sent over plaintext HTTP — consider using https://", "url", url)
		}
	}

	opts := influxdb2.DefaultOptions()
	if async {
		opts.SetBatchSize(influxBatchSize)
		opts.SetFlushInterval(influxFlushInterval)
	}

	client := influxdb2.NewClientWithOptions(url, token, opts)

	sink := &InfluxDBSink{
		client:   client,
		async:    async,
		database: database,
		filter:   filter,
		logger:   logger,
	}

	// InfluxDB 1.x uses "database/retention-policy" as bucket with empty org.
	bucket := database
	if async {
		sink.asyncAPI = client.WriteAPI("", bucket)
		sink.asyncAPI.SetWriteFailedCallback(func(_ string, err ihttp.Error, _ uint) bool {
			logger.Error("influxdb async write failed", "error", err.Error())
			return false
		})
	} else {
		sink.writeAPI = client.WriteAPIBlocking("", bucket)
	}

	// Redact credentials in log output.
	redactedToken := "***"
	if token == "" {
		redactedToken = "(none)"
	}
	logger.Info("influxdb sink started", "url", url, "database", database, "async", async, "token", redactedToken)
	return sink, nil
}

func (s *InfluxDBSink) Report(ctx context.Context, m metrics.MetricValue) {
	if s.filter != nil && !s.filter.Matches(m.Definition.Name) {
		return
	}
	if math.IsNaN(m.Value) || math.IsInf(m.Value, 0) {
		return
	}

	tags := make(map[string]string, len(m.Labels))
	for k, v := range m.Labels {
		tags[k] = v
	}

	fields := map[string]interface{}{
		"value": m.Value,
	}

	point := write.NewPoint(
		m.Definition.Name,
		tags,
		fields,
		time.Now(),
	)

	if s.async {
		s.asyncAPI.WritePoint(point)
	} else {
		if err := s.writeAPI.WritePoint(ctx, point); err != nil {
			s.logger.Error("influxdb write failed", "metric", m.Definition.Name, "error", err)
		}
	}
}

func (s *InfluxDBSink) Remove(_ context.Context, _ metrics.RemoveMetric) {
	// InfluxDB does not support metric removal — data expires via retention policy.
}

func (s *InfluxDBSink) Stop() {
	if s.async && s.asyncAPI != nil {
		s.asyncAPI.Flush()
	}
	s.client.Close()
	s.logger.Info("influxdb sink stopped")
}
