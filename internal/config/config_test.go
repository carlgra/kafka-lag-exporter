package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_Defaults(t *testing.T) {
	cfg, err := Load("")
	require.NoError(t, err)

	assert.Equal(t, 30, cfg.PollIntervalSeconds)
	assert.Equal(t, "kafkalagexporter", cfg.ClientGroupID)
	assert.Equal(t, 10, cfg.KafkaClientTimeoutSeconds)
	assert.Equal(t, 0, cfg.KafkaRetries)
	assert.Equal(t, []string{".*"}, cfg.MetricWhitelist)
	assert.Equal(t, 60, cfg.Lookup.Memory.Size)
	assert.True(t, cfg.Sinks.Prometheus.Enabled)
	assert.Equal(t, 8000, cfg.Sinks.Prometheus.Port)
	assert.False(t, cfg.Sinks.Graphite.Enabled)
	assert.False(t, cfg.Sinks.InfluxDB.Enabled)
	assert.False(t, cfg.Watchers.Strimzi)
}

func TestLoad_FromFile(t *testing.T) {
	dir := t.TempDir()
	configFile := filepath.Join(dir, "config.yaml")

	content := `
pollIntervalSeconds: 15
clientGroupId: "test-group"
kafkaClientTimeoutSeconds: 5
kafkaRetries: 3
metricWhitelist:
  - "kafka_consumergroup.*"
clusters:
  - name: "test-cluster"
    bootstrapBrokers: "localhost:9092"
    groupWhitelist:
      - "^test.*"
    labels:
      env: "test"
lookup:
  memory:
    size: 120
sinks:
  prometheus:
    enabled: true
    port: 9090
  graphite:
    enabled: true
    host: "graphite.local"
    port: 2003
    prefix: "kafka"
watchers:
  strimzi: true
`
	require.NoError(t, os.WriteFile(configFile, []byte(content), 0644))

	cfg, err := Load(configFile)
	require.NoError(t, err)

	assert.Equal(t, 15, cfg.PollIntervalSeconds)
	assert.Equal(t, "test-group", cfg.ClientGroupID)
	assert.Equal(t, 5, cfg.KafkaClientTimeoutSeconds)
	assert.Equal(t, 3, cfg.KafkaRetries)
	assert.Equal(t, []string{"kafka_consumergroup.*"}, cfg.MetricWhitelist)

	require.Len(t, cfg.Clusters, 1)
	assert.Equal(t, "test-cluster", cfg.Clusters[0].Name)
	assert.Equal(t, "localhost:9092", cfg.Clusters[0].BootstrapBrokers)
	assert.Equal(t, []string{"^test.*"}, cfg.Clusters[0].GroupWhitelist)
	assert.Equal(t, "test", cfg.Clusters[0].Labels["env"])

	assert.Equal(t, 120, cfg.Lookup.Memory.Size)
	assert.Equal(t, 9090, cfg.Sinks.Prometheus.Port)
	assert.True(t, cfg.Sinks.Graphite.Enabled)
	assert.Equal(t, "graphite.local", cfg.Sinks.Graphite.Host)
	assert.True(t, cfg.Watchers.Strimzi)
}

func TestLoad_EnvOverrides(t *testing.T) {
	t.Setenv("KAFKA_LAG_EXPORTER_POLL_INTERVAL_SECONDS", "10")
	t.Setenv("KAFKA_LAG_EXPORTER_PORT", "9999")
	t.Setenv("KAFKA_LAG_EXPORTER_LOOKUP_TABLE_SIZE", "200")
	t.Setenv("KAFKA_LAG_EXPORTER_CLIENT_GROUP_ID", "env-group")

	cfg, err := Load("")
	require.NoError(t, err)

	assert.Equal(t, 10, cfg.PollIntervalSeconds)
	assert.Equal(t, 9999, cfg.Sinks.Prometheus.Port)
	assert.Equal(t, 200, cfg.Lookup.Memory.Size)
	assert.Equal(t, "env-group", cfg.ClientGroupID)
}

func TestLoad_ValidationErrors(t *testing.T) {
	tests := []struct {
		name   string
		yaml   string
		errMsg string
	}{
		{
			name:   "invalid poll interval",
			yaml:   "pollIntervalSeconds: 0",
			errMsg: "pollIntervalSeconds must be > 0",
		},
		{
			name:   "invalid lookup size",
			yaml:   "lookup:\n  memory:\n    size: 1",
			errMsg: "lookup.memory.size must be >= 2",
		},
		{
			name:   "invalid timeout",
			yaml:   "kafkaClientTimeoutSeconds: 0",
			errMsg: "kafkaClientTimeoutSeconds must be > 0",
		},
		{
			name:   "negative retries",
			yaml:   "kafkaRetries: -1",
			errMsg: "kafkaRetries must be >= 0",
		},
		{
			name:   "empty client group ID",
			yaml:   "clientGroupId: \"\"",
			errMsg: "clientGroupId must not be empty",
		},
		{
			name:   "invalid metric whitelist regex",
			yaml:   "metricWhitelist:\n  - \"[invalid\"",
			errMsg: "metricWhitelist[0] is not a valid regex",
		},
		{
			name:   "cluster missing name",
			yaml:   "clusters:\n  - bootstrapBrokers: localhost:9092",
			errMsg: "cluster[0].name is required",
		},
		{
			name:   "cluster missing brokers",
			yaml:   "clusters:\n  - name: test",
			errMsg: "cluster[0].bootstrapBrokers is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			f := filepath.Join(dir, "config.yaml")
			require.NoError(t, os.WriteFile(f, []byte(tt.yaml), 0644))

			_, err := Load(f)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func TestConfig_PollInterval(t *testing.T) {
	cfg := &Config{PollIntervalSeconds: 15}
	assert.Equal(t, 15*time.Second, cfg.PollInterval())
}

func TestConfig_KafkaClientTimeout(t *testing.T) {
	cfg := &Config{KafkaClientTimeoutSeconds: 10}
	assert.Equal(t, 10*time.Second, cfg.KafkaClientTimeout())
}
