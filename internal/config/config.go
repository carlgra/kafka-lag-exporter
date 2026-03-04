// Package config provides application configuration loading from YAML files and environment variables.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config is the top-level application configuration.
type Config struct {
	PollIntervalSeconds       int             `mapstructure:"pollIntervalSeconds"`
	ClientGroupID             string          `mapstructure:"clientGroupId"`
	KafkaClientTimeoutSeconds int             `mapstructure:"kafkaClientTimeoutSeconds"`
	KafkaRetries              int             `mapstructure:"kafkaRetries"`
	MetricWhitelist           []string        `mapstructure:"metricWhitelist"`
	Clusters                  []ClusterConfig `mapstructure:"clusters"`
	Watchers                  WatcherConfig   `mapstructure:"watchers"`
	Lookup                    LookupConfig    `mapstructure:"lookup"`
	Sinks                     SinksConfig     `mapstructure:"sinks"`
	LogLevel                  string          `mapstructure:"logLevel"`
}

// PollInterval returns the poll interval as a duration.
func (c *Config) PollInterval() time.Duration {
	return time.Duration(c.PollIntervalSeconds) * time.Second
}

// KafkaClientTimeout returns the Kafka client timeout as a duration.
func (c *Config) KafkaClientTimeout() time.Duration {
	return time.Duration(c.KafkaClientTimeoutSeconds) * time.Second
}

// ClusterConfig defines a Kafka cluster connection and filtering.
type ClusterConfig struct {
	Name                  string            `mapstructure:"name"`
	BootstrapBrokers      string            `mapstructure:"bootstrapBrokers"`
	GroupWhitelist        []string          `mapstructure:"groupWhitelist"`
	GroupBlacklist        []string          `mapstructure:"groupBlacklist"`
	TopicWhitelist        []string          `mapstructure:"topicWhitelist"`
	TopicBlacklist        []string          `mapstructure:"topicBlacklist"`
	Labels                map[string]string `mapstructure:"labels"`
	ConsumerProperties    map[string]string `mapstructure:"consumerProperties"`
	AdminClientProperties map[string]string `mapstructure:"adminClientProperties"`
}

// WatcherConfig configures cluster watchers.
type WatcherConfig struct {
	Strimzi bool `mapstructure:"strimzi"`
}

// LookupConfig configures the offset lookup table.
type LookupConfig struct {
	Memory MemoryLookupConfig `mapstructure:"memory"`
	Redis  RedisLookupConfig  `mapstructure:"redis"`
}

// MemoryLookupConfig configures the in-memory lookup table.
type MemoryLookupConfig struct {
	Size int `mapstructure:"size"`
}

// RedisLookupConfig configures the Redis-backed lookup table.
type RedisLookupConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	Host       string `mapstructure:"host"`
	Port       int    `mapstructure:"port"`
	Database   int    `mapstructure:"database"`
	Timeout    int    `mapstructure:"timeout"`
	Prefix     string `mapstructure:"prefix"`
	Separator  string `mapstructure:"separator"`
	Retention  string `mapstructure:"retention"`
	Expiration string `mapstructure:"expiration"`
	Password   string `mapstructure:"password"`
	TLS        bool   `mapstructure:"tls"`
}

// SinksConfig configures metric sinks.
type SinksConfig struct {
	Prometheus PrometheusSinkConfig `mapstructure:"prometheus"`
	Graphite   GraphiteSinkConfig   `mapstructure:"graphite"`
	InfluxDB   InfluxDBSinkConfig   `mapstructure:"influxdb"`
}

// PrometheusSinkConfig configures the Prometheus sink.
type PrometheusSinkConfig struct {
	Enabled       bool   `mapstructure:"enabled"`
	Port          int    `mapstructure:"port"`
	BindAddress   string `mapstructure:"bindAddress"`
	MaxTimeSeries int    `mapstructure:"maxTimeSeries"`
}

// GraphiteSinkConfig configures the Graphite sink.
type GraphiteSinkConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Host    string `mapstructure:"host"`
	Port    int    `mapstructure:"port"`
	Prefix  string `mapstructure:"prefix"`
	TLS     bool   `mapstructure:"tls"`
}

// InfluxDBSinkConfig configures the InfluxDB sink.
type InfluxDBSinkConfig struct {
	Enabled  bool   `mapstructure:"enabled"`
	Endpoint string `mapstructure:"endpoint"`
	Port     int    `mapstructure:"port"`
	Database string `mapstructure:"database"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	Async    bool   `mapstructure:"async"`
}

// Load reads the configuration from the given file path and environment variables.
func Load(configPath string) (*Config, error) {
	v := viper.New()
	setDefaults(v)
	bindEnvVars(v)

	if configPath != "" {
		v.SetConfigFile(configPath)
		if err := v.ReadInConfig(); err != nil {
			if !os.IsNotExist(err) {
				return nil, fmt.Errorf("reading config file: %w", err)
			}
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling config: %w", err)
	}

	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("pollIntervalSeconds", 30)
	v.SetDefault("clientGroupId", "kafkalagexporter")
	v.SetDefault("kafkaClientTimeoutSeconds", 10)
	v.SetDefault("kafkaRetries", 0)
	v.SetDefault("metricWhitelist", []string{".*"})
	v.SetDefault("logLevel", "INFO")

	// Lookup defaults.
	v.SetDefault("lookup.memory.size", 60)
	v.SetDefault("lookup.redis.enabled", false)
	v.SetDefault("lookup.redis.host", "localhost")
	v.SetDefault("lookup.redis.port", 6379)
	v.SetDefault("lookup.redis.database", 0)
	v.SetDefault("lookup.redis.timeout", 60)
	v.SetDefault("lookup.redis.prefix", "kafka-lag-exporter")
	v.SetDefault("lookup.redis.separator", ":")
	v.SetDefault("lookup.redis.retention", "1d")
	v.SetDefault("lookup.redis.expiration", "1d")
	v.SetDefault("lookup.redis.tls", false)

	// Sink defaults.
	v.SetDefault("sinks.prometheus.enabled", true)
	v.SetDefault("sinks.prometheus.port", 8000)
	v.SetDefault("sinks.prometheus.bindAddress", "")
	v.SetDefault("sinks.prometheus.maxTimeSeries", 100000)
	v.SetDefault("sinks.graphite.enabled", false)
	v.SetDefault("sinks.graphite.host", "localhost")
	v.SetDefault("sinks.graphite.port", 2003)
	v.SetDefault("sinks.influxdb.enabled", false)
	v.SetDefault("sinks.influxdb.endpoint", "http://localhost")
	v.SetDefault("sinks.influxdb.port", 8086)
	v.SetDefault("sinks.influxdb.database", "kafka_lag_exporter")
	v.SetDefault("sinks.influxdb.async", true)

	// Watcher defaults.
	v.SetDefault("watchers.strimzi", false)
}

func bindEnvVars(v *viper.Viper) {
	v.SetEnvPrefix("")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Explicit bindings for well-known env vars.
	_ = v.BindEnv("pollIntervalSeconds", "KAFKA_LAG_EXPORTER_POLL_INTERVAL_SECONDS")
	_ = v.BindEnv("sinks.prometheus.port", "KAFKA_LAG_EXPORTER_PORT")
	_ = v.BindEnv("lookup.memory.size", "KAFKA_LAG_EXPORTER_LOOKUP_TABLE_SIZE")
	_ = v.BindEnv("clientGroupId", "KAFKA_LAG_EXPORTER_CLIENT_GROUP_ID")
	_ = v.BindEnv("kafkaClientTimeoutSeconds", "KAFKA_LAG_EXPORTER_KAFKA_CLIENT_TIMEOUT_SECONDS")
	_ = v.BindEnv("kafkaRetries", "KAFKA_LAG_EXPORTER_KAFKA_RETRIES")
	_ = v.BindEnv("watchers.strimzi", "KAFKA_LAG_EXPORTER_STRIMZI")
}

// ParseRetentionDuration parses a duration string like "1d", "12h", "30m".
// Supports d (days), h (hours), m (minutes), s (seconds) suffixes.
func ParseRetentionDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	// Try standard Go duration first.
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	// Support "d" suffix for days.
	if len(s) > 1 && s[len(s)-1] == 'd' {
		var days int
		if _, err := fmt.Sscanf(s, "%dd", &days); err == nil {
			return time.Duration(days) * 24 * time.Hour, nil
		}
	}
	return 0, fmt.Errorf("invalid duration: %q", s)
}

func validate(cfg *Config) error {
	if cfg.PollIntervalSeconds <= 0 {
		return fmt.Errorf("pollIntervalSeconds must be > 0, got %d", cfg.PollIntervalSeconds)
	}
	if cfg.Sinks.Prometheus.Enabled {
		if cfg.Sinks.Prometheus.Port <= 0 || cfg.Sinks.Prometheus.Port > 65535 {
			return fmt.Errorf("prometheus port must be between 1 and 65535, got %d", cfg.Sinks.Prometheus.Port)
		}
	}
	if cfg.Sinks.Graphite.Enabled && cfg.Sinks.Graphite.Host == "" {
		return fmt.Errorf("graphite host is required when graphite sink is enabled")
	}
	if cfg.Sinks.InfluxDB.Enabled && cfg.Sinks.InfluxDB.Endpoint == "" {
		return fmt.Errorf("influxdb endpoint is required when influxdb sink is enabled")
	}
	if cfg.Lookup.Memory.Size < 2 {
		return fmt.Errorf("lookup.memory.size must be >= 2, got %d", cfg.Lookup.Memory.Size)
	}
	for i, c := range cfg.Clusters {
		if c.Name == "" {
			return fmt.Errorf("cluster[%d].name is required", i)
		}
		if c.BootstrapBrokers == "" {
			return fmt.Errorf("cluster[%d].bootstrapBrokers is required", i)
		}
	}
	return nil
}
