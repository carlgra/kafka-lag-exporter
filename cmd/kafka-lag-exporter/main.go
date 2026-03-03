// Package main is the entry point for the Kafka Lag Exporter.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/seglo/kafka-lag-exporter/internal/config"
	"github.com/seglo/kafka-lag-exporter/internal/lookup"
	"github.com/seglo/kafka-lag-exporter/internal/manager"
	"github.com/seglo/kafka-lag-exporter/internal/sink"
)

// version is set at build time via -ldflags.
var version = "dev"

func main() {
	configPath := flag.String("config", "/etc/kafka-lag-exporter/config.yaml", "path to config file")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		os.Exit(0)
	}

	// Load configuration.
	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// Setup logging.
	logLevel := parseLogLevel(cfg.LogLevel)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	logger.Info("kafka-lag-exporter starting",
		"version", version,
		"pollInterval", cfg.PollIntervalSeconds,
		"clusters", len(cfg.Clusters),
		"strimziWatcher", cfg.Watchers.Strimzi,
	)

	// Set build version for build_info metric.
	sink.SetVersion(version)

	// Create metric filter.
	filter, err := sink.NewMetricFilter(cfg.MetricWhitelist)
	if err != nil {
		logger.Error("invalid metric whitelist patterns", "error", err)
		os.Exit(1)
	}

	// Create sinks.
	var sinks []sink.Sink
	var promSink *sink.PrometheusSink

	if cfg.Sinks.Prometheus.Enabled {
		promSink, err = sink.NewPrometheusSink(cfg.Sinks.Prometheus.Port, cfg.Sinks.Prometheus.BindAddress, filter, logger.With("sink", "prometheus"))
		if err != nil {
			logger.Error("failed to create prometheus sink", "error", err)
			os.Exit(1)
		}
		sinks = append(sinks, promSink)
	}

	if cfg.Sinks.Graphite.Enabled {
		graphiteSink := sink.NewGraphiteSink(
			cfg.Sinks.Graphite.Host,
			cfg.Sinks.Graphite.Port,
			cfg.Sinks.Graphite.Prefix,
			cfg.Sinks.Graphite.TLS,
			filter,
			logger.With("sink", "graphite"),
		)
		sinks = append(sinks, graphiteSink)
	}

	if cfg.Sinks.InfluxDB.Enabled {
		influxSink, err := sink.NewInfluxDBSink(
			cfg.Sinks.InfluxDB.Endpoint,
			cfg.Sinks.InfluxDB.Port,
			cfg.Sinks.InfluxDB.Database,
			cfg.Sinks.InfluxDB.Username,
			cfg.Sinks.InfluxDB.Password,
			cfg.Sinks.InfluxDB.Async,
			filter,
			logger.With("sink", "influxdb"),
		)
		if err != nil {
			logger.Error("failed to create influxdb sink", "error", err)
			os.Exit(1)
		}
		sinks = append(sinks, influxSink)
	}

	// Create lookup table factory.
	lookupFactory := createLookupFactory(cfg, logger)

	// Create and start manager.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := manager.New(cfg, sinks, lookupFactory, logger)

	// Wire readiness check from manager to Prometheus sink.
	if promSink != nil {
		promSink.SetReadinessCheck(mgr.ReadinessCheck)
	}

	if err := mgr.Start(ctx); err != nil {
		logger.Error("failed to start manager", "error", err)
		os.Exit(1) //nolint:gocritic // intentional exit on startup failure
	}

	logger.Info("kafka-lag-exporter started successfully")

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	logger.Info("received shutdown signal", "signal", sig)
	cancel()

	// Enforce a hard shutdown deadline.
	shutdownDone := make(chan struct{})
	go func() {
		mgr.Stop()
		close(shutdownDone)
	}()
	select {
	case <-shutdownDone:
		logger.Info("kafka-lag-exporter stopped")
	case <-time.After(15 * time.Second):
		logger.Error("graceful shutdown timed out after 15s, forcing exit")
		os.Exit(1)
	}
}

func createLookupFactory(cfg *config.Config, logger *slog.Logger) func() lookup.LookupTable {
	if cfg.Lookup.Redis.Enabled {
		retention, err := config.ParseRetentionDuration(cfg.Lookup.Redis.Retention)
		if err != nil {
			logger.Warn("invalid redis retention, using default", "error", err)
			retention = 24 * time.Hour
		}
		expiration, err := config.ParseRetentionDuration(cfg.Lookup.Redis.Expiration)
		if err != nil {
			logger.Warn("invalid redis expiration, using default", "error", err)
			expiration = 24 * time.Hour
		}

		redisCfg := lookup.RedisTableConfig{
			Host:       cfg.Lookup.Redis.Host,
			Port:       cfg.Lookup.Redis.Port,
			Database:   cfg.Lookup.Redis.Database,
			Password:   cfg.Lookup.Redis.Password,
			Timeout:    time.Duration(cfg.Lookup.Redis.Timeout) * time.Second,
			Prefix:     cfg.Lookup.Redis.Prefix,
			Separator:  cfg.Lookup.Redis.Separator,
			Retention:  retention,
			Expiration: expiration,
			MaxSize:    int64(cfg.Lookup.Memory.Size),
			TLS:        cfg.Lookup.Redis.TLS,
		}

		logger.Info("using Redis lookup tables",
			"host", redisCfg.Host,
			"port", redisCfg.Port,
			"prefix", redisCfg.Prefix,
		)

		// Redis tables need cluster/topic/partition context at creation time.
		// The collector creates lookup tables per topic-partition, so we return
		// a factory that creates a generic Redis table. The RedisTable key
		// is constructed from the prefix, so each gets a unique key.
		return func() lookup.LookupTable {
			return lookup.NewRedisTable(redisCfg, "default", "default", 0, logger.With("component", "redis-lookup"))
		}
	}

	return func() lookup.LookupTable {
		return lookup.NewMemoryTable(cfg.Lookup.Memory.Size)
	}
}

func parseLogLevel(level string) slog.Level {
	switch strings.ToUpper(level) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
