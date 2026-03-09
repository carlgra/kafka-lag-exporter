# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-03-09

### Added
- Complete Go rewrite of the original [Scala Kafka Lag Exporter](https://github.com/seglo/kafka-lag-exporter)
- Consumer group lag and latency monitoring via offset-to-time interpolation
- Prometheus, Graphite, and InfluxDB sink support
- Strimzi auto-discovery of Kafka clusters on Kubernetes
- Regex-based consumer group and topic filtering (allowlist/denylist)
- Optional Redis persistence for the offset lookup table
- Structured logging with `slog` (text and JSON formats via `logFormat` config)
- Health and readiness endpoints (`/health`, `/ready`)
- Multi-platform Docker image (linux/amd64, linux/arm64)
- Helm chart with ServiceMonitor, PrometheusRule, and PodDisruptionBudget support
- GitHub Actions CI/CD pipeline (lint, test, build, integration tests, release)
- Trivy security scanning in CI pipeline
- Docker Compose demo environment
- SECURITY.md, CONTRIBUTING.md, CODE_OF_CONDUCT.md
- Kafka client connection metrics (`client_connects_total`, `client_disconnects_total`, `client_write_errors_total`, `client_read_errors_total`)
- Configurable Helm deployment replica count (`replicaCount`)
- Support for existing Kubernetes ServiceAccount (`serviceAccount.existingName`)
- `honorLabels` and `scrapeTimeout` support in ServiceMonitor
- Documentation for lag-in-seconds estimation algorithm and edge cases
- Prometheus cardinality limit protection with `kafka_lag_exporter_dropped_series_total` metric
- Comprehensive test suite with >80% coverage on all application packages

### Fixed
- NaN aggregate bug: `max_lag_seconds` no longer reports 0 when all partitions return NaN
- Unassigned partition lag: topics with committed offsets but no active consumer members now report lag correctly
- Helm envFrom duplication: configMapRefs and secretRefs combined into single block
- Redis connection pool leak: single client shared across all lookup tables
- Graphite sink buffer growth bounded when endpoint is down
- Nil snapshot panic in `RemovedGroupTopicPartitions`
- Watcher event channel no longer silently drops events on overflow
- Config validation: rejects negative retries, zero timeouts, and empty group IDs

### Changed
- Rewritten from Scala/Akka to Go for faster startup, lower memory, and simpler deployment
- Single statically-compiled binary with no JVM dependency
- Renamed `whitelist`/`blacklist` config fields to `allowlist`/`denylist` (old names still supported for backward compatibility)

### Notes
- Full compatibility with the original project's Prometheus metrics, configuration format, and Helm chart
- Licensed under Apache 2.0, consistent with the original project
