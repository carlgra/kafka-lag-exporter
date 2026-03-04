# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Complete Go rewrite of the original [Scala Kafka Lag Exporter](https://github.com/seglo/kafka-lag-exporter)
- Consumer group lag and latency monitoring via offset-to-time interpolation
- Prometheus, Graphite, and InfluxDB sink support
- Strimzi auto-discovery of Kafka clusters on Kubernetes
- Regex-based consumer group and topic filtering (whitelist/blacklist)
- Optional Redis persistence for the offset lookup table
- Structured logging with `slog`
- Health and readiness endpoints (`/health`, `/ready`)
- Multi-platform Docker image (linux/amd64, linux/arm64)
- Helm chart with ServiceMonitor, PrometheusRule, and PodDisruptionBudget support
- GitHub Actions CI/CD pipeline (lint, test, build, integration tests, release)
- SECURITY.md, CONTRIBUTING.md, CODE_OF_CONDUCT.md

### Changed
- Rewritten from Scala/Akka to Go for faster startup, lower memory, and simpler deployment
- Single statically-compiled binary with no JVM dependency

### Notes
- Full compatibility with the original project's Prometheus metrics, configuration format, and Helm chart
- Licensed under Apache 2.0, consistent with the original project
