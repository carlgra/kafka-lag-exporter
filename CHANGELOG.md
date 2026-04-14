# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.3] - 2026-04-14

### Fixed
- Strimzi watcher now uses a namespace-scoped watch when `watchers.strimziWatchNamespace` is set, matching the chart's `Role`/`RoleBinding` scope. Previously the watcher always attempted a cluster-scoped watch, which was denied when only namespaced RBAC existed. ([#34](https://github.com/carlgra/kafka-lag-exporter/pull/34), reported in [#29](https://github.com/carlgra/kafka-lag-exporter/issues/29))
- Strimzi autodiscovery now resolves bootstrap addresses for modern Strimzi v1beta2 clusters. Listener selection matches on the listener `name` field (`plain`/`tls`) and filters on `type` to only accept in-cluster listeners (`internal`), ignoring external exposures (`route`/`loadbalancer`/`nodeport`/`ingress`/`cluster-ip`). Prefers the `bootstrapServers` field over `addresses[0]`. Fallback service address uses `.svc.cluster.local` FQDN instead of the short form. ([#35](https://github.com/carlgra/kafka-lag-exporter/pull/35), reported in [#32](https://github.com/carlgra/kafka-lag-exporter/issues/32))
- TLS-enabled Kafka clusters no longer fail at startup with "cannot set both Dialer and DialTLSConfig". The client now uses `kgo.DialTimeout` for connection timeouts, which composes with `kgo.DialTLSConfig`. ([#36](https://github.com/carlgra/kafka-lag-exporter/pull/36), reported in [#33](https://github.com/carlgra/kafka-lag-exporter/issues/33))

### Added
- PEM-style client-certificate keys for TLS (`ssl.keystore.certificate.chain` + `ssl.keystore.key`) are now honored by `buildTLSConfig`, matching the original Scala exporter's config surface. Previously only `ssl.keystore.location` + `ssl.key.location` were read, silently failing mTLS for users following the original docs. ([#36](https://github.com/carlgra/kafka-lag-exporter/pull/36))
- GitHub Release automation: the release workflow now creates a GitHub Release with auto-generated notes after the chart publish completes.

### Changed
- `charts/kafka-lag-exporter/Chart.yaml` `version`/`appVersion` bumped to `1.0.3`. Previously drifted from releases (the workflow overrode values at package time but the source file was stale); now kept in sync.

## [1.0.2] - 2026-04-13

### Security
- Removed Trivy vulnerability scanner from CI after the March 2026 Trivy supply chain compromise ([CVE-2026-33634](https://www.cvedetails.com/cve/CVE-2026-33634/), [CISA KEV](https://www.cisa.gov/news-events/alerts/2026/03/26/cisa-adds-one-known-exploited-vulnerability-catalog)). Replaced with `govulncheck` pinned to `v1.1.4` — official Go team tool distributed through the Go module proxy, call-graph aware (only flags CVEs actually reachable from application code). ([#31](https://github.com/carlgra/kafka-lag-exporter/pull/31))
- Bumped Go toolchain from `1.25.7` → `1.25.9` in CI and Dockerfile to pick up stdlib fixes surfaced by `govulncheck`: [GO-2026-4601](https://pkg.go.dev/vuln/GO-2026-4601) (net/url IPv6 parsing), [GO-2026-4870](https://pkg.go.dev/vuln/GO-2026-4870) (crypto/tls KeyUpdate DoS), [GO-2026-4946](https://pkg.go.dev/vuln/GO-2026-4946) (crypto/x509 policy validation).

### Added
- Helm chart published as an OCI artifact on GHCR at `oci://ghcr.io/carlgra/charts/kafka-lag-exporter`. No `helm repo add` is required on Helm 3.8+:
  ```bash
  helm install kafka-lag-exporter \
    oci://ghcr.io/carlgra/charts/kafka-lag-exporter \
    --version <version>
  ```
  The release workflow packages and publishes the chart on every `v*` tag push, pinning chart `version`, `appVersion`, and default `image.tag` to the release tag for a consistent install experience. ([#30](https://github.com/carlgra/kafka-lag-exporter/pull/30))

### Changed
- Dockerfile base image pinned from `golang:1.25-alpine` → `golang:1.25.9-alpine` for reproducible stdlib versioning.

## [1.0.1] - 2026-03-09

### Fixed
- Helm chart default image repository changed from `seglo/kafka-lag-exporter` to `ghcr.io/carlgra/kafka-lag-exporter`
- Chart appVersion and image tag corrected from `1.0.0-go` to `1.0.0` to match release tags
- Makefile Docker image target updated to GHCR
- README Docker examples updated to correct image path

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
