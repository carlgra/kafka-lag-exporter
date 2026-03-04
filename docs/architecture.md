# Architecture

## Overview

Kafka Lag Exporter is a Go application that monitors Apache Kafka consumer group lag. It periodically polls Kafka clusters for consumer group offsets, computes offset lag and time lag (via interpolation), and exports the metrics to one or more sinks (Prometheus, Graphite, InfluxDB). It can discover clusters statically from configuration or dynamically via Strimzi Kafka CRDs on Kubernetes.

## Component Diagram

```
                     ┌─────────────────────────────────────────────┐
                     │                  Manager                    │
                     │  (orchestrates collectors and watchers)     │
                     └──────┬──────────────┬───────────────────────┘
                            │              │
              ┌─────────────▼──┐     ┌─────▼──────────────┐
              │   Collector    │     │  Strimzi Watcher   │
              │  (per cluster) │     │  (K8s informer)    │
              │                │     │                    │
              │  poll → compute│     │  ClusterAdded /    │
              │  → report      │     │  ClusterRemoved    │
              └───┬────────┬───┘     └────────────────────┘
                  │        │
       ┌──────────▼─┐  ┌──▼──────────────────────────────┐
       │ Kafka      │  │           Sinks                  │
       │ Client     │  │  ┌────────────┐ ┌─────────────┐  │
       │ (franz-go) │  │  │ Prometheus │ │  Graphite   │  │
       │            │  │  └────────────┘ └─────────────┘  │
       │ GetGroups  │  │  ┌────────────┐                  │
       │ GetOffsets │  │  │  InfluxDB  │                  │
       └────────────┘  │  └────────────┘                  │
                       └──────────────────────────────────┘
                  │
       ┌──────────▼──────────┐
       │   Lookup Tables     │
       │  (Memory or Redis)  │
       │  offset → time      │
       └─────────────────────┘
```

## Poll Cycle Data Flow

Each collector runs an independent poll loop at the configured interval:

1. **Poll** — Ticker fires (or immediate on startup).
2. **Collect offsets** — Concurrently fetch consumer group offsets, earliest offsets, and latest offsets from Kafka using `errgroup`. Retries with exponential backoff on failure.
3. **Update lookup tables** — Insert latest offsets into per-partition lookup tables for time-lag interpolation.
4. **Compute metrics** — For each group-topic-partition: compute offset lag (`latest - group`, clamped to 0), compute time lag via lookup table interpolation. Aggregate max-lag, sum-lag per group.
5. **Report to sinks** — Send each computed `MetricValue` to all configured sinks.
6. **Evict stale** — Compare current snapshot to previous; remove metrics for groups/partitions that no longer exist.
7. **Record instrumentation** — Report poll duration, success/failure, and lookup table sizes.

## Lookup Table Design

Lookup tables map offsets to timestamps, enabling time-lag estimation.

**Sliding window**: Each table holds the last N offset-time points (default 60) per topic-partition.

**Interpolation algorithm** (`predict`): Given two points (offset₁, time₁) and (offset₂, time₂) and a target offset, compute the estimated time via linear interpolation:

```
time = time₁ + (time₂ - time₁) / (offset₂ - offset₁) × (offset - offset₁)
```

Lookup logic:
- Offset at or beyond latest → return latest time (lag = 0).
- Offset between two stored points → interpolate.
- Offset below all stored points → extrapolate from oldest and latest.
- Fewer than 2 points → return "TooFewPoints" (no estimate).

**Memory vs Redis**:
- **MemoryTable**: In-process `[]Point` slice with `sync.RWMutex`. Fast, zero external dependencies. Data lost on restart.
- **RedisTable**: Redis sorted sets (score=offset, member=timestamp). Shared across instances. Supports TTL-based expiration. Uses a single shared `*redis.Client` across all tables.

## Strimzi Watcher

When enabled, the Strimzi watcher uses a Kubernetes dynamic client to watch `kafka.strimzi.io/v1beta2` `Kafka` resources:

- **Watch** — Lists existing Kafka CRs, then watches for changes using the K8s watch API with jitter-based retry on disconnect.
- **ClusterAdded** — Extracts bootstrap brokers from `.status.listeners[]` and consumer/admin properties from `.spec.kafka.config`. Sends a `ClusterAdded` event to the manager.
- **ClusterRemoved** — Sends a `ClusterRemoved` event; the manager stops and cleans up the corresponding collector.
- **Namespace scoping** — Can watch all namespaces (ClusterRole) or a single namespace (Role) based on configuration.

## Configuration Hierarchy

Configuration is loaded with the following precedence (highest to lowest):

1. **Environment variables** — `KLE_POLL_INTERVAL_SECONDS`, `KLE_KAFKA_CLIENT_TIMEOUT_SECONDS`, etc.
2. **YAML config file** — Specified via `--config` flag (default `/etc/kafka-lag-exporter/config.yaml`).
3. **Defaults** — Hardcoded in `config.go` (e.g., poll interval 30s, memory lookup size 60).

The Helm chart renders the YAML config from `values.yaml` into a ConfigMap.

## Package Layout

| Package | Description |
|---------|-------------|
| `cmd/kafka-lag-exporter` | Entry point: flag parsing, config loading, wiring components, signal handling |
| `internal/config` | Configuration loading, validation, environment variable overrides |
| `internal/collector` | Per-cluster poll loop, offset collection, metric computation, eviction |
| `internal/domain` | Core types: `TopicPartition`, `GroupTopicPartition`, `PartitionOffset`, etc. |
| `internal/kafka` | Kafka client abstraction and franz-go implementation |
| `internal/lookup` | Offset-to-time lookup tables: `MemoryTable`, `RedisTable`, `predict()` |
| `internal/manager` | Orchestrates collectors and watchers; readiness/health checks |
| `internal/metrics` | Prometheus gauge definitions and message types |
| `internal/sink` | Metric output destinations: Prometheus, Graphite, InfluxDB, metric filter |
| `internal/watcher` | Strimzi Kafka CRD watcher (K8s dynamic client) |
| `integration` | Integration tests using testcontainers (Redpanda, Redis) |
| `charts/kafka-lag-exporter` | Helm chart for Kubernetes deployment |
