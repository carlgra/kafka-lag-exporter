# Kafka Lag Exporter

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

> Monitor Kafka Consumer Group Latency with Kafka Lag Exporter

## Overview

Kafka Lag Exporter monitors the offset lag and estimates the latency (residence time) of your [Apache Kafka](https://kafka.apache.org/) consumer groups. It can run anywhere as a standalone binary, as a Docker container, or on [Kubernetes](https://kubernetes.io/) with [Helm](https://helm.sh/).

Features:

- Exports consumer group lag metrics to [Prometheus](https://prometheus.io/), [Graphite](https://graphiteapp.org/), and/or [InfluxDB](https://www.influxdata.com/)
- Estimates consumer lag in seconds using offset-to-time interpolation
- Auto-discovers Kafka clusters managed by [Strimzi](https://strimzi.io/) on Kubernetes
- Supports regex-based filtering of consumer groups and topics (allowlist/denylist)
- Optional [Redis](https://redis.io/) persistence for the offset lookup table
- Lightweight single binary with no JVM required

<img width="1793" height="851" alt="image" src="https://github.com/user-attachments/assets/f45eae1d-17d9-4fd5-b3ea-ba8e614289b7" />

### History

This project is a **Go rewrite** of the original [Kafka Lag Exporter](https://github.com/seglo/kafka-lag-exporter) by [Sean Glover](https://github.com/seglo) and contributors. The original was an Akka Typed application written in Scala. This rewrite maintains full compatibility with the original's metrics, configuration, and Helm chart while providing the operational benefits of a statically compiled Go binary (faster startup, lower memory usage, simpler deployment).

The original Scala project has been archived on GitHub. This Go version is a derivative work under the same Apache 2.0 license.

## Contents

- [Metrics](#metrics)
  - [Labels](#labels)
- [Quick Start](#quick-start)
- [Run on Kubernetes](#run-on-kubernetes)
  - [Install with Helm](#install-with-helm)
  - [Examples](#examples)
  - [View the Health Endpoint](#view-the-health-endpoint)
- [Run Standalone](#run-standalone)
  - [Run as Binary](#run-as-binary)
  - [Run as Docker Image](#run-as-docker-image)
- [Configuration](#configuration)
  - [General Configuration](#general-configuration)
  - [Cluster Configuration](#cluster-configuration)
  - [Redis Configuration](#redis-configuration)
  - [Watchers](#watchers)
  - [Environment Variables](#environment-variables)
  - [Example Configuration](#example-configuration)
- [Metric Sinks](#metric-sinks)
  - [Prometheus](#prometheus)
  - [Graphite](#graphite)
  - [InfluxDB](#influxdb)
- [Strimzi Kafka Cluster Watcher](#strimzi-kafka-cluster-watcher)
- [Metric Filtering](#metric-filtering)
- [Health Check](#health-check)
- [Estimating Consumer Group Lag in Time](#estimating-consumer-group-lag-in-time)
- [Required Kafka ACL Permissions](#required-kafka-acl-permissions)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
  - [Building](#building)
  - [Testing](#testing)
  - [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)

## Metrics

Kafka Lag Exporter exposes the following Prometheus-compatible metrics via an HTTP `/metrics` endpoint.

### Consumer Group Per-Partition Metrics

**`kafka_consumergroup_group_offset`** — Last consumed offset for a partition.

**`kafka_consumergroup_group_lag`** — Difference between the latest produced offset and the last consumed offset.

**`kafka_consumergroup_group_lag_seconds`** — Estimated lag in seconds, computed via offset-to-time interpolation. See [Estimating Consumer Group Lag in Time](#estimating-consumer-group-lag-in-time) for details.

Labels: `cluster_name`, `group`, `topic`, `partition`, `member_host`, `consumer_id`, `client_id`

### Consumer Group Aggregate Metrics

**`kafka_consumergroup_group_max_lag`** — Maximum offset lag across all partitions in a consumer group.

**`kafka_consumergroup_group_max_lag_seconds`** — Maximum time lag across all partitions in a consumer group.

**`kafka_consumergroup_group_sum_lag`** — Sum of offset lag across all partitions in a consumer group.

Labels: `cluster_name`, `group`

### Consumer Group Per-Topic Metrics

**`kafka_consumergroup_group_topic_sum_lag`** — Sum of offset lag across all partitions of a topic for a consumer group.

Labels: `cluster_name`, `group`, `topic`

### Partition Metrics

**`kafka_partition_latest_offset`** — Latest available offset for a topic partition.

**`kafka_partition_earliest_offset`** — Earliest available offset for a topic partition.

Labels: `cluster_name`, `topic`, `partition`

### Operational Metrics

**`kafka_consumergroup_poll_time_ms`** — Time taken (in milliseconds) to poll all consumer group information.

Labels: `cluster_name`

### Labels

Every metric includes `cluster_name`. Consumer group metrics additionally include member metadata from Kafka:

| Label | Description |
|---|---|
| `cluster_name` | The configured cluster name, or `namespace/name` for Strimzi-discovered clusters |
| `group` | The Kafka consumer group ID |
| `topic` | The Kafka topic name |
| `partition` | The partition number |
| `member_host` | Hostname or IP of the consumer group member assigned this partition |
| `consumer_id` | The globally unique ID of the consumer group member |
| `client_id` | The client ID of the consumer group member |

Custom labels can be added per cluster using the `labels` configuration property.

## Quick Start

```bash
# Build from source
make build

# Run with a config file
./bin/kafka-lag-exporter --config config.yaml

# Or use Docker
docker run -p 8000:8000 -v $(pwd)/config.yaml:/etc/kafka-lag-exporter/config.yaml \
  ghcr.io/carlgra/kafka-lag-exporter:latest
```

## Run on Kubernetes

### Install with Helm

```bash
helm install kafka-lag-exporter ./charts/kafka-lag-exporter \
  --namespace kafka-lag-exporter \
  --create-namespace
```

Full configuration options are documented in the chart's [`values.yaml`](./charts/kafka-lag-exporter/values.yaml).

### Examples

**Strimzi auto-discovery** (automatically monitors all Strimzi-managed Kafka clusters):

```bash
helm install kafka-lag-exporter ./charts/kafka-lag-exporter \
  --namespace kafka-lag-exporter \
  --set watchers.strimzi=true
```

**Static cluster definition:**

```bash
helm install kafka-lag-exporter ./charts/kafka-lag-exporter \
  --namespace myproject \
  --set clusters\[0\].name=my-cluster \
  --set clusters\[0\].bootstrapBrokers=my-cluster-kafka-bootstrap:9092
```

**With Redis persistence:**

```bash
helm install kafka-lag-exporter ./charts/kafka-lag-exporter \
  --namespace myproject \
  --set lookup.redis.enabled=true \
  --set lookup.redis.host=myredisserver \
  --set clusters\[0\].name=my-cluster \
  --set clusters\[0\].bootstrapBrokers=my-cluster-kafka-bootstrap:9092
```

**Debug installation:**

```bash
helm install kafka-lag-exporter ./charts/kafka-lag-exporter \
  --namespace myproject \
  --set image.pullPolicy=Always \
  --set logLevel=DEBUG \
  --set clusters\[0\].name=my-cluster \
  --set clusters\[0\].bootstrapBrokers=my-cluster-kafka-bootstrap:9092 \
  --debug
```

### View the Health Endpoint

```bash
kubectl port-forward service/kafka-lag-exporter-service 8080:8000 --namespace myproject
curl http://localhost:8080/health
curl http://localhost:8080/metrics
```

### View Exporter Logs

```bash
kubectl logs -l app=kafka-lag-exporter --namespace myproject -f
```

## Run Standalone

### Run as Binary

Download a release binary or build from source:

```bash
make build
./bin/kafka-lag-exporter --config /path/to/config.yaml
```

### Run as Docker Image

```bash
docker run -p 8000:8000 \
  -v $(pwd)/config.yaml:/etc/kafka-lag-exporter/config.yaml \
  ghcr.io/carlgra/kafka-lag-exporter:latest
```

## Configuration

Configuration is loaded from a YAML file (default: `/etc/kafka-lag-exporter/config.yaml`). Most settings can also be set via environment variables.

### General Configuration

| Key | Default | Description |
|---|---|---|
| `pollIntervalSeconds` | `30` | How often to poll Kafka for offsets |
| `clientGroupId` | `kafkalagexporter` | Consumer group ID for the exporter's own connections |
| `kafkaClientTimeoutSeconds` | `10` | Connection timeout for Kafka API calls |
| `kafkaRetries` | `0` | Number of retries when fetching consumer groups |
| `logLevel` | `INFO` | Log level: `DEBUG`, `INFO`, `WARN`, `ERROR` |
| `logFormat` | `text` | Log output format: `text` or `json` |
| `metricAllowlist` | `[".*"]` | Regex patterns for metrics to expose (see [Metric Filtering](#metric-filtering)) |
| `lookup.memory.size` | `60` | Maximum entries in each partition's in-memory lookup table |

### Cluster Configuration

Defined under `clusters[]` in the config file.

| Key | Default | Required | Description |
|---|---|---|---|
| `name` | | Yes | Unique cluster name used in metric labels |
| `bootstrapBrokers` | | Yes | Comma-delimited list of Kafka broker addresses |
| `groupAllowlist` | `[]` | No | Regex patterns for consumer groups to monitor |
| `groupDenylist` | `[]` | No | Regex patterns for consumer groups to exclude |
| `topicAllowlist` | `[]` | No | Regex patterns for topics to monitor |
| `topicDenylist` | `[]` | No | Regex patterns for topics to exclude |
| `labels` | `{}` | No | Custom labels added to all metrics for this cluster |
| `consumerProperties` | `{}` | No | Additional Kafka consumer properties (e.g., `security.protocol: SSL`) |
| `adminClientProperties` | `{}` | No | Additional Kafka admin client properties |

> **Backward Compatibility**: The deprecated field names `groupWhitelist`, `groupBlacklist`, `topicWhitelist`, `topicBlacklist`, and `metricWhitelist` are still supported. The new names take precedence if both are set.

### Redis Configuration

Defined under `lookup.redis` in the config file. When enabled, Redis replaces the in-memory lookup table for offset-to-time interpolation, providing persistence across restarts.

| Key | Default | Description |
|---|---|---|
| `enabled` | `false` | Enable Redis-backed lookup table |
| `host` | `localhost` | Redis server hostname |
| `port` | `6379` | Redis server port |
| `database` | `0` | Redis database number |
| `password` | | Redis password |
| `timeout` | `60` | Connection timeout in seconds |
| `prefix` | `kafka-lag-exporter` | Key prefix for all Redis keys |
| `separator` | `:` | Separator used in Redis key construction |
| `retention` | `1d` | How long to retain lookup table points |
| `expiration` | `1d` | TTL for Redis keys |

### Watchers

| Key | Default | Description |
|---|---|---|
| `watchers.strimzi` | `false` | Enable automatic discovery of Strimzi Kafka clusters |

### Environment Variables

Key configuration options can be set via environment variables:

| Environment Variable | Config Key |
|---|---|
| `KAFKA_LAG_EXPORTER_POLL_INTERVAL_SECONDS` | `pollIntervalSeconds` |
| `KAFKA_LAG_EXPORTER_PORT` | `sinks.prometheus.port` |
| `KAFKA_LAG_EXPORTER_LOOKUP_TABLE_SIZE` | `lookup.memory.size` |
| `KAFKA_LAG_EXPORTER_CLIENT_GROUP_ID` | `clientGroupId` |
| `KAFKA_LAG_EXPORTER_KAFKA_CLIENT_TIMEOUT_SECONDS` | `kafkaClientTimeoutSeconds` |
| `KAFKA_LAG_EXPORTER_STRIMZI` | `watchers.strimzi` |
| `KAFKA_LAG_EXPORTER_LOG_FORMAT` | `logFormat` |

### Example Configuration

```yaml
pollIntervalSeconds: 30
clientGroupId: kafkalagexporter
kafkaClientTimeoutSeconds: 10
logLevel: INFO
metricWhitelist:
  - ".*"

clusters:
  - name: production
    bootstrapBrokers: "broker-1:9092,broker-2:9092,broker-3:9092"
    groupAllowlist:
      - "^app-.*"
    topicDenylist:
      - "^__.*"
    labels:
      environment: production
      region: us-east-1

lookup:
  memory:
    size: 120

sinks:
  prometheus:
    enabled: true
    port: 9090
```

## Metric Sinks

Kafka Lag Exporter supports reporting metrics to one or more sinks simultaneously.

### Prometheus

Enabled by default. Exposes metrics at `http://HOST:PORT/metrics` in Prometheus exposition format.

```yaml
sinks:
  prometheus:
    enabled: true
    port: 8000
```

### Graphite

Sends metrics via TCP plaintext protocol. Metric paths are constructed as `prefix.label1.label2.metric_name`.

```yaml
sinks:
  graphite:
    enabled: true
    host: graphite.example.com
    port: 2003
    prefix: kafka
```

### InfluxDB

Writes metrics as InfluxDB line protocol. Supports both synchronous and asynchronous write modes.

```yaml
sinks:
  influxdb:
    enabled: true
    endpoint: "http://influxdb.example.com"
    port: 8086
    database: kafka_lag_exporter
    username: ""
    password: ""
    async: true
```

## Strimzi Kafka Cluster Watcher

When enabled with `watchers.strimzi: true`, the exporter watches for [Strimzi](https://strimzi.io/) `Kafka` custom resources (CRD: `kafka.strimzi.io/v1beta2`) in Kubernetes. It automatically:

- Starts monitoring when a `Kafka` resource is created or updated
- Stops monitoring when a `Kafka` resource is deleted
- Extracts bootstrap broker addresses from the resource's status
- Names clusters using the format `namespace/name`

This requires the exporter to run inside a Kubernetes cluster with appropriate RBAC permissions (included in the Helm chart).

## Metric Filtering

Use `metricAllowlist` to control which metrics are exposed. Only metrics matching at least one regex pattern are reported. The deprecated `metricWhitelist` name is still supported for backward compatibility.

```yaml
# Only expose lag metrics and latest offset
metricAllowlist:
  - "kafka_consumergroup_group_lag$"
  - "kafka_consumergroup_group_lag_seconds$"
  - "kafka_partition_latest_offset"
```

## Health Check

A health check endpoint is available at `/health` on the Prometheus port (default 8000). It returns HTTP 200 when the exporter is running.

```bash
curl http://localhost:8000/health
```

The `kafka_consumergroup_poll_time_ms` metric can be used to monitor polling health. If it exceeds the poll interval, the exporter may be struggling to keep up.

## Estimating Consumer Group Lag in Time

The `kafka_consumergroup_group_lag_seconds` metric estimates how far behind a consumer group is in wall-clock time, not just offset count. This is more meaningful than offset lag alone because message production rates vary over time.

### How It Works

The exporter maintains a **sliding window lookup table** of `(offset, timestamp)` pairs for each topic-partition. On every poll cycle, it records the latest partition offset and the time it was observed. Over time, this builds a history of offset progression.

When computing time lag for a consumer group's committed offset:

1. **Find surrounding points**: The lookup table finds two points that bracket the consumer's committed offset — one below and one above.
2. **Interpolate**: Using linear interpolation between these two points, it estimates the wall-clock time when the consumer's current offset was the latest offset.
3. **Compute lag**: `lag_seconds = (current_time - estimated_time) / 1000`

### Edge Cases

| Scenario | Behavior |
|---|---|
| **Fewer than 2 points** in lookup table | Returns `NaN` (metric not reported). Happens on startup before enough data is collected. |
| **Consumer offset at or beyond latest** | Lag is 0 seconds (consumer is caught up). |
| **Consumer offset below all stored points** | Extrapolates from the oldest and newest points. May produce large values for reprocessing consumers. |
| **Negative lag** (race condition) | Returns `NaN`. Can occur when offset and time are collected at slightly different moments. |
| **Flat-line partition** (no new messages) | Timestamp of existing point is updated but no new point is added, preventing stale time estimates. |
| **All partitions return NaN** | The `kafka_consumergroup_group_max_lag_seconds` aggregate is not reported (instead of misleadingly reporting 0). |

### Lookup Table Configuration

The sliding window size determines how many historical points are kept per partition:

- **In-memory** (default): `lookup.memory.size: 60` — keeps the last 60 observations per partition
- **Redis**: Provides persistence across restarts with configurable retention and TTL

A larger window provides more accurate interpolation but uses more memory. The default of 60 points at a 30-second poll interval gives ~30 minutes of history.

### Astronomical Lag Values

If a consumer group reprocesses a topic from the beginning, the time lag can appear very large because the lookup table only has recent offset-to-time mappings. The exporter extrapolates backwards, which can produce values of hours or days. This is expected behavior — the consumer genuinely is that far behind in wall-clock time. The lag value will decrease as the consumer catches up.

## Required Kafka ACL Permissions

Kafka Lag Exporter requires `DESCRIBE` permission on consumer groups, topics, and the cluster:

```
kafka-acls --authorizer-properties "zookeeper.connect=localhost:2181" \
  --add --allow-principal "User:kafka-lag-exporter" \
  --operation DESCRIBE --group '*' --topic '*' --cluster
```

Resulting ACLs:

```
Current ACLs for resource `Cluster:LITERAL:kafka-cluster`:
    User:kafka-lag-exporter has Allow permission for operations: Describe from hosts: *

Current ACLs for resource `Group:LITERAL:*`:
    User:kafka-lag-exporter has Allow permission for operations: Describe from hosts: *

Current ACLs for resource `Topic:LITERAL:*`:
    User:kafka-lag-exporter has Allow permission for operations: Describe from hosts: *
```

## Troubleshooting

If you see inconsistent metrics, enable `DEBUG` logging:

```yaml
logLevel: DEBUG
```

Or via environment variable:

```bash
export KAFKA_LAG_EXPORTER_LOGLEVEL=DEBUG
```

Debug logs will show raw offset data from each poll cycle, which helps identify issues with consumer group metadata or offset fetching.

When running on Kubernetes, set `logLevel: DEBUG` in the Helm values.

## Development

### Building

```bash
# Build binary
make build

# Build Docker image
make docker

# Format code
make fmt

# Run linter
make lint
```

### Testing

```bash
# Unit tests (81%+ coverage)
make test

# Unit tests with coverage report
go test -race -coverprofile=coverage.out ./internal/...
go tool cover -func=coverage.out

# Integration tests (requires Docker for testcontainers)
make test-integration
```

### Project Structure

```
.
├── cmd/kafka-lag-exporter/    # Application entry point
├── internal/
│   ├── collector/             # Per-cluster offset polling and metric computation
│   ├── config/                # YAML configuration loading
│   ├── domain/                # Core value types (TopicPartition, GroupTopicPartition, etc.)
│   ├── kafka/                 # Kafka client abstraction (franz-go)
│   ├── lookup/                # Offset-to-time interpolation tables (memory + Redis)
│   ├── manager/               # Orchestrates collectors, watchers, and sinks
│   ├── metrics/               # Prometheus metric definitions
│   ├── sink/                  # Metric output destinations (Prometheus, Graphite, InfluxDB)
│   └── watcher/               # Cluster discovery (Strimzi CRD watcher)
├── integration/               # Integration tests (testcontainers)
├── charts/kafka-lag-exporter/ # Helm chart
├── Dockerfile
├── Makefile
└── config.yaml                # Example configuration
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the [Apache License 2.0](LICENSE).

This is a Go rewrite of the original [Kafka Lag Exporter](https://github.com/seglo/kafka-lag-exporter) created by [Sean Glover](https://github.com/seglo) ([@seglo](https://github.com/seglo)) and [contributors](https://github.com/seglo/kafka-lag-exporter/graphs/contributors), originally developed at [Lightbend](https://www.lightbend.com/). The original project is archived on GitHub.
