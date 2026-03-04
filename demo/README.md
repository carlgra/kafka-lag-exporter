# kafka-lag-exporter Demo

A self-contained demo that shows kafka-lag-exporter monitoring a Kafka cluster with a producer outpacing a slow consumer, creating visible lag.

## Stack

| Service             | Port  | Description                              |
|---------------------|-------|------------------------------------------|
| Redpanda            | 19092 | Kafka-compatible broker                  |
| kafka-lag-exporter  | 8000  | Metrics exporter (built from source)     |
| Prometheus          | 9090  | Scrapes exporter every 10s               |
| Grafana             | 3000  | Pre-provisioned dashboard                |
| demo-harness        | —     | Produces ~10 msg/s, consumes ~2 msg/s    |

## Quick Start

```bash
docker compose up --build
```

Wait ~30 seconds for services to start and lag to accumulate, then:

- **Grafana dashboard**: http://localhost:3000 (no login required)
- **Raw metrics**: http://localhost:8000/metrics
- **Prometheus**: http://localhost:9090

## What You'll See

The demo harness produces messages at ~10/sec across 3 partitions but consumes at only ~2/sec. This creates a steadily growing lag that the exporter captures and exposes as Prometheus metrics. The Grafana dashboard visualizes:

- Per-partition consumer group lag (offsets and seconds)
- Aggregate max/sum lag stats
- Latest partition offsets
- Exporter poll time

## Clean Up

```bash
docker compose down
```
