// Package metrics defines the Prometheus metric definitions and message types.
package metrics

// GaugeDefinition describes a Prometheus gauge metric.
type GaugeDefinition struct {
	Name   string
	Help   string
	Labels []string
}

// Partition-level metrics.
var (
	PartitionLatestOffset = GaugeDefinition{
		Name:   "kafka_partition_latest_offset",
		Help:   "Latest offset of a partition",
		Labels: []string{"cluster_name", "topic", "partition"},
	}

	PartitionEarliestOffset = GaugeDefinition{
		Name:   "kafka_partition_earliest_offset",
		Help:   "Earliest offset of a partition",
		Labels: []string{"cluster_name", "topic", "partition"},
	}
)

// Consumer group aggregate metrics.
var (
	GroupMaxLag = GaugeDefinition{
		Name:   "kafka_consumergroup_group_max_lag",
		Help:   "Max group offset lag",
		Labels: []string{"cluster_name", "group"},
	}

	GroupMaxLagSeconds = GaugeDefinition{
		Name:   "kafka_consumergroup_group_max_lag_seconds",
		Help:   "Max group time lag",
		Labels: []string{"cluster_name", "group"},
	}

	GroupSumLag = GaugeDefinition{
		Name:   "kafka_consumergroup_group_sum_lag",
		Help:   "Sum of group offset lag",
		Labels: []string{"cluster_name", "group"},
	}
)

// Consumer group per-partition metrics.
var (
	GroupOffset = GaugeDefinition{
		Name:   "kafka_consumergroup_group_offset",
		Help:   "Last group consumed offset of a partition",
		Labels: []string{"cluster_name", "group", "topic", "partition", "member_host", "consumer_id", "client_id"},
	}

	GroupLag = GaugeDefinition{
		Name:   "kafka_consumergroup_group_lag",
		Help:   "Group offset lag of a partition",
		Labels: []string{"cluster_name", "group", "topic", "partition", "member_host", "consumer_id", "client_id"},
	}

	GroupLagSeconds = GaugeDefinition{
		Name:   "kafka_consumergroup_group_lag_seconds",
		Help:   "Group time lag of a partition",
		Labels: []string{"cluster_name", "group", "topic", "partition", "member_host", "consumer_id", "client_id"},
	}
)

// Consumer group per-topic metrics.
var (
	GroupTopicSumLag = GaugeDefinition{
		Name:   "kafka_consumergroup_group_topic_sum_lag",
		Help:   "Sum of group offset lag across topic partitions",
		Labels: []string{"cluster_name", "group", "topic"},
	}
)

// Polling metrics.
var (
	PollTimeMs = GaugeDefinition{
		Name:   "kafka_consumergroup_poll_time_ms",
		Help:   "Group time poll time",
		Labels: []string{"cluster_name"},
	}
)

// AllDefinitions returns all gauge definitions in order.
func AllDefinitions() []GaugeDefinition {
	return []GaugeDefinition{
		PartitionLatestOffset,
		PartitionEarliestOffset,
		GroupMaxLag,
		GroupMaxLagSeconds,
		GroupSumLag,
		GroupOffset,
		GroupLag,
		GroupLagSeconds,
		GroupTopicSumLag,
		PollTimeMs,
	}
}
