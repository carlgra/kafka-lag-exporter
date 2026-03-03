package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllDefinitions_ReturnsAllTen(t *testing.T) {
	defs := AllDefinitions()
	require.Len(t, defs, 10)
}

func TestAllDefinitions_NamesAreUnique(t *testing.T) {
	defs := AllDefinitions()
	seen := make(map[string]bool)
	for _, d := range defs {
		assert.False(t, seen[d.Name], "duplicate metric name: %s", d.Name)
		seen[d.Name] = true
	}
}

func TestAllDefinitions_ContainsExpectedMetrics(t *testing.T) {
	defs := AllDefinitions()
	names := make([]string, len(defs))
	for i, d := range defs {
		names[i] = d.Name
	}

	expected := []string{
		"kafka_partition_latest_offset",
		"kafka_partition_earliest_offset",
		"kafka_consumergroup_group_max_lag",
		"kafka_consumergroup_group_max_lag_seconds",
		"kafka_consumergroup_group_sum_lag",
		"kafka_consumergroup_group_offset",
		"kafka_consumergroup_group_lag",
		"kafka_consumergroup_group_lag_seconds",
		"kafka_consumergroup_group_topic_sum_lag",
		"kafka_consumergroup_poll_time_ms",
	}
	assert.Equal(t, expected, names)
}

func TestAllDefinitions_LabelsAreNonEmpty(t *testing.T) {
	for _, d := range AllDefinitions() {
		assert.NotEmpty(t, d.Labels, "metric %s has no labels", d.Name)
		assert.NotEmpty(t, d.Help, "metric %s has no help text", d.Name)
	}
}

func TestPartitionMetrics_HaveThreeLabels(t *testing.T) {
	assert.Len(t, PartitionLatestOffset.Labels, 3)
	assert.Len(t, PartitionEarliestOffset.Labels, 3)
	assert.Equal(t, []string{"cluster_name", "topic", "partition"}, PartitionLatestOffset.Labels)
}

func TestGroupPerPartitionMetrics_HaveSevenLabels(t *testing.T) {
	expected := []string{"cluster_name", "group", "topic", "partition", "member_host", "consumer_id", "client_id"}
	assert.Equal(t, expected, GroupOffset.Labels)
	assert.Equal(t, expected, GroupLag.Labels)
	assert.Equal(t, expected, GroupLagSeconds.Labels)
}

func TestGroupAggregateMetrics_HaveTwoLabels(t *testing.T) {
	expected := []string{"cluster_name", "group"}
	assert.Equal(t, expected, GroupMaxLag.Labels)
	assert.Equal(t, expected, GroupMaxLagSeconds.Labels)
	assert.Equal(t, expected, GroupSumLag.Labels)
}

func TestGroupTopicSumLag_HasThreeLabels(t *testing.T) {
	assert.Equal(t, []string{"cluster_name", "group", "topic"}, GroupTopicSumLag.Labels)
}

func TestPollTimeMs_HasOneLabel(t *testing.T) {
	assert.Equal(t, []string{"cluster_name"}, PollTimeMs.Labels)
}
