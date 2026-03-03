package sink

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricFilter_Matches(t *testing.T) {
	filter, err := NewMetricFilter([]string{"kafka_consumergroup.*", "kafka_partition_latest.*"})
	require.NoError(t, err)

	assert.True(t, filter.Matches("kafka_consumergroup_group_lag"))
	assert.True(t, filter.Matches("kafka_partition_latest_offset"))
	assert.False(t, filter.Matches("kafka_partition_earliest_offset"))
	assert.False(t, filter.Matches("unrelated_metric"))
}

func TestMetricFilter_EmptyPatterns(t *testing.T) {
	filter, err := NewMetricFilter(nil)
	require.NoError(t, err)

	// No patterns → nothing matches.
	assert.False(t, filter.Matches("anything"))
}

func TestMetricFilter_MatchAll(t *testing.T) {
	filter, err := NewMetricFilter([]string{".*"})
	require.NoError(t, err)

	assert.True(t, filter.Matches("kafka_consumergroup_group_lag"))
	assert.True(t, filter.Matches("anything"))
}

func TestMetricFilter_InvalidRegex(t *testing.T) {
	_, err := NewMetricFilter([]string{"[invalid"})
	assert.Error(t, err)
}
