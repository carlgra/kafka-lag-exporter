package sink

import "testing"

func BenchmarkMetricFilter_Matches(b *testing.B) {
	patterns := []string{
		"^kafka_consumergroup_group_lag$",
		"^kafka_consumergroup_group_offset$",
		"^kafka_partition_latest_offset$",
		"^kafka_consumergroup_group_lag_seconds$",
		"^kafka_consumergroup_group_max_lag$",
	}
	filter, err := NewMetricFilter(patterns)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("early_match", func(b *testing.B) {
		for range b.N {
			filter.Matches("kafka_consumergroup_group_lag")
		}
	})

	b.Run("late_match", func(b *testing.B) {
		for range b.N {
			filter.Matches("kafka_consumergroup_group_max_lag")
		}
	})

	b.Run("no_match", func(b *testing.B) {
		for range b.N {
			filter.Matches("kafka_unknown_metric_name")
		}
	})
}
