package collector

import "github.com/seglo/kafka-lag-exporter/internal/domain"

// OffsetsSnapshot holds all offset data collected in a single poll cycle.
type OffsetsSnapshot struct {
	Timestamp       int64
	GroupOffsets    domain.GroupOffsets
	EarliestOffsets domain.PartitionOffsets
	LatestOffsets   domain.PartitionOffsets
}

// AllTopicPartitions returns the set of all topic-partitions seen in the latest offsets.
func (s *OffsetsSnapshot) AllTopicPartitions() []domain.TopicPartition {
	seen := make(map[domain.TopicPartition]bool)
	var tps []domain.TopicPartition

	for tp := range s.LatestOffsets {
		if !seen[tp] {
			seen[tp] = true
			tps = append(tps, tp)
		}
	}
	return tps
}

// AllGroupTopicPartitions returns the set of all group-topic-partitions seen in group offsets.
func (s *OffsetsSnapshot) AllGroupTopicPartitions() []domain.GroupTopicPartition {
	var gtps []domain.GroupTopicPartition
	for gtp := range s.GroupOffsets {
		gtps = append(gtps, gtp)
	}
	return gtps
}

// RemovedGroupTopicPartitions returns GTPs that were in prev but not in current.
func RemovedGroupTopicPartitions(prev, current *OffsetsSnapshot) []domain.GroupTopicPartition {
	if prev == nil {
		return nil
	}
	if current == nil {
		return nil
	}
	currentSet := make(map[domain.GroupTopicPartition]bool, len(current.GroupOffsets))
	for gtp := range current.GroupOffsets {
		currentSet[gtp] = true
	}

	var removed []domain.GroupTopicPartition
	for gtp := range prev.GroupOffsets {
		if !currentSet[gtp] {
			removed = append(removed, gtp)
		}
	}
	return removed
}

// RemovedTopicPartitions returns TPs that were in prev but not in current.
func RemovedTopicPartitions(prev, current *OffsetsSnapshot) []domain.TopicPartition {
	if prev == nil {
		return nil
	}
	if current == nil {
		return nil
	}
	currentSet := make(map[domain.TopicPartition]bool, len(current.LatestOffsets))
	for tp := range current.LatestOffsets {
		currentSet[tp] = true
	}

	var removed []domain.TopicPartition
	for tp := range prev.LatestOffsets {
		if !currentSet[tp] {
			removed = append(removed, tp)
		}
	}
	return removed
}
