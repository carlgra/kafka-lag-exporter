// Package domain defines core value types used throughout the application.
package domain

import "fmt"

// TopicPartition identifies a Kafka topic-partition pair.
type TopicPartition struct {
	Topic     string
	Partition int32
}

func (tp TopicPartition) String() string {
	return fmt.Sprintf("%s-%d", tp.Topic, tp.Partition)
}

// GroupTopicPartition identifies a consumer group's assignment to a topic-partition.
type GroupTopicPartition struct {
	Group      string
	Topic      string
	Partition  int32
	MemberHost string
	ConsumerID string
	ClientID   string
}

func (gtp GroupTopicPartition) TopicPartition() TopicPartition {
	return TopicPartition{Topic: gtp.Topic, Partition: gtp.Partition}
}

func (gtp GroupTopicPartition) String() string {
	return fmt.Sprintf("%s-%s-%d", gtp.Group, gtp.Topic, gtp.Partition)
}

// PartitionOffset holds an offset value and the timestamp when it was observed.
type PartitionOffset struct {
	Offset    int64
	Timestamp int64 // milliseconds since epoch
}

// PartitionOffsets maps topic-partitions to their offsets.
type PartitionOffsets map[TopicPartition]PartitionOffset

// GroupOffsets maps group-topic-partitions to their committed offsets.
type GroupOffsets map[GroupTopicPartition]PartitionOffset
