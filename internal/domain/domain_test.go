package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTopicPartition_String(t *testing.T) {
	tp := TopicPartition{Topic: "orders", Partition: 3}
	assert.Equal(t, "orders-3", tp.String())
}

func TestTopicPartition_String_ZeroPartition(t *testing.T) {
	tp := TopicPartition{Topic: "events", Partition: 0}
	assert.Equal(t, "events-0", tp.String())
}

func TestGroupTopicPartition_TopicPartition(t *testing.T) {
	gtp := GroupTopicPartition{
		Group:      "my-group",
		Topic:      "my-topic",
		Partition:  5,
		MemberHost: "host1",
		ConsumerID: "consumer-1",
		ClientID:   "client-1",
	}
	tp := gtp.TopicPartition()
	assert.Equal(t, TopicPartition{Topic: "my-topic", Partition: 5}, tp)
}

func TestGroupTopicPartition_String(t *testing.T) {
	gtp := GroupTopicPartition{
		Group:     "cg1",
		Topic:     "events",
		Partition: 2,
	}
	assert.Equal(t, "cg1-events-2", gtp.String())
}

func TestPartitionOffsets_MapBehavior(t *testing.T) {
	offsets := make(PartitionOffsets)
	tp1 := TopicPartition{Topic: "t1", Partition: 0}
	tp2 := TopicPartition{Topic: "t1", Partition: 1}

	offsets[tp1] = PartitionOffset{Offset: 100, Timestamp: 1000}
	offsets[tp2] = PartitionOffset{Offset: 200, Timestamp: 2000}

	assert.Len(t, offsets, 2)
	assert.Equal(t, int64(100), offsets[tp1].Offset)
	assert.Equal(t, int64(200), offsets[tp2].Offset)
}

func TestGroupOffsets_MapBehavior(t *testing.T) {
	offsets := make(GroupOffsets)
	gtp := GroupTopicPartition{Group: "g1", Topic: "t1", Partition: 0}
	offsets[gtp] = PartitionOffset{Offset: 50, Timestamp: 500}

	assert.Len(t, offsets, 1)
	assert.Equal(t, int64(50), offsets[gtp].Offset)
}
