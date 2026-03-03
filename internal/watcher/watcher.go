// Package watcher provides cluster discovery mechanisms.
package watcher

import "github.com/seglo/kafka-lag-exporter/internal/config"

// EventType represents the type of cluster event.
type EventType int

const (
	ClusterAdded EventType = iota
	ClusterRemoved
)

// ClusterEvent represents a cluster being added or removed.
type ClusterEvent struct {
	Type    EventType
	Cluster config.ClusterConfig
}

// Watcher discovers Kafka clusters and emits events when they are added or removed.
type Watcher interface {
	Events() <-chan ClusterEvent
	Stop()
}
