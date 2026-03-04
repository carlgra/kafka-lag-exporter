package watcher

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

// --- extractClusterConfig tests ---------------------------------------------

func TestExtractClusterConfig_WithBootstrapServers(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"metadata": map[string]interface{}{
				"name":      "my-kafka",
				"namespace": "kafka-ns",
			},
			"status": map[string]interface{}{
				"listeners": []interface{}{
					map[string]interface{}{
						"type":             "plain",
						"bootstrapServers": "broker1:9092,broker2:9092",
					},
				},
			},
		},
	}

	cluster, err := extractClusterConfig(obj)
	require.NoError(t, err)
	assert.Equal(t, "kafka-ns/my-kafka", cluster.Name)
	assert.Equal(t, "broker1:9092,broker2:9092", cluster.BootstrapBrokers)
}

func TestExtractClusterConfig_WithAddresses(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"metadata": map[string]interface{}{
				"name":      "my-kafka",
				"namespace": "default",
			},
			"status": map[string]interface{}{
				"listeners": []interface{}{
					map[string]interface{}{
						"type": "plain",
						"addresses": []interface{}{
							map[string]interface{}{
								"host": "kafka.example.com",
								"port": float64(9092),
							},
						},
					},
				},
			},
		},
	}

	cluster, err := extractClusterConfig(obj)
	require.NoError(t, err)
	assert.Equal(t, "default/my-kafka", cluster.Name)
	assert.Equal(t, "kafka.example.com:9092", cluster.BootstrapBrokers)
}

func TestExtractClusterConfig_Fallback(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"metadata": map[string]interface{}{
				"name":      "my-kafka",
				"namespace": "kafka-ns",
			},
		},
	}

	cluster, err := extractClusterConfig(obj)
	require.NoError(t, err)
	assert.Equal(t, "kafka-ns/my-kafka", cluster.Name)
	assert.Equal(t, "my-kafka-kafka-bootstrap.kafka-ns.svc:9092", cluster.BootstrapBrokers)
}

func TestExtractClusterConfig_NoNamespace(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"metadata": map[string]interface{}{
				"name": "my-kafka",
			},
		},
	}

	cluster, err := extractClusterConfig(obj)
	require.NoError(t, err)
	assert.Equal(t, "my-kafka", cluster.Name) // No namespace prefix.
	assert.Equal(t, "my-kafka-kafka-bootstrap..svc:9092", cluster.BootstrapBrokers)
}

func TestExtractClusterConfig_TLSListener(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"metadata": map[string]interface{}{
				"name":      "secure-kafka",
				"namespace": "ns",
			},
			"status": map[string]interface{}{
				"listeners": []interface{}{
					map[string]interface{}{
						"type":             "tls",
						"bootstrapServers": "secure-broker:9093",
					},
				},
			},
		},
	}

	cluster, err := extractClusterConfig(obj)
	require.NoError(t, err)
	assert.Equal(t, "secure-broker:9093", cluster.BootstrapBrokers)
}

func TestExtractClusterConfig_SkipsExternalListener(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"metadata": map[string]interface{}{
				"name":      "kafka",
				"namespace": "ns",
			},
			"status": map[string]interface{}{
				"listeners": []interface{}{
					map[string]interface{}{
						"type":             "external", // Not plain or tls.
						"bootstrapServers": "external:9094",
					},
					map[string]interface{}{
						"type":             "plain",
						"bootstrapServers": "internal:9092",
					},
				},
			},
		},
	}

	cluster, err := extractClusterConfig(obj)
	require.NoError(t, err)
	assert.Equal(t, "internal:9092", cluster.BootstrapBrokers)
}

func TestExtractClusterConfig_TrailingComma(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"metadata": map[string]interface{}{
				"name":      "kafka",
				"namespace": "ns",
			},
			"status": map[string]interface{}{
				"listeners": []interface{}{
					map[string]interface{}{
						"type":             "plain",
						"bootstrapServers": "broker1:9092,broker2:9092,",
					},
				},
			},
		},
	}

	cluster, err := extractClusterConfig(obj)
	require.NoError(t, err)
	assert.Equal(t, "broker1:9092,broker2:9092", cluster.BootstrapBrokers) // Trailing comma stripped.
}

func TestExtractClusterConfig_EmptyListeners(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"metadata": map[string]interface{}{
				"name":      "kafka",
				"namespace": "ns",
			},
			"status": map[string]interface{}{
				"listeners": []interface{}{},
			},
		},
	}

	cluster, err := extractClusterConfig(obj)
	require.NoError(t, err)
	// Falls back to service name.
	assert.Equal(t, "kafka-kafka-bootstrap.ns.svc:9092", cluster.BootstrapBrokers)
}

func TestExtractClusterConfig_AddressesPrioritizedOverBootstrapServers(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"metadata": map[string]interface{}{
				"name":      "kafka",
				"namespace": "ns",
			},
			"status": map[string]interface{}{
				"listeners": []interface{}{
					map[string]interface{}{
						"type": "plain",
						"addresses": []interface{}{
							map[string]interface{}{
								"host": "addr-host",
								"port": float64(9092),
							},
						},
						"bootstrapServers": "bs-host:9092",
					},
				},
			},
		},
	}

	cluster, err := extractClusterConfig(obj)
	require.NoError(t, err)
	// addresses are checked first.
	assert.Equal(t, "addr-host:9092", cluster.BootstrapBrokers)
}

// --- handleEvent tests using fake k8s watcher ------------------------------

func newFakeWatcher(t *testing.T) (*StrimziWatcher, *watch.FakeWatcher) {
	t.Helper()

	scheme := runtime.NewScheme()
	fakeClient := fakedynamic.NewSimpleDynamicClient(scheme)

	fw := watch.NewFake()
	fakeClient.PrependWatchReactor("kafkas", k8stesting.DefaultWatchReactor(fw, nil))

	w := NewStrimziWatcherFromClient(fakeClient, slog.Default())
	return w, fw
}

func TestStrimziWatcher_HandleEvent_Added(t *testing.T) {
	w, fw := newFakeWatcher(t)
	defer w.Stop()

	kafkaObj := &unstructured.Unstructured{}
	kafkaObj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "kafka.strimzi.io",
		Version: "v1beta2",
		Kind:    "Kafka",
	})
	kafkaObj.SetName("test-kafka")
	kafkaObj.SetNamespace("test-ns")
	kafkaObj.Object["status"] = map[string]interface{}{
		"listeners": []interface{}{
			map[string]interface{}{
				"type":             "plain",
				"bootstrapServers": "broker:9092",
			},
		},
	}

	fw.Add(kafkaObj)

	select {
	case event := <-w.Events():
		assert.Equal(t, ClusterAdded, event.Type)
		assert.Equal(t, "test-ns/test-kafka", event.Cluster.Name)
		assert.Equal(t, "broker:9092", event.Cluster.BootstrapBrokers)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Added event")
	}
}

func TestStrimziWatcher_HandleEvent_Modified(t *testing.T) {
	w, fw := newFakeWatcher(t)
	defer w.Stop()

	kafkaObj := &unstructured.Unstructured{}
	kafkaObj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "kafka.strimzi.io",
		Version: "v1beta2",
		Kind:    "Kafka",
	})
	kafkaObj.SetName("test-kafka")
	kafkaObj.SetNamespace("test-ns")
	kafkaObj.Object["status"] = map[string]interface{}{
		"listeners": []interface{}{
			map[string]interface{}{
				"type":             "plain",
				"bootstrapServers": "broker:9092",
			},
		},
	}
	kafkaObj.SetResourceVersion("1")

	fw.Modify(kafkaObj)

	select {
	case event := <-w.Events():
		assert.Equal(t, ClusterAdded, event.Type) // Modified → ClusterAdded.
		assert.Equal(t, "test-ns/test-kafka", event.Cluster.Name)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Modified event")
	}
}

func TestStrimziWatcher_HandleEvent_Deleted(t *testing.T) {
	w, fw := newFakeWatcher(t)
	defer w.Stop()

	kafkaObj := &unstructured.Unstructured{}
	kafkaObj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "kafka.strimzi.io",
		Version: "v1beta2",
		Kind:    "Kafka",
	})
	kafkaObj.SetName("removed-kafka")
	kafkaObj.SetNamespace("ns")

	fw.Delete(kafkaObj)

	select {
	case event := <-w.Events():
		assert.Equal(t, ClusterRemoved, event.Type)
		assert.Equal(t, "ns/removed-kafka", event.Cluster.Name)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Deleted event")
	}
}

func TestStrimziWatcher_HandleEvent_InvalidObject(t *testing.T) {
	// Directly test handleEvent with a non-Unstructured object.
	w := &StrimziWatcher{
		events: make(chan ClusterEvent, 1),
		logger: slog.Default(),
	}

	// Send a watch.Event with a metav1.Status instead of an Unstructured.
	w.handleEvent(context.Background(), watch.Event{
		Type:   watch.Added,
		Object: &metav1.Status{Message: "test"},
	})

	// Should not emit an event.
	select {
	case event := <-w.events:
		t.Fatalf("should not have received event, got: %+v", event)
	default:
	}
}

func TestStrimziWatcher_Stop(t *testing.T) {
	w, _ := newFakeWatcher(t)
	w.Stop()

	// Events channel should be closed by the run goroutine after context cancellation.
	select {
	case _, ok := <-w.Events():
		assert.False(t, ok)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for events channel to close")
	}
}
