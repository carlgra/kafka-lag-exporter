package watcher

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"time"

	"github.com/seglo/kafka-lag-exporter/internal/config"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

var kafkaGVR = schema.GroupVersionResource{
	Group:    "kafka.strimzi.io",
	Version:  "v1beta2",
	Resource: "kafkas",
}

// StrimziWatcher watches Strimzi Kafka CRDs and emits ClusterEvents.
type StrimziWatcher struct {
	client    dynamic.Interface
	namespace string
	events    chan ClusterEvent
	cancel    context.CancelFunc
	logger    *slog.Logger
}

// NewStrimziWatcher creates a new Strimzi CRD watcher.
// It requires in-cluster Kubernetes configuration.
//
// If namespace is empty, the watch is cluster-scoped and the ServiceAccount
// must have a ClusterRole granting watch on kafkas.kafka.strimzi.io. If
// namespace is non-empty, the watch is restricted to that namespace and a
// namespace-scoped Role/RoleBinding is sufficient.
func NewStrimziWatcher(namespace string, logger *slog.Logger) (*StrimziWatcher, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("getting in-cluster config: %w", err)
	}

	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating dynamic client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	w := &StrimziWatcher{
		client:    client,
		namespace: namespace,
		events:    make(chan ClusterEvent, 16),
		cancel:    cancel,
		logger:    logger,
	}

	go w.run(ctx)
	return w, nil
}

// NewStrimziWatcherFromClient creates a watcher with a provided dynamic client (for testing).
func NewStrimziWatcherFromClient(client dynamic.Interface, namespace string, logger *slog.Logger) *StrimziWatcher {
	ctx, cancel := context.WithCancel(context.Background())
	w := &StrimziWatcher{
		client:    client,
		namespace: namespace,
		events:    make(chan ClusterEvent, 16),
		cancel:    cancel,
		logger:    logger,
	}
	go w.run(ctx)
	return w
}

func (w *StrimziWatcher) Events() <-chan ClusterEvent {
	return w.events
}

func (w *StrimziWatcher) Stop() {
	w.cancel()
}

func (w *StrimziWatcher) run(ctx context.Context) {
	defer close(w.events)
	for {
		if err := w.watch(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			delay := 5*time.Second + time.Duration(rand.Int63n(int64(2*time.Second)))
			w.logger.Error("strimzi watch error, retrying", "error", err, "delay", delay)
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}
	}
}

func (w *StrimziWatcher) watch(ctx context.Context) error {
	// Empty namespace = cluster-scoped watch (requires ClusterRole).
	// Non-empty = namespace-scoped watch (Role in that namespace suffices).
	resource := w.client.Resource(kafkaGVR)
	var watcher watch.Interface
	var err error
	if w.namespace == "" {
		watcher, err = resource.Watch(ctx, metav1.ListOptions{})
	} else {
		watcher, err = resource.Namespace(w.namespace).Watch(ctx, metav1.ListOptions{})
	}
	if err != nil {
		return fmt.Errorf("starting watch: %w", err)
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return fmt.Errorf("watch channel closed")
			}
			w.handleEvent(ctx, event)
		}
	}
}

func (w *StrimziWatcher) handleEvent(ctx context.Context, event watch.Event) {
	obj, ok := event.Object.(*unstructured.Unstructured)
	if !ok {
		w.logger.Warn("unexpected object type in watch event")
		return
	}

	cluster, err := extractClusterConfig(obj)
	if err != nil {
		w.logger.Warn("failed to extract cluster config", "error", err)
		return
	}

	var ce ClusterEvent
	switch event.Type {
	case watch.Added, watch.Modified:
		w.logger.Info("cluster added/modified", "cluster", cluster.Name)
		ce = ClusterEvent{Type: ClusterAdded, Cluster: cluster}
	case watch.Deleted:
		w.logger.Info("cluster removed", "cluster", cluster.Name)
		ce = ClusterEvent{Type: ClusterRemoved, Cluster: cluster}
	default:
		return
	}

	select {
	case w.events <- ce:
	case <-ctx.Done():
	}
}

// extractClusterConfig extracts a ClusterConfig from a Strimzi Kafka CRD.
func extractClusterConfig(obj *unstructured.Unstructured) (config.ClusterConfig, error) {
	name := obj.GetName()
	namespace := obj.GetNamespace()

	// Extract bootstrap server from status.
	bootstrapServers := ""
	status, found, _ := unstructured.NestedMap(obj.Object, "status")
	if found {
		listeners, found, _ := unstructured.NestedSlice(status, "listeners")
		if found {
			bootstrapServers = pickListenerBootstrap(listeners)
		}
	}

	// Fall back to conventional service name.
	if bootstrapServers == "" {
		bootstrapServers = fmt.Sprintf("%s-kafka-bootstrap.%s.svc.cluster.local:9092", name, namespace)
	}

	clusterName := name
	if namespace != "" {
		clusterName = fmt.Sprintf("%s/%s", namespace, name)
	}

	// Remove trailing comma if present.
	bootstrapServers = strings.TrimRight(bootstrapServers, ",")

	return config.ClusterConfig{
		Name:             clusterName,
		BootstrapBrokers: bootstrapServers,
	}, nil
}

// pickListenerBootstrap selects the best in-cluster listener from the Strimzi
// status.listeners slice and returns its bootstrap address.
//
// In modern Strimzi v1beta2 the listener `name` field carries values like
// "plain"/"tls" while `type` identifies the Kubernetes exposure mechanism
// ("internal", "route", "loadbalancer", "nodeport", "ingress", "cluster-ip").
// Older Strimzi versions used `type` for "plain"/"tls" directly. We prefer
// matching by `name` and fall back to `type` for backward compatibility.
//
// Only listeners with an empty `type` (legacy) or `type == "internal"` are
// considered, since the exporter runs in-cluster and should not attempt to
// reach external listener addresses.
//
// Within the matched listener, `bootstrapServers` is preferred over
// `addresses[0]`: the former is the authoritative Strimzi-advertised bootstrap
// string, while the latter may contain a single address that is not suitable
// for bootstrap connections.
func pickListenerBootstrap(listeners []interface{}) string {
	// Priority: name=="plain", name=="tls", type=="plain" (legacy), type=="tls" (legacy).
	type candidate struct {
		priority int
		listener map[string]interface{}
	}
	best := candidate{priority: 0}

	for _, l := range listeners {
		listener, ok := l.(map[string]interface{})
		if !ok {
			continue
		}
		lname, _ := listener["name"].(string)
		ltype, _ := listener["type"].(string)

		// Skip external exposure types — exporter runs in-cluster.
		// Allowed types: empty (legacy), "internal" (modern v1beta2), or the
		// legacy values "plain"/"tls" which some older Strimzi versions used
		// in the type field.
		if ltype != "" && ltype != "internal" && ltype != "plain" && ltype != "tls" {
			continue
		}

		prio := 0
		switch {
		case lname == "plain":
			prio = 4
		case lname == "tls":
			prio = 3
		case lname == "" && ltype == "plain":
			prio = 2
		case lname == "" && ltype == "tls":
			prio = 1
		default:
			continue
		}

		if prio > best.priority {
			best = candidate{priority: prio, listener: listener}
		}
	}

	if best.listener == nil {
		return ""
	}

	if bs, ok := best.listener["bootstrapServers"].(string); ok && bs != "" {
		return bs
	}
	if addrs, ok := best.listener["addresses"].([]interface{}); ok && len(addrs) > 0 {
		if addr, ok := addrs[0].(map[string]interface{}); ok {
			host, _ := addr["host"].(string)
			port, _ := addr["port"].(float64)
			if host != "" && port > 0 {
				return fmt.Sprintf("%s:%d", host, int(port))
			}
		}
	}
	return ""
}
