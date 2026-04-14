// Package kafka provides a Kafka client abstraction for fetching consumer group and partition offsets.
package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/seglo/kafka-lag-exporter/internal/config"
	"github.com/seglo/kafka-lag-exporter/internal/domain"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Client abstracts Kafka admin/consumer operations needed by the collector.
type Client interface {
	// GetGroups returns all consumer group names and their topic-partition assignments.
	GetGroups(ctx context.Context) ([]string, []domain.GroupTopicPartition, error)
	// GetGroupOffsets returns committed offsets for all given group-topic-partitions.
	GetGroupOffsets(ctx context.Context, now int64, groups []string, gtps []domain.GroupTopicPartition) (domain.GroupOffsets, error)
	// GetEarliestOffsets returns the earliest offsets for the given topic-partitions.
	GetEarliestOffsets(ctx context.Context, now int64, tps []domain.TopicPartition) (domain.PartitionOffsets, error)
	// GetLatestOffsets returns the latest offsets for the given topic-partitions.
	GetLatestOffsets(ctx context.Context, now int64, tps []domain.TopicPartition) (domain.PartitionOffsets, error)
	// Close shuts down the client.
	Close()
}

// FranzClient implements Client using franz-go.
type FranzClient struct {
	admin   *kadm.Client
	client  *kgo.Client
	logger  *slog.Logger
	metrics *ClientMetrics
}

// NewFranzClient creates a new Kafka client using franz-go for the given cluster config.
func NewFranzClient(clusterCfg config.ClusterConfig, globalCfg *config.Config, logger *slog.Logger) (*FranzClient, error) {
	brokers := strings.Split(clusterCfg.BootstrapBrokers, ",")
	timeout := globalCfg.KafkaClientTimeout()

	// Use kgo.DialTimeout rather than kgo.Dialer so it composes with
	// kgo.DialTLSConfig below — franz-go rejects setting both Dialer and
	// DialTLSConfig simultaneously.
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConnIdleTimeout(timeout),
		kgo.RequestTimeoutOverhead(timeout),
		kgo.DialTimeout(timeout),
	}

	// Merge consumer and admin client properties (they share the same connection in franz-go).
	props := mergeProperties(clusterCfg.ConsumerProperties, clusterCfg.AdminClientProperties)

	// Apply TLS configuration if security.protocol indicates SSL.
	if protocol, ok := props["security.protocol"]; ok {
		upper := strings.ToUpper(protocol)
		if strings.Contains(upper, "SSL") {
			tlsCfg, err := buildTLSConfig(props, logger)
			if err != nil {
				return nil, fmt.Errorf("building TLS config for cluster %s: %w", clusterCfg.Name, err)
			}
			opts = append(opts, kgo.DialTLSConfig(tlsCfg))
			logger.Info("TLS configured", "cluster", clusterCfg.Name)
		}
	}

	// Apply SASL configuration if sasl.mechanism is set.
	if mechanism, ok := props["sasl.mechanism"]; ok {
		saslOpt, err := buildSASLOpt(mechanism, props)
		if err != nil {
			return nil, fmt.Errorf("building SASL config for cluster %s: %w", clusterCfg.Name, err)
		}
		opts = append(opts, saslOpt)
		logger.Info("SASL configured", "cluster", clusterCfg.Name, "mechanism", mechanism)
	}

	clientMetrics := &ClientMetrics{}
	opts = append(opts, kgo.WithHooks(clientMetrics))

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating kafka client for cluster %s: %w", clusterCfg.Name, err)
	}

	admin := kadm.NewClient(client)
	admin.SetTimeoutMillis(int32(timeout / time.Millisecond))

	return &FranzClient{
		admin:   admin,
		client:  client,
		logger:  logger.With("cluster", clusterCfg.Name),
		metrics: clientMetrics,
	}, nil
}

// mergeProperties merges two property maps, with the second taking precedence.
func mergeProperties(a, b map[string]string) map[string]string {
	merged := make(map[string]string)
	for k, v := range a {
		merged[k] = v
	}
	for k, v := range b {
		merged[k] = v
	}
	return merged
}

// buildTLSConfig creates a *tls.Config from Kafka consumer/admin properties.
func buildTLSConfig(props map[string]string, logger *slog.Logger) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// Load CA certificate if provided.
	if caFile, ok := props["ssl.truststore.location"]; ok && caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("reading CA certificate %s: %w", caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate from %s", caFile)
		}
		tlsCfg.RootCAs = pool
		logger.Debug("loaded CA certificate", "file", caFile)
	}

	// Load client certificate and key if provided. Two sets of property names
	// are accepted:
	//   - PEM-style (matches Kafka's ssl.keystore.type=PEM and the original
	//     Scala kafka-lag-exporter): ssl.keystore.certificate.chain +
	//     ssl.keystore.key.
	//   - Legacy convenience keys used by this exporter: ssl.keystore.location
	//     (PEM cert chain) + ssl.key.location (PEM private key).
	// If both are set, the PEM-style keys win and a warning is logged.
	pemCertFile := props["ssl.keystore.certificate.chain"]
	pemKeyFile := props["ssl.keystore.key"]
	legacyCertFile := props["ssl.keystore.location"]
	legacyKeyFile := props["ssl.key.location"]

	var certFile, keyFile string
	switch {
	case pemCertFile != "" && pemKeyFile != "":
		certFile, keyFile = pemCertFile, pemKeyFile
		if legacyCertFile != "" || legacyKeyFile != "" {
			logger.Warn(
				"both PEM (ssl.keystore.certificate.chain/ssl.keystore.key) "+
					"and legacy (ssl.keystore.location/ssl.key.location) client "+
					"cert properties are set; using the PEM-style properties",
				"pem_cert", pemCertFile,
				"pem_key", pemKeyFile,
			)
		}
	case legacyCertFile != "" && legacyKeyFile != "":
		certFile, keyFile = legacyCertFile, legacyKeyFile
	}

	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("loading client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
		logger.Debug("loaded client certificate", "cert", certFile, "key", keyFile)
	}

	// ssl.keystore.type / ssl.truststore.type of PEM are accepted silently —
	// this exporter only supports PEM-format files regardless, so the value is
	// informational. Unknown values are ignored for forward compatibility with
	// Kafka's property namespace.
	_ = props["ssl.keystore.type"]
	_ = props["ssl.truststore.type"]

	return tlsCfg, nil
}

// buildSASLOpt creates a kgo.Opt for SASL authentication.
func buildSASLOpt(mechanism string, props map[string]string) (kgo.Opt, error) {
	username := props["sasl.jaas.config.username"]
	if username == "" {
		username = props["sasl.username"]
	}
	password := props["sasl.jaas.config.password"]
	if password == "" {
		password = props["sasl.password"]
	}

	switch strings.ToUpper(mechanism) {
	case "PLAIN":
		return kgo.SASL(plain.Auth{
			User: username,
			Pass: password,
		}.AsMechanism()), nil
	case "SCRAM-SHA-256":
		return kgo.SASL(scram.Auth{
			User: username,
			Pass: password,
		}.AsSha256Mechanism()), nil
	case "SCRAM-SHA-512":
		return kgo.SASL(scram.Auth{
			User: username,
			Pass: password,
		}.AsSha512Mechanism()), nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", mechanism)
	}
}

func (c *FranzClient) GetGroups(ctx context.Context) ([]string, []domain.GroupTopicPartition, error) {
	groups, err := c.admin.ListGroups(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("listing groups: %w", err)
	}

	groupNames := append([]string{}, groups.Groups()...)

	if len(groupNames) == 0 {
		return nil, nil, nil
	}

	described, err := c.admin.DescribeGroups(ctx, groupNames...)
	if err != nil {
		return nil, nil, fmt.Errorf("describing groups: %w", err)
	}

	var gtps []domain.GroupTopicPartition
	for _, g := range described.Sorted() {
		if g.Err != nil {
			c.logger.Warn("failed to describe group", "group", g.Group, "error", g.Err)
			continue
		}
		for _, m := range g.Members {
			assigned, ok := m.Assigned.AsConsumer()
			if !ok || assigned == nil {
				continue
			}
			for _, t := range assigned.Topics {
				for _, p := range t.Partitions {
					gtps = append(gtps, domain.GroupTopicPartition{
						Group:      g.Group,
						Topic:      t.Topic,
						Partition:  p,
						MemberHost: m.ClientHost,
						ConsumerID: m.MemberID,
						ClientID:   m.ClientID,
					})
				}
			}
		}
	}

	return groupNames, gtps, nil
}

func (c *FranzClient) GetGroupOffsets(ctx context.Context, now int64, groups []string, gtps []domain.GroupTopicPartition) (domain.GroupOffsets, error) {
	offsets := make(domain.GroupOffsets)

	fetched := c.admin.FetchManyOffsets(ctx, groups...)

	// Build a map from group→topic→partition→member info from the described GTPs.
	memberInfo := make(map[string]map[string]map[int32]domain.GroupTopicPartition)
	for _, gtp := range gtps {
		if _, ok := memberInfo[gtp.Group]; !ok {
			memberInfo[gtp.Group] = make(map[string]map[int32]domain.GroupTopicPartition)
		}
		if _, ok := memberInfo[gtp.Group][gtp.Topic]; !ok {
			memberInfo[gtp.Group][gtp.Topic] = make(map[int32]domain.GroupTopicPartition)
		}
		memberInfo[gtp.Group][gtp.Topic][gtp.Partition] = gtp
	}

	for group, listed := range fetched {
		if listed.Err != nil {
			c.logger.Warn("failed to fetch offsets for group", "group", group, "error", listed.Err)
			continue
		}
		for topic, partitions := range listed.Fetched {
			for partition, offsetResp := range partitions {
				if offsetResp.Err != nil {
					continue
				}
				gtp := domain.GroupTopicPartition{
					Group:     group,
					Topic:     topic,
					Partition: partition,
				}
				// Enrich with member info if available.
				if groups, ok := memberInfo[group]; ok {
					if topics, ok := groups[topic]; ok {
						if info, ok := topics[partition]; ok {
							gtp.MemberHost = info.MemberHost
							gtp.ConsumerID = info.ConsumerID
							gtp.ClientID = info.ClientID
						}
					}
				}
				offsets[gtp] = domain.PartitionOffset{
					Offset:    offsetResp.At,
					Timestamp: now,
				}
			}
		}
	}

	return offsets, nil
}

func (c *FranzClient) GetEarliestOffsets(ctx context.Context, now int64, tps []domain.TopicPartition) (domain.PartitionOffsets, error) {
	topicParts := topicPartitionsToKadm(tps)

	listed, err := c.admin.ListStartOffsets(ctx, topicParts...)
	if err != nil {
		return nil, fmt.Errorf("listing earliest offsets: %w", err)
	}

	offsets := make(domain.PartitionOffsets, len(tps))
	listed.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			c.logger.Warn("error listing earliest offset", "topic", o.Topic, "partition", o.Partition, "error", o.Err)
			return
		}
		tp := domain.TopicPartition{Topic: o.Topic, Partition: o.Partition}
		offsets[tp] = domain.PartitionOffset{Offset: o.Offset, Timestamp: now}
	})

	return offsets, nil
}

func (c *FranzClient) GetLatestOffsets(ctx context.Context, now int64, tps []domain.TopicPartition) (domain.PartitionOffsets, error) {
	topicParts := topicPartitionsToKadm(tps)

	listed, err := c.admin.ListEndOffsets(ctx, topicParts...)
	if err != nil {
		return nil, fmt.Errorf("listing latest offsets: %w", err)
	}

	offsets := make(domain.PartitionOffsets, len(tps))
	listed.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			c.logger.Warn("error listing latest offset", "topic", o.Topic, "partition", o.Partition, "error", o.Err)
			return
		}
		tp := domain.TopicPartition{Topic: o.Topic, Partition: o.Partition}
		offsets[tp] = domain.PartitionOffset{Offset: o.Offset, Timestamp: now}
	})

	return offsets, nil
}

// ClientMetrics returns Kafka client connection statistics.
func (c *FranzClient) ClientMetrics() (connects, disconnects, writeErrors, readErrors int64) {
	return c.metrics.Connects(), c.metrics.Disconnects(), c.metrics.WriteErrors(), c.metrics.ReadErrors()
}

func (c *FranzClient) Close() {
	c.client.Close()
}

// topicPartitionsToKadm converts domain TopicPartitions to kadm topic-partition strings.
func topicPartitionsToKadm(tps []domain.TopicPartition) []string {
	seen := make(map[string]bool)
	var topics []string
	for _, tp := range tps {
		if !seen[tp.Topic] {
			seen[tp.Topic] = true
			topics = append(topics, tp.Topic)
		}
	}
	return topics
}
