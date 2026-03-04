package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	topic      = "demo-topic"
	partitions = 3
	group      = "demo-consumer-group"
)

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("shutting down...")
		cancel()
	}()

	if err := createTopic(ctx, broker); err != nil {
		log.Fatalf("failed to create topic: %v", err)
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		produce(ctx, broker)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		consume(ctx, broker)
	}()

	wg.Wait()
	log.Println("demo harness stopped")
}

func createTopic(ctx context.Context, broker string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)

	// Wait for broker to be ready.
	for i := 0; i < 30; i++ {
		_, err := adm.BrokerMetadata(ctx)
		if err == nil {
			break
		}
		log.Printf("waiting for broker... (%v)", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}

	resp, err := adm.CreateTopics(ctx, int32(partitions), 1, nil, topic)
	if err != nil {
		return fmt.Errorf("create topic request: %w", err)
	}
	for _, t := range resp.Sorted() {
		if t.Err != nil {
			// Topic already exists is fine.
			if t.ErrMessage == "TOPIC_ALREADY_EXISTS" {
				log.Printf("topic %q already exists", topic)
				return nil
			}
			log.Printf("topic %q create error: %v (msg: %s)", t.Topic, t.Err, t.ErrMessage)
		} else {
			log.Printf("created topic %q with %d partitions", t.Topic, partitions)
		}
	}
	return nil
}

func produce(ctx context.Context, broker string) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		log.Fatalf("producer: create client: %v", err)
	}
	defer cl.Close()

	ticker := time.NewTicker(100 * time.Millisecond) // ~10 msg/sec
	defer ticker.Stop()

	seq := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			val := fmt.Sprintf("message-%d @ %s", seq, time.Now().Format(time.RFC3339))
			record := &kgo.Record{Value: []byte(val)}
			cl.Produce(ctx, record, func(r *kgo.Record, err error) {
				if err != nil {
					log.Printf("produce error: %v", err)
					return
				}
				if seq%50 == 0 {
					log.Printf("produced msg %d to %s[%d] offset %d", seq, r.Topic, r.Partition, r.Offset)
				}
			})
			seq++
		}
	}
}

func consume(ctx context.Context, broker string) {
	// Small delay to let some messages accumulate before consuming.
	select {
	case <-ctx.Done():
		return
	case <-time.After(5 * time.Second):
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatalf("consumer: create client: %v", err)
	}
	defer cl.Close()

	consumed := 0
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			_ = iter.Next()
			consumed++

			// Consume slowly: ~2 msg/sec by sleeping 500ms per message.
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}

			if consumed%10 == 0 {
				log.Printf("consumed %d messages (slow consumer, building lag...)", consumed)
				// Commit periodically.
				if err := cl.CommitUncommittedOffsets(ctx); err != nil {
					log.Printf("commit error: %v", err)
				}
			}
		}

		// Commit after each poll batch.
		if err := cl.CommitUncommittedOffsets(ctx); err != nil {
			log.Printf("commit error: %v", err)
		}
	}
}
