package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing required environment variable: %s", key)
	}
	return v
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func connectWithRetry(dbURL string) *sql.DB {
	var db *sql.DB
	var err error

	for attempt := 1; attempt <= 20; attempt++ {
		db, err = sql.Open("pgx", dbURL)
		if err == nil {
			if pingErr := db.Ping(); pingErr == nil {
				log.Println("connected to database")
				return db
			}
			err = db.Ping()
		}
		log.Printf("database not ready yet (attempt %d/20): %v", attempt, err)
		time.Sleep(2 * time.Second)
	}
	log.Fatalf("could not connect to database: %v", err)
	return nil
}

func newReader(broker, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
}

// ensureTopics makes sure every topic exists with at least topicPartitions
// partitions. Left to Kafka's auto-create, a topic gets just 1 partition,
// which means kafka.Hash has only one lane to choose from and a consumer
// group can never have more than one active reader.
//
// It's safe to call on every startup and from every service at once:
// "already exists" is ignored, and partitions are only ever added, never
// removed (Kafka doesn't allow shrinking a topic).
func ensureTopics(broker string, topics ...string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &kafka.Client{Addr: kafka.TCP(broker)}

	configs := make([]kafka.TopicConfig, len(topics))
	for i, topic := range topics {
		configs[i] = kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     topicPartitions,
			ReplicationFactor: 1, // single-node dev cluster
		}
	}
	created, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: configs})
	if err != nil {
		log.Fatalf("create topics: %v", err)
	}
	for topic, err := range created.Errors {
		if err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
			log.Fatalf("create topic %s: %v", topic, err)
		}
	}

	// A topic that already existed (e.g. auto-created earlier with 1
	// partition) keeps its old count, so grow it to the target.
	meta, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: topics})
	if err != nil {
		log.Fatalf("read topic metadata: %v", err)
	}
	var grow []kafka.TopicPartitionsConfig
	for _, t := range meta.Topics {
		if len(t.Partitions) < topicPartitions {
			grow = append(grow, kafka.TopicPartitionsConfig{Name: t.Name, Count: topicPartitions})
		}
	}
	if len(grow) == 0 {
		return
	}
	res, err := client.CreatePartitions(ctx, &kafka.CreatePartitionsRequest{Topics: grow})
	if err != nil {
		log.Fatalf("add partitions: %v", err)
	}
	for topic, err := range res.Errors {
		// InvalidPartitionNumber here means another service grew the
		// topic between our Metadata call and this one — that's fine.
		if err != nil && !errors.Is(err, kafka.InvalidPartitionNumber) {
			log.Fatalf("add partitions to %s: %v", topic, err)
		}
		if err == nil {
			log.Printf("topic %s grown to %d partitions", topic, topicPartitions)
		}
	}
}
