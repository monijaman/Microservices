package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

// mustEnv reads a required environment variable or crashes with a clear
// message. Failing fast at startup beats a confusing nil-pointer panic
// three files later.
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

// connectWithRetry opens the database connection and retries for a while
// instead of crashing immediately. This matters because docker-compose
// starts containers in parallel: Postgres might report "healthy" and this
// service might still lose the very first connection race. In Kubernetes
// this same problem is normally solved with readiness/liveness probes
// (see MICROSERVICES_PRACTICE.md section 34.5) — here we handle it in code.
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

// newReader creates a Kafka consumer for one topic, belonging to the given
// consumer group. All instances of the same service should use the same
// groupID: Kafka then spreads the topic's partitions across whichever
// instances are alive, so scaling this service to 3 replicas gets you free
// load-balancing with no code changes (see section 9, Consumer Groups).
func newReader(broker, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
}

// readEvent blocks until the next message arrives, decodes it as an Event,
// and returns it. Returning ok=false means "something went wrong, but keep
// the consumer loop running" (e.g. malformed JSON from a bad producer) —
// we log and move on rather than crashing the whole service over one bad
// message.
//
// Committing the Kafka offset happens automatically as part of FetchMessage
// + ReadMessage in kafka-go's default mode: by the time this function
// returns, the message is considered "processed". That's an "at-most-once
// per crash" trade-off; see section 11 (Idempotency) and section 28
// (Delivery Semantics) for why real systems need an idempotent-consumer
// safety net on top of this.
func readEvent(reader *kafka.Reader) (Event, bool) {
	msg, err := reader.ReadMessage(context.Background())
	if err != nil {
		log.Printf("kafka read error on topic %s: %v", reader.Config().Topic, err)
		return Event{}, false
	}

	var event Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("failed to decode event from topic %s: %v", reader.Config().Topic, err)
		return Event{}, false
	}
	return event, true
}
