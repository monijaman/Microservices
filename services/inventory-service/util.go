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

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing required environment variable: %s", key)
	}
	return v
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
