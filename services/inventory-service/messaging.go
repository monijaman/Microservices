package main

// Reliable messaging, used the same way by every service:
//
//   - Transactional outbox: an event is written to the "outbox" table in
//     the SAME database transaction as the change it describes, and a
//     background relay publishes it to Kafka afterwards. A change can
//     therefore never be saved without its event (or vice versa), even if
//     Kafka is down at that moment — the relay just keeps retrying.
//   - Idempotent consumer: the ID of every handled event is recorded in
//     "processed_events" in the same transaction as the event's effects.
//     Kafka delivers at-least-once (a message can arrive twice after a
//     crash or a retry), and this makes the second delivery a no-op.
//   - Retry + dead-letter queue (DLQ): a handler that fails is retried a
//     few times with backoff. If it still fails, the message is parked on
//     "<group>.dlq" with headers saying why, so one bad message can't
//     block its partition forever and nothing is silently dropped.
//
// See MICROSERVICES_PRACTICE.md section 17 (Transactional Outbox) and
// section 11 (Idempotency).

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

const (
	maxHandleAttempts  = 3
	outboxBatchSize    = 100
	outboxPollInterval = 500 * time.Millisecond
)

// newWriter creates the Kafka producer. RequireAll makes WriteMessages
// wait until Kafka has actually stored the message (on every in-sync
// replica) before returning success. kafka-go's default is RequireNone,
// which is fire-and-forget: a lost message would never report an error.
func newWriter(broker string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Balancer:     &kafka.Hash{}, // same key -> same partition -> stays in order
		RequiredAcks: kafka.RequireAll,
		// Topics are created up front by ensureTopics, so a typo'd topic
		// name fails loudly instead of silently creating a new topic.
		AllowAutoTopicCreation: false,
		// The default waits up to 1s to fill a batch before sending. The
		// relay already sends in batches, so flush right away.
		BatchTimeout: 10 * time.Millisecond,
	}
}

func migrateMessaging(db *sql.DB) {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS outbox (
			seq BIGSERIAL PRIMARY KEY,         -- publish order
			event_id UUID NOT NULL UNIQUE,
			topic TEXT NOT NULL,
			key TEXT NOT NULL,
			value BYTEA NOT NULL,              -- the JSON-encoded Event
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			published_at TIMESTAMPTZ
		);
		CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (seq) WHERE published_at IS NULL;

		CREATE TABLE IF NOT EXISTS processed_events (
			event_id UUID PRIMARY KEY,
			processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`)
	if err != nil {
		log.Fatalf("messaging migration failed: %v", err)
	}
}

// --- Producing: transactional outbox ----------------------------------------

// enqueueEvent stores an event in the outbox as part of tx. Nothing is sent
// to Kafka here; runOutboxRelay publishes it once tx has committed. If tx
// rolls back, the event disappears along with the change it described.
func enqueueEvent(ctx context.Context, tx *sql.Tx, topic, eventType, aggregateID string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	event := Event{
		EventID:     uuid.NewString(),
		EventType:   eventType,
		AggregateID: aggregateID,
		Timestamp:   time.Now().UTC(),
		Payload:     body,
	}
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO outbox (event_id, topic, key, value) VALUES ($1, $2, $3, $4)`,
		event.EventID, topic, aggregateID, value,
	)
	return err
}

// runOutboxRelay publishes unpublished outbox rows to Kafka, forever. If
// Kafka is unreachable the rows simply stay unpublished and are retried on
// the next tick, so events are delayed, never lost.
func runOutboxRelay(db *sql.DB, writer *kafka.Writer) {
	for {
		n, err := publishOutboxBatch(db, writer)
		if err != nil {
			log.Printf("outbox relay: %v (will retry)", err)
		}
		if err != nil || n < outboxBatchSize {
			time.Sleep(outboxPollInterval)
		}
	}
}

func publishOutboxBatch(db *sql.DB, writer *kafka.Writer) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	// FOR UPDATE SKIP LOCKED: if several copies of this service are running,
	// each relay grabs different rows instead of publishing the same ones.
	rows, err := tx.QueryContext(ctx, `
		SELECT seq, topic, key, value FROM outbox
		WHERE published_at IS NULL
		ORDER BY seq
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, outboxBatchSize)
	if err != nil {
		return 0, err
	}
	var seqs []int64
	var msgs []kafka.Message
	for rows.Next() {
		var seq int64
		var msg kafka.Message
		var key string
		if err := rows.Scan(&seq, &msg.Topic, &key, &msg.Value); err != nil {
			rows.Close()
			return 0, err
		}
		msg.Key = []byte(key)
		seqs = append(seqs, seq)
		msgs = append(msgs, msg)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(msgs) == 0 {
		return 0, nil
	}

	if err := writer.WriteMessages(ctx, msgs...); err != nil {
		return 0, fmt.Errorf("publish %d events: %w", len(msgs), err)
	}
	// If we crash right here, the rows are published again on restart.
	// That's the at-least-once trade-off; processed_events on the consumer
	// side turns the duplicate into a no-op.
	if _, err := tx.ExecContext(ctx,
		`UPDATE outbox SET published_at = now() WHERE seq = ANY($1)`, seqs,
	); err != nil {
		return 0, err
	}
	return len(msgs), tx.Commit()
}

// --- Consuming: idempotency, retry, dead-letter queue -----------------------

// handlerFunc applies one event's effects inside tx (and may enqueue new
// events into the outbox with the same tx). Returning an error rolls
// everything back and the event is retried; wrap the error with permanent()
// when retrying can't help, e.g. a malformed payload.
type handlerFunc func(ctx context.Context, tx *sql.Tx, event Event) error

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// permanent marks err as not worth retrying: the message goes straight to
// the DLQ.
func permanent(err error) error { return permanentError{err} }

func dlqTopic(groupID string) string { return groupID + ".dlq" }

// consume reads topic as part of groupID and runs handle for every event.
// The offset is committed only after the event was handled or parked on
// the DLQ, so a crash mid-way means the message is redelivered, not lost.
func consume(db *sql.DB, writer *kafka.Writer, broker, topic, groupID string, handle handlerFunc) {
	reader := newReader(broker, topic, groupID)
	defer reader.Close()

	for {
		msg, err := reader.FetchMessage(context.Background())
		if err != nil {
			log.Printf("kafka read error on topic %s: %v", topic, err)
			time.Sleep(time.Second)
			continue
		}

		if err := processWithRetry(db, msg, handle); err != nil {
			sendToDLQ(writer, groupID, msg, err)
		}

		if err := reader.CommitMessages(context.Background(), msg); err != nil {
			// Not fatal: the message will be delivered again and skipped
			// by the processed_events check.
			log.Printf("commit offset on %s/%d@%d failed: %v", msg.Topic, msg.Partition, msg.Offset, err)
		}
	}
}

func processWithRetry(db *sql.DB, msg kafka.Message, handle handlerFunc) error {
	var event Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return permanent(fmt.Errorf("decode event: %w", err))
	}
	if _, err := uuid.Parse(event.EventID); err != nil {
		return permanent(fmt.Errorf("invalid eventId %q: %w", event.EventID, err))
	}

	var err error
	for attempt := 1; attempt <= maxHandleAttempts; attempt++ {
		err = processOnce(db, event, handle)
		if err == nil || errors.As(err, &permanentError{}) {
			return err
		}
		log.Printf("handling %s %s failed (attempt %d/%d): %v",
			event.EventType, event.EventID, attempt, maxHandleAttempts, err)
		if attempt < maxHandleAttempts {
			time.Sleep(time.Duration(attempt) * time.Second) // 1s, 2s, ...
		}
	}
	return err
}

func processOnce(db *sql.DB, event Event, handle handlerFunc) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		event.EventID,
	)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		log.Printf("skipping duplicate %s %s", event.EventType, event.EventID)
		return nil
	}

	if err := handle(ctx, tx, event); err != nil {
		return err
	}
	return tx.Commit()
}

// sendToDLQ parks a message that couldn't be processed on the group's DLQ
// topic, unchanged, with headers saying where it came from and why it
// failed. It keeps retrying until Kafka accepts it: committing the offset
// before the message is safely parked would lose it.
//
// To replay a parked message once the bug is fixed, publish its value back
// to the topic in the "dlq-original-topic" header.
func sendToDLQ(writer *kafka.Writer, groupID string, msg kafka.Message, cause error) {
	dlqMsg := kafka.Message{
		Topic: dlqTopic(groupID),
		Key:   msg.Key,
		Value: msg.Value,
		Headers: []kafka.Header{
			{Key: "dlq-original-topic", Value: []byte(msg.Topic)},
			{Key: "dlq-original-partition", Value: []byte(strconv.Itoa(msg.Partition))},
			{Key: "dlq-original-offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
			{Key: "dlq-error", Value: []byte(cause.Error())},
			{Key: "dlq-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := writer.WriteMessages(ctx, dlqMsg)
		cancel()
		if err == nil {
			log.Printf("moved %s/%d@%d to %s: %v", msg.Topic, msg.Partition, msg.Offset, dlqMsg.Topic, cause)
			return
		}
		log.Printf("writing to %s failed, retrying: %v", dlqMsg.Topic, err)
		time.Sleep(2 * time.Second)
	}
}
