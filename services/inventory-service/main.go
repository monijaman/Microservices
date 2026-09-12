// Inventory Service
// =================
// Reacts to OrderCreated:
//   - if there's enough stock, reserve it and publish InventoryReserved
//   - otherwise publish InventoryReservationFailed
//
// Also reacts to PaymentFailed by releasing whatever stock it reserved for
// that order. This is the Saga *compensation* step: since there's no
// distributed transaction spanning Inventory + Payment + Order, the only
// way to "undo" a reservation after the fact is to run another, explicit
// action that reverses it. See MICROSERVICES_PRACTICE.md section 15.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/segmentio/kafka-go"
)

type service struct {
	db          *sql.DB
	kafkaWriter *kafka.Writer
}

func main() {
	dbURL := mustEnv("DATABASE_URL")
	kafkaBroker := mustEnv("KAFKA_BROKER")

	db := connectWithRetry(dbURL)
	defer db.Close()
	mustMigrate(db)

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBroker),
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	svc := &service{db: db, kafkaWriter: writer}

	log.Println("inventory-service started, waiting for events...")

	// consumePaymentEvents runs in the background; consumeOrderEvents runs
	// on the main goroutine so the process stays alive.
	go svc.consumePaymentEvents(kafkaBroker)
	svc.consumeOrderEvents(kafkaBroker)
}

func mustMigrate(db *sql.DB) {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS inventory (
			item TEXT PRIMARY KEY,
			available_quantity INT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS reservations (
			order_id UUID PRIMARY KEY,
			item TEXT NOT NULL,
			quantity INT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		-- Seed a couple of products so there's something to reserve
		-- against. ON CONFLICT DO NOTHING makes this safe to run every
		-- time the service restarts.
		INSERT INTO inventory (item, available_quantity) VALUES
			('widget', 100),
			('gadget', 50)
		ON CONFLICT (item) DO NOTHING;
	`)
	if err != nil {
		log.Fatalf("migration failed: %v", err)
	}
}

// consumeOrderEvents is the main Saga step this service performs.
func (s *service) consumeOrderEvents(broker string) {
	// Note: the group ID includes the topic name. A Kafka consumer group's
	// partition assignment is computed per topic subscription, so two
	// readers in the same service that subscribe to *different* topics
	// must NOT share a group ID — if they did, Kafka would treat them as
	// two members of one group and split/confuse the assignment between
	// them instead of giving each reader all the partitions it needs.
	reader := newReader(broker, topicOrderEvents, "inventory-service-order-events")
	defer reader.Close()

	for {
		event, ok := readEvent(reader)
		if !ok {
			continue
		}
		if event.EventType != eventOrderCreated {
			continue
		}

		var p orderCreatedPayload
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			log.Printf("bad OrderCreated payload: %v", err)
			continue
		}

		s.tryReserve(p.OrderID, p.Item, p.Quantity)
	}
}

// tryReserve attempts to reserve stock inside a single DB transaction.
// "SELECT ... FOR UPDATE" locks the inventory row so that if two orders for
// the same item arrive at (almost) the same time, they're checked and
// decremented one after another instead of both reading the same "stock
// available" number and overselling.
func (s *service) tryReserve(orderID, item string, quantity int) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("begin tx failed: %v", err)
		return
	}
	defer tx.Rollback() // no-op if we already committed

	var available int
	err = tx.QueryRowContext(ctx,
		`SELECT available_quantity FROM inventory WHERE item = $1 FOR UPDATE`,
		item,
	).Scan(&available)

	if err == sql.ErrNoRows || (err == nil && available < quantity) {
		reason := "unknown item"
		if err == nil {
			reason = "insufficient stock"
		}
		tx.Rollback()
		s.publishReservationFailed(orderID, reason)
		log.Printf("order %s: reservation FAILED (%s)", orderID, reason)
		return
	}
	if err != nil {
		log.Printf("check stock failed: %v", err)
		return
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE inventory SET available_quantity = available_quantity - $1 WHERE item = $2`,
		quantity, item,
	); err != nil {
		log.Printf("decrement stock failed: %v", err)
		return
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO reservations (order_id, item, quantity) VALUES ($1, $2, $3)`,
		orderID, item, quantity,
	); err != nil {
		log.Printf("insert reservation failed: %v", err)
		return
	}
	if err := tx.Commit(); err != nil {
		log.Printf("commit reservation failed: %v", err)
		return
	}

	s.publishReserved(orderID, item, quantity)
	log.Printf("order %s: reserved %d x %s", orderID, quantity, item)
}

// consumePaymentEvents listens for PaymentFailed so it can undo a
// reservation it made earlier — the Saga compensating action.
func (s *service) consumePaymentEvents(broker string) {
	reader := newReader(broker, topicPaymentEvents, "inventory-service-payment-events")
	defer reader.Close()

	for {
		event, ok := readEvent(reader)
		if !ok {
			continue
		}
		if event.EventType != eventPaymentFailed {
			continue
		}

		var p paymentFailedPayload
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			log.Printf("bad PaymentFailed payload: %v", err)
			continue
		}
		s.release(p.OrderID)
	}
}

func (s *service) release(orderID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("begin tx failed: %v", err)
		return
	}
	defer tx.Rollback()

	var item string
	var quantity int
	err = tx.QueryRowContext(ctx,
		`DELETE FROM reservations WHERE order_id = $1 RETURNING item, quantity`,
		orderID,
	).Scan(&item, &quantity)
	if err == sql.ErrNoRows {
		// Nothing was ever reserved for this order (e.g. the reservation
		// itself had already failed earlier) — nothing to release.
		return
	}
	if err != nil {
		log.Printf("lookup reservation failed: %v", err)
		return
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE inventory SET available_quantity = available_quantity + $1 WHERE item = $2`,
		quantity, item,
	); err != nil {
		log.Printf("restock failed: %v", err)
		return
	}
	if err := tx.Commit(); err != nil {
		log.Printf("commit release failed: %v", err)
		return
	}

	s.publishReleased(orderID)
	log.Printf("order %s: released %d x %s back to stock (payment failed)", orderID, quantity, item)
}

// --- Publishing helpers -----------------------------------------------

func (s *service) publishReserved(orderID, item string, quantity int) {
	payload, _ := json.Marshal(inventoryReservedPayload{OrderID: orderID, Item: item, Quantity: quantity})
	s.publish(orderID, eventInventoryReserved, payload)
}

func (s *service) publishReservationFailed(orderID, reason string) {
	payload, _ := json.Marshal(inventoryReservationFailedPayload{OrderID: orderID, Reason: reason})
	s.publish(orderID, eventInventoryReservationFailed, payload)
}

func (s *service) publishReleased(orderID string) {
	payload, _ := json.Marshal(inventoryReleasedPayload{OrderID: orderID})
	s.publish(orderID, eventInventoryReleased, payload)
}

func (s *service) publish(orderID, eventType string, payload []byte) {
	event := Event{
		EventID:     uuid.NewString(),
		EventType:   eventType,
		AggregateID: orderID,
		Timestamp:   time.Now().UTC(),
		Payload:     payload,
	}
	body, _ := json.Marshal(event)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.kafkaWriter.WriteMessages(ctx, kafka.Message{
		Topic: topicInventoryEvents,
		Key:   []byte(orderID), // same key as OrderCreated -> same partition -> stays in order
		Value: body,
	}); err != nil {
		log.Printf("failed to publish %s for order %s: %v", eventType, orderID, err)
	}
}
