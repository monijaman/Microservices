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
	"fmt"
	"log"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type service struct {
	db *sql.DB
}

// Consumer group IDs. Each also names that consumer's dead-letter topic
// (see dlqTopic).
//
// Note: the group IDs include the topic name. A Kafka consumer group's
// partition assignment is computed per topic subscription, so two readers
// in the same service that subscribe to *different* topics must NOT share
// a group ID — if they did, Kafka would treat them as two members of one
// group and split/confuse the assignment between them instead of giving
// each reader all the partitions it needs.
const (
	groupOrderEvents   = "inventory-service-order-events"
	groupPaymentEvents = "inventory-service-payment-events"
)

func main() {
	dbURL := mustEnv("DATABASE_URL")
	kafkaBroker := mustEnv("KAFKA_BROKER")
	ensureTopics(kafkaBroker, topicOrderEvents, topicInventoryEvents, topicPaymentEvents,
		dlqTopic(groupOrderEvents), dlqTopic(groupPaymentEvents))

	db := connectWithRetry(dbURL)
	defer db.Close()
	mustMigrate(db)
	migrateMessaging(db)

	writer := newWriter(kafkaBroker)
	defer writer.Close()

	svc := &service{db: db}

	go runOutboxRelay(db, writer)

	log.Println("inventory-service started, waiting for events...")

	// The payment consumer runs in the background; the order consumer runs
	// on the main goroutine so the process stays alive.
	go consume(db, writer, kafkaBroker, topicPaymentEvents, groupPaymentEvents, svc.handlePaymentEvent)
	consume(db, writer, kafkaBroker, topicOrderEvents, groupOrderEvents, svc.handleOrderEvent)
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

// handleOrderEvent is the main Saga step this service performs. Like all
// handlers it runs inside a DB transaction opened by consume() (see
// messaging.go): returning an error rolls it back and the event is retried,
// then parked on the DLQ if it keeps failing.
func (s *service) handleOrderEvent(ctx context.Context, tx *sql.Tx, event Event) error {
	if event.EventType != eventOrderCreated {
		return nil
	}

	var p orderCreatedPayload
	if err := json.Unmarshal(event.Payload, &p); err != nil {
		return permanent(fmt.Errorf("bad OrderCreated payload: %w", err))
	}
	return s.tryReserve(ctx, tx, p.OrderID, p.Item, p.Quantity)
}

// tryReserve attempts to reserve stock inside tx. "SELECT ... FOR UPDATE"
// locks the inventory row so that if two orders for the same item arrive
// at (almost) the same time, they're checked and decremented one after
// another instead of both reading the same "stock available" number and
// overselling.
//
// The resulting event goes into the outbox in the same transaction, so the
// stock change and the event announcing it are saved together or not at all.
func (s *service) tryReserve(ctx context.Context, tx *sql.Tx, orderID, item string, quantity int) error {
	var available int
	err := tx.QueryRowContext(ctx,
		`SELECT available_quantity FROM inventory WHERE item = $1 FOR UPDATE`,
		item,
	).Scan(&available)

	if err == sql.ErrNoRows || (err == nil && available < quantity) {
		reason := "unknown item"
		if err == nil {
			reason = "insufficient stock"
		}
		log.Printf("order %s: reservation FAILED (%s)", orderID, reason)
		return enqueueEvent(ctx, tx, topicInventoryEvents, eventInventoryReservationFailed, orderID,
			inventoryReservationFailedPayload{OrderID: orderID, Reason: reason})
	}
	if err != nil {
		return fmt.Errorf("check stock: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE inventory SET available_quantity = available_quantity - $1 WHERE item = $2`,
		quantity, item,
	); err != nil {
		return fmt.Errorf("decrement stock: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO reservations (order_id, item, quantity) VALUES ($1, $2, $3)`,
		orderID, item, quantity,
	); err != nil {
		return fmt.Errorf("insert reservation: %w", err)
	}
	if err := enqueueEvent(ctx, tx, topicInventoryEvents, eventInventoryReserved, orderID,
		inventoryReservedPayload{OrderID: orderID, Item: item, Quantity: quantity}); err != nil {
		return err
	}

	log.Printf("order %s: reserved %d x %s", orderID, quantity, item)
	return nil
}

// handlePaymentEvent listens for PaymentFailed so it can undo a
// reservation it made earlier — the Saga compensating action.
func (s *service) handlePaymentEvent(ctx context.Context, tx *sql.Tx, event Event) error {
	if event.EventType != eventPaymentFailed {
		return nil
	}

	var p paymentFailedPayload
	if err := json.Unmarshal(event.Payload, &p); err != nil {
		return permanent(fmt.Errorf("bad PaymentFailed payload: %w", err))
	}
	return s.release(ctx, tx, p.OrderID)
}

func (s *service) release(ctx context.Context, tx *sql.Tx, orderID string) error {
	var item string
	var quantity int
	err := tx.QueryRowContext(ctx,
		`DELETE FROM reservations WHERE order_id = $1 RETURNING item, quantity`,
		orderID,
	).Scan(&item, &quantity)
	if err == sql.ErrNoRows {
		// Nothing was ever reserved for this order (e.g. the reservation
		// itself had already failed earlier) — nothing to release.
		return nil
	}
	if err != nil {
		return fmt.Errorf("lookup reservation: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE inventory SET available_quantity = available_quantity + $1 WHERE item = $2`,
		quantity, item,
	); err != nil {
		return fmt.Errorf("restock: %w", err)
	}
	if err := enqueueEvent(ctx, tx, topicInventoryEvents, eventInventoryReleased, orderID,
		inventoryReleasedPayload{OrderID: orderID}); err != nil {
		return err
	}

	log.Printf("order %s: released %d x %s back to stock (payment failed)", orderID, quantity, item)
	return nil
}
