// Payment Service
// ===============
// Reacts to InventoryReserved by "charging" the customer. There's no real
// payment gateway here on purpose (see MICROSERVICES_PRACTICE.md section 5)
// — we simulate success/failure with a configurable random failure rate so
// you can reliably trigger both the happy path and the Saga compensation
// path just by creating enough orders.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// unitPrices simulates a product catalog. A real system would look this up
// from a Product/Pricing service instead of hardcoding it here.
var unitPrices = map[string]float64{
	"widget": 10.0,
	"gadget": 25.0,
}

const defaultUnitPrice = 15.0

type service struct {
	db          *sql.DB
	failureRate float64
}

// Consumer group ID; it also names the dead-letter topic (see dlqTopic).
const groupInventoryEvents = "payment-service-inventory-events"

func main() {
	dbURL := mustEnv("DATABASE_URL")
	kafkaBroker := mustEnv("KAFKA_BROKER")
	ensureTopics(kafkaBroker, topicInventoryEvents, topicPaymentEvents, dlqTopic(groupInventoryEvents))
	failureRate, err := strconv.ParseFloat(envOr("PAYMENT_FAILURE_RATE", "0.3"), 64)
	if err != nil {
		log.Fatalf("invalid PAYMENT_FAILURE_RATE: %v", err)
	}

	db := connectWithRetry(dbURL)
	defer db.Close()
	mustMigrate(db)
	migrateMessaging(db)

	writer := newWriter(kafkaBroker)
	defer writer.Close()

	svc := &service{db: db, failureRate: failureRate}

	go runOutboxRelay(db, writer)

	log.Printf("payment-service started (simulated failure rate: %.0f%%), waiting for events...", failureRate*100)
	consume(db, writer, kafkaBroker, topicInventoryEvents, groupInventoryEvents, svc.handleInventoryEvent)
}

func mustMigrate(db *sql.DB) {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS payments (
			id UUID PRIMARY KEY,
			order_id UUID NOT NULL,
			amount NUMERIC NOT NULL,
			status TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`)
	if err != nil {
		log.Fatalf("migration failed: %v", err)
	}
}

// handleInventoryEvent runs inside a DB transaction opened by consume()
// (see messaging.go): returning an error rolls it back and the event is
// retried, then parked on the DLQ if it keeps failing.
func (s *service) handleInventoryEvent(ctx context.Context, tx *sql.Tx, event Event) error {
	if event.EventType != eventInventoryReserved {
		return nil
	}

	var p inventoryReservedPayload
	if err := json.Unmarshal(event.Payload, &p); err != nil {
		return permanent(fmt.Errorf("bad InventoryReserved payload: %w", err))
	}
	return s.charge(ctx, tx, p.OrderID, p.Item, p.Quantity)
}

// charge records the payment and its outcome event in the same
// transaction, so a saved payment always has its event (and vice versa).
func (s *service) charge(ctx context.Context, tx *sql.Tx, orderID, item string, quantity int) error {
	price, ok := unitPrices[item]
	if !ok {
		price = defaultUnitPrice
	}
	amount := price * float64(quantity)

	// The simulated outcome. rand.Float64() returns [0.0, 1.0); if it
	// lands below the configured failure rate, we treat the charge as
	// failed. With the default 0.3 that's roughly 3 in 10 orders.
	succeeded := rand.Float64() >= s.failureRate

	paymentID := uuid.NewString()
	status := "COMPLETED"
	if !succeeded {
		status = "FAILED"
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO payments (id, order_id, amount, status) VALUES ($1, $2, $3, $4)`,
		paymentID, orderID, amount, status,
	); err != nil {
		return fmt.Errorf("save payment for order %s: %w", orderID, err)
	}

	if !succeeded {
		log.Printf("order %s: payment FAILED (simulated decline)", orderID)
		return enqueueEvent(ctx, tx, topicPaymentEvents, eventPaymentFailed, orderID,
			paymentFailedPayload{OrderID: orderID, Reason: "simulated payment decline"})
	}
	log.Printf("order %s: payment COMPLETED (amount=%.2f)", orderID, amount)
	return enqueueEvent(ctx, tx, topicPaymentEvents, eventPaymentCompleted, orderID,
		paymentCompletedPayload{OrderID: orderID, PaymentID: paymentID, Amount: amount})
}
