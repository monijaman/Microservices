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
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/segmentio/kafka-go"
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
	kafkaWriter *kafka.Writer
	failureRate float64
}

func main() {
	dbURL := mustEnv("DATABASE_URL")
	kafkaBroker := mustEnv("KAFKA_BROKER")
	failureRate, err := strconv.ParseFloat(envOr("PAYMENT_FAILURE_RATE", "0.3"), 64)
	if err != nil {
		log.Fatalf("invalid PAYMENT_FAILURE_RATE: %v", err)
	}

	db := connectWithRetry(dbURL)
	defer db.Close()
	mustMigrate(db)

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBroker),
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	svc := &service{db: db, kafkaWriter: writer, failureRate: failureRate}

	log.Printf("payment-service started (simulated failure rate: %.0f%%), waiting for events...", failureRate*100)
	svc.consumeInventoryEvents(kafkaBroker)
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

func (s *service) consumeInventoryEvents(broker string) {
	reader := newReader(broker, topicInventoryEvents, "payment-service")
	defer reader.Close()

	for {
		event, ok := readEvent(reader)
		if !ok {
			continue
		}
		if event.EventType != eventInventoryReserved {
			continue
		}

		var p inventoryReservedPayload
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			log.Printf("bad InventoryReserved payload: %v", err)
			continue
		}

		s.charge(p.OrderID, p.Item, p.Quantity)
	}
}

func (s *service) charge(orderID, item string, quantity int) {
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

	if _, err := s.db.Exec(
		`INSERT INTO payments (id, order_id, amount, status) VALUES ($1, $2, $3, $4)`,
		paymentID, orderID, amount, status,
	); err != nil {
		log.Printf("failed to save payment for order %s: %v", orderID, err)
		return
	}

	if succeeded {
		s.publishCompleted(orderID, paymentID, amount)
		log.Printf("order %s: payment COMPLETED (amount=%.2f)", orderID, amount)
	} else {
		s.publishFailed(orderID, "simulated payment decline")
		log.Printf("order %s: payment FAILED (simulated decline)", orderID)
	}
}

func (s *service) publishCompleted(orderID, paymentID string, amount float64) {
	payload, _ := json.Marshal(paymentCompletedPayload{OrderID: orderID, PaymentID: paymentID, Amount: amount})
	s.publish(orderID, eventPaymentCompleted, payload)
}

func (s *service) publishFailed(orderID, reason string) {
	payload, _ := json.Marshal(paymentFailedPayload{OrderID: orderID, Reason: reason})
	s.publish(orderID, eventPaymentFailed, payload)
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
		Topic: topicPaymentEvents,
		Key:   []byte(orderID),
		Value: body,
	}); err != nil {
		log.Printf("failed to publish %s for order %s: %v", eventType, orderID, err)
	}
}
