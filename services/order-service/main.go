// Order Service
// ==============
// The entry point of the whole system. It:
//  1. Exposes a REST API so a client can create/read orders.
//  2. Publishes OrderCreated onto Kafka to kick off the Saga.
//  3. Listens to Kafka for how the Saga turned out (inventory reserved
//     or not, payment completed or not) and updates the order's status.
//
// This is "choreography": nobody tells Order Service what to do next.
// It just reacts to events published by other services, and those
// services react to events published by it. There is no central
// coordinator (that's what Exercise 8 / the Saga Orchestrator adds later).
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	"github.com/segmentio/kafka-go"
)

// Order mirrors one row of the "orders" table. It's also what we return
// as JSON from the API.
type Order struct {
	ID        string    `json:"id"`
	Item      string    `json:"item"`
	Quantity  int       `json:"quantity"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// Order status values. The order moves through these as Saga events arrive.
const (
	statusPending   = "PENDING"
	statusCompleted = "COMPLETED"
	statusCancelled = "CANCELLED"
)

// server bundles the dependencies our HTTP handlers need.
type server struct {
	db          *sql.DB
	kafkaWriter *kafka.Writer
}

func main() {
	dbURL := mustEnv("DATABASE_URL")
	kafkaBroker := mustEnv("KAFKA_BROKER")
	port := envOr("PORT", "8081")

	db := connectWithRetry(dbURL)
	defer db.Close()
	mustMigrate(db)

	// A single Writer can be shared safely across goroutines/requests;
	// kafka-go batches and load-balances writes across partitions for us.
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBroker),
		Balancer:               &kafka.Hash{}, // same key -> same partition, see below
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	srv := &server{db: db, kafkaWriter: writer}

	// Two background consumers: this service reacts to what Inventory and
	// Payment report back, so it needs to listen on both of their topics.
	go srv.consumeInventoryEvents(kafkaBroker)
	go srv.consumePaymentEvents(kafkaBroker)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /orders", srv.handleCreateOrder)
	mux.HandleFunc("GET /orders/{id}", srv.handleGetOrder)

	log.Printf("order-service listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

// mustMigrate creates the table this service owns if it doesn't exist yet.
// A real project would use a proper migration tool; for local practice a
// plain CREATE TABLE IF NOT EXISTS is simpler and good enough.
func mustMigrate(db *sql.DB) {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS orders (
			id UUID PRIMARY KEY,
			item TEXT NOT NULL,
			quantity INT NOT NULL,
			status TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`)
	if err != nil {
		log.Fatalf("migration failed: %v", err)
	}
}

// --- HTTP handlers -------------------------------------------------------

func (s *server) handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Item     string `json:"item"`
		Quantity int    `json:"quantity"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Item == "" || req.Quantity <= 0 {
		http.Error(w, `{"error":"body must be {\"item\": string, \"quantity\": int > 0}"}`, http.StatusBadRequest)
		return
	}

	order := Order{
		ID:       uuid.NewString(),
		Item:     req.Item,
		Quantity: req.Quantity,
		Status:   statusPending,
	}

	// Note: this is the "naive" approach the guide calls out in section 17
	// (Transactional Outbox) — we write to Postgres and publish to Kafka as
	// two separate steps, so it's possible for the DB write to succeed and
	// the Kafka publish to fail (or vice versa). That's intentional for
	// this first milestone; the Outbox exercise fixes it properly later.
	_, err := s.db.Exec(
		`INSERT INTO orders (id, item, quantity, status) VALUES ($1, $2, $3, $4)`,
		order.ID, order.Item, order.Quantity, order.Status,
	)
	if err != nil {
		log.Printf("insert order failed: %v", err)
		http.Error(w, `{"error":"failed to save order"}`, http.StatusInternalServerError)
		return
	}

	payload, _ := json.Marshal(orderCreatedPayload{
		OrderID:  order.ID,
		Item:     order.Item,
		Quantity: order.Quantity,
	})
	event := Event{
		EventID:     uuid.NewString(),
		EventType:   eventOrderCreated,
		AggregateID: order.ID,
		Timestamp:   time.Now().UTC(),
		Payload:     payload,
	}
	if err := s.publish(topicOrderEvents, order.ID, event); err != nil {
		// The order already exists in Postgres but nobody will ever hear
		// about it. In a real system this is exactly why the Outbox
		// pattern exists. For now we just log it loudly.
		log.Printf("WARNING: order %s saved but publishing OrderCreated failed: %v", order.ID, err)
	}

	log.Printf("order %s created (item=%s qty=%d) -> published OrderCreated", order.ID, order.Item, order.Quantity)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(order)
}

func (s *server) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var o Order
	err := s.db.QueryRow(
		`SELECT id, item, quantity, status, created_at, updated_at FROM orders WHERE id = $1`,
		id,
	).Scan(&o.ID, &o.Item, &o.Quantity, &o.Status, &o.CreatedAt, &o.UpdatedAt)
	if err == sql.ErrNoRows {
		http.Error(w, `{"error":"order not found"}`, http.StatusNotFound)
		return
	}
	if err != nil {
		log.Printf("query order failed: %v", err)
		http.Error(w, `{"error":"failed to load order"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(o)
}

// publish sends an event to Kafka using the AggregateID (the OrderID) as
// the message key. Kafka guarantees that all messages with the same key
// land on the same partition and stay in order *within that partition* —
// so every event about order-123 is processed in the order it was
// published, even though other orders' events may interleave with it on
// other partitions. See section 10 of the guide.
func (s *server) publish(topic, key string, event Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.kafkaWriter.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: body,
	})
}

// --- Kafka consumers -------------------------------------------------------

// consumeInventoryEvents reacts only to failures: if Inventory couldn't
// reserve stock, the order can never succeed, so we cancel it.
// (InventoryReserved on its own doesn't complete the order — payment still
// has to happen — so Order Service doesn't need to react to it.)
func (s *server) consumeInventoryEvents(broker string) {
	// Note: the group ID includes the topic name because this service also
	// runs a second reader (consumePaymentEvents) on a different topic —
	// see the comment in inventory-service/main.go's consumeOrderEvents
	// for why they can't share a group ID.
	reader := newReader(broker, topicInventoryEvents, "order-service-inventory-events")
	defer reader.Close()

	for {
		event, ok := readEvent(reader)
		if !ok {
			continue
		}

		if event.EventType != eventInventoryReservationFailed {
			continue
		}

		var p inventoryReservationFailedPayload
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			log.Printf("bad InventoryReservationFailed payload: %v", err)
			continue
		}
		s.updateStatus(p.OrderID, statusCancelled)
		log.Printf("order %s CANCELLED (inventory reservation failed: %s)", p.OrderID, p.Reason)
	}
}

// consumePaymentEvents reacts to the final step of the happy path
// (PaymentCompleted -> order is done) and to the failure path
// (PaymentFailed -> order is cancelled). Note that Inventory Service is
// *also* listening to payment.events directly, to release the stock it
// reserved earlier — that's the Saga compensation step, and it happens
// independently of what Order Service does here.
func (s *server) consumePaymentEvents(broker string) {
	reader := newReader(broker, topicPaymentEvents, "order-service-payment-events")
	defer reader.Close()

	for {
		event, ok := readEvent(reader)
		if !ok {
			continue
		}

		switch event.EventType {
		case eventPaymentCompleted:
			var p paymentCompletedPayload
			if err := json.Unmarshal(event.Payload, &p); err != nil {
				log.Printf("bad PaymentCompleted payload: %v", err)
				continue
			}
			s.updateStatus(p.OrderID, statusCompleted)
			log.Printf("order %s COMPLETED (payment %s succeeded)", p.OrderID, p.PaymentID)

		case eventPaymentFailed:
			var p paymentFailedPayload
			if err := json.Unmarshal(event.Payload, &p); err != nil {
				log.Printf("bad PaymentFailed payload: %v", err)
				continue
			}
			s.updateStatus(p.OrderID, statusCancelled)
			log.Printf("order %s CANCELLED (payment failed: %s)", p.OrderID, p.Reason)
		}
	}
}

func (s *server) updateStatus(orderID, status string) {
	_, err := s.db.Exec(
		`UPDATE orders SET status = $1, updated_at = now() WHERE id = $2`,
		status, orderID,
	)
	if err != nil {
		log.Printf("failed to update order %s to %s: %v", orderID, status, err)
	}
}
