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
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
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
	db *sql.DB
}

// Consumer group IDs. Each also names that consumer's dead-letter topic
// (see dlqTopic).
const (
	groupInventoryEvents = "order-service-inventory-events"
	groupPaymentEvents   = "order-service-payment-events"
)

func main() {
	dbURL := mustEnv("DATABASE_URL")
	kafkaBroker := mustEnv("KAFKA_BROKER")
	ensureTopics(kafkaBroker, topicOrderEvents, topicInventoryEvents, topicPaymentEvents,
		dlqTopic(groupInventoryEvents), dlqTopic(groupPaymentEvents))
	port := envOr("PORT", "8081")

	db := connectWithRetry(dbURL)
	defer db.Close()
	mustMigrate(db)
	migrateMessaging(db)

	// A single Writer can be shared safely across goroutines; it's used by
	// the outbox relay and for parking failed messages on a DLQ.
	writer := newWriter(kafkaBroker)
	defer writer.Close()

	srv := &server{db: db}

	go runOutboxRelay(db, writer)

	// Two background consumers: this service reacts to what Inventory and
	// Payment report back, so it needs to listen on both of their topics.
	// Note the group IDs include the topic name — see the comment in
	// inventory-service/main.go's main for why two readers in one service
	// can't share a group ID.
	go consume(db, writer, kafkaBroker, topicInventoryEvents, groupInventoryEvents, srv.handleInventoryEvent)
	go consume(db, writer, kafkaBroker, topicPaymentEvents, groupPaymentEvents, srv.handlePaymentEvent)

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

	// Transactional outbox (section 17): the order row and its OrderCreated
	// event are saved in ONE transaction, so either both exist or neither
	// does. The outbox relay publishes the event to Kafka afterwards and
	// keeps retrying if Kafka is down, so the order can't get stuck PENDING
	// because a publish was lost.
	if err := s.createOrder(r.Context(), order); err != nil {
		log.Printf("create order failed: %v", err)
		http.Error(w, `{"error":"failed to save order"}`, http.StatusInternalServerError)
		return
	}

	log.Printf("order %s created (item=%s qty=%d) -> OrderCreated queued in outbox", order.ID, order.Item, order.Quantity)

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

func (s *server) createOrder(ctx context.Context, order Order) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() // no-op if we already committed

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO orders (id, item, quantity, status) VALUES ($1, $2, $3, $4)`,
		order.ID, order.Item, order.Quantity, order.Status,
	); err != nil {
		return err
	}
	// The OrderID is the event's key: Kafka guarantees that all messages
	// with the same key land on the same partition and stay in order
	// *within that partition* — so every event about order-123 is processed
	// in the order it was published, even though other orders' events may
	// interleave with it on other partitions. See section 10 of the guide.
	if err := enqueueEvent(ctx, tx, topicOrderEvents, eventOrderCreated, order.ID, orderCreatedPayload{
		OrderID:  order.ID,
		Item:     order.Item,
		Quantity: order.Quantity,
	}); err != nil {
		return err
	}
	return tx.Commit()
}

// --- Kafka event handlers ----------------------------------------------------
//
// Each handler runs inside a DB transaction opened by consume() (see
// messaging.go). Returning an error rolls it back and the event is retried,
// then parked on the DLQ if it keeps failing.

// handleInventoryEvent reacts only to failures: if Inventory couldn't
// reserve stock, the order can never succeed, so we cancel it.
// (InventoryReserved on its own doesn't complete the order — payment still
// has to happen — so Order Service doesn't need to react to it.)
func (s *server) handleInventoryEvent(ctx context.Context, tx *sql.Tx, event Event) error {
	if event.EventType != eventInventoryReservationFailed {
		return nil
	}

	var p inventoryReservationFailedPayload
	if err := json.Unmarshal(event.Payload, &p); err != nil {
		return permanent(fmt.Errorf("bad InventoryReservationFailed payload: %w", err))
	}
	if err := updateStatus(ctx, tx, p.OrderID, statusCancelled); err != nil {
		return err
	}
	log.Printf("order %s CANCELLED (inventory reservation failed: %s)", p.OrderID, p.Reason)
	return nil
}

// handlePaymentEvent reacts to the final step of the happy path
// (PaymentCompleted -> order is done) and to the failure path
// (PaymentFailed -> order is cancelled). Note that Inventory Service is
// *also* listening to payment.events directly, to release the stock it
// reserved earlier — that's the Saga compensation step, and it happens
// independently of what Order Service does here.
func (s *server) handlePaymentEvent(ctx context.Context, tx *sql.Tx, event Event) error {
	switch event.EventType {
	case eventPaymentCompleted:
		var p paymentCompletedPayload
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			return permanent(fmt.Errorf("bad PaymentCompleted payload: %w", err))
		}
		if err := updateStatus(ctx, tx, p.OrderID, statusCompleted); err != nil {
			return err
		}
		log.Printf("order %s COMPLETED (payment %s succeeded)", p.OrderID, p.PaymentID)

	case eventPaymentFailed:
		var p paymentFailedPayload
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			return permanent(fmt.Errorf("bad PaymentFailed payload: %w", err))
		}
		if err := updateStatus(ctx, tx, p.OrderID, statusCancelled); err != nil {
			return err
		}
		log.Printf("order %s CANCELLED (payment failed: %s)", p.OrderID, p.Reason)
	}
	return nil
}

func updateStatus(ctx context.Context, tx *sql.Tx, orderID, status string) error {
	if _, err := tx.ExecContext(ctx,
		`UPDATE orders SET status = $1, updated_at = now() WHERE id = $2`,
		status, orderID,
	); err != nil {
		return fmt.Errorf("update order %s to %s: %w", orderID, status, err)
	}
	return nil
}
