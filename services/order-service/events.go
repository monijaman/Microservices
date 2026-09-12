package main

import (
	"encoding/json"
	"time"
)

// Event is the common "envelope" every service in this system uses when it
// publishes something to Kafka. Wrapping every message in the same shape
// means any consumer can read the "outside" of an event (what type is this?
// which order does it belong to?) before it even looks at the payload.
//
// See MICROSERVICES_PRACTICE.md section 7 for why each field exists.
type Event struct {
	EventID     string          `json:"eventId"`
	EventType   string          `json:"eventType"`
	AggregateID string          `json:"aggregateId"` // the OrderID this event is about
	Timestamp   time.Time       `json:"timestamp"`
	Payload     json.RawMessage `json:"payload"`
}

// Kafka topic names. Keeping them as constants avoids typos like publishing
// to "order.events" in one place and "order-events" in another.
const (
	topicOrderEvents     = "order.events"
	topicInventoryEvents = "inventory.events"
	topicPaymentEvents   = "payment.events"
)

// Event type names carried inside Event.EventType.
const (
	eventOrderCreated               = "OrderCreated"
	eventInventoryReservationFailed = "InventoryReservationFailed"
	eventPaymentCompleted           = "PaymentCompleted"
	eventPaymentFailed              = "PaymentFailed"
)

// --- Payload shapes -----------------------------------------------------
// Every service defines its own copy of the payloads it needs. That's
// intentional: microservices don't share a library of internal types with
// each other, they only agree on the JSON shape that crosses the wire.

type orderCreatedPayload struct {
	OrderID  string `json:"orderId"`
	Item     string `json:"item"`
	Quantity int    `json:"quantity"`
}

type inventoryReservationFailedPayload struct {
	OrderID string `json:"orderId"`
	Reason  string `json:"reason"`
}

type paymentCompletedPayload struct {
	OrderID   string  `json:"orderId"`
	PaymentID string  `json:"paymentId"`
	Amount    float64 `json:"amount"`
}

type paymentFailedPayload struct {
	OrderID string `json:"orderId"`
	Reason  string `json:"reason"`
}
