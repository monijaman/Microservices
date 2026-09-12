package main

import (
	"encoding/json"
	"time"
)

// Event is the common envelope every service uses when publishing to Kafka.
// See order-service/events.go for the full explanation — every service
// keeps its own copy of this instead of importing a shared library, which
// is deliberate: microservices only agree on the JSON shape on the wire,
// not on shared Go code.
type Event struct {
	EventID     string          `json:"eventId"`
	EventType   string          `json:"eventType"`
	AggregateID string          `json:"aggregateId"`
	Timestamp   time.Time       `json:"timestamp"`
	Payload     json.RawMessage `json:"payload"`
}

const (
	topicOrderEvents     = "order.events"
	topicInventoryEvents = "inventory.events"
	topicPaymentEvents   = "payment.events"
)

const (
	eventOrderCreated               = "OrderCreated"
	eventInventoryReserved          = "InventoryReserved"
	eventInventoryReservationFailed = "InventoryReservationFailed"
	eventInventoryReleased          = "InventoryReleased"
	eventPaymentFailed              = "PaymentFailed"
)

type orderCreatedPayload struct {
	OrderID  string `json:"orderId"`
	Item     string `json:"item"`
	Quantity int    `json:"quantity"`
}

type inventoryReservedPayload struct {
	OrderID  string `json:"orderId"`
	Item     string `json:"item"`
	Quantity int    `json:"quantity"`
}

type inventoryReservationFailedPayload struct {
	OrderID string `json:"orderId"`
	Reason  string `json:"reason"`
}

type inventoryReleasedPayload struct {
	OrderID string `json:"orderId"`
}

type paymentFailedPayload struct {
	OrderID string `json:"orderId"`
	Reason  string `json:"reason"`
}
