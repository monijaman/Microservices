package main

import (
	"encoding/json"
	"time"
)

// See order-service/events.go for why this envelope is duplicated per
// service instead of shared.
type Event struct {
	EventID     string          `json:"eventId"`
	EventType   string          `json:"eventType"`
	AggregateID string          `json:"aggregateId"`
	Timestamp   time.Time       `json:"timestamp"`
	Payload     json.RawMessage `json:"payload"`
}

const (
	topicInventoryEvents = "inventory.events"
	topicPaymentEvents   = "payment.events"
)

const (
	eventInventoryReserved = "InventoryReserved"
	eventPaymentCompleted  = "PaymentCompleted"
	eventPaymentFailed     = "PaymentFailed"
)

type inventoryReservedPayload struct {
	OrderID  string `json:"orderId"`
	Item     string `json:"item"`
	Quantity int    `json:"quantity"`
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
