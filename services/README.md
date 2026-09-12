# How this works

Three tiny Go services implement the "First Milestone" from
[MICROSERVICES_PRACTICE.md](../MICROSERVICES_PRACTICE.md) (section 31):
Order, Inventory, and Payment, talking to each other only through Kafka —
never by calling each other's HTTP APIs directly. This is the "choreography"
style of the Saga pattern: each service reacts to events, nobody is in
charge.

## The happy path

```text
POST /orders (you)
      |
      v
Order Service --INSERT--> orders table (status=PENDING)
      |
      | publish OrderCreated  ------------------->  Kafka topic: order.events
      |                                                      |
      |                                                      v
      |                                          Inventory Service consumes it
      |                                          - locks the inventory row
      |                                          - has enough stock? decrement it,
      |                                            record a reservation
      |                                          | publish InventoryReserved --> inventory.events
      |                                                      |
      |                                                      v
      |                                          Payment Service consumes it
      |                                          - "charges" the customer (simulated)
      |                                          | publish PaymentCompleted --> payment.events
      |                                                      |
      v                                                      v
Order Service consumes PaymentCompleted, sets status=COMPLETED
```

## The failure / compensation path

Payment Service randomly fails ~30% of charges (`PAYMENT_FAILURE_RATE` in
`.env`). When it does:

```text
Payment Service
      |
      | publish PaymentFailed --> payment.events
      |
      +-----------------------------+
      |                             |
      v                             v
Inventory Service consumes it       Order Service consumes it
- deletes the reservation           - sets status=CANCELLED
- adds the stock back
- publish InventoryReleased
```

Nobody rolled back a distributed transaction — there isn't one. Instead,
each service ran its own **compensating action** in response to an event.
That's the core idea of the Saga pattern: replace "one big ACID transaction
across services" (which Kafka/microservices can't give you) with "a
sequence of local transactions, each with a corresponding undo action."

If Inventory itself can't reserve stock (not enough left), it publishes
`InventoryReservationFailed` directly and Payment Service never even sees
the order — there's nothing to compensate yet.

## Where to look for each Kafka concept

| Concept | Where in the code |
|---|---|
| Producer | `s.kafkaWriter.WriteMessages(...)` in each service's `publish` function |
| Consumer | `newReader` + `readEvent` in each service's `util.go` |
| Consumer group | The `groupID` argument to `newReader` — e.g. `"inventory-service-order-events"`. Every instance of the same service shares one group, so Kafka spreads partitions across however many replicas are running. Each topic a service subscribes to gets its **own** group ID (e.g. Order Service uses `order-service-inventory-events` and `order-service-payment-events`) — sharing one group ID across two different topic subscriptions confuses Kafka's partition assignment, since a group's assignment is computed per subscription |
| Message key / ordering | Every publish uses the **order ID** as the Kafka key (`Key: []byte(orderID)`), so every event about one order lands on the same partition and is processed in order, even though events about *different* orders can be processed out of order relative to each other |
| Topics | `order.events`, `inventory.events`, `payment.events` — see the `const` blocks in each service's `events.go` |
| Event envelope | The `Event` struct — `eventId`, `eventType`, `aggregateId`, `timestamp`, `payload` |

## Things to try (in order)

1. **Watch it happen.** Run `docker compose logs -f order-service inventory-service payment-service` in one terminal, then `POST /orders` a few times in another. Watch the same order ID move through all three services' logs.
2. **Watch it fail.** Keep creating orders until you see a `payment FAILED` log line, then `GET /orders/<id>` and confirm it ends up `CANCELLED`, and check in Kafka UI (http://localhost:8080) that `InventoryReleased` was published.
3. **Oversell it.** `widget` starts with 100 in stock. Create orders with a huge `quantity` (e.g. 200) and confirm you get `InventoryReservationFailed` and the order is cancelled without ever reaching Payment Service.
4. **Open Kafka UI** and look at the `order.events` topic's messages, partitions, and the `order-service` / `inventory-service` / `payment-service` consumer groups. This is the same information `kafka-consumer-groups --describe` shows on the CLI (section 27 of the guide).
5. **Scale a consumer.** `docker compose up --scale inventory-service=3 -d` and create several orders — Kafka's consumer-group rebalancing spreads the topic's partitions across the three instances automatically (see Exercise 2 in the guide; with only 1 partition on `order.events` by default, only one replica will actually receive traffic — recreate the topic with more partitions via the `kafka-topics` CLI to see the rebalance properly).

## What's intentionally left out (for now)

This is the *first* milestone, not the finished lab. On purpose, this code
does **not** yet implement:

- **Transactional Outbox** — `handleCreateOrder` writes to Postgres and
  publishes to Kafka as two separate steps, so it's possible (rare, but
  possible) for the DB write to succeed while the Kafka publish fails.
  Section 17 of the guide fixes this properly.
- **Idempotent consumers** — if a consumer crashes after processing a
  message but before its offset is committed, it will reprocess that
  message on restart. Section 11 covers adding a `processed_messages`
  table to guard against that.
- **Retry / Dead Letter Queue** — a permanently failing message currently
  just gets logged and skipped. Sections 12-13 add real retry topics and a
  DLQ.
- **Saga Orchestrator, Kafka Streams, Schema Registry, observability** —
  later milestones in the guide.

Add these one at a time as you work through the guide — the code here is
deliberately small so each addition is easy to see the effect of.
