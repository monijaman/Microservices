# Microservices

A practical local lab for learning microservices, Kafka, event-driven architecture, and Kubernetes.

This workspace contains:

- a full microservices practice guide in [MICROSERVICES_PRACTICE.md](MICROSERVICES_PRACTICE.md)
- a working, beginner-friendly, heavily-commented implementation of the guide's "First Milestone" (Order + Inventory + Payment services talking over Kafka, with Postgres and the Saga pattern)
- reliable messaging on top of that milestone: a transactional outbox, idempotent consumers, retries with a dead-letter queue, confirmed Kafka writes, and 3 partitions per topic
- a Kubernetes-focused practice path that starts after the Docker Compose version is working

For a detailed walkthrough of the code, how each pattern works, and step-by-step tests, see **[services/README.md](services/README.md)**.

## Architecture

```text
                 POST /orders
                      |
                      v
            +-------------------+          +-------------------+          +-------------------+
            |   Order Service   |          | Inventory Service |          |  Payment Service  |
            |   (HTTP :8081)    |          |                   |          |                   |
            +---------+---------+          +---------+---------+          +---------+---------+
                      |                              |                              |
                  orderdb                       inventorydb                     paymentdb
            (orders, outbox,             (inventory, reservations,       (payments, outbox,
             processed_events)            outbox, processed_events)        processed_events)
                      |                              |                              |
                      |          +-------------------+-------------------+          |
                      +--------->|              Kafka (KRaft)            |<---------+
                                 |  order.events  inventory.events       |
                                 |  payment.events   + one .dlq per group|
                                 +---------------------------------------+
```

- The services **never call each other**. They only publish and consume Kafka events.
- Each service owns **its own database** (database per service). The three databases share one Postgres container to keep the lab light.
- The order flow is a **choreographed Saga**: each service reacts to events, and a failure triggers compensating actions (e.g. releasing reserved stock) instead of a distributed transaction.

## Prerequisites

- Docker with the Compose plugin (`docker compose version`)
- `curl` for the API examples
- Optional: Go 1.24+ to build the services outside Docker, and a Postgres GUI such as HeidiSQL or DBeaver

## Quick start

```bash
docker compose up -d --build
```

This starts Kafka, Kafka UI, Postgres, Redis, and the three Go microservices. On startup each service creates the Kafka topics it needs (3 partitions each) and its database tables.

Wait ~15-20 seconds for everything to become healthy (`docker compose ps` shows `(healthy)`), then create an order:

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"item": "widget", "quantity": 2}'
```

Copy the `id` from the response and check its status a moment later:

```bash
curl http://localhost:8081/orders/<id>
```

It moves from `PENDING` to `COMPLETED`, or to `CANCELLED` if the payment or stock reservation failed.

Watch it happen live:

- `docker compose logs -f order-service inventory-service payment-service` — see each service react to the Kafka events
- http://localhost:8080 — Kafka UI, browse topics/messages/partitions/consumer groups
- `docker compose exec postgres psql -U appuser -d orderdb -c "select * from orders;"` — inspect the data directly

Run the same POST a bunch of times — roughly 30% of orders will have their payment simulated-fail, which triggers the Saga's compensation path (inventory gets released, order gets cancelled). See [services/README.md](services/README.md) for exactly how that flow works.

## What's running

| Container | Address | Purpose |
|---|---|---|
| `order-service` | http://localhost:8081 | REST API; starts the Saga and tracks each order's final status |
| `inventory-service` | (no HTTP) | Reserves stock for new orders; releases it if payment fails |
| `payment-service` | (no HTTP) | Simulates charging the customer (fails `PAYMENT_FAILURE_RATE` of the time) |
| `kafka` | `localhost:9092` (host), `kafka:29092` (containers) | Message broker, single node in KRaft mode |
| `kafka-ui` | http://localhost:8080 | Web dashboard for topics, messages and consumer groups |
| `postgres` | `localhost:5432` | Databases `orderdb`, `inventorydb`, `paymentdb`; user `appuser` / `apppass` |
| `redis` | `localhost:6379` | Not used yet; ready for the caching / locking exercises |

Data is kept in two Docker volumes, `pg_data` and `kafka_data`, so it survives `docker compose down`. Use `docker compose down -v` to wipe it.

## API

### `POST /orders`

Creates an order and starts the Saga.

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"item": "widget", "quantity": 2}'
```

```json
{
  "id": "c8570f1e-59f7-480d-b025-b7066d102f38",
  "item": "widget",
  "quantity": 2,
  "status": "PENDING",
  "createdAt": "0001-01-01T00:00:00Z",
  "updatedAt": "0001-01-01T00:00:00Z"
}
```

- `201 Created` on success. The order row and its `OrderCreated` event are saved in one transaction, so this succeeds even if Kafka is briefly down.
- `400 Bad Request` if `item` is empty or `quantity` isn't a positive integer.
- Only `widget` (100 in stock) and `gadget` (50) exist. Any other item is cancelled with `unknown item`.
- The timestamps in this response are placeholders; `GET` returns the real ones.

### `GET /orders/{id}`

Returns the order with its current `status`: `PENDING`, `COMPLETED` or `CANCELLED`. `404` if it doesn't exist.

### `GET /health`

Returns `200 OK` when the service is up.

## Kafka topics and events

Every message uses the same envelope: `eventId`, `eventType`, `aggregateId` (the order ID), `timestamp`, `payload`. The **order ID is the message key**, so all events for one order land on the same partition and are processed in order.

| Topic | Event | Published by | Payload | Consumed by |
|---|---|---|---|---|
| `order.events` | `OrderCreated` | Order | `orderId`, `item`, `quantity` | Inventory |
| `inventory.events` | `InventoryReserved` | Inventory | `orderId`, `item`, `quantity` | Payment |
| `inventory.events` | `InventoryReservationFailed` | Inventory | `orderId`, `reason` | Order (cancels) |
| `inventory.events` | `InventoryReleased` | Inventory | `orderId` | (informational) |
| `payment.events` | `PaymentCompleted` | Payment | `orderId`, `paymentId`, `amount` | Order (completes) |
| `payment.events` | `PaymentFailed` | Payment | `orderId`, `reason` | Order (cancels), Inventory (releases stock) |

Each consumer group also has a dead-letter topic named `<group-id>.dlq`, e.g. `inventory-service-order-events.dlq`, for messages that keep failing.

## Reliability features

| Feature | Problem it solves | Details |
|---|---|---|
| Confirmed writes (`RequiredAcks: RequireAll`) | kafka-go's default is fire-and-forget, so a lost message never reports an error | [services/README.md](services/README.md#1-confirmed-writes-newwriter) |
| Transactional outbox | A DB change saved without its event (or the other way round) leaves an order stuck in `PENDING` | [services/README.md](services/README.md#2-transactional-outbox-enqueueevent-runoutboxrelay) |
| Idempotent consumers | Kafka is at-least-once, so the same event can arrive twice | [services/README.md](services/README.md#3-idempotent-consumers-processed_events) |
| Retry + dead-letter queue | A failing message was logged and dropped | [services/README.md](services/README.md#4-retry-and-dead-letter-queue-consume) |
| 3 partitions per topic | Auto-created topics get 1 partition, so there's no parallelism | [services/README.md](services/README.md#5-partitions-ensuretopics) |
| Kafka data volume | `docker compose down` wiped every message and offset | [services/README.md](services/README.md#6-kafka-data-survives-restarts) |

Each of these is tested by hand in [Part 3 of the testing guide](services/README.md#part-3-reliability): stopping Kafka mid-order, stopping a consumer, sending broken and duplicate messages, and simulating a database outage.

## Testing

There are no automated tests yet. The **[testing guide](services/README.md#testing-guide)** walks through 15 manual tests, each with its goal, commands, expected log output and SQL checks:

| Part | Tests |
|---|---|
| [1. Business flow](services/README.md#part-1-business-flow) | Happy path, payment failure and compensation, unknown item, not enough stock, invalid input |
| [2. Kafka](services/README.md#part-2-kafka) | Events in each topic, partitions and keys, consumer lag, consumers sharing partitions |
| [3. Reliability](services/README.md#part-3-reliability) | Kafka down, consumer down, broken messages, duplicates, retries, DLQ replay, data surviving a restart |

The quickest smoke test, once everything is healthy:

```bash
curl -s -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"item": "widget", "quantity": 1}'
# copy the id, then after ~1 second:
curl -s http://localhost:8081/orders/<id>     # "status":"COMPLETED" (or CANCELLED ~30% of the time)
```

To check that the Go code compiles without Docker, run `go vet .` in each service folder.

## Project structure

```text
.
├── README.md                    <- you are here
├── MICROSERVICES_PRACTICE.md    <- the full practice guide
├── docker-compose.yml           <- all containers, volumes and wiring
├── .env                         <- Postgres login, PAYMENT_FAILURE_RATE
├── infra/
│   └── postgres/init.sql        <- creates inventorydb and paymentdb
└── services/
    ├── README.md                <- detailed walkthrough, tests, troubleshooting
    ├── order-service/
    ├── inventory-service/
    └── payment-service/
```

Each service folder has the same layout:

| File | Contents |
|---|---|
| `main.go` | Startup, DB tables, HTTP handlers (Order only), and the Kafka event handlers |
| `events.go` | Topic names, partition count, event type names and payload structs |
| `messaging.go` | Outbox, relay, idempotent consumer, retry and DLQ. Identical in every service |
| `util.go` | `mustEnv`, `envOr`, DB connection retry, `newReader`, `ensureTopics` |
| `Dockerfile` | Two-stage build: compile with Go, run on a small Alpine image |

Each service is its own Go module, so shared helpers are copied rather than imported. That keeps every service independently buildable and deployable.

## Configuration

Set in [.env](.env), which Docker Compose reads automatically:

| Variable | Default | Meaning |
|---|---|---|
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | `appuser` / `apppass` | Postgres login used by every service |
| `POSTGRES_DB` | `orderdb` | The first database Postgres creates; the other two come from `infra/postgres/init.sql` |
| `PAYMENT_FAILURE_RATE` | `0.3` | Share of payments that fail on purpose: `0` never, `1` always |

After changing `.env`, recreate the affected containers, e.g. `docker compose up -d payment-service`. After changing Go code, rebuild: `docker compose up -d --build <service>`.

More settings (partition count, retry attempts, per-service environment variables) are listed in [services/README.md](services/README.md#configuration).

## Troubleshooting

The most common issues:

- **`database "appuser" does not exist`**: a client connected without naming a database. Use `psql -d orderdb`, or fill in the database field in your GUI client.
- **Order cancelled with `unknown item`**: only `widget` and `gadget` exist.
- **Order cancelled with `simulated payment decline`**: working as intended. Set `PAYMENT_FAILURE_RATE=0` to turn it off.
- **Order stuck in `PENDING`**: check `docker compose ps` and the service logs for `outbox relay` or `.dlq` lines.

The full table, plus how to connect HeidiSQL to each database, is in [services/README.md](services/README.md#troubleshooting).

## Learning path

Start with the practice guide ([MICROSERVICES_PRACTICE.md](MICROSERVICES_PRACTICE.md)) and use this implementation as the "First Milestone" checkpoint (section 31).

Progress against the guide's exercises:

| Guide section | Exercise | Status |
|---|---|---|
| 8 | Basic producer / consumer | Done |
| 9 | Consumer groups | Done (one group per service and topic) |
| 10 | Message ordering | Done (order ID as key, 3 partitions) |
| 11 | Idempotency | Done (`processed_events`) |
| 12 | Retry | Partly done: in-process retries with backoff. Separate retry topics are next |
| 13 | Dead letter queue | Done (`<group-id>.dlq`) |
| 14-15 | Saga choreography and compensation | Done |
| 16 | Saga orchestration | Not yet |
| 17 | Transactional outbox | Done |
| 18+ | Kafka Streams, Schema Registry, Redis, API Gateway, auth, circuit breakers, observability | Not yet |

Keep extending this codebase one exercise at a time before moving into the Kubernetes exercises (section 34.5).
