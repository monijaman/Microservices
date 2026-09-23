# How this works

Three tiny Go services implement the "First Milestone" from
[MICROSERVICES_PRACTICE.md](../MICROSERVICES_PRACTICE.md) (section 31):
Order, Inventory, and Payment, talking to each other only through Kafka —
never by calling each other's HTTP APIs directly. This is the "choreography"
style of the Saga pattern: each service reacts to events, nobody is in
charge.

On top of the milestone, the messaging is made reliable: a transactional
outbox, idempotent consumers, retries with a dead-letter queue, confirmed
Kafka writes, and 3 partitions per topic. See [Reliable messaging](#reliable-messaging).

## The happy path

```text
POST /orders (you)
      |
      v
Order Service --one DB transaction--> orders table (status=PENDING)
      |                               + outbox table (OrderCreated)
      |
      | outbox relay publishes OrderCreated --->  Kafka topic: order.events
      |                                                      |
      |                                                      v
      |                                          Inventory Service consumes it
      |                                          - locks the inventory row
      |                                          - has enough stock? decrement it,
      |                                            record a reservation
      |                                          | InventoryReserved --> inventory.events
      |                                                      |
      |                                                      v
      |                                          Payment Service consumes it
      |                                          - "charges" the customer (simulated)
      |                                          | PaymentCompleted --> payment.events
      |                                                      |
      v                                                      v
Order Service consumes PaymentCompleted, sets status=COMPLETED
```

Every service publishes the same way Order Service does: the event is saved
to its own `outbox` table in the same DB transaction as the change, and a
background relay sends it to Kafka.

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

If Inventory itself can't reserve stock (unknown item, or not enough left),
it publishes `InventoryReservationFailed` directly and Payment Service never
even sees the order — there's nothing to compensate yet.

Only `widget` (100 in stock) and `gadget` (50) exist. They're seeded by
`mustMigrate` in [inventory-service/main.go](inventory-service/main.go).
Ordering anything else (e.g. `Apple`) is cancelled with `unknown item`.

## Reliable messaging

All of this lives in `messaging.go`. The file is identical in every service,
because each service is its own Go module (like `util.go`).

### 1. Confirmed writes (`newWriter`)

| Setting | Why |
|---|---|
| `RequiredAcks: kafka.RequireAll` | `WriteMessages` only succeeds once Kafka has stored the message. kafka-go's default (`RequireNone`) is fire-and-forget: a lost message would never report an error |
| `AllowAutoTopicCreation: false` | Topics are created up front by `ensureTopics`, so a typo'd topic name fails loudly instead of silently creating a new topic |
| `BatchTimeout: 10ms` | The default waits up to 1s to fill a batch before sending, which made every Saga step ~1s slower |

### 2. Transactional outbox (`enqueueEvent`, `runOutboxRelay`)

Without it, a service writes to Postgres and then publishes to Kafka as two
separate steps. If the publish fails, the change is saved but nobody ever
hears about it, e.g. an order stuck in `PENDING` forever.

With it:

1. The handler calls `enqueueEvent(ctx, tx, ...)`, which inserts the event
   into the `outbox` table **in the same transaction** as the business
   change. Both are saved, or neither is.
2. `runOutboxRelay` polls every 0.5s for rows where `published_at IS NULL`,
   sends them to Kafka in order (`ORDER BY seq`), then sets `published_at`.
3. If Kafka is down, the rows simply stay unpublished and are retried on the
   next tick. Events are delayed, never lost.

`FOR UPDATE SKIP LOCKED` lets several copies of a service run their relays
side by side without publishing the same rows twice.

The relay is **at-least-once**: if it crashes after sending but before
marking a row as published, that row is sent again on restart. The next
section makes that harmless.

### 3. Idempotent consumers (`processed_events`)

Kafka can deliver a message more than once (a relay resend, or a consumer
crash before committing its offset). Each consumer records the event's
`eventId` in `processed_events` in the same transaction as the event's
effects. A second delivery finds the ID already there, logs
`skipping duplicate ...`, and does nothing.

### 4. Retry and dead-letter queue (`consume`)

```text
message --> handle --x--> wait 1s --> handle --x--> wait 2s --> handle --x--> <group-id>.dlq
                                                                              then commit offset
```

- Each handler runs inside one DB transaction. If it returns an error,
  everything is rolled back, including outbox rows and the
  `processed_events` row.
- **Temporary errors** (DB unreachable, a Postgres deadlock, a timeout) are
  retried up to 3 times with backoff.
- **Permanent errors** (not valid JSON, a bad `eventId`, a payload that
  doesn't match its struct) go straight to the DLQ, because retrying can't
  fix them. Handlers mark these with `permanent(err)`.
- A message that still fails is copied, unchanged, to `<group-id>.dlq`, with
  headers saying where it came from and why it failed:

  | Header | Example |
  |---|---|
  | `dlq-original-topic` | `order.events` |
  | `dlq-original-partition` | `0` |
  | `dlq-original-offset` | `3` |
  | `dlq-error` | `bad OrderCreated payload: json: cannot unmarshal ...` |
  | `dlq-failed-at` | `2026-09-23T21:45:20Z` |

- The offset is committed **only after** the message was handled or parked,
  so a crash mid-way means it's redelivered, never lost. Writing to the DLQ
  retries until Kafka accepts it for the same reason.

One DLQ per consumer group:

| DLQ topic | Holds failures from |
|---|---|
| `order-service-inventory-events.dlq` | Order Service reading `inventory.events` |
| `order-service-payment-events.dlq` | Order Service reading `payment.events` |
| `inventory-service-order-events.dlq` | Inventory Service reading `order.events` |
| `inventory-service-payment-events.dlq` | Inventory Service reading `payment.events` |
| `payment-service-inventory-events.dlq` | Payment Service reading `inventory.events` |

### 5. Partitions (`ensureTopics`)

Kafka's auto-create gives a topic just 1 partition, so the key-based
balancer has only one lane to choose from and a consumer group can never
have more than one active reader. At startup, every service calls
`ensureTopics` (in `util.go`) before its readers and writers start. It:

- creates each topic it uses, plus its DLQs, with `topicPartitions` (3,
  set in each `events.go`) partitions;
- grows an existing topic that has fewer partitions. Kafka never allows
  shrinking.

It's safe for all three services to call it at the same time.

Every event is keyed by the **order ID**, so all events for one order land
on the same partition number in every topic and are processed in order.
Events for different orders are spread across the 3 partitions.

### 6. Kafka data survives restarts

Kafka stores its data in the `kafka_data` volume (see
[docker-compose.yml](../docker-compose.yml)), so topics, messages and
consumer offsets survive `docker compose down` / `up`. Messages are kept for
7 days (`log.retention.hours=168`, the image default).

`docker compose down -v` deletes this volume **and** the Postgres volume.

### What's still simplified

- **Replication factor 1.** There's a single broker, so there are no copies
  of the data. A real cluster uses 3 brokers, replication factor 3, and
  `min.insync.replicas=2`.
- **Retries block the partition.** While a message is being retried (up to
  about 3s), messages behind it on the same partition wait. Sections 12-13
  of the guide add separate retry topics to avoid that.
- **No cleanup.** `outbox` and `processed_events` grow forever. A real
  system deletes old published rows with a scheduled job.
- **No automatic DLQ replay.** Replaying is manual (see below).
- **Saga Orchestrator, Kafka Streams, Schema Registry, observability** are
  later milestones in the guide.

## Where to look for each Kafka concept

| Concept | Where in the code |
|---|---|
| Producer | `enqueueEvent` saves an event to the `outbox` table in the same DB transaction as the change; `runOutboxRelay` publishes it (both in each service's `messaging.go`). The writer uses `RequiredAcks: RequireAll`, so a write only counts once Kafka has stored it |
| Consumer | `consume` in each service's `messaging.go`: reads with `newReader`, skips already-handled events (`processed_events` table), retries a failing handler 3 times, then parks the message on `<group-id>.dlq`, and only then commits the offset |
| Event handlers | `handleInventoryEvent`, `handlePaymentEvent`, `handleOrderEvent` in each service's `main.go`. Each receives a `*sql.Tx` and returns an `error` |
| Consumer group | The `group...` constants at the top of each `main.go`, e.g. `"inventory-service-order-events"`. Every instance of the same service shares one group, so Kafka spreads partitions across however many replicas are running. Each topic a service subscribes to gets its **own** group ID (e.g. Order Service uses `order-service-inventory-events` and `order-service-payment-events`) — sharing one group ID across two different topic subscriptions confuses Kafka's partition assignment, since a group's assignment is computed per subscription |
| Message key / ordering | Every event uses the **order ID** as the Kafka key, so every event about one order lands on the same partition and is processed in order, even though events about *different* orders can be processed out of order relative to each other |
| Partitions | `ensureTopics` in each `util.go`; the count is `topicPartitions` in each `events.go` |
| Topics | `order.events`, `inventory.events`, `payment.events` — see the `const` blocks in each service's `events.go` |
| Event envelope | The `Event` struct — `eventId`, `eventType`, `aggregateId`, `timestamp`, `payload` |

## Testing guide

There are no automated tests yet, so everything is tested by hand against
the running stack. Each test below lists its **goal**, the **steps**, what
you should **see**, and how to **verify** it. All the example output comes
from real runs; your IDs, offsets and stock numbers will differ.

| # | Test | What it proves |
|---|---|---|
| 1 | [Happy path](#test-1-happy-path) | An order flows through all three services and completes |
| 2 | [Payment failure and compensation](#test-2-payment-failure-and-compensation) | A failed payment cancels the order and puts the stock back |
| 3 | [Unknown item](#test-3-unknown-item) | Inventory rejects items it doesn't have |
| 4 | [Not enough stock](#test-4-not-enough-stock) | Inventory never oversells |
| 5 | [Invalid input](#test-5-invalid-input) | The API rejects bad requests |
| 6 | [Events in Kafka](#test-6-events-in-kafka) | Each step publishes the expected event |
| 7 | [Partitions and keys](#test-7-partitions-and-keys) | Topics have 3 partitions; one order always uses the same one |
| 8 | [Consumers share partitions](#test-8-consumers-share-partitions) | A consumer group splits partitions and rebalances |
| 9 | [Kafka is down](#test-9-kafka-is-down-outbox) | The outbox holds events until Kafka is back |
| 10 | [A consumer is down](#test-10-a-consumer-is-down) | A stopped service catches up from its saved offset |
| 11 | [Broken messages](#test-11-broken-messages-go-to-the-dlq) | Messages that can't be processed go to the DLQ |
| 12 | [Duplicate messages](#test-12-duplicate-messages-are-skipped) | The same event is never applied twice |
| 13 | [Temporary DB error](#test-13-temporary-errors-are-retried) | Short failures are retried and succeed |
| 14 | [Retries run out, then replay](#test-14-retries-run-out-then-replay-from-the-dlq) | A message that keeps failing is parked, and replaying it finishes the order |
| 15 | [Data survives restarts](#test-15-data-survives-a-restart) | Kafka and Postgres data outlive `docker compose down` |

### Before you start

1. **Start everything** and wait until all containers are healthy:
   ```bash
   docker compose up -d --build
   docker compose ps
   ```

2. **Open the logs** in a second terminal and leave them running. Most
   tests are checked here:
   ```bash
   docker compose logs -f order-service inventory-service payment-service
   ```

3. **Add these helper functions** to the terminal you'll test in. They
   need `python3` to read the JSON responses. They only last for that
   terminal session.
   ```bash
   # Create an order and print its id:   order widget 2
   order() {
     curl -s -X POST http://localhost:8081/orders \
       -H "Content-Type: application/json" \
       -d "{\"item\":\"$1\",\"quantity\":$2}" \
       | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])'
   }

   # Print an order's status:   status <id>
   status() {
     curl -s http://localhost:8081/orders/$1 \
       | python3 -c 'import sys,json; print(json.load(sys.stdin)["status"])'
   }

   # Run SQL against one database:   sql inventorydb "select * from inventory"
   sql() { docker compose exec -T postgres psql -U appuser -d "$1" -c "$2"; }
   ```

4. **Make payments predictable** when a test says so. A shell variable
   overrides the value in `.env` for that one command:
   ```bash
   PAYMENT_FAILURE_RATE=0 docker compose up -d payment-service   # payments always succeed
   PAYMENT_FAILURE_RATE=1 docker compose up -d payment-service   # payments always fail
   docker compose up -d payment-service                          # back to .env (0.3)
   ```

> **Wait about 30 seconds after restarting a service.** Kafka keeps the
> old container's place in the consumer group until its session times
> out, so the first order after a restart can sit in `PENDING` for up to
> ~30s. After that, an order normally finishes in **about 1 second**.

---

### Part 1: Business flow

#### Test 1: Happy path

**Goal:** an order goes Order → Inventory → Payment → Order and ends up
`COMPLETED`.

1. Make payments always succeed, then wait ~30s:
   ```bash
   PAYMENT_FAILURE_RATE=0 docker compose up -d payment-service
   ```
2. Note the stock:
   ```bash
   sql inventorydb "select * from inventory order by item"
   ```
3. Place an order and check it after a second:
   ```bash
   ID=$(order widget 2); echo $ID
   sleep 2; status $ID
   ```

**Expected:** `COMPLETED`. The logs show the four steps for this ID:

```text
order-service      | order 04c0ee33-... created (item=widget qty=2) -> OrderCreated queued in outbox
inventory-service  | order 04c0ee33-...: reserved 2 x widget
payment-service    | order 04c0ee33-...: payment COMPLETED (amount=20.00)
order-service      | order 04c0ee33-... COMPLETED (payment ea3339d2-... succeeded)
```

**Verify** that each service recorded its part:

```bash
sql inventorydb "select * from inventory order by item"                        # widget is 2 lower
sql inventorydb "select * from reservations where order_id = '$ID'"            # 1 row, quantity 2
sql paymentdb   "select amount, status from payments where order_id = '$ID'"   # 20 | COMPLETED
sql orderdb     "select topic, published_at from outbox where key = '$ID'"     # order.events, published
```

#### Test 2: Payment failure and compensation

**Goal:** a failed payment cancels the order **and** Inventory puts the
reserved stock back (the Saga's compensating action).

1. Make payments always fail, then wait ~30s:
   ```bash
   PAYMENT_FAILURE_RATE=1 docker compose up -d payment-service
   ```
2. Note the `gadget` stock, place an order, and check it:
   ```bash
   sql inventorydb "select available_quantity from inventory where item = 'gadget'"
   ID=$(order gadget 3)
   sleep 2; status $ID
   ```

**Expected:** `CANCELLED`, and these logs:

```text
inventory-service  | order ad664fc0-...: reserved 3 x gadget
payment-service    | order ad664fc0-...: payment FAILED (simulated decline)
order-service      | order ad664fc0-... CANCELLED (payment failed: simulated payment decline)
inventory-service  | order ad664fc0-...: released 3 x gadget back to stock (payment failed)
```

**Verify:**

```bash
sql inventorydb "select available_quantity from inventory where item = 'gadget'"  # same as before
sql inventorydb "select count(*) from reservations where order_id = '$ID'"        # 0 (released)
sql paymentdb   "select status from payments where order_id = '$ID'"              # FAILED
```

Set payments back to normal when you're done:
`docker compose up -d payment-service`.

#### Test 3: Unknown item

```bash
ID=$(order Apple 1)
sleep 2; status $ID
```

**Expected:** `CANCELLED`, and Payment Service never sees the order:

```text
inventory-service  | order 38294137-...: reservation FAILED (unknown item)
order-service      | order 38294137-... CANCELLED (inventory reservation failed: unknown item)
```

#### Test 4: Not enough stock

```bash
ID=$(order widget 500)
sleep 2; status $ID
```

**Expected:** `CANCELLED` with `insufficient stock`. **Verify** the stock
didn't change and didn't go negative:

```bash
sql inventorydb "select * from inventory order by item"
```

#### Test 5: Invalid input

```bash
# Each line prints the HTTP status code
curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:8081/orders -d '{"item":""}'                       # 400
curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:8081/orders -d '{"item":"widget","quantity":0}'    # 400
curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:8081/orders -d 'not json'                          # 400
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8081/orders/00000000-0000-0000-0000-000000000000          # 404
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8081/health                                               # 200
```

---

### Part 2: Kafka

#### Test 6: Events in Kafka

**Goal:** see the actual events each step published.

```bash
for t in order.events inventory.events payment.events; do
  echo "== $t"
  docker exec kafka timeout 8 kafka-console-consumer --bootstrap-server kafka:29092 \
    --topic $t --from-beginning 2>/dev/null | grep "$ID"
done
```

**Expected** for a completed order: `OrderCreated`, then `InventoryReserved`,
then `PaymentCompleted`. For Test 2's order you'd also see `PaymentFailed`
and `InventoryReleased`.

You can see the same thing in Kafka UI: http://localhost:8080 → **Topics** →
a topic → **Messages**.

#### Test 7: Partitions and keys

**Goal:** every topic has 3 partitions, and all events for one order use
the same partition number.

1. Check the partition count:
   ```bash
   docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic order.events
   ```
   **Expected:** `PartitionCount: 3`, with one line each for partitions 0, 1 and 2.

2. Find which partition each of an order's events landed in:
   ```bash
   for t in order.events inventory.events payment.events; do
     echo "== $t"
     docker exec kafka timeout 8 kafka-console-consumer --bootstrap-server kafka:29092 \
       --topic $t --from-beginning --property print.partition=true \
       --property print.offset=true 2>/dev/null | grep "$ID" | cut -c1-80
   done
   ```
   **Expected:** the same partition number in all three topics. The
   offsets differ, because each partition counts its own messages:
   ```text
   == order.events
   Partition:2  Offset:4  {"eventId":"...","eventType":"OrderCreated",...
   == inventory.events
   Partition:2  Offset:5  {"eventId":"...","eventType":"InventoryReserved",...
   == payment.events
   Partition:2  Offset:4  {"eventId":"...","eventType":"PaymentCompleted",...
   ```

3. Check that every consumer group is caught up:
   ```bash
   docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --all-groups
   ```
   **Expected:** `LAG` is `0` on every row. Lag is how many messages are
   waiting to be read:
   ```text
   GROUP                          TOPIC         PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
   inventory-service-order-events order.events  0          7               7               0
   inventory-service-order-events order.events  1          6               6               0
   inventory-service-order-events order.events  2          4               4               0
   ```

#### Test 8: Consumers share partitions

**Goal:** see a consumer group split partitions between its members. This
uses a throwaway group called `demo`, so the services aren't affected.

1. In terminal A:
   ```bash
   docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:29092 \
     --topic order.events --group demo --property print.partition=true
   ```
2. In terminal B, run the same command.
3. In terminal C, see who owns which partition:
   ```bash
   docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group demo
   ```
   **Expected:** the 3 partitions are split between two `CONSUMER-ID`s.
4. Create a few orders. **Expected:** each message appears in only one
   terminal.
5. Stop terminal B with `Ctrl+C` and repeat step 3. **Expected:** after a
   few seconds (the **rebalance**), terminal A owns all 3 partitions.

**Scaling a real service** works the same way, but first remove
`container_name: inventory-service` from
[docker-compose.yml](../docker-compose.yml), because two containers can't
share a name. Then `docker compose up -d --scale inventory-service=3` gives
each copy one partition.

---

### Part 3: Reliability

#### Test 9: Kafka is down (outbox)

**Goal:** orders are accepted while Kafka is down, and finish once it's back.

```bash
docker compose stop kafka

ID=$(order gadget 1); echo $ID      # still accepted (201)
status $ID                          # PENDING

# The event is waiting in the outbox:
sql orderdb "select topic, published_at from outbox where key = '$ID'"   # published_at is empty

docker compose start kafka
```

**Expected:** while Kafka is down, Order Service logs
`outbox relay: publish 1 events: ... (will retry)` every half second.
About 30-40 seconds after Kafka starts, the order becomes `COMPLETED` or
`CANCELLED`, and `published_at` is filled in.

#### Test 10: A consumer is down

**Goal:** a stopped service picks up where it left off, and nothing is lost.

```bash
docker compose stop inventory-service
ID=$(order widget 1)
sleep 3; status $ID                                   # PENDING

docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 \
  --describe --group inventory-service-order-events   # LAG = 1 on one partition

docker compose start inventory-service
```

**Expected:** the order stays `PENDING` while Inventory is down, and the lag
shows the waiting message:

```text
GROUP                          TOPIC         PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
inventory-service-order-events order.events  2          4               5               1
```

Within ~30 seconds of starting it again, the order finishes and `LAG` is back to `0`.

#### Test 11: Broken messages go to the DLQ

**Goal:** a message that can never be processed is parked instead of
blocking or crashing the consumer.

```bash
# Not JSON at all
echo 'this is not json' | docker exec -i kafka \
  kafka-console-producer --bootstrap-server kafka:29092 --topic order.events

# Valid envelope, but quantity is a string
echo '{"eventId":"5b2a7a52-0000-4000-8000-000000000001","eventType":"OrderCreated","aggregateId":"x","payload":{"quantity":"lots"}}' \
  | docker exec -i kafka kafka-console-producer --bootstrap-server kafka:29092 --topic order.events
```

**Expected:** both go straight to the DLQ with no retries, because retrying
can't fix them:

```text
inventory-service | moved order.events/0@3 to inventory-service-order-events.dlq: decode event: invalid character 'h' in literal true (expecting 'r')
inventory-service | moved order.events/0@4 to inventory-service-order-events.dlq: bad OrderCreated payload: json: cannot unmarshal string into Go struct field orderCreatedPayload.quantity of type int
```

**Verify** they're in the DLQ with the reason in the headers:

```bash
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic inventory-service-order-events.dlq --from-beginning --property print.headers=true
```

```text
dlq-original-topic:order.events,dlq-original-partition:0,dlq-original-offset:3,dlq-error:decode event: ...,dlq-failed-at:2026-09-23T21:45:19Z	this is not json
```

Then place a normal order to confirm the consumer is still working.

#### Test 12: Duplicate messages are skipped

**Goal:** the same event delivered twice is applied only once.

1. Note the stock:
   ```bash
   sql inventorydb "select * from inventory order by item"
   ```
2. Copy the first message from `order.events` (with its key) and publish it again:
   ```bash
   msg=$(docker exec kafka timeout 6 kafka-console-consumer --bootstrap-server kafka:29092 \
     --topic order.events --from-beginning --max-messages 1 \
     --property print.key=true --property key.separator='|')

   echo "$msg" | docker exec -i kafka kafka-console-producer --bootstrap-server kafka:29092 \
     --topic order.events --property parse.key=true --property key.separator='|'
   ```

**Expected:**

```text
inventory-service | skipping duplicate OrderCreated 0d14f22d-21bd-4aff-9db0-7a48a92a66ab
```

**Verify** the stock is unchanged, since nothing was reserved twice.

#### Test 13: Temporary errors are retried

**Goal:** a short database problem is retried and then succeeds. We fake
the problem by hiding the `inventory` table for about 2 seconds.

```bash
sql inventorydb "ALTER TABLE inventory RENAME TO inventory_hidden"
ID=$(order widget 1)
sleep 2
sql inventorydb "ALTER TABLE inventory_hidden RENAME TO inventory"
sleep 3; status $ID
```

**Expected:** two failed attempts, then success (the waits between tries are 1s, then 2s):

```text
inventory-service | handling OrderCreated 07643c15-... failed (attempt 1/3): check stock: ERROR: relation "inventory" does not exist ...
inventory-service | handling OrderCreated 07643c15-... failed (attempt 2/3): check stock: ERROR: relation "inventory" does not exist ...
inventory-service | order 61d6ec1b-...: reserved 1 x widget
```

The order ends up `COMPLETED` or `CANCELLED` as usual.

#### Test 14: Retries run out, then replay from the DLQ

**Goal:** a message that fails all 3 attempts is parked on the DLQ, and
replaying it after the problem is fixed finishes the order.

1. Hide the table for longer than the ~3 seconds of retries:
   ```bash
   sql inventorydb "ALTER TABLE inventory RENAME TO inventory_hidden"
   ID=$(order widget 1)
   sleep 6
   sql inventorydb "ALTER TABLE inventory_hidden RENAME TO inventory"
   status $ID          # PENDING, and it will stay that way
   ```
   **Expected:**
   ```text
   inventory-service | handling OrderCreated a983131d-... failed (attempt 1/3): ...
   inventory-service | handling OrderCreated a983131d-... failed (attempt 2/3): ...
   inventory-service | handling OrderCreated a983131d-... failed (attempt 3/3): ...
   inventory-service | moved order.events/1@6 to inventory-service-order-events.dlq: check stock: ERROR: relation "inventory" does not exist (SQLSTATE 42P01)
   ```

2. The table is back, so replay that order's message from the DLQ to
   `order.events`. The key is the order ID, so `grep` picks out just this
   order's message:
   ```bash
   docker exec kafka timeout 8 kafka-console-consumer --bootstrap-server kafka:29092 \
     --topic inventory-service-order-events.dlq --from-beginning \
     --property print.key=true --property key.separator='|' 2>/dev/null \
     | grep "^$ID|" \
     | docker exec -i kafka kafka-console-producer --bootstrap-server kafka:29092 \
         --topic order.events --property parse.key=true --property key.separator='|'

   sleep 3; status $ID
   ```
   **Expected:** the order is now `COMPLETED` or `CANCELLED`.

This is why a DLQ matters: nothing was lost, and once the cause was fixed
the order finished normally.

#### Test 15: Data survives a restart

```bash
docker compose down        # no -v
docker compose up -d
```

Wait ~30 seconds, then:

- `status <id>` for an older order still works (Postgres volume);
- the topics and their messages are still there (Kafka volume):
  ```bash
  docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
  ```
- a new order completes as usual.

---

### Checking that the code compiles

There are no unit tests yet. To check the Go code without Docker, run this
in each service folder:

```bash
cd services/order-service   # or inventory-service, payment-service
gofmt -l .                  # prints nothing if formatting is fine
go vet .                    # prints nothing if there are no problems
go build -buildvcs=false -o /dev/null .
```

`-buildvcs=false` is only needed while Git reports `dubious ownership`
(see [Troubleshooting](#troubleshooting)).

### Resetting after testing

```bash
# Payments back to the .env failure rate
docker compose up -d payment-service

# Stock back to the starting values
sql inventorydb "update inventory set available_quantity = 100 where item = 'widget';
                 update inventory set available_quantity = 50 where item = 'gadget';"

# Or start completely fresh: deletes ALL Kafka and Postgres data
docker compose down -v && docker compose up -d --build
```

## Working with dead-letter queues

**List them:**

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list | grep dlq
```

**Read one, including why each message failed:**

```bash
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic inventory-service-order-events.dlq --from-beginning \
  --property print.headers=true
```

Or open Kafka UI (http://localhost:8080) → Topics → the `.dlq` topic →
Messages. The headers are shown on each message.

**Replay a message** once the bug that broke it is fixed: publish its value
back to the topic in its `dlq-original-topic` header, with the same key.
[Test 14](#test-14-retries-run-out-then-replay-from-the-dlq) shows the
exact command. Replaying is safe even if the message was partly processed,
because duplicates are skipped.

## Connecting to the databases

There's one Postgres server with three databases: `orderdb`, `inventorydb`,
`paymentdb`. User `appuser`, password `apppass` (from `.env`), port `5432`.

**psql:** always pass `-d`. Without it, Postgres assumes a database named
after the user (`appuser`), which doesn't exist.

```bash
docker compose exec postgres psql -U appuser -d orderdb
```

**GUI clients (HeidiSQL, DBeaver, ...):** host `127.0.0.1`, port `5432`,
and fill in the **database** field. For PostgreSQL, HeidiSQL opens one
database per session, so `orderdb;inventorydb;paymentdb` doesn't work.
Create one session and clone it for each database. Tables are under the
`public` schema.

Tables each service owns:

| Database | Business tables | Messaging tables |
|---|---|---|
| `orderdb` | `orders` | `outbox`, `processed_events` |
| `inventorydb` | `inventory`, `reservations` | `outbox`, `processed_events` |
| `paymentdb` | `payments` | `outbox`, `processed_events` |

## Configuration

| Variable | Where | Meaning |
|---|---|---|
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | `.env` | Postgres login, and the first database it creates (`orderdb`). The other two come from [infra/postgres/init.sql](../infra/postgres/init.sql) |
| `PAYMENT_FAILURE_RATE` | `.env` | Share of payments that fail on purpose. `0` = never, `0.3` = default, `1` = always. Apply with `docker compose up -d payment-service` |
| `DATABASE_URL`, `KAFKA_BROKER`, `PORT` | `docker-compose.yml` | Per-service settings. Required ones are read with `mustEnv`, which stops the service at startup with `missing required environment variable: ...` if one is missing |
| `topicPartitions` | each `events.go` | Partitions per topic (3). Keep it the same in all three services |
| `maxHandleAttempts` | `messaging.go` | Tries before a message goes to the DLQ (3) |

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Postgres logs `FATAL: database "appuser" does not exist` | A client connected without naming a database, so Postgres tried one named after the user | Pass a database: `psql -d orderdb`, or fill in the database field in your GUI client. The healthcheck already uses `-d ${POSTGRES_DB}` |
| Order `CANCELLED` with `unknown item` | The item isn't in the `inventory` table | Order `widget` or `gadget`, or `INSERT INTO inventory (item, available_quantity) VALUES ('Apple', 20);` in `inventorydb` |
| Order `CANCELLED` with `simulated payment decline` | Working as intended: about 30% of payments fail | Set `PAYMENT_FAILURE_RATE=0` in `.env` and run `docker compose up -d payment-service` |
| Order stays `PENDING` | A service is down, or Kafka is down and events are waiting in the outbox | `docker compose ps`, then `docker compose logs <service>`. Look for `outbox relay` or `moved ... to ... .dlq` lines |
| Consumer group shows `rebalancing` | Partitions or consumers just changed | Wait a few seconds |
| Code changes have no effect | The container still runs the old build | `docker compose up -d --build <service>` |
| `git` says `detected dubious ownership` and VS Code shows no changes | The project is on an NTFS drive, not owned by your Linux user | `git config --global --add safe.directory /mnt/A614AE7D14AE4FDB/Microservices` |

## Command cheat sheet

```bash
# Build and start everything (Kafka, Kafka UI, Postgres, Redis, and the 3 Go services)
docker compose up -d --build

# Create an order
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"item": "widget", "quantity": 2}'

# Check its status (replace <id> with the id from the response above)
curl http://localhost:8081/orders/<id>

# Watch the services react live
docker compose logs -f order-service inventory-service payment-service

# Inspect the data directly
docker compose exec postgres psql -U appuser -d orderdb -c "select * from orders;"

# Topics, partitions and consumer groups
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --all-groups

# Kafka UI in the browser
xdg-open http://localhost:8080

# Stop everything (data is kept)
docker compose down

# Stop everything AND delete all Kafka and Postgres data
docker compose down -v
```
