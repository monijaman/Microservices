# Order Analytics — Kafka Streams practice

A Java [Kafka Streams](https://kafka.apache.org/documentation/streams/) app that reads the events the Go services already publish and turns them into analytics topics. This is Exercise 10 (section 18) of [MICROSERVICES_PRACTICE.md](../../MICROSERVICES_PRACTICE.md).

You **don't need Java installed**. Everything builds and runs in Docker.

It is deliberately a **read-only downstream consumer**: it never calls the
Go services and never writes to `orderdb`, `inventorydb`, or `paymentdb`.
If this app is stopped, orders still complete normally; only the derived
analytics stop updating until the app catches up.

## What it does

```text
order.events ──filter(OrderCreated)──┬── A: filter(quantity >= 5) ──────────▶ analytics.large-orders
                                     ├── groupBy(item) ─┬─ B: count ────────▶ analytics.item-order-counts
                                     │                  └─ C: 1-min window ─▶ analytics.item-orders-per-minute
                                     └── D: join ◀── payment.events (PaymentCompleted)
                                            └───────────────────────────────▶ analytics.paid-orders
```

| Exercise | Kafka Streams concepts |
|---|---|
| A. Large orders | `KStream`, `filter`, `mapValues`, `to` (stateless) |
| B. Orders per item | `groupBy` (repartition), `count`, `KTable`, state store, changelog topic |
| C. Orders per item per minute | `windowedBy(TimeWindows)`, grace period, event time (`EventTimeExtractor`) |
| D. Paid orders | stream-stream `join`, `JoinWindows`, co-partitioning |
| `/counts` endpoint | interactive queries (reading a state store directly) |

## Inputs, outputs, and state

Every input record is the Go services JSON event envelope. The Kafka key is
the order ID, which matters for ordering and the join.

| Topic | Direction | Key | Value or meaning |
|---|---|---|---|
| `order.events` | input | order ID | Only `OrderCreated` records are used |
| `payment.events` | input | order ID | Only `PaymentCompleted` records are used |
| `analytics.large-orders` | output | order ID | Order payloads where `quantity >= 5` |
| `analytics.item-order-counts` | output | item | A new `Long` value whenever that item total changes |
| `analytics.item-orders-per-minute` | output | item plus window start | A new `Long` value whenever a one-minute window changes |
| `analytics.paid-orders` | output | order ID | Combined order and successful-payment JSON |

Kafka Streams also creates internal topics with the `order-analytics-`
prefix. A `repartition` topic moves records from the order-ID key to the item
key before counting. `changelog` topics back up local RocksDB state, allowing
state to be rebuilt after a restart. Do not treat internal topics as a public API.

## How each branch works

- **A — large orders:** a stateless filter; no state store is needed.
- **B — total orders by item:** `groupBy(item)` changes the key, then
  `count()` builds a `KTable`, the latest count for each item.
- **C — orders per minute:** fixed non-overlapping one-minute windows use
  the event timestamp and accept events up to 30 seconds late.
- **D — paid orders:** an inner join emits only when `OrderCreated` and
  `PaymentCompleted` share an order-ID key and occur within five minutes.

The sources have three partitions and use the same order-ID key, so matching
records reach the same Streams task for the join.

## Files

| File | What's in it |
|---|---|
| `src/main/java/lab/streams/OrderAnalyticsTopology.java` | **The processing logic. Start here.** |
| `src/main/java/lab/streams/App.java` | Config, topic creation, startup, `/counts` HTTP endpoint |
| `src/main/java/lab/streams/EventTimeExtractor.java` | Uses the event's own `timestamp` as its time |
| `src/main/java/lab/streams/JsonSerde.java` | JSON ⇄ bytes for keys/values |
| `src/test/java/lab/streams/OrderAnalyticsTopologyTest.java` | In-memory tests with `TopologyTestDriver` |

## Run it

From the repo root:

```bash
docker compose up -d --build
docker compose logs -f order-analytics      # watch [A] [B] [C] [D] lines
```

On startup the app prints `topology.describe()`: every sub-topology, processor, store and internal topic. Read it once. It shows what the DSL built.

Send some orders from another terminal:

```bash
for q in 2 7 1 6 3; do
  curl -s -X POST http://localhost:8081/orders -H "Content-Type: application/json" \
    -d "{\"item\":\"widget\",\"quantity\":$q}"; echo
done
```

Then look at the results:

```bash
curl -s localhost:8082/counts           # {"widget":5,...}  (interactive query)
curl -s localhost:8082/counts/widget

docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic analytics.item-order-counts --from-beginning \
  --property print.key=true --value-deserializer org.apache.kafka.common.serialization.LongDeserializer
```

Or open Kafka UI at http://localhost:8080. Besides the `analytics.*` topics, look at the internal topics Kafka Streams created itself: `order-analytics-orders-by-item-repartition`, `order-analytics-item-order-counts-store-changelog`, and so on.

## Test it (fast, no Kafka needed)

```bash
cd kafka-streams/order-analytics
docker run --rm -v "$PWD":/src -v order-analytics-m2:/root/.m2 -w /src \
  maven:3.9-eclipse-temurin-21 mvn -q test
```

The `order-analytics-m2` volume caches Maven downloads, so later runs are quick. The recommended practice loop: change the topology, add a test, run it.

## Rebuild after a change

```bash
docker compose up -d --build order-analytics
```

To **reprocess everything from scratch** (empty state, re-read all input):

```bash
docker compose stop order-analytics
docker exec kafka kafka-streams-application-reset --bootstrap-server kafka:29092 \
  --application-id order-analytics --input-topics order.events,payment.events
docker compose up -d --force-recreate order-analytics
```

## Interactive queries, configuration, and troubleshooting

`GET /counts` returns every item currently held by this instance count store.
`GET /counts/widget` returns one item, or `{"widget":0}` when absent. While
Streams is starting, rebalancing, or rebuilding its state, either returns `503`.
With one Compose instance all state is local; multiple instances must locate
the owner of a key with `queryMetadataForKey` before serving a query.

| Setting | Default | Meaning |
|---|---|---|
| `KAFKA_BROKER` | `localhost:9092` outside Docker; `kafka:29092` in Compose | Broker address |
| `HTTP_PORT` | `8082` | Interactive-query HTTP port |
| `STATE_DIR` | `/tmp/kafka-streams` | Local RocksDB state directory |
| `application.id` | `order-analytics` | Consumer group and internal-topic prefix |

```bash
docker compose ps order-analytics
docker compose logs --tail=100 order-analytics
curl -i http://localhost:8082/counts
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group order-analytics
```

| Symptom | Check or fix |
|---|---|
| Counts are empty | Create an order, then inspect `order.events` and the `order-analytics` group lag |
| A paid order is missing | Failed/cancelled orders do not join; inspect both input topics and verify matching order-ID keys |
| Code change is not visible | Run `docker compose up -d --build order-analytics` |
| Reset reports a running app | Stop `order-analytics`, reset, then recreate it |

## Practice exercises (try these yourself)

1. **Filter on money:** write `analytics.high-value-payments` for `PaymentCompleted` with `amount > 100`.
2. **Aggregate, not just count:** total quantity ordered per item with `groupBy(...).aggregate(...)` or `reduce`.
3. **Branching:** split `payment.events` into `analytics.payments-ok` and `analytics.payments-failed` with `split().branch(...)`.
4. **KTable:** build an "order status" table: latest `eventType` per order ID across `order.events` and `payment.events` (`builder.table(...)` or `groupByKey().reduce(...)`).
5. **Failed-payment join:** `leftJoin` orders with `PaymentFailed` events, and emit orders that had no payment at all.
6. **GlobalKTable:** put item prices on a compacted `catalog.prices` topic, read it with `builder.globalTable(...)`, and enrich each order with its price. No co-partitioning is needed, because every instance has the full table.
7. **One result per window:** add `.suppress(Suppressed.untilWindowCloses(unbounded()))` to exercise C, so each minute emits one final count instead of every update.
8. **Hopping and session windows:** replace the tumbling window with `TimeWindows.ofSizeAndGrace(5m, ...).advanceBy(1m)`, then with `SessionWindows`.
9. **Exactly-once:** set `processing.guarantee=exactly_once_v2`. On this single-broker cluster, first add `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1` and `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1` to the `kafka` service.
10. **Scale out:** run two instances (`docker compose up -d --scale` needs `container_name` and the port removed first), and watch the partitions get split between them.

## Interview checklist

- `KStream` vs `KTable` vs `GlobalKTable`
- Stateless vs stateful operations; where state lives (RocksDB + changelog topic)
- Why `groupBy` with a new key creates a repartition topic, and why `groupByKey` doesn't
- Co-partitioning requirements for joins; stream-stream vs stream-table vs table-table joins
- Event time vs processing time; windows (tumbling, hopping, session); grace periods
- At-least-once vs exactly-once (`exactly_once_v2`)
- How `application.id` relates to consumer groups and internal topic names
- Scaling: max parallelism = number of input partitions (3 here)
