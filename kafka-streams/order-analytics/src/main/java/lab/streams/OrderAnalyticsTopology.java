package lab.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

/**
 * The stream processing logic. This is the file to read and change while
 * practicing.
 *
 * A Kafka Streams app is a TOPOLOGY: a graph of steps that records flow
 * through. You describe the graph with the DSL (stream, filter, groupBy,
 * count, join, to...), and Kafka Streams runs it: one task per input
 * partition, with state kept in local RocksDB stores that are backed up to
 * Kafka "changelog" topics.
 *
 * <pre>
 *   order.events ──filter(OrderCreated)──┬── A: filter(quantity >= 5) ─────────────▶ analytics.large-orders
 *                                        │
 *                                        ├── groupBy(item) ─┬─ B: count ───────────▶ analytics.item-order-counts
 *                                        │                  └─ C: 1-min windows ───▶ analytics.item-orders-per-minute
 *                                        │
 *                                        └── D: join ◀── payment.events ──filter(PaymentCompleted)
 *                                               └──────────────────────────────────▶ analytics.paid-orders
 * </pre>
 */
public final class OrderAnalyticsTopology {

    // Input topics, written by the Go services.
    public static final String ORDER_EVENTS = "order.events";
    public static final String PAYMENT_EVENTS = "payment.events";

    // Output topics, written by this app.
    public static final String LARGE_ORDERS = "analytics.large-orders";
    public static final String ITEM_ORDER_COUNTS = "analytics.item-order-counts";
    public static final String ITEM_ORDERS_PER_MINUTE = "analytics.item-orders-per-minute";
    public static final String PAID_ORDERS = "analytics.paid-orders";

    public static final List<String> INPUT_TOPICS = List.of(ORDER_EVENTS, PAYMENT_EVENTS);
    public static final List<String> OUTPUT_TOPICS =
            List.of(LARGE_ORDERS, ITEM_ORDER_COUNTS, ITEM_ORDERS_PER_MINUTE, PAID_ORDERS);

    // State store names. Naming them makes the internal topics readable in
    // Kafka UI and lets App.java query the counts over HTTP.
    public static final String ITEM_COUNTS_STORE = "item-order-counts-store";
    public static final String ITEM_COUNTS_PER_MINUTE_STORE = "item-orders-per-minute-store";

    static final int LARGE_ORDER_QUANTITY = 5;
    static final Duration COUNT_WINDOW = Duration.ofMinutes(1);
    static final Duration PAYMENT_JOIN_WINDOW = Duration.ofMinutes(5);

    private static final Serde<String> STRING = Serdes.String();
    private static final Serde<Long> LONG = Serdes.Long();
    private static final Serde<JsonNode> JSON = JsonSerde.jsonNode();

    private OrderAnalyticsTopology() {}

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        // --- Sources -----------------------------------------------------
        // A KStream is an unbounded sequence of independent events, like an
        // append-only log: every record is a new fact.
        //
        // Key = order ID (the Go services' Kafka key), value = the JSON
        // event envelope {eventId, eventType, aggregateId, timestamp, payload}.
        Consumed<String, JsonNode> consumed =
                Consumed.with(STRING, JSON).withTimestampExtractor(new EventTimeExtractor());

        KStream<String, JsonNode> orderCreated = builder
                .stream(ORDER_EVENTS, consumed)
                .filter((orderId, event) -> isType(event, "OrderCreated"));

        KStream<String, JsonNode> paymentCompleted = builder
                .stream(PAYMENT_EVENTS, consumed)
                .filter((orderId, event) -> isType(event, "PaymentCompleted"));

        // --- Exercise A: stateless filter + map --------------------------
        // Stateless means each record is handled on its own: nothing is
        // remembered between records, so no state store is needed.
        orderCreated
                .filter((orderId, event) -> payload(event).path("quantity").asInt() >= LARGE_ORDER_QUANTITY)
                .mapValues(OrderAnalyticsTopology::payload) // drop the envelope, keep the order
                .peek((orderId, order) -> System.out.printf("[A] large order %s: %s%n", orderId, order))
                .to(LARGE_ORDERS, Produced.with(STRING, JSON));

        // --- Re-key by item ---------------------------------------------
        // To count per item, all orders for the same item must reach the
        // same task. They are currently spread across partitions by ORDER
        // ID, so groupBy with a new key makes Kafka Streams write them to an
        // internal REPARTITION topic keyed by item, and read them back.
        // (Look for "order-analytics-orders-by-item-repartition" in Kafka UI.)
        KGroupedStream<String, JsonNode> ordersByItem = orderCreated.groupBy(
                (orderId, event) -> payload(event).path("item").asText(),
                Grouped.with("orders-by-item", STRING, JSON));

        // --- Exercise B: stateful count -> KTable -------------------------
        // A KTable is a changelog: for each key, only the LATEST value
        // matters (like a database table). Each new order for an item
        // updates that item's row. The table lives in a local state store,
        // backed up to a "-changelog" topic so it survives restarts.
        KTable<String, Long> itemCounts = ordersByItem.count(
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(ITEM_COUNTS_STORE)
                        .withKeySerde(STRING)
                        .withValueSerde(LONG));

        // Turning a table back into a stream emits every update:
        // widget=1, gadget=1, widget=2, ...
        itemCounts
                .toStream()
                .peek((item, count) -> System.out.printf("[B] %s total orders: %d%n", item, count))
                .to(ITEM_ORDER_COUNTS, Produced.with(STRING, LONG));

        // --- Exercise C: windowed aggregation ---------------------------
        // Same count, but reset every minute (a TUMBLING window: fixed size,
        // no overlap). The window is chosen by EVENT time (see
        // EventTimeExtractor), not by when the record is processed.
        // Grace = how long a window still accepts late (out-of-order)
        // events after it ends.
        ordersByItem
                .windowedBy(TimeWindows.ofSizeAndGrace(COUNT_WINDOW, Duration.ofSeconds(30)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(ITEM_COUNTS_PER_MINUTE_STORE)
                        .withKeySerde(STRING)
                        .withValueSerde(LONG))
                .toStream()
                // The key is now Windowed<String> (item + window). Flatten it
                // to a readable string such as "widget@2026-09-25T10:15:00Z".
                .map((window, count) ->
                        KeyValue.pair(window.key() + "@" + window.window().startTime(), count))
                .peek((itemWindow, count) -> System.out.printf("[C] %s: %d orders%n", itemWindow, count))
                .to(ITEM_ORDERS_PER_MINUTE, Produced.with(STRING, LONG));

        // --- Exercise D: stream-stream join ------------------------------
        // Matches an OrderCreated with the PaymentCompleted for the same
        // order ID, if both happen within PAYMENT_JOIN_WINDOW of each other.
        // Both sides are buffered in window stores until a match arrives.
        //
        // A join needs both topics CO-PARTITIONED: the same partition count
        // and the same key in the same partition number. Both are keyed by
        // order ID with 3 partitions, and both are written by kafka-go's Hash
        // balancer, so this holds and no repartition is needed. (kafka-go
        // hashes with FNV-1a while Java uses murmur2, which is fine only
        // because BOTH sides use the same one.)
        orderCreated
                .join(paymentCompleted,
                        OrderAnalyticsTopology::paidOrder,
                        JoinWindows.ofTimeDifferenceAndGrace(PAYMENT_JOIN_WINDOW, Duration.ofMinutes(1)),
                        StreamJoined.with(STRING, JSON, JSON)
                                .withName("orders-join-payments")
                                .withStoreName("orders-join-payments"))
                .peek((orderId, paid) -> System.out.printf("[D] paid order %s: %s%n", orderId, paid))
                .to(PAID_ORDERS, Produced.with(STRING, JSON));

        return builder.build();
    }

    /** Combines the two matched events into one "paid order" record. */
    static JsonNode paidOrder(JsonNode orderEvent, JsonNode paymentEvent) {
        JsonNode order = payload(orderEvent);
        JsonNode payment = payload(paymentEvent);
        ObjectNode out = JsonSerde.MAPPER.createObjectNode();
        out.put("orderId", order.path("orderId").asText());
        out.put("item", order.path("item").asText());
        out.put("quantity", order.path("quantity").asInt());
        out.put("paymentId", payment.path("paymentId").asText());
        out.put("amount", payment.path("amount").asDouble());
        return out;
    }

    static boolean isType(JsonNode event, String eventType) {
        return event != null && eventType.equals(event.path("eventType").asText());
    }

    static JsonNode payload(JsonNode event) {
        return event.path("payload");
    }
}
