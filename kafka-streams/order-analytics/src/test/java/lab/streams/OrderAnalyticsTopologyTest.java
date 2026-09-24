package lab.streams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * TopologyTestDriver runs the topology in memory: no Kafka, no Docker, and
 * it finishes in milliseconds. You pipe records into input topics and read
 * what came out of the output topics. This is the fastest way to try out a
 * change to OrderAnalyticsTopology.
 */
class OrderAnalyticsTopologyTest {

    private static final Instant T0 = Instant.parse("2026-09-25T10:00:00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, JsonNode> orders;
    private TestInputTopic<String, JsonNode> payments;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-analytics-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        driver = new TopologyTestDriver(OrderAnalyticsTopology.build(), props);

        orders = driver.createInputTopic(OrderAnalyticsTopology.ORDER_EVENTS,
                Serdes.String().serializer(), JsonSerde.jsonNode().serializer());
        payments = driver.createInputTopic(OrderAnalyticsTopology.PAYMENT_EVENTS,
                Serdes.String().serializer(), JsonSerde.jsonNode().serializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void exerciseA_onlyLargeOrdersAreForwarded() {
        orders.pipeInput("o1", orderCreated("o1", "widget", 2, T0));
        orders.pipeInput("o2", orderCreated("o2", "widget", 5, T0));
        orders.pipeInput("o3", orderCreated("o3", "gadget", 9, T0));

        List<String> largeOrderIds = output(OrderAnalyticsTopology.LARGE_ORDERS)
                .readKeyValuesToList().stream().map(kv -> kv.key).toList();

        assertEquals(List.of("o2", "o3"), largeOrderIds);
    }

    @Test
    void exerciseB_countsOrdersPerItem() {
        orders.pipeInput("o1", orderCreated("o1", "widget", 1, T0));
        orders.pipeInput("o2", orderCreated("o2", "gadget", 1, T0));
        orders.pipeInput("o3", orderCreated("o3", "widget", 1, T0));
        // Other event types on the same topic are ignored.
        orders.pipeInput("o1", event("OrderCancelled", "o1", T0, JsonSerde.MAPPER.createObjectNode()));

        // Every update to the KTable comes out as a new record.
        TestOutputTopic<String, Long> counts = driver.createOutputTopic(OrderAnalyticsTopology.ITEM_ORDER_COUNTS,
                Serdes.String().deserializer(), Serdes.Long().deserializer());
        assertEquals(List.of(
                KeyValue.pair("widget", 1L),
                KeyValue.pair("gadget", 1L),
                KeyValue.pair("widget", 2L)), counts.readKeyValuesToList());

        // The state store holds only the latest value per key.
        KeyValueStore<String, Long> store = driver.getKeyValueStore(OrderAnalyticsTopology.ITEM_COUNTS_STORE);
        assertEquals(2L, store.get("widget"));
        assertEquals(1L, store.get("gadget"));
    }

    @Test
    void exerciseC_countsPerOneMinuteWindow() {
        orders.pipeInput("o1", orderCreated("o1", "widget", 1, T0.plusSeconds(10)));
        orders.pipeInput("o2", orderCreated("o2", "widget", 1, T0.plusSeconds(50)));
        orders.pipeInput("o3", orderCreated("o3", "widget", 1, T0.plusSeconds(70))); // next minute

        TestOutputTopic<String, Long> perMinute = driver.createOutputTopic(
                OrderAnalyticsTopology.ITEM_ORDERS_PER_MINUTE,
                Serdes.String().deserializer(), Serdes.Long().deserializer());
        assertEquals(List.of(
                KeyValue.pair("widget@2026-09-25T10:00:00Z", 1L),
                KeyValue.pair("widget@2026-09-25T10:00:00Z", 2L),
                KeyValue.pair("widget@2026-09-25T10:01:00Z", 1L)), perMinute.readKeyValuesToList());
    }

    @Test
    void exerciseD_joinsOrderWithItsPayment() {
        orders.pipeInput("o1", orderCreated("o1", "gadget", 3, T0));
        payments.pipeInput("o1", paymentCompleted("o1", "p1", 75.0, T0.plusSeconds(2)));

        List<KeyValue<String, JsonNode>> paid = output(OrderAnalyticsTopology.PAID_ORDERS).readKeyValuesToList();

        assertEquals(1, paid.size());
        JsonNode row = paid.get(0).value;
        assertEquals("gadget", row.get("item").asText());
        assertEquals(3, row.get("quantity").asInt());
        assertEquals("p1", row.get("paymentId").asText());
        assertEquals(75.0, row.get("amount").asDouble());
    }

    @Test
    void exerciseD_noMatchOutsideTheJoinWindow() {
        orders.pipeInput("o1", orderCreated("o1", "widget", 1, T0));
        payments.pipeInput("o1", paymentCompleted("o1", "p1", 10.0, T0.plus(
                OrderAnalyticsTopology.PAYMENT_JOIN_WINDOW).plusSeconds(1)));
        // A payment for an order the stream never saw doesn't match either.
        payments.pipeInput("o2", paymentCompleted("o2", "p2", 10.0, T0));

        assertTrue(output(OrderAnalyticsTopology.PAID_ORDERS).isEmpty());
    }

    // --- helpers: build events shaped like the Go services' envelope --------

    private TestOutputTopic<String, JsonNode> output(String topic) {
        return driver.createOutputTopic(topic, Serdes.String().deserializer(), JsonSerde.jsonNode().deserializer());
    }

    private static JsonNode orderCreated(String orderId, String item, int quantity, Instant at) {
        ObjectNode payload = JsonSerde.MAPPER.createObjectNode()
                .put("orderId", orderId).put("item", item).put("quantity", quantity);
        return event("OrderCreated", orderId, at, payload);
    }

    private static JsonNode paymentCompleted(String orderId, String paymentId, double amount, Instant at) {
        ObjectNode payload = JsonSerde.MAPPER.createObjectNode()
                .put("orderId", orderId).put("paymentId", paymentId).put("amount", amount);
        return event("PaymentCompleted", orderId, at, payload);
    }

    private static JsonNode event(String type, String orderId, Instant at, JsonNode payload) {
        ObjectNode event = JsonSerde.MAPPER.createObjectNode()
                .put("eventId", orderId + "-" + type)
                .put("eventType", type)
                .put("aggregateId", orderId)
                .put("timestamp", at.toString());
        event.set("payload", payload);
        return event;
    }
}
