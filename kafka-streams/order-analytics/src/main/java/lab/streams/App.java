package lab.streams;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/**
 * Starts the Kafka Streams app, plus a tiny HTTP server for querying its
 * state. The processing logic itself is in {@link OrderAnalyticsTopology}.
 */
public final class App {

    private static final int PARTITIONS = 3; // same as topicPartitions in the Go services

    public static void main(String[] args) throws Exception {
        String broker = env("KAFKA_BROKER", "localhost:9092");
        int httpPort = Integer.parseInt(env("HTTP_PORT", "8082"));

        Properties props = new Properties();
        // application.id is the consumer group ID AND the prefix of every
        // internal topic (repartition + changelog). Changing it starts the
        // app from scratch with empty state.
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-analytics");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        // Skip (and log) a record that can't be deserialized instead of
        // crashing on it. The Go services use a DLQ for the same problem.
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);
        // Single-broker dev cluster: internal topics can only have 1 copy.
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        // Learning-friendly settings: emit every KTable update right away
        // instead of caching and de-duplicating them for up to 30s. In
        // production you usually keep the cache for less output traffic.
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        // Where local RocksDB state stores live on disk.
        props.put(StreamsConfig.STATE_DIR_CONFIG, env("STATE_DIR", "/tmp/kafka-streams"));

        ensureTopics(broker);

        Topology topology = OrderAnalyticsTopology.build();
        // Prints the sub-topologies, processors, stores and internal topics.
        // Reading this output is a great way to see what the DSL built.
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.setStateListener((newState, oldState) ->
                System.out.printf("streams state: %s -> %s%n", oldState, newState));
        streams.setUncaughtExceptionHandler(e -> {
            System.err.println("stream thread crashed: " + e);
            return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        HttpServer http = startHttp(streams, httpPort);

        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            http.stop(0);
            streams.close(Duration.ofSeconds(10));
            stopped.countDown();
        }));

        streams.start();
        stopped.await();
    }

    /**
     * Creates the input and output topics with 3 partitions if they don't
     * exist yet (the Go services do the same for their topics). Kafka
     * Streams creates its own internal topics itself.
     */
    private static void ensureTopics(String broker) throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        try (Admin admin = Admin.create(adminProps)) {
            Set<String> existing = admin.listTopics().names().get();
            List<NewTopic> missing = new ArrayList<>();
            List<String> wanted = new ArrayList<>(OrderAnalyticsTopology.INPUT_TOPICS);
            wanted.addAll(OrderAnalyticsTopology.OUTPUT_TOPICS);
            for (String topic : wanted) {
                if (!existing.contains(topic)) missing.add(new NewTopic(topic, PARTITIONS, (short) 1));
            }
            if (missing.isEmpty()) return;
            try {
                admin.createTopics(missing).all().get();
            } catch (ExecutionException e) {
                // A Go service may have created one of them at the same moment.
                if (!(e.getCause() instanceof TopicExistsException)) throw e;
            }
        }
    }

    /**
     * INTERACTIVE QUERIES: read a state store directly, without going through
     * a topic. Useful for serving "current value" APIs from a stream app.
     *
     *   GET /counts          -> {"widget": 7, "gadget": 3}
     *   GET /counts/widget   -> {"widget": 7}
     *
     * With one instance, every partition's state is local. With several
     * instances, each holds only some partitions, and you'd use
     * streams.queryMetadataForKey(...) to find which instance has a key.
     */
    private static HttpServer startHttp(KafkaStreams streams, int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/counts", exchange -> {
            if (streams.state() != KafkaStreams.State.RUNNING) {
                reply(exchange, 503, "{\"error\":\"streams is " + streams.state() + "\"}");
                return;
            }
            ReadOnlyKeyValueStore<String, Long> store = streams.store(StoreQueryParameters.fromNameAndType(
                    OrderAnalyticsTopology.ITEM_COUNTS_STORE, QueryableStoreTypes.keyValueStore()));

            ObjectNode body = JsonSerde.MAPPER.createObjectNode();
            String item = exchange.getRequestURI().getPath().replaceFirst("^/counts/?", "");
            if (item.isEmpty()) {
                try (KeyValueIterator<String, Long> all = store.all()) {
                    while (all.hasNext()) {
                        KeyValue<String, Long> row = all.next();
                        body.put(row.key, row.value);
                    }
                }
            } else {
                Long count = store.get(item);
                body.put(item, count == null ? 0 : count);
            }
            reply(exchange, 200, body.toString());
        });
        server.start();
        System.out.printf("interactive queries on http://localhost:%d/counts%n", port);
        return server;
    }

    private static void reply(HttpExchange exchange, int status, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
