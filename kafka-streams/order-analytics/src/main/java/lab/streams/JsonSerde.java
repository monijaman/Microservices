package lab.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A Serde ("serializer + deserializer") turns Kafka's raw bytes into Java
 * objects and back. Kafka Streams needs one for every key and value it reads,
 * writes, or keeps in a state store.
 *
 * The Go services publish plain JSON, so we read every value as a Jackson
 * {@link JsonNode} (a generic JSON tree) instead of defining a Java class
 * for each event. Keys are plain strings (the order ID), so they use the
 * built-in {@code Serdes.String()}.
 */
public final class JsonSerde {

    static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonSerde() {}

    public static Serde<JsonNode> jsonNode() {
        Serializer<JsonNode> serializer = (topic, node) -> {
            if (node == null) return null;
            try {
                return MAPPER.writeValueAsBytes(node);
            } catch (Exception e) {
                throw new SerializationException("could not write JSON for topic " + topic, e);
            }
        };
        Deserializer<JsonNode> deserializer = (topic, bytes) -> {
            if (bytes == null) return null;
            try {
                return MAPPER.readTree(bytes);
            } catch (Exception e) {
                // Thrown errors go to the deserialization exception handler.
                // App.java sets it to log-and-continue, so one bad message
                // is skipped instead of stopping the whole app.
                throw new SerializationException("invalid JSON on topic " + topic, e);
            }
        };
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
