package lab.streams;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Decides what "time" a record happened at. Windows and joins use this time.
 *
 * By default Kafka Streams uses the Kafka record timestamp, which is when the
 * producer sent the message. Here that is when the outbox relay published
 * it, which can be later than when the order was actually created (for
 * example if Kafka was down for a while).
 *
 * This extractor uses the "timestamp" field from the event envelope instead.
 * That is EVENT TIME (when it really happened), not PROCESSING TIME (when we
 * got around to handling it). Event time keeps window counts correct even
 * when events arrive late or get replayed.
 */
public class EventTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof JsonNode event && event.hasNonNull("timestamp")) {
            try {
                // The Go services write RFC 3339, e.g. "2026-09-25T10:15:30.123456Z".
                return Instant.parse(event.get("timestamp").asText()).toEpochMilli();
            } catch (Exception ignored) {
                // Fall through to the record's own timestamp.
            }
        }
        return record.timestamp();
    }
}
