package cdm.plugin.kafka.impl;

import cdm.plugin.api.PluginResult;
import cdm.plugin.api.messaging.CdmMessageConsumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * SPI implementation of CdmMessageConsumer using Apache Kafka.
 *
 * Wraps the standard Kafka Consumer client. Provides synchronous
 * polling suitable for integration with Rune-generated functions
 * (KafkaConsume calls poll() and returns CDM-typed messages).
 *
 * Registered via META-INF/services for ServiceLoader discovery.
 */
public class KafkaCdmConsumer implements CdmMessageConsumer {

    private KafkaConsumer<String, String> consumer;
    private Map<String, String> config;

    // --- CdmPlugin lifecycle ---

    @Override
    public String getId()      { return "cdm-kafka-consumer"; }

    @Override
    public String getName()    { return "CDM Kafka Consumer"; }

    @Override
    public String getVersion() { return "1.0.0"; }

    @Override
    public void initialize(Map<String, String> config) {
        this.config = new HashMap<>(config);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            config.getOrDefault("bootstrap.servers", "localhost:9092"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            config.getOrDefault("auto.offset.reset", "earliest"));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
            config.getOrDefault("enable.auto.commit", "true"));
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
            config.getOrDefault("max.poll.records", "100"));

        // Group ID can be set at init time or at subscribe time
        String groupId = config.get("group.id");
        if (groupId != null) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }

        // Pass through additional Kafka consumer properties
        config.entrySet().stream()
            .filter(e -> e.getKey().startsWith("kafka."))
            .forEach(e -> props.put(
                e.getKey().substring(6), e.getValue()));

        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void shutdown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    // --- CdmMessageConsumer operations ---

    @Override
    public void subscribe(List<String> topics, String groupId) {
        ensureInitialized();
        // Note: groupId was set at init time via config.
        // If a different groupId is needed, reinitialize.
        consumer.subscribe(topics);
    }

    @Override
    public PluginResult<List<ReceivedMessage>> poll(long timeoutMs) {
        ensureInitialized();

        try {
            ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(timeoutMs));

            List<ReceivedMessage> messages = new ArrayList<>();

            for (ConsumerRecord<String, String> record : records) {
                Map<String, String> headers = new HashMap<>();
                record.headers().forEach(h -> headers.put(
                    h.key(),
                    new String(h.value())));

                messages.add(new ReceivedMessage(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    record.value(),
                    headers,
                    record.timestamp()
                ));
            }

            return PluginResult.ok(messages, Map.of(
                "count", String.valueOf(messages.size())
            ));

        } catch (Exception e) {
            return PluginResult.fail(
                "Kafka poll failed: " + e.getMessage());
        }
    }

    @Override
    public void unsubscribe() {
        if (consumer != null) {
            consumer.unsubscribe();
        }
    }

    private void ensureInitialized() {
        if (consumer == null) {
            throw new IllegalStateException(
                "Plugin not initialized. Call initialize() first.");
        }
    }
}
