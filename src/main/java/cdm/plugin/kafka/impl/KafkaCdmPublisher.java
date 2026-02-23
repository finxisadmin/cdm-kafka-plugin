package cdm.plugin.kafka.impl;

import cdm.plugin.api.PluginResult;
import cdm.plugin.api.messaging.CdmMessagePublisher;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * SPI implementation of CdmMessagePublisher using Apache Kafka.
 *
 * Wraps the standard Kafka Producer client. Sends CDM events as
 * JSON string values with optional partition keys and headers.
 *
 * Supports all standard Kafka producer configuration via the
 * "kafka." config prefix (stripped and passed through to the
 * underlying producer).
 *
 * Registered via META-INF/services for ServiceLoader discovery.
 */
public class KafkaCdmPublisher implements CdmMessagePublisher {

    private KafkaProducer<String, String> producer;
    private Map<String, String> config;
    private int sendTimeoutSeconds;

    // --- CdmPlugin lifecycle ---

    @Override
    public String getId()      { return "cdm-kafka-publisher"; }

    @Override
    public String getName()    { return "CDM Kafka Publisher"; }

    @Override
    public String getVersion() { return "1.0.0"; }

    @Override
    public void initialize(Map<String, String> config) {
        this.config = new HashMap<>(config);
        this.sendTimeoutSeconds = Integer.parseInt(
            config.getOrDefault("send.timeout.seconds", "30"));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            config.getOrDefault("bootstrap.servers", "localhost:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,
            config.getOrDefault("acks", "all"));
        props.put(ProducerConfig.RETRIES_CONFIG,
            Integer.parseInt(config.getOrDefault("retries", "3")));
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
            config.getOrDefault("enable.idempotence", "true"));

        // Pass through any additional Kafka producer properties
        // prefixed with "kafka." (e.g. kafka.linger.ms=5)
        config.entrySet().stream()
            .filter(e -> e.getKey().startsWith("kafka."))
            .forEach(e -> props.put(
                e.getKey().substring(6), e.getValue()));

        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void shutdown() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }

    // --- CdmMessagePublisher operations ---

    @Override
    public PluginResult<String> publish(String topic,
                                         String cdmEventJson) {
        return publish(topic, null, cdmEventJson, Map.of());
    }

    @Override
    public PluginResult<String> publish(String topic, String key,
                                         String cdmEventJson) {
        return publish(topic, key, cdmEventJson, Map.of());
    }

    @Override
    public PluginResult<String> publish(String topic, String key,
                                         String cdmEventJson,
                                         Map<String, String> headers) {
        ensureInitialized();

        try {
            ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, key, cdmEventJson);

            // Add CDM standard headers
            record.headers().add(new RecordHeader(
                "cdm-content-type", "application/json"
                    .getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader(
                "cdm-plugin-version", getVersion()
                    .getBytes(StandardCharsets.UTF_8)));

            // Add custom headers
            if (headers != null) {
                headers.forEach((k, v) -> record.headers().add(
                    new RecordHeader(k,
                        v.getBytes(StandardCharsets.UTF_8))));
            }

            // Synchronous send with timeout
            RecordMetadata metadata = producer.send(record)
                .get(sendTimeoutSeconds, TimeUnit.SECONDS);

            return PluginResult.ok(
                metadata.topic() + ":" + metadata.partition()
                    + ":" + metadata.offset(),
                Map.of(
                    "topic", metadata.topic(),
                    "partition", String.valueOf(metadata.partition()),
                    "offset", String.valueOf(metadata.offset()),
                    "timestamp", String.valueOf(metadata.timestamp())
                )
            );

        } catch (Exception e) {
            return PluginResult.fail(
                "Kafka publish to " + topic + " failed: "
                    + e.getMessage());
        }
    }

    private void ensureInitialized() {
        if (producer == null) {
            throw new IllegalStateException(
                "Plugin not initialized. Call initialize() first.");
        }
    }
}
