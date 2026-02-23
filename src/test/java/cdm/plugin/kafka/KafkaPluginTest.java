package cdm.plugin.kafka;

import cdm.plugin.api.CdmPluginModule;
import cdm.plugin.api.PluginResult;
import cdm.plugin.api.messaging.CdmMessageConsumer;
import cdm.plugin.api.messaging.CdmMessagePublisher;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the CDM Kafka Plugin.
 *
 * Note: Tests marked "requires Kafka broker" need a running Kafka
 * instance. All other tests validate SPI wiring and configuration
 * without a live broker.
 */
class KafkaPluginTest {

    // -------------------------------------------------------
    // SPI Discovery
    // -------------------------------------------------------

    @Test
    @DisplayName("ServiceLoader discovers KafkaCdmPublisher")
    void testPublisherDiscovery() {
        var publisher = ServiceLoader.load(CdmMessagePublisher.class)
            .findFirst();
        assertTrue(publisher.isPresent(),
            "ServiceLoader should discover KafkaCdmPublisher");
        assertEquals("cdm-kafka-publisher", publisher.get().getId());
        assertEquals("CDM Kafka Publisher", publisher.get().getName());
        assertEquals("1.0.0", publisher.get().getVersion());
    }

    @Test
    @DisplayName("ServiceLoader discovers KafkaCdmConsumer")
    void testConsumerDiscovery() {
        var consumer = ServiceLoader.load(CdmMessageConsumer.class)
            .findFirst();
        assertTrue(consumer.isPresent(),
            "ServiceLoader should discover KafkaCdmConsumer");
        assertEquals("cdm-kafka-consumer", consumer.get().getId());
        assertEquals("CDM Kafka Consumer", consumer.get().getName());
        assertEquals("1.0.0", consumer.get().getVersion());
    }

    @Test
    @DisplayName("ServiceLoader discovers KafkaPluginModuleSpi for CdmPluginRegistry")
    void testPluginModuleSpiDiscovery() {
        var module = ServiceLoader.load(CdmPluginModule.class)
            .findFirst();
        assertTrue(module.isPresent(),
            "ServiceLoader should discover KafkaPluginModuleSpi");
        assertEquals("cdm-plugin-kafka", module.get().getId());
        assertEquals("CDM Kafka Plugin", module.get().getName());
        assertNotNull(module.get().getModule());
    }

    // -------------------------------------------------------
    // Publisher lifecycle
    // -------------------------------------------------------

    @Test
    @DisplayName("Publisher throws IllegalStateException before initialization")
    void testPublisherNotInitialized() {
        CdmMessagePublisher publisher = ServiceLoader
            .load(CdmMessagePublisher.class).findFirst().orElseThrow();

        // Should throw because initialize() hasn't been called
        assertThrows(IllegalStateException.class, () ->
            publisher.publish("test-topic", "test payload"));
    }

    @Test
    @DisplayName("Publisher initializes with default config")
    void testPublisherInitDefault() {
        CdmMessagePublisher publisher = ServiceLoader
            .load(CdmMessagePublisher.class).findFirst().orElseThrow();

        // Initialize with defaults — doesn't connect until first send
        assertDoesNotThrow(() ->
            publisher.initialize(Map.of()));
        publisher.shutdown();
    }

    @Test
    @DisplayName("Publisher initializes with custom broker config")
    void testPublisherInitCustom() {
        CdmMessagePublisher publisher = ServiceLoader
            .load(CdmMessagePublisher.class).findFirst().orElseThrow();

        assertDoesNotThrow(() ->
            publisher.initialize(Map.of(
                "bootstrap.servers", "broker1:9092,broker2:9092",
                "acks", "1",
                "retries", "5",
                "send.timeout.seconds", "10"
            )));
        publisher.shutdown();
    }

    // -------------------------------------------------------
    // Consumer lifecycle
    // -------------------------------------------------------

    @Test
    @DisplayName("Consumer throws IllegalStateException before initialization")
    void testConsumerNotInitialized() {
        CdmMessageConsumer consumer = ServiceLoader
            .load(CdmMessageConsumer.class).findFirst().orElseThrow();

        assertThrows(IllegalStateException.class, () ->
            consumer.subscribe(List.of("test"), "group1"));
    }

    @Test
    @DisplayName("Consumer initializes with default config")
    void testConsumerInitDefault() {
        CdmMessageConsumer consumer = ServiceLoader
            .load(CdmMessageConsumer.class).findFirst().orElseThrow();

        assertDoesNotThrow(() ->
            consumer.initialize(Map.of(
                "group.id", "test-group"
            )));
        consumer.shutdown();
    }

    // -------------------------------------------------------
    // Integration tests (requires running Kafka broker)
    // -------------------------------------------------------

    // Uncomment these tests when a Kafka broker is available.
    // To run: mvn test -Dkafka.broker=localhost:9092

    /*
    @Test
    @DisplayName("Publish and consume a CDM event via Kafka")
    void testPublishAndConsume() throws Exception {
        String broker = System.getProperty(
            "kafka.broker", "localhost:9092");
        String topic = "cdm-plugin-test-" + System.currentTimeMillis();

        // Setup publisher
        CdmMessagePublisher publisher = ServiceLoader
            .load(CdmMessagePublisher.class).findFirst().orElseThrow();
        publisher.initialize(Map.of(
            "bootstrap.servers", broker,
            "acks", "all"
        ));

        // Publish a CDM event
        String cdmJson = "{\"eventType\":\"Execution\",\"trade\":{}}";
        PluginResult<String> pubResult = publisher.publish(
            topic, "TRADE-001", cdmJson,
            Map.of("cdm.event.type", "Execution"));

        assertTrue(pubResult.success(),
            "Publish should succeed: " + pubResult.error());
        assertNotNull(pubResult.metadata().get("partition"));
        assertNotNull(pubResult.metadata().get("offset"));

        // Setup consumer
        CdmMessageConsumer consumer = ServiceLoader
            .load(CdmMessageConsumer.class).findFirst().orElseThrow();
        consumer.initialize(Map.of(
            "bootstrap.servers", broker,
            "group.id", "cdm-test-group-" + System.currentTimeMillis(),
            "auto.offset.reset", "earliest"
        ));
        consumer.subscribe(List.of(topic), "ignored");

        // Poll for the message
        PluginResult<List<CdmMessageConsumer.ReceivedMessage>> pollResult =
            null;
        List<CdmMessageConsumer.ReceivedMessage> messages = List.of();
        for (int i = 0; i < 10 && messages.isEmpty(); i++) {
            pollResult = consumer.poll(1000);
            if (pollResult.success() && pollResult.data() != null) {
                messages = pollResult.data();
            }
        }

        assertFalse(messages.isEmpty(), "Should receive at least one message");
        assertEquals("TRADE-001", messages.get(0).key());
        assertEquals(cdmJson, messages.get(0).payload());
        assertEquals("Execution",
            messages.get(0).headers().get("cdm.event.type"));

        publisher.shutdown();
        consumer.shutdown();
    }
    */
}
