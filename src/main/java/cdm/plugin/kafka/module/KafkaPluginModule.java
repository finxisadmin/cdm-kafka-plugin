package cdm.plugin.kafka.module;

import cdm.plugin.api.messaging.CdmMessagePublisher;
import cdm.plugin.api.messaging.CdmMessageConsumer;
import cdm.plugin.kafka.bridge.KafkaPublishBridge;
import cdm.plugin.kafka.bridge.KafkaConsumeBridge;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import java.util.Map;
import java.util.ServiceLoader;

/**
 * Guice module that wires the Kafka plugin into the CDM runtime.
 *
 * Install alongside CdmRuntimeModule:
 *
 *   Injector injector = Guice.createInjector(
 *       new CdmRuntimeModule(),
 *       new KafkaPluginModule(Map.of(
 *           "bootstrap.servers", "broker1:9092,broker2:9092"
 *       ))
 *   );
 *
 * Supported config keys:
 *   bootstrap.servers       - Kafka broker addresses (default: localhost:9092)
 *   acks                    - Producer acks (default: all)
 *   retries                 - Producer retries (default: 3)
 *   send.timeout.seconds    - Send timeout (default: 30)
 *   group.id                - Consumer group ID (default: cdm-plugin-group)
 *   auto.offset.reset       - Consumer offset reset (default: earliest)
 *   kafka.*                 - Pass-through to underlying Kafka client
 */
public class KafkaPluginModule extends AbstractModule {

    private final Map<String, String> config;

    public KafkaPluginModule() {
        this(Map.of());
    }

    public KafkaPluginModule(Map<String, String> config) {
        this.config = config;
    }

    @Override
    protected void configure() {
        // Bind Rune-generated abstract functions to bridges:
        // bind(KafkaPublish.class).to(KafkaPublishBridge.class);
        // bind(KafkaPublishKeyed.class).to(KafkaPublishKeyedBridge.class);
        // bind(KafkaConsume.class).to(KafkaConsumeBridge.class);

        bind(KafkaPublishBridge.class).asEagerSingleton();
        bind(KafkaConsumeBridge.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    CdmMessagePublisher providePublisher() {
        CdmMessagePublisher publisher =
            ServiceLoader.load(CdmMessagePublisher.class)
                .findFirst()
                .orElseThrow(() -> new RuntimeException(
                    "No CdmMessagePublisher found on classpath."));
        publisher.initialize(config);
        return publisher;
    }

    @Provides
    @Singleton
    CdmMessageConsumer provideConsumer() {
        CdmMessageConsumer consumer =
            ServiceLoader.load(CdmMessageConsumer.class)
                .findFirst()
                .orElseThrow(() -> new RuntimeException(
                    "No CdmMessageConsumer found on classpath."));
        consumer.initialize(config);
        return consumer;
    }
}
