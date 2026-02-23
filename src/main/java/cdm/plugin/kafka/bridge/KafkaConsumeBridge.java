package cdm.plugin.kafka.bridge;

import cdm.plugin.api.PluginResult;
import cdm.plugin.api.messaging.CdmMessageConsumer;
import cdm.plugin.api.messaging.CdmMessageConsumer.ReceivedMessage;
import cdm.plugin.kafka.type.KafkaSubscription;
import cdm.plugin.kafka.type.KafkaMessage;
import cdm.plugin.kafka.type.KafkaHeader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Bridge class connecting the Rune-generated abstract KafkaConsume
 * function to the SPI-discovered CdmMessageConsumer implementation.
 *
 * Translates CDM KafkaSubscription type into consumer subscribe/poll
 * calls and converts the results back to CDM KafkaMessage types.
 */
public class KafkaConsumeBridge /* extends KafkaConsume */ {

    private final CdmMessageConsumer consumer;
    private boolean subscribed = false;

    public KafkaConsumeBridge() {
        this.consumer = ServiceLoader.load(CdmMessageConsumer.class)
            .findFirst()
            .orElseThrow(() -> new RuntimeException(
                "No CdmMessageConsumer plugin found on classpath. "
                + "Add cdm-plugin-kafka to your dependencies."));
    }

    public KafkaConsumeBridge(CdmMessageConsumer consumer) {
        this.consumer = consumer;
    }

    // @Override
    // protected List<KafkaMessage> doEvaluate(
    //     KafkaSubscription kafkaSubscription, Integer kafkaPollTimeoutMs)
    public List<KafkaMessage> evaluate(KafkaSubscription kafkaSubscription,
                                        Integer kafkaPollTimeoutMs) {

        // Subscribe if not already done (idempotent in Kafka client)
        if (!subscribed) {
            consumer.subscribe(
                kafkaSubscription.getKafkaTopics(),
                kafkaSubscription.getKafkaGroupId());
            subscribed = true;
        }

        long timeout = (kafkaPollTimeoutMs != null)
            ? kafkaPollTimeoutMs : 500L;

        PluginResult<List<ReceivedMessage>> result = consumer.poll(timeout);

        if (!result.success() || result.data() == null) {
            return List.of();
        }

        List<KafkaMessage> messages = new ArrayList<>();
        for (ReceivedMessage msg : result.data()) {
            KafkaMessage.KafkaMessageBuilder builder =
                KafkaMessage.builder()
                    .setKafkaTopic(msg.topic())
                    .setKafkaPartition(msg.partition())
                    .setKafkaOffset((int) msg.offset())
                    .setKafkaKey(msg.key())
                    .setKafkaPayload(msg.payload())
                    .setKafkaTimestamp((int) msg.timestamp());

            // Convert headers
            if (msg.headers() != null) {
                List<KafkaHeader> headers = msg.headers().entrySet()
                    .stream()
                    .map(e -> KafkaHeader.builder()
                        .setKafkaHeaderName(e.getKey())
                        .setKafkaHeaderValue(e.getValue())
                        .build())
                    .toList();
                builder.setKafkaHeaders(headers);
            }

            messages.add(builder.build());
        }

        return messages;
    }
}
