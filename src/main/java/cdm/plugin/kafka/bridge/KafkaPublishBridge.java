package cdm.plugin.kafka.bridge;

import cdm.plugin.api.PluginResult;
import cdm.plugin.api.messaging.CdmMessagePublisher;
import cdm.plugin.kafka.type.KafkaTopicConfig;
import cdm.plugin.kafka.type.KafkaHeader;
import cdm.plugin.kafka.type.KafkaDeliveryResult;
import cdm.plugin.kafka.enums.KafkaDeliveryStatusEnum;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * Bridge class connecting the Rune-generated abstract KafkaPublish
 * function to the SPI-discovered CdmMessagePublisher implementation.
 *
 * When the Rune code generator runs, it produces an abstract class:
 *
 *   public abstract class KafkaPublish {
 *       protected abstract KafkaDeliveryResult doEvaluate(
 *           String kafkaPayload, KafkaTopicConfig kafkaTopicConfig);
 *   }
 *
 * This bridge extends that and delegates to the Kafka plugin.
 */
public class KafkaPublishBridge /* extends KafkaPublish */ {

    private final CdmMessagePublisher publisher;

    public KafkaPublishBridge() {
        this.publisher = ServiceLoader.load(CdmMessagePublisher.class)
            .findFirst()
            .orElseThrow(() -> new RuntimeException(
                "No CdmMessagePublisher plugin found on classpath. "
                + "Add cdm-plugin-kafka to your dependencies."));
    }

    public KafkaPublishBridge(CdmMessagePublisher publisher) {
        this.publisher = publisher;
    }

    // @Override
    // protected KafkaDeliveryResult doEvaluate(
    //     String kafkaPayload, KafkaTopicConfig kafkaTopicConfig)
    public KafkaDeliveryResult evaluate(String kafkaPayload,
                                         KafkaTopicConfig kafkaTopicConfig) {

        String topic = kafkaTopicConfig.getKafkaTopic();
        String key = kafkaTopicConfig.getKafkaPartitionKey();
        Map<String, String> headers = convertHeaders(
            kafkaTopicConfig.getKafkaHeaders());

        PluginResult<String> result;
        if (key != null && !key.isEmpty()) {
            result = publisher.publish(topic, key, kafkaPayload, headers);
        } else {
            result = publisher.publish(topic, kafkaPayload);
        }

        return buildDeliveryResult(result);
    }

    private Map<String, String> convertHeaders(List<KafkaHeader> headers) {
        if (headers == null || headers.isEmpty()) {
            return Map.of();
        }
        return headers.stream().collect(Collectors.toMap(
            KafkaHeader::getKafkaHeaderName,
            KafkaHeader::getKafkaHeaderValue,
            (a, b) -> b
        ));
    }

    private KafkaDeliveryResult buildDeliveryResult(
            PluginResult<String> result) {

        KafkaDeliveryResult.KafkaDeliveryResultBuilder builder =
            KafkaDeliveryResult.builder();

        if (result.success()) {
            builder.setKafkaStatus(KafkaDeliveryStatusEnum.DELIVERED);

            Map<String, String> meta = result.metadata();
            if (meta != null) {
                builder.setKafkaTopic(meta.getOrDefault("topic", ""));
                String partition = meta.get("partition");
                if (partition != null) {
                    builder.setKafkaPartition(Integer.parseInt(partition));
                }
                String offset = meta.get("offset");
                if (offset != null) {
                    builder.setKafkaOffset(Integer.parseInt(offset));
                }
                String timestamp = meta.get("timestamp");
                if (timestamp != null) {
                    builder.setKafkaTimestamp(Integer.parseInt(timestamp));
                }
            }
        } else {
            builder.setKafkaStatus(KafkaDeliveryStatusEnum.FAILED)
                .setKafkaTopic("")
                .setKafkaError(result.error());
        }

        return builder.build();
    }
}
