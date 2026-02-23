package cdm.plugin.api.messaging;

import cdm.plugin.api.CdmPlugin;
import cdm.plugin.api.PluginResult;

import java.util.List;
import java.util.Map;

/**
 * SPI for consuming CDM events from a messaging system (Kafka, etc.)
 *
 * Supports both synchronous polling (for Rune function integration)
 * and asynchronous handler-based consumption (for Java applications).
 */
public interface CdmMessageConsumer extends CdmPlugin {

    /**
     * Subscribe to one or more topics.
     *
     * @param topics  topic names to subscribe to
     * @param groupId consumer group identifier
     */
    void subscribe(List<String> topics, String groupId);

    /**
     * Poll for available messages. Returns immediately if no
     * messages are ready within the timeout period.
     *
     * @param timeoutMs poll timeout in milliseconds
     * @return list of received messages as JSON strings
     */
    PluginResult<List<ReceivedMessage>> poll(long timeoutMs);

    /**
     * Unsubscribe from all topics.
     */
    void unsubscribe();

    /**
     * A message received from the messaging system.
     */
    record ReceivedMessage(
        String topic,
        int partition,
        long offset,
        String key,
        String payload,
        Map<String, String> headers,
        long timestamp
    ) {}
}
