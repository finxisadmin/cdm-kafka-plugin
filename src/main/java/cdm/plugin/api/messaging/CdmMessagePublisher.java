package cdm.plugin.api.messaging;

import cdm.plugin.api.CdmPlugin;
import cdm.plugin.api.PluginResult;

import java.util.Map;

/**
 * SPI for publishing CDM events to a messaging system (Kafka, etc.)
 *
 * Plugin authors implement this interface and register it via
 * META-INF/services. The bridge class discovers the implementation
 * using ServiceLoader at runtime.
 */
public interface CdmMessagePublisher extends CdmPlugin {

    /**
     * Publish a CDM event (as JSON) to a topic.
     *
     * @param topic        destination topic name
     * @param cdmEventJson CDM event serialized as JSON
     * @return delivery confirmation (topic:partition:offset) on success
     */
    PluginResult<String> publish(String topic, String cdmEventJson);

    /**
     * Publish with a partition key for ordering guarantees.
     *
     * @param topic        destination topic name
     * @param key          partition key (e.g. trade ID, LEI)
     * @param cdmEventJson CDM event serialized as JSON
     * @return delivery confirmation on success
     */
    PluginResult<String> publish(String topic, String key,
                                  String cdmEventJson);

    /**
     * Publish with a partition key and custom headers.
     *
     * @param topic        destination topic name
     * @param key          partition key
     * @param cdmEventJson CDM event serialized as JSON
     * @param headers      custom message headers
     * @return delivery confirmation on success
     */
    PluginResult<String> publish(String topic, String key,
                                  String cdmEventJson,
                                  Map<String, String> headers);
}
