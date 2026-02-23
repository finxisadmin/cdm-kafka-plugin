package cdm.plugin.kafka.module;

import cdm.plugin.api.CdmPluginModule;
import com.google.inject.Module;

import java.util.Map;

/**
 * SPI implementation for CdmPluginRegistry auto-discovery.
 *
 * When cdm-plugin-kafka.jar is on the classpath (or in a plugin
 * directory), CdmPluginRegistry finds this class and installs
 * the KafkaPluginModule into the Guice injector.
 *
 * Configuration via system properties or environment variables:
 *   cdm.plugin.kafka.bootstrap.servers / CDM_PLUGIN_KAFKA_BOOTSTRAP_SERVERS
 *   cdm.plugin.kafka.acks / CDM_PLUGIN_KAFKA_ACKS
 *   cdm.plugin.kafka.group.id / CDM_PLUGIN_KAFKA_GROUP_ID
 */
public class KafkaPluginModuleSpi implements CdmPluginModule {

    @Override
    public String getId()      { return "cdm-plugin-kafka"; }

    @Override
    public String getName()    { return "CDM Kafka Plugin"; }

    @Override
    public String getVersion() { return "1.0.0"; }

    @Override
    public Module getModule() {
        Map<String, String> config = Map.ofEntries(
            entry("bootstrap.servers",
                envOrProp("cdm.plugin.kafka.bootstrap.servers",
                    "localhost:9092")),
            entry("acks",
                envOrProp("cdm.plugin.kafka.acks", "all")),
            entry("retries",
                envOrProp("cdm.plugin.kafka.retries", "3")),
            entry("send.timeout.seconds",
                envOrProp("cdm.plugin.kafka.send.timeout", "30")),
            entry("group.id",
                envOrProp("cdm.plugin.kafka.group.id",
                    "cdm-plugin-group")),
            entry("auto.offset.reset",
                envOrProp("cdm.plugin.kafka.auto.offset.reset",
                    "earliest"))
        );

        return new KafkaPluginModule(config);
    }

    private static String envOrProp(String key, String defaultValue) {
        String envKey = key.replace('.', '_').toUpperCase();
        String envVal = System.getenv(envKey);
        if (envVal != null && !envVal.isEmpty()) {
            return envVal;
        }
        return System.getProperty(key, defaultValue);
    }

    private static Map.Entry<String, String> entry(String k, String v) {
        return Map.entry(k, v);
    }
}
