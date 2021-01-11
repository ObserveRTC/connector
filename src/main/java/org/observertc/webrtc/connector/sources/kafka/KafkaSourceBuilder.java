package org.observertc.webrtc.connector.sources.kafka;

import io.micronaut.context.annotation.Prototype;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.observertc.webrtc.connector.configbuilders.AbstractBuilder;
import org.observertc.webrtc.connector.configbuilders.Builder;
import org.observertc.webrtc.connector.configbuilders.ConfigConverter;
import org.observertc.webrtc.connector.sources.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.function.BiConsumer;

@Prototype
public class KafkaSourceBuilder extends AbstractBuilder implements Builder<Source> {

    private final static Logger logger = LoggerFactory.getLogger(KafkaSourceBuilder.class);
    private String sourceName;

    public Source build() {
        Config config = this.convertAndValidate(Config.class);
        KafkaSource result = new KafkaSource();
        Map<String, Object> flattenedProperties = ConfigConverter.flatten(config.properties, ".");
        this.evaluateProperties(this.sourceName, flattenedProperties);
        flattenedProperties.entrySet().stream().forEach(entry -> result.withProperty(entry.getKey(), entry.getValue()));

        result.forTopic(config.topic);
        return result;
    }

    private void evaluateProperties(String name, Map<String, Object> flattenedMap) {
        BiConsumer<String, Object> check = (property, defaultValue)->  {
            Object value = flattenedMap.get(property);
            if (Objects.isNull(value)) {
                logger.info("KafkaSource {} have no value for property {}, thus the default is used {}",
                        name, property, defaultValue);
                flattenedMap.put(property, defaultValue);
            }
        };
        check.accept(ConsumerConfig.CLIENT_ID_CONFIG, KafkaSource.class.getSimpleName() + new Random().nextInt(10000));
        check.accept(ConsumerConfig.GROUP_ID_CONFIG, KafkaSource.class.getSimpleName());
        check.accept(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, UUIDDeserializer.class);
        check.accept(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
        check.accept(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    public KafkaSourceBuilder withSourceName(String value) {
        this.sourceName = value;
        return this;
    }

    public static class Config {

        @NotNull
        public Map<String, Object> properties;

        @NotNull
        public String topic;
    }

}
