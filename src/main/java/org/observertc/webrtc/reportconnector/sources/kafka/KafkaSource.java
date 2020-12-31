package org.observertc.webrtc.reportconnector.sources.kafka;

import io.micronaut.context.annotation.Prototype;
import io.reactivex.rxjava3.core.Observable;
import org.apache.kafka.common.utils.Bytes;
import org.observertc.webrtc.reportconnector.sources.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.*;

public class KafkaSource extends Source {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSource.class);

    private final Properties properties;
    private String topic;

    public KafkaSource() {
        this.properties = new Properties();
    }

    @Override
    protected Observable<byte[]> makeObservable() {
        ReceiverOptions<UUID, Bytes> receiverOptions = ReceiverOptions.create(this.properties);
        ReceiverOptions<UUID, Bytes> subscribedOptions = receiverOptions.subscription(Collections.singleton(this.topic));
        Flux<ReceiverRecord<UUID, Bytes>> kafkaFlux = KafkaReceiver.create(subscribedOptions).receive();

        return Observable.fromPublisher(kafkaFlux)
            .map(record -> {
                record.receiverOffset().acknowledge();
                return record.value();
            })
            .filter(Objects::nonNull)
            .map(Bytes::get);
    }

    KafkaSource withProperty(String key, Object value) {
        this.properties.put(key, value);
        return this;
    }

    KafkaSource withProperties(Map<String, Object> properties) {
        this.properties.putAll(properties);
        return this;
    }

    KafkaSource forTopic(String topicName) {
        if (Objects.nonNull(this.topic)) {
            logger.warn("TopicName is overwritten from {} to {}", this.topic, topicName);
        }
        this.topic = topicName;
        return this;
    }
}
