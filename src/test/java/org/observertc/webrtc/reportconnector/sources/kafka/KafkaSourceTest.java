package org.observertc.webrtc.reportconnector.sources.kafka;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

class KafkaSourceTest {

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.5.0"));

    @BeforeAll
    static void setup() {
        kafka.start();
    }

    @AfterAll
    static void teardown() {
        kafka.stop();
    }

    @Test
    public void shouldWork() throws InterruptedException {
        // Given
        String topic = UUID.randomUUID().toString();
        KafkaSource kafkaSource = new KafkaSource()
                .withProperties(givenValidConsumerProperties())
                .forTopic(topic);
        KafkaProducer<UUID, Bytes> producer = new KafkaProducer<UUID, Bytes>(givenValidProducerProperties());
        byte[] sentMessage = "MyMessage".getBytes(StandardCharsets.UTF_8);
        AtomicReference<byte[]> receivedMessage = new AtomicReference<>(null);

        // When
        producer.<UUID, Bytes>send(new ProducerRecord<UUID, Bytes>(topic, UUID.randomUUID(), new Bytes(sentMessage)));
        kafkaSource.subscribe(receivedMessage::set);
        kafkaSource.run();
        Thread.sleep(1000);

        // Then
        Assert.assertNotNull(receivedMessage.get());
        Assert.assertArrayEquals(sentMessage, receivedMessage.get());
    }

    private static Map<String, Object> givenValidConsumerProperties() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, UUIDDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class
        );
    }

    private static Map<String, Object> givenValidProducerProperties() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, UUIDSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BytesSerializer.class
        );
    }

}