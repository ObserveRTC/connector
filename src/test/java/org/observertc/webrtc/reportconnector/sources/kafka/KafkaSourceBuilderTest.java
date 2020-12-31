package org.observertc.webrtc.reportconnector.sources.kafka;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.observertc.webrtc.reportconnector.Utils;
import org.observertc.webrtc.reportconnector.configbuilders.ObservableConfig;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@MicronautTest
class KafkaSourceBuilderTest {

    private static final String KAFKASOURCE_EXAMPLE_YAML = "kafkasource_example.yaml";

    @Inject
    ObservableConfig observableConfig;

    @Test
    public void shouldBuildWithValidConfig() throws FileNotFoundException {

        // Given
        String input = Utils.getResourceFileAsString(KAFKASOURCE_EXAMPLE_YAML);
        AtomicReference<Map<String, Object>> configHolder = new AtomicReference<>();
        this.observableConfig
                .fromYamlString(input)
                .subscribe(configHolder::set);

        // When
        KafkaSourceBuilder builder = new KafkaSourceBuilder();
        builder.withConfiguration(configHolder.get());

        // Then
        Assert.assertNotNull(builder.build());

    }

    @Test
    public void shouldNotBuildWithInValidConfig() {

    }

}