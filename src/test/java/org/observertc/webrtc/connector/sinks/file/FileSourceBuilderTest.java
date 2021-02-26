package org.observertc.webrtc.connector.sinks.file;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.observertc.webrtc.connector.Utils;
import org.observertc.webrtc.connector.configbuilders.Builder;
import org.observertc.webrtc.connector.configbuilders.ObservableConfig;
import org.observertc.webrtc.connector.sources.Source;
import org.observertc.webrtc.connector.sources.file.FileSourceBuilder;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@MicronautTest
class FileSinkBuilderTest {

    private static final String FILESOURCE_EXAMPLE_YAML = "filesource_example.yaml";

    @Inject
    ObservableConfig observableConfig;

    @Test
    public void shouldBuildWithValidConfig() throws FileNotFoundException {

        // Given
        String input = Utils.getResourceFileAsString(FILESOURCE_EXAMPLE_YAML);
        AtomicReference<Map<String, Object>> configHolder = new AtomicReference<>();
        this.observableConfig
                .fromYamlString(input)
                .subscribe(configHolder::set);

        // When
        Builder<Source> builder = new FileSourceBuilder();
        builder.withConfiguration(configHolder.get());

        // Then
        Assert.assertNotNull(builder.build());

    }

    @Test
    public void shouldNotBuildWithInValidConfig() {

    }

}