package org.observertc.webrtc.reportconnector.configbuilders;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;

@MicronautTest
class ConfigurationSourceProviderTest {

    @Inject
    ConfigurationSourceProvider configurationSourceProvider;

    @Test
    public void loadString() {
        final String yaml = "pipeline:\n" +
                "  name: \"the name\"\n" +
                "  source:\n" +
                "    type: KafkaSource\n" +
                "    config:\n" +
                "      bootstrap:\n" +
                "        servers: localhost:9092";
        this.configurationSourceProvider.fromYamlString(yaml);
    }
}