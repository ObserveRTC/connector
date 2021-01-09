package org.observertc.webrtc.connector.decoders;

import io.micronaut.context.annotation.Prototype;
import org.observertc.webrtc.connector.configbuilders.AbstractBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Prototype
public class DecoderBuilder extends AbstractBuilder {

    private static final Logger logger = LoggerFactory.getLogger(DecoderBuilder.class);

    public DecoderBuilder() {

    }

    public Optional<Decoder> build() {
        Config config = this.convertAndValidate(Config.class);

        Optional<Decoder> result = this.tryInvoke(config.type);
        return result;
    }

    public static class Config {

        public String type = AvroDecoder.class.getName();

        public Map<String, Object> config = new HashMap<>();
    }
}
