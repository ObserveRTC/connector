package org.observertc.webrtc.connector.transformations;

import io.micronaut.context.annotation.Prototype;
import org.observertc.webrtc.connector.configbuilders.AbstractBuilder;
import org.observertc.webrtc.connector.configbuilders.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Prototype
public class CallSanitizerBuilder extends AbstractBuilder implements Builder<Transformation> {

    private static final Logger logger = LoggerFactory.getLogger(CallSanitizerBuilder.class);

    @Override
    public CallSanitizer build() {
        Config config = this.convertAndValidate(Config.class);
        CallSanitizer result = new CallSanitizer();
        return result;
    }

    public static class Config {

    }
}
