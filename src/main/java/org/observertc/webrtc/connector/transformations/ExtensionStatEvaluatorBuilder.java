package org.observertc.webrtc.connector.transformations;

import io.micronaut.context.annotation.Prototype;
import org.observertc.webrtc.connector.configbuilders.AbstractBuilder;
import org.observertc.webrtc.connector.configbuilders.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Prototype
public class ExtensionStatEvaluatorBuilder extends AbstractBuilder implements Builder<Transformation> {
    private static final Logger logger = LoggerFactory.getLogger(ExtensionStatEvaluatorBuilder.class);

    @Override
    public ExtensionStatEvaluator build() {
        Config config = this.convertAndValidate(Config.class);
        ExtensionStatEvaluator result = new ExtensionStatEvaluator()
                .withConfig(config);
        return result;
    }


    public static class Config {
        public boolean enabled = false;
    }
}
