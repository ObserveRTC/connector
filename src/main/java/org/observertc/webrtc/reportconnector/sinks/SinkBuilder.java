package org.observertc.webrtc.reportconnector.sinks;

import org.observertc.webrtc.reportconnector.configbuilders.AbstractBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.*;
import java.util.stream.Collectors;

public class SinkBuilder extends AbstractBuilder {
    private static final Logger logger = LoggerFactory.getLogger(SinkBuilder.class);
    private final List<String> packages;

    public SinkBuilder() {
        Package thisPackage = this.getClass().getPackage();
        this.packages = Arrays.stream(Package.getPackages())
                .filter(p -> p.getName().startsWith(thisPackage.getName()))
                .map(Package::getName)
                .collect(Collectors.toList());
    }

    public Sink build() {
        Config config = this.convertAndValidate(Config.class);
        Optional<Object> sinkTypeBuilderHolder = this.packages.stream()
                .map(pName -> AbstractBuilder.builderClassName(pName, config.type))
                .map(this::invoke)
                .filter(Objects::nonNull)
                .filter(o -> o instanceof SinkTypeBuilder)
                .findFirst();
        if (!sinkTypeBuilderHolder.isPresent()) {
            logger.error("Cannot find sink builder for {} in packages: {}", config.type, String.join(",", this.packages ));
            return null;
        }
        SinkTypeBuilder sinkTypeBuilder = (SinkTypeBuilder) sinkTypeBuilderHolder.get();
        sinkTypeBuilder.withConfiguration(config.config);
        return sinkTypeBuilder.build();
    }

    public static class Config {

        @NotNull
        public String type;

        public Map<String, Object> config;

    }
}
