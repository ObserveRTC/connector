package org.observertc.webrtc.reportconnector.sources;

import org.observertc.webrtc.reportconnector.configbuilders.AbstractBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.*;
import java.util.stream.Collectors;

public class SourceBuilder extends AbstractBuilder {
    private static final Logger logger = LoggerFactory.getLogger(SourceBuilder.class);
    private final List<String> packages;

    public SourceBuilder() {
        Package thisPackage = this.getClass().getPackage();
        this.packages = Arrays.stream(Package.getPackages())
                .filter(p -> p.getName().startsWith(thisPackage.getName()))
                .map(Package::getName)
                .collect(Collectors.toList());
    }

    public Source build() {
        Config config = this.convertAndValidate(Config.class);
        Optional<Object> sourceTypeBuilderHolder = this.packages.stream()
                .map(pName -> AbstractBuilder.builderClassName(pName, config.type))
                .map(this::invoke)
                .filter(Objects::nonNull)
                .filter(o -> o instanceof SourceTypeBuilder)
                .findFirst();
        if (!sourceTypeBuilderHolder.isPresent()) {
            logger.error("Cannot find source builder for {} in packages: {}", config.type, String.join(",", this.packages ));
            return null;
        }
        SourceTypeBuilder sourceTypeBuilder = (SourceTypeBuilder) sourceTypeBuilderHolder.get();

        sourceTypeBuilder.withConfiguration(config.config);
        Source result = sourceTypeBuilder.build();
        return result
                .withName(config.name);
    }

    public static class Config {

        @NotNull
        public String type;

        public String name = "Unknown";

        @NotNull
        public Map<String, Object> config;

    }
}
