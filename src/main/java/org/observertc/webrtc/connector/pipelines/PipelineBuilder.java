package org.observertc.webrtc.connector.pipelines;

import io.micronaut.context.annotation.Prototype;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Function;
import org.observertc.webrtc.ObjectToString;
import org.observertc.webrtc.connector.configbuilders.AbstractBuilder;
import org.observertc.webrtc.connector.decoders.Decoder;
import org.observertc.webrtc.connector.decoders.DecoderBuilder;
import org.observertc.webrtc.connector.sinks.Sink;
import org.observertc.webrtc.connector.sinks.SinkBuilder;
import org.observertc.webrtc.connector.sources.Source;
import org.observertc.webrtc.connector.sources.SourceBuilder;
import org.observertc.webrtc.connector.transformations.Transformation;
import org.observertc.webrtc.connector.transformations.TransformationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.*;

@Prototype
public class PipelineBuilder extends AbstractBuilder implements Function<Map<String, Object>, Optional<Pipeline>> {
    private static final Logger logger = LoggerFactory.getLogger(PipelineBuilder.class);
    private Disposable disposable;

    public PipelineBuilder() {

    }

    @Override
    public Optional<Pipeline> apply(Map<String, Object> source) throws Throwable {
        this.getConfigs().clear();
        this.withConfiguration(source);
        return this.build();
    }

    public Optional<Pipeline> build() {
        Config config = this.convertAndValidate(Config.class);
        Pipeline result = new Pipeline()
                .withName(config.name);

        SourceBuilder sourceBuilder = new SourceBuilder();
        sourceBuilder.withConfiguration(config.source);
        Source source = sourceBuilder.build();
        if (Objects.isNull(source)) {
            logger.warn("Source was not build for pipeline {}, this pipeline cannot be built.", config.name);
            return Optional.empty();
        }
        result.withSource(source);

        DecoderBuilder decoderBuilder = new DecoderBuilder();
        decoderBuilder.withConfiguration(config.decoder);
        Optional<Decoder> decoderHolder = decoderBuilder.build();
        if (!decoderHolder.isPresent()) {
            logger.warn("{} is cannot build without a decoder.",
                    config.name);
            return Optional.empty();
        }
        result.withDecoder(decoderHolder.get());

        for (Map<String, Object> transformationConfig : config.transformations) {
            TransformationBuilder transformationBuilder = new TransformationBuilder();
            transformationBuilder.withConfiguration(transformationConfig);
            Optional<Transformation> transformationOptional = transformationBuilder.build();
            if (!transformationOptional.isPresent()) {
                logger.warn("Cannot make a transformation object from {}", ObjectToString.toString(transformationConfig));
                continue;
            }
            Transformation transformation = transformationOptional.get();
            result.withTransformation(transformation);
        }

        result.withBuffer(config.buffer);

        SinkBuilder sinkBuilder = new SinkBuilder();
        sinkBuilder.withConfiguration(config.sink);
        Sink sink = sinkBuilder.build();
        if (Objects.isNull(sink)) {
            logger.warn("Sink was not build for pipeline {}, this pipeline cannot be built.", config.name);
            return Optional.empty();
        }
        result.withSink(sink);

        return Optional.of(result);
    }

    public static class Config {

        public String name;

        public MetaConfig meta = new MetaConfig();

        @NotNull
        public Map<String, Object> source;

        public Map<String, Object> decoder = new HashMap<>();

        public List<Map<String, Object>> transformations = new ArrayList<>();

        public BufferConfig buffer = new BufferConfig();

        @NotNull
        public Map<String, Object> sink;

    }

}
