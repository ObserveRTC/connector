package org.observertc.webrtc.connector;

import io.micronaut.context.annotation.Prototype;
import io.reactivex.rxjava3.core.ObservableOperator;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Function;
import org.observertc.webrtc.connector.configbuilders.AbstractBuilder;
import org.observertc.webrtc.connector.decoders.AvroDecoder;
import org.observertc.webrtc.connector.sinks.Sink;
import org.observertc.webrtc.connector.sinks.SinkBuilder;
import org.observertc.webrtc.connector.sources.Source;
import org.observertc.webrtc.connector.sources.SourceBuilder;
import org.observertc.webrtc.schemas.reports.Report;
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

        ObservableOperator<Report, byte[]> decoder = this.invoke(config.decoder.type);
        if (Objects.isNull(decoder)) {
            logger.warn("Decoder {} for pipeline {}, cannot be loaded or cannot be casted to {}.",
                    config.decoder, config.name, ObservableOperator.class.getSimpleName());
            return Optional.empty();
        }
        result.withDecoder(decoder);

        result.withBuffer(config.buffer.maxItems, config.buffer.maxWaitInS);

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

        public static class DecoderConfig {
            public String type = AvroDecoder.class.getName();
            public Map<String, Object> config = new HashMap<>();
        }

        public static class TransformationConfig {
            public String type;
            public Map<String, Object> config;
        }

        public static class BufferConfig {
            public int maxItems = 100;
            public int maxWaitInS = 5;
        }

        @NotNull
        public String name;

        @NotNull
        public Map<String, Object> source;

        public DecoderConfig decoder = new DecoderConfig();

        public List<TransformationConfig> transformations = new ArrayList<>();

        public BufferConfig buffer = new BufferConfig();

        @NotNull
        public Map<String, Object> sink;

        public MetaConfig meta = new MetaConfig();

        public static class MetaConfig {
            public int replicas = 1;
        }
    }
}
