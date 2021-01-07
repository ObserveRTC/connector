package org.observertc.webrtc.connector;

import io.micronaut.context.annotation.Prototype;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Function;
import org.observertc.webrtc.connector.configbuilders.AbstractBuilder;
import org.observertc.webrtc.connector.evaluators.Evaluator;
import org.observertc.webrtc.connector.evaluators.EvaluatorBuilder;
import org.observertc.webrtc.connector.sinks.Sink;
import org.observertc.webrtc.connector.sinks.SinkBuilder;
import org.observertc.webrtc.connector.sources.Source;
import org.observertc.webrtc.connector.sources.SourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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

        EvaluatorBuilder evaluatorBuilder = new EvaluatorBuilder();
        if (Objects.nonNull(config.evaluator)) {
            evaluatorBuilder.withConfiguration(config.evaluator);
        }
        Evaluator evaluator = evaluatorBuilder.build();
        result.withEvaluator(evaluator);

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

        @NotNull
        public String name;

        @NotNull
        public Map<String, Object> source;

        public Map<String, Object> evaluator;

        @NotNull
        public Map<String, Object> sink;
    }
}
