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

import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Objects;

@Prototype
public class PipelineBuilder extends AbstractBuilder implements Function<Map<String, Object>, Pipeline> {

    private Disposable disposable;

    public PipelineBuilder() {

    }

    @Override
    public Pipeline apply(Map<String, Object> source) throws Throwable {
        this.getConfigs().clear();
        this.withConfiguration(source);
        return this.build();
    }

    public Pipeline build() {
        Config config = this.convertAndValidate(Config.class);
        Pipeline result = new Pipeline()
                .withName(config.name);

        SourceBuilder sourceBuilder = new SourceBuilder();
        sourceBuilder.withConfiguration(config.source);
        Source source = sourceBuilder.build();
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
        result.withSink(sink);

        return result;
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
