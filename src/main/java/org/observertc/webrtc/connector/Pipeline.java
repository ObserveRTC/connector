package org.observertc.webrtc.connector;

import org.observertc.webrtc.connector.decoders.AvroDecoder;
import org.observertc.webrtc.connector.evaluators.Evaluator;
import org.observertc.webrtc.connector.sinks.Sink;
import org.observertc.webrtc.connector.sources.Source;

import java.util.Objects;

public class Pipeline implements Runnable {
    private String name;
    private Source source;
    private Evaluator evaluator;
    private Sink sink;

    public Pipeline() {
    }

    @Override
    public void run() {
        if (Objects.isNull(this.source)) {
            throw new IllegalStateException("A pipeline cannot be started without a source");
        }
        if (Objects.isNull(this.sink)) {
            throw new IllegalStateException("A pipeline cannot be started without a sink");
        }
        this.source
                .lift(new AvroDecoder())
                .subscribe(this.evaluator);
        this.evaluator.subscribe(this.sink);
        this.source.run();
    }

    public String getName() {
        if (Objects.isNull(this.name)) {
            return "Unkown pipeline";
        }
        return this.name;
    }

    Pipeline withName(String name) {
        this.name = name;
        return this;
    }

    Pipeline withSource(Source source) {
        if (Objects.nonNull(this.source)) {
            throw new IllegalStateException(this.getName() + ": cannot set the source for a pipeline twice");
        }
        this.source = source
                .inPipeline(this);
        return this;
    }

    Pipeline withEvaluator(Evaluator evaluator) {
        if (Objects.nonNull(this.evaluator)) {
            throw new IllegalStateException(this.getName() + ": cannot set the Evaluator for a pipeline twice");
        }
        this.evaluator = evaluator
                .inPipeline(this);
        return this;
    }

    Pipeline withSink(Sink sink) {
        if (Objects.nonNull(this.sink)) {
            throw new IllegalStateException(this.getName() + ": cannot set the source for a pipeline twice");
        }
        this.sink = sink
                .inPipeline(this);
        return this;
    }

}
