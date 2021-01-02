package org.observertc.webrtc.connector;

import io.reactivex.rxjava3.core.Observer;
import org.observertc.webrtc.connector.decoders.AvroDecoder;
import org.observertc.webrtc.connector.evaluators.Evaluator;
import org.observertc.webrtc.connector.models.Entry;
import org.observertc.webrtc.connector.sources.Source;

import java.util.List;
import java.util.Objects;

public class Pipeline implements Runnable {
    private String name;
    private Source source;
    private Evaluator evaluator;
    private Observer<List<Entry>> sink;

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

    Pipeline withName(String name) {
        this.name = name;
        return this;
    }

    Pipeline withSource(Source source) {
        if (Objects.nonNull(this.source)) {
            throw new IllegalStateException("Cannot set the source for a pipeline twice");
        }
        this.source = source;
        return this;
    }

    Pipeline withEvaluator(Evaluator evaluator) {
        if (Objects.nonNull(this.evaluator)) {
            throw new IllegalStateException("Cannot set the Evaluator for a pipeline twice");
        }
        this.evaluator = evaluator;
        return this;
    }

    Pipeline withSink(Observer<List<Entry>> sink) {
        if (Objects.nonNull(this.sink)) {
            throw new IllegalStateException("Cannot set the source for a pipeline twice");
        }
        this.sink = sink;
        return this;
    }

}
