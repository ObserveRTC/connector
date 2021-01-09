package org.observertc.webrtc.connector;

import io.reactivex.rxjava3.core.ObservableOperator;
import org.observertc.webrtc.connector.sinks.Sink;
import org.observertc.webrtc.connector.sources.Source;
import org.observertc.webrtc.schemas.reports.Report;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class Pipeline implements Runnable {
    private String name;
    private Source source;
    private ObservableOperator<Report, byte[]> decoder;
    private List<ObservableOperator<Report, Report>> transformations = new LinkedList<>();

    private Sink sink;
    private int maxItems = 10000;
    private int maxWaitingTimeInS = 30;

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
                .lift(this.decoder)
//                .map(e -> List.of(e))
                .buffer(this.maxItems)
//                .buffer(this.maxItems, this.maxWaitingTimeInS, TimeUnit.SECONDS)
                .subscribe(this.sink)
        ;

//        for (ObservableOperator<Report, Report> transformation : this.transformations) {
//            out = out.lift(transformation);
//        }
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

    Pipeline withDecoder(ObservableOperator<Report, byte[]> decoder) {
        if (Objects.nonNull(this.decoder)) {
            throw new IllegalStateException(this.getName() + ": cannot set the source for a pipeline twice");
        }
        this.decoder = decoder;
        return this;
    }

    Pipeline withBuffer(int maxItems, int maxWaitingTimeInS) {
        this.maxItems = maxItems;
        this.maxWaitingTimeInS = maxWaitingTimeInS;
        return this;
    }

    Pipeline withTransformation(ObservableOperator<Report, Report> transformation) {
        this.transformations.add(transformation);
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
