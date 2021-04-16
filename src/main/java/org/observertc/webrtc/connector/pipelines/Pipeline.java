package org.observertc.webrtc.connector.pipelines;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableOperator;
import org.observertc.webrtc.connector.sinks.Sink;
import org.observertc.webrtc.connector.sources.Source;
import org.observertc.webrtc.connector.transformations.Transformation;
import org.observertc.webrtc.schemas.reports.Report;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class Pipeline implements Runnable {
    public enum State {
        CREATED,
        RUN,
        FINISHED;

        Throwable thrown;

        State withException(Throwable t) {
            this.thrown = t;
            return this;
        }
    }

    private static final Logger DEFAULT_LOGGER = LoggerFactory.getLogger(Pipeline.class);
    private String name;
    private Source source;
    private ObservableOperator<Report, byte[]> decoder;
    private List<Transformation> transformations = new LinkedList<>();
    private Runnable closingCallback = () -> {};
    private Sink sink;
    private BufferConfig bufferConfig = null;
    private final Logger logger;
    private volatile State state = State.CREATED;

    public Pipeline(String name) {
        this.logger = LoggerFactory.getLogger(name);
    }

    @Override
    public void run() {
        if (Objects.isNull(this.source)) {
            throw new IllegalStateException("A pipeline cannot be started without a source");
        }
        if (Objects.isNull(this.decoder)) {
            throw new IllegalStateException("A pipeline cannot be started without a decoder");
        }
        if (Objects.isNull(this.sink)) {
            throw new IllegalStateException("A pipeline cannot be started without a sink");
        }

        Observable<byte[]> observableBytes = this.source;

        Observable<Report> observableReport = observableBytes.lift(this.decoder).share();

        for (Transformation transformation : this.transformations) {
            observableReport = observableReport.lift(transformation).share();
        }

        Observable<List<Report>> observableReports;
        if (this.bufferConfig.maxWaitingTimeInS < 1) {
            observableReports = observableReport.buffer(this.bufferConfig.maxItems).share();
        } else {
            observableReports = observableReport.buffer(this.bufferConfig.maxWaitingTimeInS, TimeUnit.SECONDS, this.bufferConfig.maxItems).share();
        }

        observableReports.subscribe(this.sink);
        try {
            this.state = State.RUN;
            this.source.run();
            this.state = State.FINISHED;
        } catch(Throwable t) {
            logger.error("Exception occurred during the execution of the pipeline", t);
            this.state = State.FINISHED.withException(t);
        } finally{
            try {
                this.closingCallback.run();
            } catch (Throwable t) {
                String message = String.format("At pipeline %s the callback called " +
                                "right after the pipeline itself has ended its operation " +
                                "is just crashed. That's wonderful!",
                        this.getName());
                logger.error(message, t);
            }
        }
    }

    public State getState() {
        return this.state;
    }

    public String getName() {
        if (Objects.isNull(this.name)) {
            return "Unkown pipeline";
        }
        return this.name;
    }

    Pipeline withSource(Source source) {
        if (Objects.nonNull(this.source)) {
            throw new IllegalStateException(this.getName() + ": cannot set the source for a pipeline twice");
        }
        this.source = source
                .withLogger(logger)
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

    Pipeline withBuffer(BufferConfig bufferConfig) {
        this.bufferConfig = bufferConfig;
        return this;
    }

    Pipeline withTransformation(Transformation transformation) {
        this.transformations.add(transformation);
        return this;
    }

    Pipeline withSink(Sink sink) {
        if (Objects.nonNull(this.sink)) {
            throw new IllegalStateException(this.getName() + ": cannot set the source for a pipeline twice");
        }
        this.sink = sink
                .withLogger(logger)
                .inPipeline(this);
        return this;
    }

    Pipeline withClosingCallback(Runnable closingCallback) {
        this.closingCallback = closingCallback;
        return this;
    }
}
