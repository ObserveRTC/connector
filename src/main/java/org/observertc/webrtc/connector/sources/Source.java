package org.observertc.webrtc.connector.sources;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import org.observertc.webrtc.connector.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.Pipe;
import java.util.Objects;
import java.util.Optional;

public abstract class Source extends Observable<byte[]> implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Source.class);
    private Optional<Pipeline> pipelineHolder = Optional.empty();

    private Observer<? super byte[]> observer = null;
    private Observable<byte[]> source;

    @Override
    protected void subscribeActual(@NonNull Observer<? super byte[]> observer) {
        if (Objects.nonNull(this.observer)) {
            throw new IllegalStateException(this.getPipelineName() + ": for Source " + this.getClass().getSimpleName() + " Cannot have more than one observer to be subscribed to a source.");
        }

        this.observer = observer;
    }

    @Override
    public void run() {
        this.start();
    }

    public void start() {
        if (Objects.isNull(this.observer)) {
            logger.error("{}: No observer has been subscribed for {}, therefore the pipeline cannot run",
                    this.getPipelineName(),
                    this.getClass().getSimpleName()
            );
            return;
        }
        this.source = this.makeObservable();
        this.source.subscribe(this.observer);
    }

    public void stop() {
        logger.warn("{}: {} stop method is called, but there is no actual implementation for it. ",
                this.getPipelineName(),
                this.getClass().getSimpleName());
    }

    protected abstract Observable<byte[]> makeObservable();

    public Source inPipeline(Pipeline pipeline) {
        if (Objects.isNull(pipeline)) {
            logger.warn("{} tried to be assigned with a null pipeline", this.getClass().getSimpleName());
            return this;
        }
        this.pipelineHolder = Optional.of(pipeline);
        return this;
    }

    protected String getPipelineName() {
        if (!this.pipelineHolder.isPresent()) {
            return "Unknown pipeline";
        }
        Pipeline pipeline = this.pipelineHolder.get();
        return pipeline.getName();
    }
}
