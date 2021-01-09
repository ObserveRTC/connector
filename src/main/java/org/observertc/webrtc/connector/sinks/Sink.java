package org.observertc.webrtc.connector.sinks;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import org.observertc.webrtc.connector.pipelines.Pipeline;
import org.observertc.webrtc.schemas.reports.Report;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public abstract class Sink implements Observer<List<Report>> {
    private static final Logger logger = LoggerFactory.getLogger(Sink.class);
    private Optional<Pipeline> pipelineHolder = Optional.empty();

    @Override
    public void onSubscribe(@NonNull Disposable d) {

    }

    @Override
    public void onError(@NonNull Throwable e) {

    }

    @Override
    public void onComplete() {

    }

    public Sink inPipeline(Pipeline pipeline) {
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
