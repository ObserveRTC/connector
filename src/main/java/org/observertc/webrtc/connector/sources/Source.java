package org.observertc.webrtc.connector.sources;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public abstract class Source extends Observable<byte[]> implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Source.class);

    private Observer<? super byte[]> observer = null;
    private Observable<byte[]> source;

    @Override
    protected void subscribeActual(@NonNull Observer<? super byte[]> observer) {
        if (Objects.nonNull(this.observer)) {
            throw new IllegalStateException("For Source " + this.getClass().getSimpleName() + " Cannot have more than one observer to be subscribed to a source.");
        }

        this.observer = observer;
    }

    @Override
    public void run() {
        this.start();
    }

    public void start() {
        if (Objects.isNull(this.observer)) {
            logger.error("No observer has been subscribed for {}, therefore the pipeline cannot run", this.getClass().getSimpleName());
            return;
        }
        this.source = this.makeObservable();
        this.source.subscribe(this.observer);
    }

    public void stop() {
        logger.warn("{} stop method is called, but there is no actual implementation for it. ", this.getClass().getSimpleName());
    }

    protected abstract Observable<byte[]> makeObservable();

}
