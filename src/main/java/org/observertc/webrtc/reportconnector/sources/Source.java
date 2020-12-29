package org.observertc.webrtc.reportconnector.sources;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public abstract class Source extends Observable<byte[]> implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Source.class);

    private String name;
    private Observer<? super byte[]> observer = null;
    private Observable<byte[]> source;

    @Override
    protected void subscribeActual(@NonNull Observer<? super byte[]> observer) {
        if (Objects.nonNull(this.observer)) {
            throw new IllegalStateException("For Source " + this.name + " Cannot have more than one observer to be subscribed to a source. That's life! Oh no, just this implementation it is how it is.");
        }

        this.observer = observer;
    }

    @Override
    public void run() {
        if (Objects.isNull(this.observer)) {
            logger.error("No observer has been subscribed for {}, therefore the pipeline cannot run", this.name);
            return;
        }
        this.source = this.makeObservable();
        this.source.subscribe(this.observer);
    }

    public void stop() {

    }

    Source withName(String value) {
        this.name = value;
        return this;
    }

    protected abstract Observable<byte[]> makeObservable();

}
