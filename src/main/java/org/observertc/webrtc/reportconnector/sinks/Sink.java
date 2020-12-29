package org.observertc.webrtc.reportconnector.sinks;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import org.observertc.webrtc.reportconnector.models.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class Sink implements Observer<List<Entry>> {
    private static final Logger logger = LoggerFactory.getLogger(Sink.class);

    @Override
    public void onSubscribe(@NonNull Disposable d) {

    }

    @Override
    public void onError(@NonNull Throwable e) {

    }

    @Override
    public void onComplete() {

    }
}
