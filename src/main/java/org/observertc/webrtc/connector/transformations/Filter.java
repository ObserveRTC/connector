package org.observertc.webrtc.connector.transformations;

import io.reactivex.ObservableOperator;
import io.reactivex.Observer;
import io.reactivex.annotations.NonNull;
import io.reactivex.disposables.Disposable;
import org.observertc.webrtc.schemas.reports.Report;

public class Filter implements ObservableOperator<Report, Report> {

    @NonNull
    @Override
    public Observer<? super Report> apply(@NonNull Observer<? super Report> observer)  {
        return new Observer<>() {
            @Override
            public void onSubscribe(@NonNull Disposable d) {

            }

            @Override
            public void onNext(@NonNull Report report) {
                observer.onNext(report);
            }

            @Override
            public void onError(@NonNull Throwable e) {

            }

            @Override
            public void onComplete() {

            }
        };
    }
}
