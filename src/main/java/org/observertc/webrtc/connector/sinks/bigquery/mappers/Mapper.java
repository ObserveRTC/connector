package org.observertc.webrtc.connector.sinks.bigquery.mappers;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import org.observertc.webrtc.connector.sinks.bigquery.models.Entry;
import org.observertc.webrtc.schemas.reports.Report;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Mapper extends Observable<Entry> implements Observer<Report> {
    private static Logger logger = LoggerFactory.getLogger(Mapper.class);

    private final PublishSubject<Entry> entrySink;
    private final ReportObserver reportObserver;

    public Mapper() {
        this.reportObserver = new ReportObserver();
        this.entrySink = PublishSubject.create();

        this.reportObserver.initiatedCallReport.map(new InitiatedCallMapper()).subscribe(this.entrySink);
        this.reportObserver.finishedCallReport.map(new FinishedCallMapper()).subscribe(this.entrySink);
        this.reportObserver.inboundRTPReport.map(new InboundRTPMapper()).subscribe(this.entrySink);
        this.reportObserver.remoteInboundRTPReport.map(new RemoteInboundRTPMapper()).subscribe(this.entrySink);
        this.reportObserver.outboundRTPReport.map(new OutboundRTPMapper()).subscribe(this.entrySink);
        this.reportObserver.iceLocalCandidateReport.map(new ICELocalCandidateMapper()).subscribe(this.entrySink);
        this.reportObserver.iceRemoteCandidateReport.map(new ICERemoteCandidateMapper()).subscribe(this.entrySink);
        this.reportObserver.iceCandidatePairReport.map(new ICELocalCandidateMapper()).subscribe(this.entrySink);
        this.reportObserver.trackReport.map(new TrackMapper()).subscribe(this.entrySink);
        this.reportObserver.userMediaErrorReport.map(new UserMediaErrorMapper()).subscribe(this.entrySink);
        this.reportObserver.mediaSourceReport.map(new MediaSourceMapper()).subscribe(this.entrySink);
        this.reportObserver.joinedPeerConnectionCallReport.map(new JoinedPeerConnectionMapper()).subscribe(this.entrySink);
        this.reportObserver.detachedPeerConnectionCallReport.map(new DetachedPeerConnectionMapper()).subscribe(this.entrySink);
        this.reportObserver.observerEventReport.map(new ObserverEventMapper()).subscribe(this.entrySink);
    }

    @Override
    protected void subscribeActual(@NonNull Observer<? super Entry> observer) {
        entrySink.subscribe(observer);
    }

    @Override
    public void onSubscribe(@NonNull Disposable d) {

    }

    @Override
    public void onNext(Report report) {
        this.reportObserver.onNext(report);
    }

    @Override
    public void onError(@NonNull Throwable e) {

    }

    @Override
    public void onComplete() {

    }
}
