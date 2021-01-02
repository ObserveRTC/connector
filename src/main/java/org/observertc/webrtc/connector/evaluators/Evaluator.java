package org.observertc.webrtc.connector.evaluators;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Function;
import io.reactivex.rxjava3.subjects.PublishSubject;
import org.observertc.webrtc.connector.models.*;
import org.observertc.webrtc.schemas.reports.Report;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class Evaluator extends Observable<List<Entry>> implements Observer<Report> {

    private final PublishSubject<Entry> entrySink;
    private final ReportObserver reportObserver;
    private int bufferThresholdNum = 10000;
    private int bufferThresholdInS = 30;

    public Evaluator() {
        this.reportObserver = new ReportObserver();
        this.entrySink = PublishSubject.create();
    }

    @Override
    protected void subscribeActual(@NonNull Observer<? super List<Entry>> observer) {
        entrySink.buffer(this.bufferThresholdInS, TimeUnit.SECONDS, this.bufferThresholdNum)
                .subscribe(observer);
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

    Evaluator withBufferThresholdInS(int value) {
        this.bufferThresholdInS = value;
        return this;
    }

    Evaluator withBufferThresholdNum(int value) {
        this.bufferThresholdNum = value;
        return this;
    }

    Evaluator withInitiatedCallMapper(Function<Report, InitiatedCallEntry> mapper) {
        this.reportObserver.initiatedCallReport.map(mapper).subscribe(this.entrySink);
        return this;
    }

    Evaluator withFinishedCallMapper(Function<Report, FinishedCallEntry> mapper) {
        this.reportObserver.finishedCallReport.map(mapper).subscribe(this.entrySink);
        return this;
    }

    Evaluator withInboundRTPMapper(Function<Report, InboundRTPEntry> mapper) {
        this.reportObserver.inboundRTPReport.map(mapper).subscribe(this.entrySink);
        return this;
    }

    Evaluator withRemoteInboundRTPMapper(Function<Report, RemoteInboundRTPEntry> mapper) {
        this.reportObserver.remoteInboundRTPReport.map(mapper).subscribe(this.entrySink);
        return this;
    }

    Evaluator withOutboundRTPMapper(Function<Report, OutboundRTPEntry> mapper) {
        this.reportObserver.outboundRTPReport.map(mapper).subscribe(this.entrySink);
        return this;
    }

    Evaluator withICELocalCandidateMapper(Function<Report, ICELocalCandidateEntry> mapper) {
        this.reportObserver.iceLocalCandidateReport.map(mapper).subscribe(this.entrySink);
        return this;
    }

    Evaluator withICERemoteCandidateMapper(Function<Report, ICERemoteCandidateEntry> mapper) {
        this.reportObserver.iceRemoteCandidateReport.map(mapper).subscribe(this.entrySink);
        return this;
    }

    Evaluator withICECandidatePairMapper(Function<Report, ICECandidatePairEntry> mapper) {
        this.reportObserver.iceCandidatePairReport.map(mapper).subscribe(this.entrySink);
        return this;
    }

    Evaluator withTrackMapper(Function<Report, TrackEntry> mapper) {
        this.reportObserver.trackReport.map(mapper).subscribe(this.entrySink);
        return this;
    }

    Evaluator withUserMediaError(Function<Report, UserMediaErrorEntry> mapper) {
        this.reportObserver.userMediaErrorReport.map(mapper).subscribe(this.entrySink);
        return this;
    }

    Evaluator withMediaSource(Function<Report, MediaSourceEntry> mapper) {
        this.reportObserver.mediaSourceReport.map(mapper).subscribe(this.entrySink);
        return this;
    }

    Evaluator withJoinedPeerConnectionMapper(Function<Report, JoinedPeerConnectionEntry> mapper) {
        this.reportObserver.joinedPeerConnectionCallReport.map(mapper).subscribe(this.entrySink);
        return this;
    }

    Evaluator withDetachedPeerConnectionMapper(Function<Report, DetachedPeerConnectionEntry> mapper) {
        this.reportObserver.detachedPeerConnectionCallReport.map(mapper).subscribe(this.entrySink);
        return this;
    }
}
