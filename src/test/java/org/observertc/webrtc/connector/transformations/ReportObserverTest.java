package org.observertc.webrtc.connector.transformations;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.subjects.PublishSubject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.observertc.webrtc.connector.ReportGenerator;
import org.observertc.webrtc.schemas.reports.Report;
import org.observertc.webrtc.schemas.reports.ReportType;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

class ReportObserverTest {

    @Test
    void testItWorks() {
        // When
        ReportObserver observer = new ReportObserver();

        // Then
        Map<PublishSubject<Report>, ReportType> types = new HashMap<>();
        types.put(observer.initiatedCallReport, ReportType.INITIATED_CALL);
        types.put(observer.finishedCallReport, ReportType.FINISHED_CALL);
        types.put(observer.detachedPeerConnectionCallReport, ReportType.DETACHED_PEER_CONNECTION);
        types.put(observer.joinedPeerConnectionCallReport, ReportType.JOINED_PEER_CONNECTION);
        types.put(observer.inboundRTPReport, ReportType.INBOUND_RTP);
        types.put(observer.outboundRTPReport, ReportType.OUTBOUND_RTP);
        types.put(observer.remoteInboundRTPReport, ReportType.REMOTE_INBOUND_RTP);
        types.put(observer.iceLocalCandidateReport, ReportType.ICE_LOCAL_CANDIDATE);
        types.put(observer.iceRemoteCandidateReport, ReportType.ICE_REMOTE_CANDIDATE);
        types.put(observer.iceCandidatePairReport, ReportType.ICE_CANDIDATE_PAIR);
        types.put(observer.userMediaErrorReport, ReportType.USER_MEDIA_ERROR);
        types.put(observer.observerEventReport, ReportType.OBSERVER_EVENT);
        types.put(observer.trackReport, ReportType.TRACK);
        types.put(observer.unrecognizedReport, null);


        types
                .entrySet()
                .forEach(entry -> testItWorks(observer, entry.getKey(), entry.getValue())
                );
    }

    private void testItWorks(Observer<Report> reportObserver, PublishSubject<Report> subject, ReportType type) {
        // Given
        AtomicReference<ReportType> passed = new AtomicReference<>(null);
        subject.subscribe(r -> passed.set(r.getType()));
        Report report = new ReportGenerator().emptyReportSupplier(type).get();

        // When
        Observable.fromArray(report)
                .subscribe(reportObserver);

        // Then
        Assertions.assertEquals(passed.get(), type, "ReportType " + type + " is not processed properly");
    }
}