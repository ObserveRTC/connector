package org.observertc.webrtc.connector.sinks.bigquery.mappers;

import io.reactivex.rxjava3.functions.Function;
import org.observertc.webrtc.connector.sinks.bigquery.models.ObserverEventEntry;
import org.observertc.webrtc.schemas.reports.ObserverEventReport;
import org.observertc.webrtc.schemas.reports.Report;


class ObserverEventMapper implements Function<Report, ObserverEventEntry> {

    @Override
    public ObserverEventEntry apply(Report report) {
        ObserverEventReport observerEvent = (ObserverEventReport) report.getPayload();
        ObserverEventEntry entry = new ObserverEventEntry()
                .withServiceUUID(report.getServiceUUID())
                .withServiceName(report.getServiceName())
                .withCallName(observerEvent.getCallName())
                .withMarker(report.getMarker())
                .withTimestamp(report.getTimestamp())
                //
                .withMediaUnitId(observerEvent.getMediaUnitId())
                .withUserId(observerEvent.getUserId())
                .withBrowserId(observerEvent.getBrowserId())
                .withPeerConnectionUUID(observerEvent.getPeerConnectionUUID())
                //
                .withEventType(observerEvent.getEventType())
                .withMessage(observerEvent.getMessage())
                ;
        return entry;
    }
}
