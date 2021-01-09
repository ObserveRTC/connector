package org.observertc.webrtc.connector.sinks.bigquery.mappers;

import io.reactivex.rxjava3.functions.Function;
import org.observertc.webrtc.connector.sinks.bigquery.models.FinishedCallEntry;
import org.observertc.webrtc.schemas.reports.FinishedCall;
import org.observertc.webrtc.schemas.reports.Report;


class FinishedCallMapper implements Function<Report, FinishedCallEntry> {

    @Override
    public FinishedCallEntry apply(Report report) {
        FinishedCall finishedCall = (FinishedCall) report.getPayload();
        FinishedCallEntry entry = new FinishedCallEntry()
                .withServiceUUID(report.getServiceUUID())
                .withServiceName(report.getServiceName())
                .withCallUUID(finishedCall.getCallUUID())
                .withCallName(finishedCall.getCallName())
                .withMarker(report.getMarker())
                .withTimestamp(report.getTimestamp())
                //
                ;
        return entry;
    }
}
