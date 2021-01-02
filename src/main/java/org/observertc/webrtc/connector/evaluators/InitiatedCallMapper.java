package org.observertc.webrtc.connector.evaluators;

import io.reactivex.rxjava3.functions.Function;
import org.observertc.webrtc.connector.models.InitiatedCallEntry;
import org.observertc.webrtc.schemas.reports.InitiatedCall;
import org.observertc.webrtc.schemas.reports.Report;



class InitiatedCallMapper implements Function<Report, InitiatedCallEntry> {

    @Override
    public InitiatedCallEntry apply(Report report) {
        InitiatedCall initiatedCall = (InitiatedCall) report.getPayload();
        InitiatedCallEntry entry = new InitiatedCallEntry()
                .withServiceUUID(report.getServiceUUID())
                .withServiceName(report.getServiceName())
                .withCallUUID(initiatedCall.getCallUUID())
                .withCallName(initiatedCall.getCallName())
                .withMarker(report.getMarker())
                .withTimestamp(report.getTimestamp())
                //
                ;
        return entry;
    }
}
