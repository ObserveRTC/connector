package org.observertc.webrtc.connector.sinks.bigquery.mappers;

import io.reactivex.rxjava3.functions.Function;
import org.observertc.webrtc.connector.sinks.bigquery.models.UserMediaErrorEntry;
import org.observertc.webrtc.schemas.reports.Report;
import org.observertc.webrtc.schemas.reports.UserMediaError;


class UserMediaErrorMapper implements Function<Report, UserMediaErrorEntry> {

    @Override
    public UserMediaErrorEntry apply(Report report) {
        UserMediaError userMediaErrorReport = (UserMediaError) report.getPayload();
        UserMediaErrorEntry entry = new UserMediaErrorEntry()
                .withServiceUUID(report.getServiceUUID())
                .withServiceName(report.getServiceName())
                .withCallName(userMediaErrorReport.getCallName())
                .withMarker(report.getMarker())
                .withTimestamp(report.getTimestamp())
                //
                .withUserId(userMediaErrorReport.getUserId())
                .withBrowserId(userMediaErrorReport.getBrowserId())
                .withMediaUnitId(userMediaErrorReport.getMediaUnitId())
                .withMessage(userMediaErrorReport.getMessage())
                .withPeerConnectionUUID(userMediaErrorReport.getPeerConnectionUUID())
                //
                ;
        return entry;
    }
}
