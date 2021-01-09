package org.observertc.webrtc.connector.sinks.bigquery.mappers;

import io.reactivex.rxjava3.functions.Function;
import org.observertc.webrtc.connector.sinks.bigquery.models.JoinedPeerConnectionEntry;
import org.observertc.webrtc.schemas.reports.JoinedPeerConnection;
import org.observertc.webrtc.schemas.reports.Report;


class JoinedPeerConnectionMapper implements Function<Report, JoinedPeerConnectionEntry> {

    @Override
    public JoinedPeerConnectionEntry apply(Report report) {
        JoinedPeerConnection joinedPeerConnection = (JoinedPeerConnection) report.getPayload();
        JoinedPeerConnectionEntry entry = new JoinedPeerConnectionEntry()
                .withServiceUUID(report.getServiceUUID())
                .withServiceName(report.getServiceName())
                .withCallUUID(joinedPeerConnection.getCallUUID())
                .withCallName(joinedPeerConnection.getCallName())
                .withMarker(report.getMarker())
                .withTimestamp(report.getTimestamp())
                .withTimeZone(joinedPeerConnection.getTimeZoneId())
                //
                .withMediaUnitId(joinedPeerConnection.getMediaUnitId())
                .withUserId(joinedPeerConnection.getUserId())
                .withBrowserId(joinedPeerConnection.getBrowserId())
                .withPeerConnectionUUID(joinedPeerConnection.getPeerConnectionUUID())
                //
                ;
        return entry;
    }
}
