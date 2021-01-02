package org.observertc.webrtc.connector.evaluators;

import io.reactivex.rxjava3.functions.Function;
import org.observertc.webrtc.connector.models.DetachedPeerConnectionEntry;
import org.observertc.webrtc.schemas.reports.DetachedPeerConnection;
import org.observertc.webrtc.schemas.reports.Report;


class DetachedPeerConnectionMapper implements Function<Report, DetachedPeerConnectionEntry> {

    @Override
    public DetachedPeerConnectionEntry apply(Report report) {
        DetachedPeerConnection detachedPeerConnection = (DetachedPeerConnection) report.getPayload();
        DetachedPeerConnectionEntry entry = new DetachedPeerConnectionEntry()
                .withServiceUUID(report.getServiceUUID())
                .withServiceName(report.getServiceName())
                .withCallUUID(detachedPeerConnection.getCallUUID())
                .withCallName(detachedPeerConnection.getCallName())
                .withMarker(report.getMarker())
                .withTimestamp(report.getTimestamp())
                .withTimeZone("NOT IMPLEMENTED")
                //
                .withMediaUnitId(detachedPeerConnection.getMediaUnitId())
                .withUserId(detachedPeerConnection.getUserId())
                .withBrowserId(detachedPeerConnection.getBrowserId())
                .withPeerConnectionUUID(detachedPeerConnection.getPeerConnectionUUID())
                //
                ;
        return entry;
    }
}
