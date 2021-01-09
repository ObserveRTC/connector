package org.observertc.webrtc.connector.sinks.bigquery.mappers;

import io.reactivex.rxjava3.functions.Function;
import org.observertc.webrtc.connector.sinks.bigquery.models.RemoteInboundRTPEntry;
import org.observertc.webrtc.schemas.reports.RemoteInboundRTP;
import org.observertc.webrtc.schemas.reports.Report;


class RemoteInboundRTPMapper implements Function<Report, RemoteInboundRTPEntry> {

    @Override
    public RemoteInboundRTPEntry apply(Report report) {
        RemoteInboundRTP remoteInboundRTP = (RemoteInboundRTP) report.getPayload();
        RemoteInboundRTPEntry entry = new RemoteInboundRTPEntry()
                .withServiceUUID(report.getServiceUUID())
                .withServiceName(report.getServiceName())
                .withCallName(remoteInboundRTP.getCallName())
                .withMarker(report.getMarker())
                .withTimestamp(report.getTimestamp())
                //
                .withMediaUnitId(remoteInboundRTP.getMediaUnitId())
                .withUserId(remoteInboundRTP.getUserId())
                .withBrowserId(remoteInboundRTP.getBrowserId())
                .withPeerConnectionUUID(remoteInboundRTP.getPeerConnectionUUID())
                //
                .withSSRC(remoteInboundRTP.getSsrc())
                .withPacketsLost(remoteInboundRTP.getPacketsLost())
                .withRTT(remoteInboundRTP.getRoundTripTime())
                .withJitter(remoteInboundRTP.getJitter())
                .withCodec(remoteInboundRTP.getCodecID())
                .withMediaType(remoteInboundRTP.getMediaType())
                .withTransportId(remoteInboundRTP.getTransportID())
                //
                ;
        return entry;
    }
}
