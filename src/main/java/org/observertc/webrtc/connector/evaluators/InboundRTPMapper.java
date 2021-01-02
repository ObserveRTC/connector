package org.observertc.webrtc.connector.evaluators;

import io.reactivex.rxjava3.functions.Function;
import org.observertc.webrtc.connector.models.InboundRTPEntry;
import org.observertc.webrtc.schemas.reports.InboundRTP;
import org.observertc.webrtc.schemas.reports.Report;


class InboundRTPMapper implements Function<Report, InboundRTPEntry> {

    @Override
    public InboundRTPEntry apply(Report report) {
        InboundRTP inboundRTP = (InboundRTP) report.getPayload();
        InboundRTPEntry entry = new InboundRTPEntry()
                .withServiceUUID(report.getServiceUUID())
                .withServiceName(report.getServiceName())
                .withCallName(inboundRTP.getCallName())
                .withMarker(report.getMarker())
                .withTimestamp(report.getTimestamp())
                //
                .withMediaUnitId(inboundRTP.getMediaUnitId())
                .withUserId(inboundRTP.getUserId())
                .withBrowserId(inboundRTP.getBrowserId())
                .withPeerConnectionUUID(inboundRTP.getPeerConnectionUUID())
                //
                .withSSRC(inboundRTP.getSsrc())
                .withMediaType(inboundRTP.getMediaType())
                .withBytesReceived(inboundRTP.getBytesReceived())
                .withFirCount(inboundRTP.getFirCount())
                .withFramesDecoded(inboundRTP.getFramesDecoded())
                .withHeaderBytesReceived(inboundRTP.getHeaderBytesReceived())
                .withKeyFramesDecoded(inboundRTP.getKeyFramesDecoded())
                .withNackCount(inboundRTP.getNackCount())
                .withPacketsReceived(inboundRTP.getPacketsReceived())
                .withPLICount(inboundRTP.getPliCount())
                .withQPSum(inboundRTP.getQpSum())
                .withDecoderImplementation(inboundRTP.getDecoderImplementation())
                .withEstimatedPlayoutTimestamp(inboundRTP.getEstimatedPlayoutTimestamp())
                .withJitter(inboundRTP.getJitter())
                .withLastPacketReceivedTimestamp(inboundRTP.getLastPacketReceivedTimestamp())
                .withPacketsLost(inboundRTP.getPacketsLost())
                .withTotalDecodeTime(inboundRTP.getTotalDecodeTime())
                .withTotalInterFrameDelay(inboundRTP.getTotalInterFrameDelay())
                .withTotalSquaredInterFrameDelay(inboundRTP.getTotalSquaredInterFrameDelay())
                .withFECPacketsDiscarded(inboundRTP.getFecPacketsDiscarded())
                .withFECPacketsReceived(inboundRTP.getFecPacketsReceived())
                //
                ;
        return entry;
    }
}
