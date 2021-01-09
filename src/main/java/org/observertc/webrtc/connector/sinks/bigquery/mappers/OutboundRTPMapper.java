package org.observertc.webrtc.connector.sinks.bigquery.mappers;

import io.reactivex.rxjava3.functions.Function;
import org.observertc.webrtc.connector.sinks.bigquery.models.OutboundRTPEntry;
import org.observertc.webrtc.schemas.reports.OutboundRTP;
import org.observertc.webrtc.schemas.reports.Report;


class OutboundRTPMapper implements Function<Report, OutboundRTPEntry> {

    @Override
    public OutboundRTPEntry apply(Report report) {
        OutboundRTP outboundRTP = (OutboundRTP) report.getPayload();
        OutboundRTPEntry entry = new OutboundRTPEntry()
                .withServiceUUID(report.getServiceUUID())
                .withServiceName(report.getServiceName())
                .withCallName(outboundRTP.getCallName())
                .withMarker(report.getMarker())
                .withTimestamp(report.getTimestamp())
                //
                .withMediaUnitId(outboundRTP.getMediaUnitId())
                .withUserId(outboundRTP.getUserId())
                .withBrowserId(outboundRTP.getBrowserId())
                .withPeerConnectionUUID(outboundRTP.getPeerConnectionUUID())
                //
                .withSSRC(outboundRTP.getSsrc())
                .withMediaType(outboundRTP.getMediaType())
                .withBytesSent(outboundRTP.getBytesSent())
                .withEncoderImplementation(outboundRTP.getEncoderImplementation())
                .withFirCount(outboundRTP.getFirCount())
                .withFramesEncoded(outboundRTP.getFramesEncoded())
                .withHeaderBytesSent(outboundRTP.getHeaderBytesSent())
                .withKeyFramesEncoded(outboundRTP.getKeyFramesEncoded())
                .withTransportId(outboundRTP.getTransportID())
                .withNackCount(outboundRTP.getNackCount())
                .withPacketsSent(outboundRTP.getPacketsSent())
                .withPLICount(outboundRTP.getPliCount())
                .withQPSum(outboundRTP.getQpSum())
                .withQualityLimitationReason(outboundRTP.getQualityLimitationReason())
                .withQualityLimitationResolutionChanges(outboundRTP.getQualityLimitationResolutionChanges())
                .withRetransmittedBytesSent(outboundRTP.getRetransmittedBytesSent())
                .withRetransmittedPacketsSent(outboundRTP.getRetransmittedPacketsSent())
                .withTotalEncodedTime(outboundRTP.getTotalEncodeTime())
                .withTotalPacketsSendDelay(outboundRTP.getTotalPacketSendDelay())
                .withTotalEncodedByesTarget(outboundRTP.getTotalEncodedBytesTarget());
        return entry;
    }
}
