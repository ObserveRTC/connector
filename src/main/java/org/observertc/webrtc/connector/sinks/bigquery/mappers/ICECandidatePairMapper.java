package org.observertc.webrtc.connector.sinks.bigquery.mappers;

import io.reactivex.rxjava3.functions.Function;
import org.observertc.webrtc.connector.sinks.bigquery.models.ICECandidatePairEntry;
import org.observertc.webrtc.schemas.reports.ICECandidatePair;
import org.observertc.webrtc.schemas.reports.Report;


class ICECandidatePairMapper implements Function<Report, ICECandidatePairEntry> {

    @Override
    public ICECandidatePairEntry apply(Report report) {
        ICECandidatePair iceCandidatePair = (ICECandidatePair) report.getPayload();
        ICECandidatePairEntry entry = new ICECandidatePairEntry()
                .withServiceUUID(report.getServiceUUID())
                .withServiceName(report.getServiceName())
                .withCallName(iceCandidatePair.getCallName())
                .withMarker(report.getMarker())
                .withTimestamp(report.getTimestamp())
                //
                .withMediaUnitId(iceCandidatePair.getMediaUnitId())
                .withUserId(iceCandidatePair.getUserId())
                .withBrowserId(iceCandidatePair.getBrowserId())
                .withPeerConnectionUUID(iceCandidatePair.getPeerConnectionUUID())
                //
                .withCandidatePairId(iceCandidatePair.getCandidatePairId())
                .withLocalCandidateId(iceCandidatePair.getLocalCandidateID())
                .withRemoteCandidateId(iceCandidatePair.getRemoteCandidateID())
                .withNominated(iceCandidatePair.getNominated())
                .withAvailableOutgoingBitrate(iceCandidatePair.getAvailableOutgoingBitrate())
                .withBytesReceived(iceCandidatePair.getBytesReceived())
                .withBytesSent(iceCandidatePair.getBytesSent())
                .withConsentRequestsSent(iceCandidatePair.getConsentRequestsSent())
                .withCurrentRoundTripTime(iceCandidatePair.getCurrentRoundTripTime())
                .withPriority(iceCandidatePair.getPriority())
                .withRequestsReceived(iceCandidatePair.getRequestsReceived())
                .withRequestsSent(iceCandidatePair.getRequestsSent())
                .withResponseReceived(iceCandidatePair.getResponsesReceived())
                .withResponseSent(iceCandidatePair.getResponsesSent())
                .withICEState(iceCandidatePair.getState())
                .withTotalRoundTripTime(iceCandidatePair.getTotalRoundTripTime())
                .withWritable(iceCandidatePair.getWritable())
                //
                ;
        return entry;
    }
}
