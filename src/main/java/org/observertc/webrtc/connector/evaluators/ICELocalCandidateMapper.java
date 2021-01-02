package org.observertc.webrtc.connector.evaluators;

import io.reactivex.rxjava3.functions.Function;
import org.observertc.webrtc.connector.models.ICELocalCandidateEntry;
import org.observertc.webrtc.schemas.reports.ICELocalCandidate;
import org.observertc.webrtc.schemas.reports.Report;


class ICELocalCandidateMapper implements Function<Report, ICELocalCandidateEntry> {

    @Override
    public ICELocalCandidateEntry apply(Report report) {
        ICELocalCandidate iceLocalCandidate = (ICELocalCandidate) report.getPayload();
        ICELocalCandidateEntry entry = new ICELocalCandidateEntry()
                .withServiceUUID(report.getServiceUUID())
                .withServiceName(report.getServiceName())
                .withCallName(iceLocalCandidate.getCallName())
                .withMarker(report.getMarker())
                .withTimestamp(report.getTimestamp())
                //
                .withMediaUnitId(iceLocalCandidate.getMediaUnitId())
                .withUserId(iceLocalCandidate.getUserId())
                .withBrowserId(iceLocalCandidate.getBrowserId())
                .withPeerConnectionUUID(iceLocalCandidate.getPeerConnectionUUID())
                //
                .withCandidateID(iceLocalCandidate.getCandidateId())
                .withCandidateType(iceLocalCandidate.getCandidateType())
                .withDeleted(iceLocalCandidate.getDeleted())
                .withIPLSH(iceLocalCandidate.getIpLSH())
                .withNetworkType(iceLocalCandidate.getNetworkType())
                .withPort(iceLocalCandidate.getPort())
                .withPriority(iceLocalCandidate.getPriority())
                .withProtocol(iceLocalCandidate.getProtocol())
                //
                ;
        return entry;
    }
}
