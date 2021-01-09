package org.observertc.webrtc.connector.sinks.bigquery.mappers;

import io.reactivex.rxjava3.functions.Function;
import org.observertc.webrtc.connector.sinks.bigquery.models.ICERemoteCandidateEntry;
import org.observertc.webrtc.schemas.reports.ICERemoteCandidate;
import org.observertc.webrtc.schemas.reports.Report;


class ICERemoteCandidateMapper implements Function<Report, ICERemoteCandidateEntry> {

    @Override
    public ICERemoteCandidateEntry apply(Report report) {
        ICERemoteCandidate iceRemoteCandidate = (ICERemoteCandidate) report.getPayload();
        ICERemoteCandidateEntry entry = new ICERemoteCandidateEntry()
                .withServiceUUID(report.getServiceUUID())
                .withServiceName(report.getServiceName())
                .withCallName(iceRemoteCandidate.getCallName())
                .withMarker(report.getMarker())
                .withTimestamp(report.getTimestamp())
                //
                .withMediaUnitId(iceRemoteCandidate.getMediaUnitId())
                .withUserId(iceRemoteCandidate.getUserId())
                .withBrowserId(iceRemoteCandidate.getBrowserId())
                .withPeerConnectionUUID(iceRemoteCandidate.getPeerConnectionUUID())
                //
                .withCandidateID(iceRemoteCandidate.getCandidateId())
                .withCandidateType(iceRemoteCandidate.getCandidateType())
                .withDeleted(iceRemoteCandidate.getDeleted())
                .withIPLSH(iceRemoteCandidate.getIpLSH())
                .withPort(iceRemoteCandidate.getPort())
                .withPriority(iceRemoteCandidate.getPriority())
                .withProtocol(iceRemoteCandidate.getProtocol())
                //
                ;
        return entry;
    }
}
