package org.observertc.webrtc.connector.evaluators;

import io.reactivex.rxjava3.functions.Function;
import org.observertc.webrtc.connector.models.TrackEntry;
import org.observertc.webrtc.schemas.reports.Report;
import org.observertc.webrtc.schemas.reports.Track;


class TrackMapper implements Function<Report, TrackEntry> {

    @Override
    public TrackEntry apply(Report report) {
        Track track = (Track) report.getPayload();
        TrackEntry entry = new TrackEntry()
                .withServiceUUID(report.getServiceUUID())
                .withServiceName(report.getServiceName())
                .withCallName(track.getCallName())
                .withMarker(report.getMarker())
                .withTimestamp(report.getTimestamp())
                //
                .withMediaUnitId(track.getMediaUnitId())
                .withUserId(track.getUserId())
                .withBrowserId(track.getBrowserId())
                .withPeerConnectionUUID(track.getPeerConnectionUUID())
                //
                .withTrackID(track.getTrackId())
                .withMediaType(track.getMediaType())
                .withConcealmentSamples(track.getConcealedSamples())
                .withConcealmentEvents(track.getConcealmentEvents())
                .withDetached(track.getDetached())
                .withEnded(track.getEnded())
                .withConcealedSamples(track.getConcealedSamples())
                .withFramesDecoded(track.getFramesDecoded())
                .withFramesDropped(track.getFramesDropped())
                .withFramesReceived(track.getFramesReceived())
                .withFramesSent(track.getFramesSent())
                .withHugeFramesSent(track.getHugeFramesSent())
                .withInsertedSamplesForDeceleration(track.getInsertedSamplesForDeceleration())
                .withJitterBufferDelay(track.getJitterBufferDelay())
                .withJitterBufferEmittedCount(track.getJitterBufferEmittedCount())
                .withRemoteSource(track.getRemoteSource())
                .withRemovedSamplesForAcceleration(track.getRemovedSamplesForAcceleration())
                .withSilentConcealedSamples(track.getSilentConcealedSamples())
                .withTotalSamplesReceived(track.getTotalSamplesReceived())
                .withMediaSourceID(track.getMediaSourceID());
        return entry;
    }
}
