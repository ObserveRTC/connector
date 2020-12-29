package org.observertc.webrtc.reportconnector.evaluators;

import io.reactivex.rxjava3.functions.Function;
import org.observertc.webrtc.reportconnector.models.MediaSourceEntry;
import org.observertc.webrtc.schemas.reports.MediaSource;
import org.observertc.webrtc.schemas.reports.Report;


class MediaSourceMapper implements Function<Report, MediaSourceEntry> {

    @Override
    public MediaSourceEntry apply(Report report) {
        MediaSource mediaSource = (MediaSource) report.getPayload();
        MediaSourceEntry entry = new MediaSourceEntry()
                .withServiceUUID(report.getServiceUUID())
                .withServiceName(report.getServiceName())
                .withCallName(mediaSource.getCallName())
                .withMarker(report.getMarker())
                .withTimestamp(report.getTimestamp())
                //
                .withMediaUnitId(mediaSource.getMediaUnitId())
                .withUserId(mediaSource.getUserId())
                .withBrowserId(mediaSource.getBrowserId())
                .withPeerConnectionUUID(mediaSource.getPeerConnectionUUID())
                //
                .withMediaSourceID(mediaSource.getMediaSourceId())
                .withAudioLevel(mediaSource.getAudioLevel())
                .withFramesPerSecond(mediaSource.getFramesPerSecond())
                .withHeight(mediaSource.getHeight())
                .withWidth(mediaSource.getWidth())
                .withAudioLevel(mediaSource.getAudioLevel())
                .withMediaType(mediaSource.getMediaType())
                .withTotalAudioEnergy(mediaSource.getTotalAudioEnergy())
                .withTotalSamplesDuration(mediaSource.getTotalSamplesDuration())
                //
                ;
        return entry;
    }
}
