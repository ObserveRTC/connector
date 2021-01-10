package org.observertc.webrtc.connector.sources.notversionedbigquery;

import io.reactivex.rxjava3.core.Observable;
import org.observertc.webrtc.connector.common.BigQueryService;
import org.observertc.webrtc.connector.sources.Source;
import org.observertc.webrtc.connector.sources.notversionedbigquery.observabletables.*;
import org.observertc.webrtc.schemas.reports.Report;
import org.observertc.webrtc.schemas.reports.ReportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NotVersionedBigQuerySource extends Source {
    private static final Logger logger = LoggerFactory.getLogger(NotVersionedBigQuerySource.class);
    private final Map<ReportType, String> tableNames;
    private final BigQueryService bigQueryService;
    public NotVersionedBigQuerySource(BigQueryService bigQueryService) {
        this.tableNames = new HashMap<>();
        this.bigQueryService = bigQueryService;
    }

    @Override
    protected Observable<byte[]> makeObservable() {
        List<RecordMapperAbstract> sources = List.of(
                new FinishedCalls(this.bigQueryService, this.tableNames.get(ReportType.FINISHED_CALL)),
                new InitiatedCalls(this.bigQueryService, this.tableNames.get(ReportType.INITIATED_CALL)),
                new ObserverEvents(this.bigQueryService, this.tableNames.get(ReportType.OBSERVER_EVENT)),
                new JoinedPeerConnections(this.bigQueryService, this.tableNames.get(ReportType.JOINED_PEER_CONNECTION)),
                new DetachedPeerConnections(this.bigQueryService, this.tableNames.get(ReportType.DETACHED_PEER_CONNECTION)),
                new ICECandidatePairs(this.bigQueryService, this.tableNames.get(ReportType.ICE_CANDIDATE_PAIR)),
                new ICELocalCandidates(this.bigQueryService, this.tableNames.get(ReportType.ICE_LOCAL_CANDIDATE)),
                new ICERemoteCandidates(this.bigQueryService, this.tableNames.get(ReportType.ICE_REMOTE_CANDIDATE)),
                new InboundRTPs(this.bigQueryService, this.tableNames.get(ReportType.INBOUND_RTP)),
                new OutboundRTPs(this.bigQueryService, this.tableNames.get(ReportType.OUTBOUND_RTP)),
                new RemoteInboundRTPs(this.bigQueryService, this.tableNames.get(ReportType.REMOTE_INBOUND_RTP)),
                new MediaSources(this.bigQueryService, this.tableNames.get(ReportType.MEDIA_SOURCE)),
                new Tracks(this.bigQueryService, this.tableNames.get(ReportType.TRACK)),
                new UserMediaErrors(this.bigQueryService, this.tableNames.get(ReportType.USER_MEDIA_ERROR))
        );
        var encoder = Report.getEncoder();
        return Observable.concat(sources).map(encoder::encode).map(ByteBuffer::array);
    }

    NotVersionedBigQuerySource withTableName(ReportType reportType, String tableName) {
        this.tableNames.put(reportType, tableName);
        return this;
    }
}
