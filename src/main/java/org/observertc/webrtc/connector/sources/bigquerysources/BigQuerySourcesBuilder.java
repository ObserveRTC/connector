package org.observertc.webrtc.connector.sources.bigquerysources;

import io.micronaut.context.annotation.Prototype;
import io.reactivex.rxjava3.core.Observable;
import org.observertc.webrtc.connector.common.BigQueryService;
import org.observertc.webrtc.connector.configbuilders.AbstractBuilder;
import org.observertc.webrtc.connector.configbuilders.Builder;
import org.observertc.webrtc.connector.sources.Source;
import org.observertc.webrtc.schemas.reports.ReportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@Prototype
public class BigQuerySourcesBuilder extends AbstractBuilder implements Builder<Source> {

    private static final Logger logger = LoggerFactory.getLogger(BigQuerySourcesBuilder.class);

    public BigQuerySourcesBuilder() {

    }

    @Override
    public Source build() {
        Config mainConfig = this.convertAndValidate(Config.class);
        List<BigQuerySources> bigQuerySources = new LinkedList<>();
        for (InnerConfig config : mainConfig.sources) {
            BigQueryService bigQueryService = new BigQueryService(config.projectId, config.datasetId, config.credentialFile);

            BigQuerySources result = new BigQuerySources(bigQueryService)
                    .withTableName(ReportType.FINISHED_CALL, config.finishedCallsTable)
                    .withTableName(ReportType.INITIATED_CALL, config.initiatedCallsTable)
                    .withTableName(ReportType.JOINED_PEER_CONNECTION, config.joinedPeerConnectionsTable)
                    .withTableName(ReportType.DETACHED_PEER_CONNECTION, config.detachedPeerConnectionsTable)
                    .withTableName(ReportType.INBOUND_RTP, config.inboundRTPSamplesTable)
                    .withTableName(ReportType.REMOTE_INBOUND_RTP, config.remoteInboundRTPSamplesTable)
                    .withTableName(ReportType.OUTBOUND_RTP, config.outboundRTPSamplesTable)
                    .withTableName(ReportType.ICE_CANDIDATE_PAIR, config.iceCandidatePairsTable)
                    .withTableName(ReportType.ICE_LOCAL_CANDIDATE, config.iceLocalCandidatesTable)
                    .withTableName(ReportType.ICE_REMOTE_CANDIDATE, config.iceRemoteCandidatesTable)
                    .withTableName(ReportType.MEDIA_SOURCE, config.mediaSourcesTable)
                    .withTableName(ReportType.USER_MEDIA_ERROR, config.userMediaErrorsTable)
                    .withTableName(ReportType.TRACK, config.trackReportsTable)
                    .withTableName(ReportType.OBSERVER_EVENT, config.observerEventTable)

                    ;
            result.withForcedMarker(config.forcedMarker);
            bigQuerySources.add(result);
        }

        return new Source() {
            List<BigQuerySources> sources = bigQuerySources;
            @Override
            protected Observable<byte[]> makeObservable() {
                List<Observable<byte[]>> observables = new ArrayList<>();
                for (BigQuerySources s : this.sources) {
                    Observable<byte[]> source = s.makeObservable().share();
                    observables.add(source);
                }

                return Observable.concat(observables);
            }
        };
    }

    public static class Config {

        public List<InnerConfig> sources;
    }

    public static class InnerConfig {

        @NotNull
        public String credentialFile;

        @NotNull
        public String projectId;

        @NotNull
        public String datasetId;

        public String forcedMarker = null;

        public String initiatedCallsTable = "InitiatedCalls";

        public String finishedCallsTable = "FinishedCalls";

        public String joinedPeerConnectionsTable = "JoinedPeerConnections";

        public String detachedPeerConnectionsTable = "DetachedPeerConnections";

        public String remoteInboundRTPSamplesTable = "RemoteInboundRTPSamples";

        public String outboundRTPSamplesTable = "OutboundRTPSamples";

        public String inboundRTPSamplesTable = "InboundRTPSamples";

        public String iceCandidatePairsTable = "ICECandidatePairs";

        public String iceLocalCandidatesTable = "ICELocalCandidates";

        public String iceRemoteCandidatesTable = "ICERemoteCandidates";

        public String mediaSourcesTable = "MediaSources";

        public String trackReportsTable = "TrackReports";

        public String userMediaErrorsTable = "UserMediaErrors";

        public String observerEventTable = "ObserverEventReports";

    }
}
