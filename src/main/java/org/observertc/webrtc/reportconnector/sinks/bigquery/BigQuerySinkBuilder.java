package org.observertc.webrtc.reportconnector.sinks.bigquery;

import org.observertc.webrtc.reportconnector.configbuilders.AbstractBuilder;
import org.observertc.webrtc.reportconnector.models.EntryType;
import org.observertc.webrtc.reportconnector.sinks.Sink;
import org.observertc.webrtc.reportconnector.sinks.SinkTypeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;

public class BigQuerySinkBuilder extends AbstractBuilder implements SinkTypeBuilder {

    private static final Logger logger = LoggerFactory.getLogger(BigQuerySinkBuilder.class);
    private final Map<EntryType, String> entryTypeMaps;

    public BigQuerySinkBuilder() {
        this.entryTypeMaps = new HashMap<>();
    }

    @Override
    public Sink build() {
        Config config = this.convertAndValidate(Config.class);
        this.entryTypeMaps.put(EntryType.InitiatedCall, config.initiatedCallsTable);
        this.entryTypeMaps.put(EntryType.FinishedCall, config.finishedCallsTable);
        this.entryTypeMaps.put(EntryType.JoinedPeerConnection, config.joinedPeerConnectionsTable);
        this.entryTypeMaps.put(EntryType.DetachedPeerConnection, config.detachedPeerConnectionsTable);
        this.entryTypeMaps.put(EntryType.InboundRTP, config.inboundRTPSamplesTable);
        this.entryTypeMaps.put(EntryType.RemoteInboundRTP, config.remoteInboundRTPSamplesTable);
        this.entryTypeMaps.put(EntryType.OutboundRTP, config.outboundRTPSamplesTable);
        this.entryTypeMaps.put(EntryType.ICECandidatePair, config.iceCandidatePairsTable);
        this.entryTypeMaps.put(EntryType.ICELocalCandidate, config.iceLocalCandidatesTable);
        this.entryTypeMaps.put(EntryType.ICERemoteCandidate, config.iceRemoteCandidatesTable);
        this.entryTypeMaps.put(EntryType.MediaSource, config.mediaSourcesTable);
        this.entryTypeMaps.put(EntryType.UserMediaError, config.userMediaErrorsTable);
        this.entryTypeMaps.put(EntryType.Track, config.trackReportsTable);
        BigQueryService bigQueryService = new BigQueryService(config.projectId, config.datasetId, config.credentialFile);

        try (SchemaCheckerJob schemaCheckerJob = new SchemaCheckerJob(bigQueryService.getBigQuery())) {
            schemaCheckerJob
                    .withCreateDatasetIfNotExists(config.createDatasetIfNotExists)
                    .withCreateTableIfNotExists(config.createTableIfNotExists)
                    .withDatasetId(config.datasetId)
                    .withProjectId(config.projectId);
            this.entryTypeMaps.entrySet()
                    .stream()
                    .forEach(entry -> schemaCheckerJob.withEntryName(entry.getKey(), entry.getValue()));
            schemaCheckerJob.perform();
        } catch (Exception e) {
            logger.error("Error occured during schema checking process", e);
            return null;
        }

        BigQuerySink result = new BigQuerySink(bigQueryService);
        this.entryTypeMaps.entrySet()
                .stream()
                .forEach(entry -> result.withEntryRoute(entry.getKey(), entry.getValue()));

        return result;
    }

    public static class Config {

        @NotNull
        public String credentialFile;

        @NotNull
        public String projectId;

        @NotNull
        public String datasetId;

        public boolean createDatasetIfNotExists = true;

        public boolean createTableIfNotExists = true;

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

    }
}
