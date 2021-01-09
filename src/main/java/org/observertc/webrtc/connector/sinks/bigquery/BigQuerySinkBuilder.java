package org.observertc.webrtc.connector.sinks.bigquery;

import io.micronaut.context.annotation.Prototype;
import org.observertc.webrtc.connector.adapters.bigquery.Adapter;
import org.observertc.webrtc.connector.adapters.bigquery.version1.SchemaAdapter;
import org.observertc.webrtc.connector.configbuilders.AbstractBuilder;
import org.observertc.webrtc.connector.sinks.Sink;
import org.observertc.webrtc.connector.sinks.SinkTypeBuilder;
import org.observertc.webrtc.schemas.reports.ReportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Prototype
public class BigQuerySinkBuilder extends AbstractBuilder implements SinkTypeBuilder {

    private static final Logger logger = LoggerFactory.getLogger(BigQuerySinkBuilder.class);
    private final Map<ReportType, String> mapping;

    public BigQuerySinkBuilder() {
        this.mapping = new HashMap<>();
    }

    @Override
    public Sink build() {
        Config config = this.convertAndValidate(Config.class);
        this.mapping.put(ReportType.INITIATED_CALL, config.initiatedCallsTable);
        this.mapping.put(ReportType.FINISHED_CALL, config.finishedCallsTable);
        this.mapping.put(ReportType.JOINED_PEER_CONNECTION, config.joinedPeerConnectionsTable);
        this.mapping.put(ReportType.DETACHED_PEER_CONNECTION, config.detachedPeerConnectionsTable);
        this.mapping.put(ReportType.INBOUND_RTP, config.inboundRTPSamplesTable);
        this.mapping.put(ReportType.REMOTE_INBOUND_RTP, config.remoteInboundRTPSamplesTable);
        this.mapping.put(ReportType.OUTBOUND_RTP, config.outboundRTPSamplesTable);
        this.mapping.put(ReportType.ICE_CANDIDATE_PAIR, config.iceCandidatePairsTable);
        this.mapping.put(ReportType.ICE_LOCAL_CANDIDATE, config.iceLocalCandidatesTable);
        this.mapping.put(ReportType.ICE_REMOTE_CANDIDATE, config.iceRemoteCandidatesTable);
        this.mapping.put(ReportType.MEDIA_SOURCE, config.mediaSourcesTable);
        this.mapping.put(ReportType.USER_MEDIA_ERROR, config.userMediaErrorsTable);
        this.mapping.put(ReportType.TRACK, config.trackReportsTable);
        this.mapping.put(ReportType.OBSERVER_EVENT, config.observerEventTable);
        BigQueryService bigQueryService = new BigQueryService(config.projectId, config.datasetId, config.credentialFile);

        Map<ReportType, Adapter> adapters = this.runSchemaAdapter(bigQueryService, config);
        if (Objects.isNull(adapters)) {
            logger.error("The schema cannot be built, because it does not have adapters");
            return null;
        }

        BigQuerySink result = new BigQuerySink(bigQueryService);
        adapters.entrySet()
                .forEach(
                        entry -> {
                            ReportType reportType = entry.getKey();
                            String tableName = this.mapping.get(reportType);
                            result.withRoute(
                                    reportType,
                                    tableName,
                                    entry.getValue()
                            );
                        });

        return result;
    }

    private Map<ReportType, Adapter> runSchemaAdapter(BigQueryService bigQueryService, Config config) {
        try (SchemaAdapter schemaCheckerJob = new SchemaAdapter(bigQueryService.getBigQuery(), config.projectId, config.datasetId)) {
            schemaCheckerJob
                    .withSchemaCheckEnabled(config.schemaCheck.enabled)
                    .withCreateDatasetIfNotExists(config.schemaCheck.createDatasetIfNotExists)
                    .withCreateTableIfNotExists(config.schemaCheck.createTableIfNotExists)
                    .withDeleteTableIfExists(config.schemaCheck.deleteTableIfExists)
                    ;
            this.mapping.entrySet()
                    .forEach(entry -> schemaCheckerJob.withTableName(entry.getKey(), entry.getValue()));
            schemaCheckerJob.run();
            return schemaCheckerJob.getAdapters();
        } catch (Exception e) {
            logger.error("Error occured during schema checking process", e);
            return null;
        }
    }

    public static class Config {

        @NotNull
        public String credentialFile;

        @NotNull
        public String projectId;

        @NotNull
        public String datasetId;

        public SchemaCheckConfig schemaCheck = new SchemaCheckConfig();

        public static class SchemaCheckConfig {
            public boolean enabled = true;

            public boolean createDatasetIfNotExists = true;

            public boolean createTableIfNotExists = true;

            public String deleteTableIfExists = null;
        }

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
