package org.observertc.webrtc.connector.databases;

import org.apache.avro.Schema;
import org.observertc.webrtc.connector.common.AbstractTask;
import org.observertc.webrtc.connector.common.Job;
import org.observertc.webrtc.connector.common.Task;
import org.observertc.webrtc.schemas.reports.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class SchemaMapperAbstract extends Job {
    private static final Logger DEFAULT_LOGGER = LoggerFactory.getLogger(SchemaMapperAbstract.class);
    private static final String CREATE_DATASET_TASK_NAME = "CreateDatasetTask";
    private static final String CREATE_TABLES_TASK_NAME = "CreateTablesTask";

    private boolean createDatasetIfNotExists = false;
    private boolean createTableIfNotExists = false;
    private boolean schemaCheckIsEnabled = true;
    private final Map<ReportType, Schema> schemaMap = new HashMap<>();
    protected Logger logger = DEFAULT_LOGGER;

    public SchemaMapperAbstract() {
        this.schemaMap.put(ReportType.INITIATED_CALL, InitiatedCall.getClassSchema());
        this.schemaMap.put(ReportType.FINISHED_CALL, FinishedCall.getClassSchema());
        this.schemaMap.put(ReportType.JOINED_PEER_CONNECTION, JoinedPeerConnection.getClassSchema());
        this.schemaMap.put(ReportType.DETACHED_PEER_CONNECTION, DetachedPeerConnection.getClassSchema());
        this.schemaMap.put(ReportType.OBSERVER_EVENT, ObserverEventReport.getClassSchema());
        this.schemaMap.put(ReportType.EXTENSION, ExtensionReport.getClassSchema());
        this.schemaMap.put(ReportType.INBOUND_RTP, InboundRTP.getClassSchema());
        this.schemaMap.put(ReportType.OUTBOUND_RTP, OutboundRTP.getClassSchema());
        this.schemaMap.put(ReportType.REMOTE_INBOUND_RTP, RemoteInboundRTP.getClassSchema());
        this.schemaMap.put(ReportType.ICE_CANDIDATE_PAIR, ICECandidatePair.getClassSchema());
        this.schemaMap.put(ReportType.ICE_LOCAL_CANDIDATE, ICELocalCandidate.getClassSchema());
        this.schemaMap.put(ReportType.ICE_REMOTE_CANDIDATE, ICERemoteCandidate.getClassSchema());
        this.schemaMap.put(ReportType.TRACK, Track.getClassSchema());
        this.schemaMap.put(ReportType.MEDIA_SOURCE, MediaSource.getClassSchema());
        this.schemaMap.put(ReportType.USER_MEDIA_ERROR, UserMediaError.getClassSchema());
        this.schemaMap.put(ReportType.MEDIA_DEVICE, MediaDevice.getClassSchema());
        this.schemaMap.put(ReportType.CLIENT_DETAILS, ClientDetails.getClassSchema());
        this.schemaMap.put(ReportType.EXTENSION, ExtensionReport.getClassSchema());

        ReportType[] types = this.schemaMap.keySet().toArray(new ReportType[0]);
        Task createDataset = this.makeCreateDatasetTask();
        Task createTables = this.createTables(types);
        this.withTask(createDataset)
                .withTask(createTables, createDataset)
        ;
    }

    public SchemaMapperAbstract withLogger(Logger logger) {
        this.logger.info("Default logger for {} is switched to {}", this.getClass().getSimpleName(), logger.getName());
        this.logger = logger;
        return this;
    }

    public SchemaMapperAbstract createDatasetIfNotExists(boolean createDatasetIfNotExists) {
        this.createDatasetIfNotExists = createDatasetIfNotExists;
        return this;
    }

    public SchemaMapperAbstract createTableIfNotExists(boolean createTableIfNotExists) {
        this.createTableIfNotExists = createTableIfNotExists;
        return this;
    }

    public SchemaMapperAbstract withSchemaCheckEnabled(boolean value) {
        this.schemaCheckIsEnabled = value;
        return this;
    }

    protected abstract boolean isDatabaseExists();
    protected abstract boolean isTableExistsForReportType(ReportType reportType, Schema schema);
    protected abstract void createDatabase();
    protected abstract void createTableForReportType(ReportType reportType, Schema schema);

    private Task createTables(ReportType... types) {
        return new AbstractTask(CREATE_TABLES_TASK_NAME) {
            @Override
            protected void execute() {
                if (!schemaCheckIsEnabled) {
                    return;
                }
                for (ReportType type : types) {
                    Schema schema = schemaMap.get(type);
                    if (isTableExistsForReportType(type, schema)) {
                        continue;
                    }
                    if (!createTableIfNotExists) {
                        logger.warn("Table for report type {} does not exists, and it will not be created due to configuration", type);
                        continue;
                    }
                    createTableForReportType(type, schema);
                }

            }
        };
    }

    private Task makeCreateDatasetTask() {
        return new AbstractTask(CREATE_DATASET_TASK_NAME) {
            @Override
            protected void execute() {
                if (!schemaCheckIsEnabled) {
                    return;
                }
                if (isDatabaseExists()) {
                    return;
                }
                if (!createDatasetIfNotExists) {
                    logger.warn("Database is not exists, but it is not allowed to create one due to configuration");
                    return;
                }
                createDatabase();
            }
        };
    }
}
