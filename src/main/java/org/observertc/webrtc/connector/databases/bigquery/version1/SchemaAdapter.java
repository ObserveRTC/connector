package org.observertc.webrtc.connector.databases.bigquery.version1;

import com.google.cloud.bigquery.*;
import org.apache.avro.Schema;
import org.observertc.webrtc.connector.databases.AbstractTask;
import org.observertc.webrtc.connector.databases.Job;
import org.observertc.webrtc.connector.databases.Task;
import org.observertc.webrtc.connector.databases.bigquery.Adapter;
import org.observertc.webrtc.connector.databases.bigquery.AdapterBuilder;
import org.observertc.webrtc.connector.sinks.Sink;
import org.observertc.webrtc.schemas.reports.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class SchemaAdapter extends Job {
    private static final Logger DEFAULT_LOGGER = LoggerFactory.getLogger(SchemaAdapter.class);
    private static final String CREATE_DATASET_TASK_NAME = "CreateDatasetTask";
    private static final String CREATE_TABLES_TASK_NAME = "CreateTablesTask";

    private final BigQuery bigQuery;
    private final String projectId;
    private final String datasetId;
    private boolean createDatasetIfNotExists = false;
    private boolean createTableIfNotExists = false;
    private String deleteTableIfExists = null;
    private boolean schemaCheckIsEnabled = true;
    private final Map<ReportType, Schema> schemaMap = new HashMap<>();
    private final Map<ReportType, String> tableNames = new HashMap<>();
    private final Map<ReportType, Adapter> adapters = new HashMap<>();
    private Logger logger = DEFAULT_LOGGER;
    private boolean useTimestampResolver = true;

    public SchemaAdapter(BigQuery bigQuery, String projectId, String datasetId) {
        this.bigQuery = bigQuery;
        this.projectId = projectId;
        this.datasetId = datasetId;
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

        Task createDataset = this.makeCreateDatasetTask();
        Task createTables = this.createTables(
                ReportType.INITIATED_CALL,
                ReportType.FINISHED_CALL,
                ReportType.JOINED_PEER_CONNECTION,
                ReportType.DETACHED_PEER_CONNECTION,
                ReportType.OBSERVER_EVENT,
                ReportType.EXTENSION,
                ReportType.INBOUND_RTP,
                ReportType.OUTBOUND_RTP,
                ReportType.REMOTE_INBOUND_RTP,
                ReportType.ICE_CANDIDATE_PAIR,
                ReportType.ICE_LOCAL_CANDIDATE,
                ReportType.ICE_REMOTE_CANDIDATE,
                ReportType.TRACK,
                ReportType.MEDIA_SOURCE,
                ReportType.USER_MEDIA_ERROR
        );
        this.withTask(createDataset)
                .withTask(createTables, createDataset)
        ;
    }

    public SchemaAdapter withLogger(Logger logger) {
        this.logger.info("Default logger for {} is switched to {}", this.getClass().getSimpleName(), logger.getName());
        this.logger = logger;
        return this;
    }

    public SchemaAdapter withTableName(ReportType reportType, String tableName) {
        this.tableNames.put(reportType, tableName);
        return this;
    }

    public SchemaAdapter byUsingTimestampResolver(boolean value) {
        this.useTimestampResolver = value;
        return this;
    }

    public Map<ReportType, Adapter> getAdapters() {
        return this.adapters;
    }

    public SchemaAdapter withCreateDatasetIfNotExists(boolean createDatasetIfNotExists) {
        this.createDatasetIfNotExists = createDatasetIfNotExists;
        return this;
    }

    public SchemaAdapter withCreateTableIfNotExists(boolean createTableIfNotExists) {
        this.createTableIfNotExists = createTableIfNotExists;
        return this;
    }

    public SchemaAdapter withDeleteTableIfExists(String deleteTableIfExists) {
        this.deleteTableIfExists = deleteTableIfExists;
        return this;
    }

    public SchemaAdapter withSchemaCheckEnabled(boolean value) {
        this.schemaCheckIsEnabled = value;
        return this;
    }


    private AdapterBuilder makeBaseAdapterBuilder() {
        Function<Long, Long> resolver;
        if (this.useTimestampResolver) {
            resolver = epoch -> {
                return epoch / 1000L;
            };
        } else {
            resolver = Function.identity();
        }

        AdapterBuilder result = new AdapterBuilder()
                .forSchema(Report.getClassSchema())
                .excludeFields("version")
                .excludeFields("type")
                .explicitTypeMapping("timestamp", LegacySQLTypeName.TIMESTAMP)
                .mapFieldBy("timestamp", resolver)
                ;
        return result;
    }

    private Task createTables(ReportType... types) {
        return new AbstractTask(CREATE_TABLES_TASK_NAME) {
            @Override
            protected void execute() {
                for (ReportType type : types) {
                    Schema schema = schemaMap.get(type);
                    if (Objects.isNull(type)) {
                        logger.warn("No schema mapping exists for report type {}", type);
                        continue;
                    }
                    AdapterBuilder subAdapterBuilder = new AdapterBuilder()
                            .forSchema(schema)
                    ;
                    AdapterBuilder adapterBuilder = makeBaseAdapterBuilder()
                            .flatMap("payload", subAdapterBuilder);
                    AtomicReference<com.google.cloud.bigquery.Schema> schemaHolder = new AtomicReference<>(null);
                    Adapter adapter = adapterBuilder.build(schemaHolder);
                    try {
                        com.google.cloud.bigquery.Schema bgSchema = schemaHolder.get();
                        if (Objects.nonNull(bgSchema)) {
                            String tableName = tableNames.getOrDefault(type, schema.getName());
                            applySchemaForTable(tableName, bgSchema);
                        }
                    } catch (Exception ex) {
                        logger.error("Exception during execution", ex);
                        return;
                    }
                    adapters.put(type, adapter);
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
                if (!createDatasetIfNotExists) {
                    return;
                }
                logger.info("Checking dataset {} existance in project {}", datasetId, projectId);
                DatasetId indatasetId = DatasetId.of(projectId, datasetId);
                Dataset dataset = bigQuery.getDataset(indatasetId);
                if (dataset != null && dataset.exists()) {
                    return;
                }
                logger.info("Dataset {} does not exists, try to create it", indatasetId);

                DatasetInfo datasetInfo = DatasetInfo.newBuilder(indatasetId).build();
                Dataset newDataset = bigQuery.create(datasetInfo);
                String newdatasetId = newDataset.getDatasetId().getDataset();
                logger.info("BigQuery dataset {} created successfully", newdatasetId);
            }
        };
    }

    private void applySchemaForTable(String tableName, com.google.cloud.bigquery.Schema schema) {
        TableId tableId = TableId.of(this.projectId, this.datasetId, tableName);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        logger.info("Checking table {} exist in dataset: {}, project: {}", tableId.getTable(), tableId.getDataset(), tableId.getProject());
        Table table = bigQuery.getTable(tableId);
        if (!schemaCheckIsEnabled) {
            return;
        }
        if (table != null && table.exists()) {
            if (Objects.nonNull(this.deleteTableIfExists)) {
                if (this.deleteTableIfExists.equals(tableId.getTable()) || this.deleteTableIfExists.toLowerCase().equals("all")) {
                    logger.info("Delete table is on for {}, and the pattern matched now", this.deleteTableIfExists);
                    try {
                        bigQuery.delete(tableId);
                        logger.info("Table {} is successfully deleted", tableId.getTable());
                    } catch (Exception ex) {
                        logger.error("Cannot delete table " + tableId.toString() + ", some error happened", ex);
                        return;
                    }
                }
            } else {
                return;
            }
        }
        if (!this.createTableIfNotExists) {
            return;
        }
        try {
            bigQuery.create(tableInfo);
            logger.info("Table {} is successfully created", tableId.getTable());
        } catch (BigQueryException e) {
            logger.error("Error during table creation. Table: " + tableId.getTable(), e);
        }
    }

    private void delete() {

    }

}
