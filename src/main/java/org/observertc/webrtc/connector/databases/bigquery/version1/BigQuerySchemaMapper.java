package org.observertc.webrtc.connector.databases.bigquery.version1;

import com.google.cloud.bigquery.*;
import org.apache.avro.Schema;
import org.observertc.webrtc.connector.databases.SchemaMapperAbstract;
import org.observertc.webrtc.connector.databases.bigquery.Adapter;
import org.observertc.webrtc.connector.databases.bigquery.AdapterBuilder;
import org.observertc.webrtc.schemas.reports.Report;
import org.observertc.webrtc.schemas.reports.ReportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class BigQuerySchemaMapper extends SchemaMapperAbstract {
    private final BigQuery bigQuery;
    private final String projectId;
    private final String datasetId;
    private final Map<ReportType, String> tableNames = new HashMap<>();
    private final Map<ReportType, Adapter> adapters = new HashMap<>();
    private boolean useTimestampResolver = true;

    public BigQuerySchemaMapper(BigQuery bigQuery, String projectId, String datasetId) {
        this.bigQuery = bigQuery;
        this.projectId = projectId;
        this.datasetId = datasetId;
    }

    public BigQuerySchemaMapper byUsingTimestampResolver(boolean value) {
        this.useTimestampResolver = value;
        return this;
    }

    public BigQuerySchemaMapper withTableName(ReportType reportType, String tableName) {
        this.tableNames.put(reportType, tableName);
        return this;
    }

    public Map<ReportType, Adapter> getAdapters() {
        return this.adapters;
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

    @Override
    protected boolean isDatabaseExists() {
        logger.info("Checking dataset {} existance in project {}", datasetId, projectId);
        DatasetId indatasetId = DatasetId.of(projectId, datasetId);
        Dataset dataset = bigQuery.getDataset(indatasetId);
        return dataset != null && dataset.exists();
    }

    @Override
    protected boolean isTableExistsForReportType(ReportType reportType, Schema schema) {
        String tableName = this.tableNames.getOrDefault(reportType, schema.getName());
        TableId tableId = TableId.of(this.projectId, this.datasetId, tableName);
        logger.info("Checking table {} exist in dataset: {}, project: {}", tableId.getTable(), tableId.getDataset(), tableId.getProject());
        Table table = bigQuery.getTable(tableId);
        return table != null && table.exists();
    }

    @Override
    protected void createDatabase() {
        DatasetId indatasetId = DatasetId.of(projectId, datasetId);
        logger.info("Dataset {} does not exists, try to create it", indatasetId);

        DatasetInfo datasetInfo = DatasetInfo.newBuilder(indatasetId).build();
        Dataset newDataset = bigQuery.create(datasetInfo);
        String newdatasetId = newDataset.getDatasetId().getDataset();
        logger.info("BigQuery dataset {} created successfully", newdatasetId);
    }

    @Override
    protected void createTableForReportType(ReportType reportType, Schema schema) {
        AdapterBuilder subAdapterBuilder = new AdapterBuilder()
                .forSchema(schema)
                ;
        AdapterBuilder adapterBuilder = makeBaseAdapterBuilder()
                .flatMap("payload", subAdapterBuilder);
        AtomicReference<com.google.cloud.bigquery.Schema> schemaHolder = new AtomicReference<>(null);
        Adapter adapter = adapterBuilder.build(schemaHolder);
        this.adapters.put(reportType, adapter);
        com.google.cloud.bigquery.Schema bgSchema = schemaHolder.get();
        if (Objects.isNull(bgSchema)) {
            return;
        }
        String tableName = tableNames.getOrDefault(reportType, schema.getName());
        TableId tableId = TableId.of(this.projectId, this.datasetId, tableName);
        TableDefinition tableDefinition = StandardTableDefinition.of(bgSchema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        try {
            bigQuery.create(tableInfo);
            logger.info("Table {} is successfully created", tableId.getTable());
        } catch (BigQueryException e) {
            logger.error("Error during table creation. Table: " + tableId.getTable(), e);
        }
    }
}
