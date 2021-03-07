package org.observertc.webrtc.connector.databases.bigquery.version1;

import com.google.cloud.bigquery.*;
import org.apache.avro.Schema;
import org.observertc.webrtc.connector.databases.ReportMapper;
import org.observertc.webrtc.connector.databases.SchemaMapperAbstract;
import org.observertc.webrtc.schemas.reports.ReportType;

import java.util.*;
import java.util.function.Function;

public class BigQuerySchemaMapper extends SchemaMapperAbstract {
    private final BigQuery bigQuery;
    private final String projectId;
    private final String datasetId;
    private final Map<ReportType, String> tableNames = new HashMap<>();
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
        List<Field> bgFields = new LinkedList<>();
        bgFields.add(Field.newBuilder("serviceUUID", LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build());
        bgFields.add(Field.newBuilder("serviceName", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        bgFields.add(Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.REQUIRED).build());
        bgFields.add(Field.newBuilder("marker", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        this.schemaFieldWalker(schema, avroFieldInfo -> {
            Field.Mode mode;
            if (avroFieldInfo.embedded || avroFieldInfo.schema.isNullable()) {
                mode = Field.Mode.NULLABLE;
            } else {
                mode = Field.Mode.REQUIRED;
            }
            LegacySQLTypeName bgFieldType = convertTypLegacySQLType(avroFieldInfo.schema.getType());
            com.google.cloud.bigquery.Field bgField = Field.newBuilder(
                    avroFieldInfo.fieldName,
                    bgFieldType
            ).setMode(mode)
                    .setDescription(avroFieldInfo.schema.getDoc())
                    .build();
            bgFields.add(bgField);
        });
        com.google.cloud.bigquery.Schema bgSchema = com.google.cloud.bigquery.Schema.of(bgFields);
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

    @Override
    protected ReportMapper makeReportMapper(ReportType reportType, Schema schema) {
        ReportMapper reportMapper = new ReportMapper();
        reportMapper
                .add("serviceUUID", Function.identity(),Function.identity())
                .add("serviceName", Function.identity(),Function.identity())
                .add("marker", Function.identity(),Function.identity())
                .<Long, Long>add("timestamp", Function.identity(), epochInMillis -> epochInMillis / 1000L);
        ReportMapper payloadMapper = new ReportMapper();
        this.schemaFieldWalker(schema, avroFieldInfo -> {
            Function valueAdapter = makeValueAdapter(avroFieldInfo.schema.getType());
            payloadMapper.add(avroFieldInfo.fieldName, Function.identity(), valueAdapter);
        });
        reportMapper.add("payload", payloadMapper);
        return reportMapper;
    }

    private LegacySQLTypeName convertTypLegacySQLType(Schema.Type type) {
        switch (type) {
            case LONG:
            case INT:
                return LegacySQLTypeName.INTEGER;
            case BYTES:
                return LegacySQLTypeName.BYTES;
            case BOOLEAN:
                return LegacySQLTypeName.BOOLEAN;
            case ENUM:
            case STRING:
                return LegacySQLTypeName.STRING;
            case DOUBLE:
            case FLOAT:
                return LegacySQLTypeName.FLOAT;
            default:
                throw new NoSuchElementException("No mapping exists from avro field type of " + type + " and to bigquery");
        }
    }

    private Function makeValueAdapter(Schema.Type type) {
        switch (type) {
            case LONG:
            case INT:
            case BYTES:
            case BOOLEAN:
            case STRING:
            case DOUBLE:
            case FLOAT:
                return Function.identity();
            case ENUM:
                Function<Enum, String> enumConverter = e -> e.name();
                return enumConverter;
            default:
                throw new NoSuchElementException("No mapping exists from avro field type of " + type + " and to bigquery");
        }
    }
}
