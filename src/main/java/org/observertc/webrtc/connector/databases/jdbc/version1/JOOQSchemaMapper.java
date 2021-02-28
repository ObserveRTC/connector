package org.observertc.webrtc.connector.databases.jdbc.version1;

import org.apache.avro.Schema;
import org.jooq.*;
import org.jooq.impl.SQLDataType;
import org.observertc.webrtc.connector.databases.ReportMapper;
import org.observertc.webrtc.connector.databases.SchemaMapperAbstract;
import org.observertc.webrtc.connector.databases.jdbc.TableInfoConfig;
import org.observertc.webrtc.schemas.reports.ReportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.jooq.impl.DSL.constraint;

public class JOOQSchemaMapper extends SchemaMapperAbstract {
    private static final Logger DEFAULT_LOGGER = LoggerFactory.getLogger(JOOQSchemaMapper.class);

    private final Map<ReportType, TableInfoConfig> tableConfigs = new HashMap<>();
    private Logger logger = DEFAULT_LOGGER;
    private final Supplier<DSLContext> contextSupplier;
    private final String databaseName;
    private List<Table<?>> tables = null;
    private boolean obsolatedTableCache = true;

    public JOOQSchemaMapper(Supplier<DSLContext> contextSupplier, String databaseName) {
        super();
        this.contextSupplier = contextSupplier;
        this.databaseName = databaseName;
//        DSLContextProvider contextProvider = Application.context.getBean(DSLContextProvider.class);
    }

    @Override
    protected boolean isDatabaseExists() {
        var context = this.contextSupplier.get();
        var dbSchemas = context.meta().getSchemas(this.databaseName);
        if (Objects.isNull(dbSchemas) || dbSchemas.size() < 1) {
            return false;
        } else if (1 < dbSchemas.size()) {
            logger.warn("More than one dbschema is retrieved for the database name {}", this.databaseName);
        }
        return true;
    }

    @Override
    protected boolean isTableExistsForReportType(ReportType reportType, Schema schema) {
        if (Objects.isNull(this.tables) || this.obsolatedTableCache) {
            this.tables = this.fetchTables();
        }

        TableInfoConfig tableConfig = this.tableConfigs.get(reportType);
        if (Objects.isNull(tableConfig)) {
            logger.warn("Do not have table config for report type: {}", reportType);
            return false;
        }

        if (this.tables.stream().anyMatch(table -> table.getName().equals(tableConfig.tableName))) {
            return true;
        }
        return false;
    }

    @Override
    protected void createDatabase() {
        if (!this.getFlagCreateDatasetIfNotExists()) {
            return;
        }
        contextSupplier.get().createDatabaseIfNotExists(this.databaseName).execute();
        logger.info("Database {} is created.", this.databaseName);
        logger.warn("Make Sure your datasource is connected to the database name {}, " +
                "otherwise tables and records are created and inserted the one you given in the datasource", this.databaseName);
    }

    public JOOQSchemaMapper addTableConfig(ReportType reportType, TableInfoConfig tableInfoConfig) {
        this.tableConfigs.put(reportType, tableInfoConfig);
        return this;
    }

    @Override
    protected void createTableForReportType(ReportType reportType, Schema schema) {
        TableInfoConfig tableConfig = this.tableConfigs.get(reportType);
        if (Objects.isNull(tableConfig)) {
            logger.warn("We do not have table config for report type: {}", reportType);
            return;
        }

        var context = contextSupplier.get();
        final CreateTableColumnStep createTableColumnStep = context
                .createTableIfNotExists(tableConfig.tableName)
                ;
        if (Objects.nonNull(tableConfig.autoIncrementPrimaryKeyName)) {
            createTableColumnStep.column(tableConfig.autoIncrementPrimaryKeyName, SQLDataType.BIGINT.identity(true));
        }

        // common columns
        createTableColumnStep
                .column("timestamp", SQLDataType.BIGINT)
                .column("marker", SQLDataType.VARCHAR(255));

        this.schemaFieldWalker(schema, fieldInfo -> {
            DataType dataType = this.makeDataType(fieldInfo.schema.getType());
            dataType.nullable(fieldInfo.schema.isNullable());
            createTableColumnStep.column(fieldInfo.fieldName, dataType);
        });

        CreateTableConstraintStep createTableConstraintStep;
        if (Objects.nonNull(tableConfig.autoIncrementPrimaryKeyName)) {
            createTableConstraintStep = createTableColumnStep
                    .constraints(constraint("pk").primaryKey(tableConfig.autoIncrementPrimaryKeyName)
                    );
        } else {
            createTableConstraintStep = createTableColumnStep
                    .constraints(constraint("pk").primaryKey(tableConfig.primaryKeyColumns)
                    );
        }
        try {
            createTableConstraintStep.execute();
            logger.info("Table {} is theoretically creaed, SQL {} is created",
                    tableConfig.tableName,
                    createTableConstraintStep.getSQL());
            this.obsolatedTableCache = true;
        } catch (Throwable t) {
            logger.warn("Cannot create table", t);
        }
    }

    @Override
    protected ReportMapper makeReportMapper(ReportType reportType, Schema schema) {
        ReportMapper result = new ReportMapper();
        // common fields
        result
                .add("timestamp", Function.identity(), Function.identity())
                .add("marker", Function.identity(), Function.identity())
                ;
        ReportMapper payloadMapper = new ReportMapper();
        this.schemaFieldWalker(schema, fieldInfo -> {
            // excluding fields:
            if (List.of("version", "type")
                    .stream()
                    .anyMatch(f -> f.equals(fieldInfo.fieldName)))
            {
                return;
            }
            Function valueConverter = makeValueConverter(fieldInfo.schema.getType());
            payloadMapper.add(fieldInfo.fieldName, Function.identity(), valueConverter);
        });
        result.add("payload", payloadMapper);
        return result;
    }

    private DataType makeDataType(Schema.Type fieldType) {
        switch (fieldType) {
            case LONG:
                return SQLDataType.BIGINT;
            case INT:
                return SQLDataType.INTEGER;
            case BYTES:
                return SQLDataType.BINARY(1024);
            case BOOLEAN:
                return SQLDataType.BOOLEAN;
            case ENUM:
            case STRING:
                return SQLDataType.VARCHAR(255);
            case DOUBLE:
                return SQLDataType.DOUBLE;
            case FLOAT:
                return SQLDataType.FLOAT;
            default:
                throw new NoSuchElementException("No field mapping exists from avro field type of " + fieldType + " and to JDBC");
        }
    }

    private Function makeValueConverter(Schema.Type fieldType) {
        switch (fieldType) {
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
                throw new NoSuchElementException("No value mapping exists from avro field type of " + fieldType + " and to JDBC");
        }
    }

    private List<Table<?>> fetchTables() {
        var context = this.contextSupplier.get();
        var dbSchemas = context.meta().getSchemas(this.databaseName);
        if (Objects.isNull(dbSchemas) || dbSchemas.size() < 1) {
            return Collections.EMPTY_LIST;
        } else if (1 < dbSchemas.size()) {
            logger.warn("More than one dbschema is retrieved for the database name {}. The first one will be used", this.databaseName);
        }
        var dbSchema = dbSchemas.get(0);
        return context.meta(dbSchema).getTables();
    }
}
