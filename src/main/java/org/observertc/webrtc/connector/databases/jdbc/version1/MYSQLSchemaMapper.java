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

import java.sql.Connection;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.jooq.impl.DSL.constraint;

public class MYSQLSchemaMapper extends SchemaMapperAbstract implements JOOQSchemaMapper{
    private static final Logger DEFAULT_LOGGER = LoggerFactory.getLogger(MYSQLSchemaMapper.class);

    private final Map<ReportType, TableInfoConfig> tableConfigs = new HashMap<>();
    private Logger logger = DEFAULT_LOGGER;
    private final Supplier<DSLContext> contextSupplier;
    private final String databaseName;
    private Map<ReportType, Table<?>> tables = null;

    public MYSQLSchemaMapper(Supplier<DSLContext> contextSupplier, String databaseName) {
        super();
        this.contextSupplier = contextSupplier;
        this.databaseName = databaseName;
//        DSLContextProvider contextProvider = Application.context.getBean(DSLContextProvider.class);
    }

    @Override
    protected boolean isDatabaseExists() {
        if (Objects.isNull(this.databaseName)) {
            logger.info("No databasename is provided for the schema mapping, make sure your datasource connect to an existing one");
            return true;
        }
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
        if (Objects.isNull(this.tables)) {
            this.tables = this.fetchTables();
        }

        TableInfoConfig tableConfig = this.tableConfigs.get(reportType);
        if (Objects.isNull(tableConfig)) {
            logger.warn("Do not have table config for report type: {}", reportType);
            this.tableConfigs.remove(reportType);
            return false;
        }

        return this.tables.containsKey(reportType);
    }

    @Override
    protected void createDatabase() {
        if (!this.getFlagCreateDatasetIfNotExists()) {
            return;
        }
        var context = contextSupplier.get();
        context.connection(connection -> {
            int levelBefore = connection.getTransactionIsolation();
            try {
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                connection.createStatement().execute("CREATE DATABASE "+ this.databaseName);
            } finally {
                connection.setTransactionIsolation(levelBefore);
            }

        });

        contextSupplier.get().createDatabaseIfNotExists(this.databaseName).execute();

        logger.info("Database {} is created.", this.databaseName);
        logger.warn("Make Sure your datasource is connected to the database name {}, " +
                "otherwise tables and records are created and inserted the one you given in the datasource", this.databaseName);
    }

    public MYSQLSchemaMapper addTableConfig(ReportType reportType, TableInfoConfig tableInfoConfig) {
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
            DataType dataType = JOOQSchemaMapper.makeDataType(fieldInfo.schema.getType());
            dataType.nullable(fieldInfo.schema.isNullable());
            createTableColumnStep.column(fieldInfo.fieldName, dataType);
        });

        CreateTableConstraintStep createTableConstraintStep;
        if (Objects.nonNull(tableConfig.autoIncrementPrimaryKeyName)) {
            createTableConstraintStep = createTableColumnStep
                    .constraints(constraint("pk-" + tableConfig.tableName).primaryKey(tableConfig.autoIncrementPrimaryKeyName)
                    );
        } else {
            createTableConstraintStep = createTableColumnStep
                    .constraints(constraint("pk-" + tableConfig.tableName).primaryKey(tableConfig.primaryKeyColumns)
                    );
        }
        try {
            createTableConstraintStep.execute();
            logger.info("Table {} is theoretically creaed, SQL {} is created",
                    tableConfig.tableName,
                    createTableConstraintStep.getSQL());
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
            Function valueConverter = JOOQSchemaMapper.makeValueConverter(fieldInfo.schema.getType());
            payloadMapper.add(fieldInfo.fieldName, Function.identity(), valueConverter);
        });
        result.add("payload", payloadMapper);
        return result;
    }

    @Override
    public Map<ReportType, Table<?>> getTables() {
        return this.fetchTables();
    }

    private Map<ReportType, Table<?>> fetchTables() {
        var context = this.contextSupplier.get();
        var meta = context.meta();
        var tables = meta.getTables();
        Map<ReportType, Table<?>> result = new HashMap<>();
        tables.stream().forEach(table -> {
            Optional<ReportType> foundReportType = tableConfigs.entrySet().stream()
                    .filter(entry -> entry.getValue().tableName.equals(table.getName()))
                    .map(Map.Entry::getKey)
                    .findFirst();
            if (foundReportType.isPresent()) {
                result.put(foundReportType.get(), table);
            }
        });
        return Collections.unmodifiableMap(result);
    }
}
