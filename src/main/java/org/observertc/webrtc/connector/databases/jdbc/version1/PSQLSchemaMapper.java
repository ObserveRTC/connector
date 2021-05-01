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

public class PSQLSchemaMapper extends SchemaMapperAbstract implements JOOQSchemaMapper {
    private static final Logger DEFAULT_LOGGER = LoggerFactory.getLogger(PSQLSchemaMapper.class);

    private final Map<ReportType, TableInfoConfig> tableConfigs = new HashMap<>();
    private Logger logger = DEFAULT_LOGGER;
    private final Supplier<DSLContext> contextSupplier;
    private Map<ReportType, Table<?>> tables = null;

    public PSQLSchemaMapper(Supplier<DSLContext> contextSupplier) {
        super();
        this.contextSupplier = contextSupplier;
//        DSLContextProvider contextProvider = Application.context.getBean(DSLContextProvider.class);
    }

    @Override
    protected boolean isDatabaseExists() {
        logger.info("NOTE: checking and creating database for PSQL with JOOQ has not been solved or implemented yet. " +
                "Make sure your datasource is connected to the right database.");
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
            return false;
        }

        return this.tables.containsKey(reportType);
    }

    @Override
    protected void createDatabase() {
        throw new RuntimeException("Create Database in PSQL with JOOQ is not supported currently!");
    }

    public PSQLSchemaMapper addTableConfig(ReportType reportType, TableInfoConfig tableInfoConfig) {
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
            logger.info("Table {} is creaed, Executed SQL: {}",
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
                    .filter(entry -> {
                        String tableName = table.getName();
                        String configuredTableName = entry.getValue().tableName;
                        String lowerCaseTableName = tableName.toLowerCase();
                        String lowerCaseConfiguredTableName = configuredTableName.toLowerCase();
                        return lowerCaseTableName.equals(lowerCaseConfiguredTableName);
                    })
                    .map(Map.Entry::getKey)
                    .findFirst();
            if (foundReportType.isPresent()) {
                result.put(foundReportType.get(), table);
            }
        });
        return Collections.unmodifiableMap(result);
    }
}