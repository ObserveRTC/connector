package org.observertc.webrtc.connector.sinks.jdbc;

import io.micronaut.context.annotation.Prototype;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.observertc.webrtc.connector.Application;
import org.observertc.webrtc.connector.common.DatasourceProvider;
import org.observertc.webrtc.connector.configbuilders.AbstractBuilder;
import org.observertc.webrtc.connector.configbuilders.Builder;
import org.observertc.webrtc.connector.databases.ReportMapper;
import org.observertc.webrtc.connector.databases.jdbc.TableInfoConfig;
import org.observertc.webrtc.connector.databases.jdbc.version1.JOOQSchemaMapper;
import org.observertc.webrtc.connector.databases.jdbc.version1.MYSQLSchemaMapper;
import org.observertc.webrtc.connector.databases.jdbc.version1.PSQLSchemaMapper;
import org.observertc.webrtc.connector.sinks.Sink;
import org.observertc.webrtc.schemas.reports.ReportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import javax.validation.constraints.NotNull;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Prototype
public class JDBCSinkBuilder extends AbstractBuilder implements Builder<Sink> {

    private static final Logger logger = LoggerFactory.getLogger(JDBCSinkBuilder.class);
    private final Map<ReportType, TableInfoConfig> tableConfigs;

    public JDBCSinkBuilder() {
        this.tableConfigs = new HashMap<>();
    }

    @Override
    public Sink build() {
        Config config = this.convertAndValidate(Config.class);
        this.tableConfigs.put(ReportType.INITIATED_CALL, config.initiatedCallsTable);
        this.tableConfigs.put(ReportType.FINISHED_CALL, config.finishedCallsTable);
        this.tableConfigs.put(ReportType.JOINED_PEER_CONNECTION, config.joinedPeerConnectionsTable);
        this.tableConfigs.put(ReportType.DETACHED_PEER_CONNECTION, config.detachedPeerConnectionsTable);
        this.tableConfigs.put(ReportType.INBOUND_RTP, config.inboundRTPSamplesTable);
        this.tableConfigs.put(ReportType.REMOTE_INBOUND_RTP, config.remoteInboundRTPSamplesTable);
        this.tableConfigs.put(ReportType.OUTBOUND_RTP, config.outboundRTPSamplesTable);
        this.tableConfigs.put(ReportType.ICE_CANDIDATE_PAIR, config.iceCandidatePairsTable);
        this.tableConfigs.put(ReportType.ICE_LOCAL_CANDIDATE, config.iceLocalCandidatesTable);
        this.tableConfigs.put(ReportType.ICE_REMOTE_CANDIDATE, config.iceRemoteCandidatesTable);
        this.tableConfigs.put(ReportType.MEDIA_SOURCE, config.mediaSourcesTable);
        this.tableConfigs.put(ReportType.USER_MEDIA_ERROR, config.userMediaErrorsTable);
        this.tableConfigs.put(ReportType.TRACK, config.trackReportsTable);
        this.tableConfigs.put(ReportType.OBSERVER_EVENT, config.observerEventTable);
        this.tableConfigs.put(ReportType.MEDIA_DEVICE, config.mediaDeviceTable);
        this.tableConfigs.put(ReportType.CLIENT_DETAILS, config.clientDetailsTable);
        this.tableConfigs.put(ReportType.EXTENSION, config.extensionTable);

        SQLDialect dialect;
        try {
            dialect = SQLDialect.valueOf(config.SQLDialect);
        } catch (Throwable t) {
            logger.error("Cannot identify SQLDialect {}. Possible values are: {} and the config is case sensitive",
                    config.SQLDialect, SQLDialect.values());
            return null;
        }

        DatasourceProvider datasourceProvider = Application.context.createBean(DatasourceProvider.class);
        var datasource = datasourceProvider.apply(config.datasource);
        if (Objects.isNull(datasource)) {
            logger.error("Cannot beam (up) datasource. JDBCSink cannot be built");
            return null;
        }
        JOOQSchemaMapper jooqSchemaMapper = this.makeSchemaMapperFor(dialect, config.database, datasource);
        Map<ReportType, ReportMapper> reportMappers = this.runSchemaAdapter(jooqSchemaMapper, config);
        if (Objects.isNull(reportMappers)) {
            logger.error("The schema cannot be built, because it does not have mappers");
            return null;
        }
        var fetchedTables = jooqSchemaMapper.getTables();
        if (Objects.isNull(fetchedTables)) {
            logger.warn("No tables were fetched");
            return null;
        } else {

        }
        Supplier<DSLContext> contextSupplier = () -> DSL.using(datasource, dialect);
        JDBCSink result = new JDBCSink(contextSupplier);
        reportMappers.entrySet()
                .forEach(
                        entry -> {
                            ReportType reportType = entry.getKey();
                            TableInfoConfig tableConfig = this.tableConfigs.get(reportType);
                            Table table = fetchedTables.get(reportType);
                            if (Objects.isNull(table)) {
                                logger.warn("No table found for report type {}", reportType);
                                return;
                            }
                            List<Field> fields = getFields(dialect, tableConfig, table);
                            result.withRoute(
                                    reportType,
                                    table,
                                    entry.getValue(),
                                    fields
                            );
                        });

        return result;
    }

    private List<Field> getFields(SQLDialect dialect, TableInfoConfig tableConfig, Table<?> table) {
        switch (dialect) {
            case POSTGRES:
                if (Objects.nonNull(tableConfig.autoIncrementPrimaryKeyName)) {
                    return Arrays.stream(table.fields()).filter(f -> !f.getName().equals("recordid")).collect(Collectors.toList());
                } else {
                    return Arrays.asList(table.fields());
                }
            default:
                return Arrays.asList(table.fields());
        }
    }

    private Map<ReportType, ReportMapper> runSchemaAdapter(JOOQSchemaMapper schemaCheckerJob, Config config) {
        try {
            schemaCheckerJob
                    .withSchemaCheckEnabled(config.schemaCheck.enabled)
                    .createDatasetIfNotExists(config.schemaCheck.createDatasetIfNotExists)
                    .createTableIfNotExists(config.schemaCheck.createTableIfNotExists)
            ;

            this.tableConfigs.entrySet()
                    .forEach(entry -> schemaCheckerJob.addTableConfig(entry.getKey(), entry.getValue()));
            schemaCheckerJob.run();
            Map<ReportType, ReportMapper> result = this.tableConfigs.keySet()
                    .stream()
                    .map(type -> Map.entry(type, schemaCheckerJob.getReportMapper(type)))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return result;
        } catch (Exception e) {
            logger.error("Error occured during schema checking process", e);
            return null;
        }
    }

    private JOOQSchemaMapper makeSchemaMapperFor(SQLDialect dialect, String databaseName, DataSource dataSource) {
        var context = DSL.using(dataSource, dialect);
        switch (dialect) {
            case MYSQL:
                return new MYSQLSchemaMapper(() -> context, databaseName);
            case POSTGRES:
                return new PSQLSchemaMapper(() -> context);
            default:
                throw new RuntimeException("Schema Mapper is not implemented for SQL Dialect " + dialect.getName());
        }
    }

    public static class Config {

        public String database = null;

        public String datasource = "default";

        @NotNull
        public String SQLDialect;

        public SchemaCheckConfig schemaCheck = new SchemaCheckConfig();

        public static class SchemaCheckConfig {
            public boolean enabled = true;

            public boolean createDatasetIfNotExists = true;

            public boolean createTableIfNotExists = true;
        }

        public TableInfoConfig initiatedCallsTable = TableInfoConfig.of(
                "InitiatedCalls"
        );

        public TableInfoConfig finishedCallsTable = TableInfoConfig.of(
                "FinishedCalls"
        );

        public TableInfoConfig joinedPeerConnectionsTable = TableInfoConfig.of(
                "JoinedPeerConnections"
        );

        public TableInfoConfig detachedPeerConnectionsTable = TableInfoConfig.of(
                "DetachedPeerConnections"
        );

        public TableInfoConfig remoteInboundRTPSamplesTable = TableInfoConfig.of(
                "RemoteInboundRTPSamples"
        );

        public TableInfoConfig outboundRTPSamplesTable = TableInfoConfig.of(
                "OutboundRTPSamples"
        );

        public TableInfoConfig inboundRTPSamplesTable = TableInfoConfig.of(
                "InboundRTPSamples"
        );

        public TableInfoConfig iceCandidatePairsTable = TableInfoConfig.of(
                "ICECandidatePairs"
        );

        public TableInfoConfig iceLocalCandidatesTable = TableInfoConfig.of(
                "ICELocalCandidates"
        );

        public TableInfoConfig iceRemoteCandidatesTable = TableInfoConfig.of(
                "ICERemoteCandidates"
        );

        public TableInfoConfig mediaSourcesTable = TableInfoConfig.of(
                "MediaSources"
        );

        public TableInfoConfig trackReportsTable = TableInfoConfig.of(
                "TrackReports"
        );

        public TableInfoConfig userMediaErrorsTable = TableInfoConfig.of(
                "UserMediaErrors"
        );

        public TableInfoConfig observerEventTable = TableInfoConfig.of(
                "ObserverEventReports"
        );

        public TableInfoConfig mediaDeviceTable = TableInfoConfig.of(
                "MediaDevices"
        );

        public TableInfoConfig clientDetailsTable = TableInfoConfig.of(
                "ClientDetails"
        );

        public TableInfoConfig extensionTable = TableInfoConfig.of(
                "Extensions"
        );

    }
}