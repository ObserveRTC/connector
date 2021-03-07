//package org.observertc.webrtc.connector.sinks.jdbc;
//
//import io.micronaut.context.ApplicationContext;
//import io.micronaut.context.annotation.Replaces;
//import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
//import org.jooq.SQLDialect;
//import org.jooq.impl.DSL;
//import org.junit.ClassRule;
//import org.junit.Ignore;
//import org.junit.jupiter.api.AfterAll;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.Test;
//import org.junit.platform.commons.logging.Logger;
//import org.junit.platform.commons.logging.LoggerFactory;
//import org.observertc.webrtc.connector.Application;
//import org.observertc.webrtc.connector.ReportGenerator;
//import org.observertc.webrtc.connector.common.DatasourceProvider;
//import org.observertc.webrtc.connector.sinks.Sink;
//import org.testcontainers.containers.PostgreSQLContainer;
//import org.testcontainers.utility.DockerImageName;
//
//import javax.annotation.PostConstruct;
//import javax.inject.Inject;
//import javax.inject.Singleton;
//import javax.sql.DataSource;
//import java.util.List;
//import java.util.Map;
//
//@MicronautTest
//public class PostgresSinkTest {
//
//    private static final Logger logger = LoggerFactory.getLogger(PostgresSinkTest.class);
//
//    @ClassRule
//    public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:latest"));
//
//    private static DataSource dataSource;
//
//    @Inject
//    ApplicationContext applicationContext;
//
//    @BeforeAll
//    static void setup() {
//        postgres.start();
//        dataSource = new HikariDataSourceBuilder()
//                .withURL(postgres.getJdbcUrl())
//                .withDriverClassName(postgres.getDriverClassName())
//                .withUsername(postgres.getUsername())
//                .withPassword(postgres.getPassword())
//                .build();
//    }
//
//    @AfterAll
//    static void teardown() {
//        postgres.stop();
//    }
//
//    @Inject
//    ReportGenerator reportGenerator;
//
//    // TODO: fix it
//    @Ignore
//    @Test
//    void shouldInsert_1() {
//        Sink sink = this.makeJDBCSink();
//        var reportSupplier = reportGenerator.initiatedCallReportSupplier("callName");
//        var initiatedCallsTableName = new JDBCSinkBuilder.Config().initiatedCallsTable.tableName;
//        var context = DSL.using(dataSource, SQLDialect.POSTGRES);
//
//        sink.onNext(List.of(reportSupplier.get()));
//
//        Assertions.assertEquals(1, context.selectCount().from(initiatedCallsTableName));
//    }
//
//    @Replaces(DatasourceProvider.class)
//    @Singleton
//    public class MockedDatasourceProvider extends DatasourceProvider {
//
//        @PostConstruct
//        void init() {
//
//        }
//
//        @Override
//        public DataSource apply(String datasourceName) {
//            return dataSource;
//        }
//    }
//
//    private Sink makeJDBCSink() {
//        Application.context = applicationContext;
//        var builder = new JDBCSinkBuilder();
//        builder.withConfiguration(this.getConfig());
//        return builder.build();
//    }
//
//    private Map<String, Object> getConfig() {
//        return Map.of(
//                "SQLDialect", SQLDialect.POSTGRES.name()
//        );
//    }
//}
