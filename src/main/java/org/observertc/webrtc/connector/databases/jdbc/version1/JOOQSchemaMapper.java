package org.observertc.webrtc.connector.databases.jdbc.version1;

import org.apache.avro.Schema;
import org.jooq.DataType;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.observertc.webrtc.connector.databases.SchemaMapper;
import org.observertc.webrtc.connector.databases.jdbc.TableInfoConfig;
import org.observertc.webrtc.schemas.reports.ReportType;

import javax.sql.DataSource;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;

public interface JOOQSchemaMapper extends SchemaMapper {

    static JOOQSchemaMapper makeSchemaMapperFor(SQLDialect dialect, String databaseName, DataSource dataSource) {
        var context = DSL.using(dataSource, dialect);
        switch (dialect) {
            case MYSQL:
                return new MYSQLSchemaMapper(() -> context, databaseName);
            case POSTGRES:
                return new PostgresSchemaMapper(() -> context);
            default:
                throw new RuntimeException("Schema Mapper is not implemented for SQL Dialect " + dialect.getName());
        }
    }

    static DataType makeDataType(Schema.Type fieldType) {
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

    static Function makeValueConverter(Schema.Type fieldType) {
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

    Map<ReportType, Table<?>> getTables();

    JOOQSchemaMapper addTableConfig(ReportType reportType, TableInfoConfig tableInfoConfig);



}
