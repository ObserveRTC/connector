package org.observertc.webrtc.connector.databases.bigquery;

import com.google.cloud.bigquery.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.observertc.webrtc.connector.databases.ReportMapper;
import org.observertc.webrtc.schemas.reports.Report;

import java.util.concurrent.atomic.AtomicReference;

class ReportMapperBuilderTest {

    @Test
    public void shouldWork() {
        // Given
        ReportMapperBuilder builder = new ReportMapperBuilder();
        builder.forSchema(Report.getClassSchema());
        builder.excludeFields("payload");

        // When
        AtomicReference<Schema> bqschema = new AtomicReference<>();
        ReportMapper reportMapper = builder.build(bqschema);
        Schema schema = bqschema.get();

        // Then
        Assertions.assertNotNull(reportMapper);
        Assertions.assertNotNull(schema);
        Assertions.assertEquals(Report.getClassSchema().getFields().size()-1, schema.getFields().size());
    }
}