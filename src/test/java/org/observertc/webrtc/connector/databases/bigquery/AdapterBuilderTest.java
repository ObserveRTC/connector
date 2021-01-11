package org.observertc.webrtc.connector.databases.bigquery;

import com.google.cloud.bigquery.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.observertc.webrtc.schemas.reports.Report;

import java.util.concurrent.atomic.AtomicReference;

class AdapterBuilderTest {

    @Test
    public void shouldWork() {
        // Given
        AdapterBuilder builder = new AdapterBuilder();
        builder.forSchema(Report.getClassSchema());
        builder.excludeFields("payload");

        // When
        AtomicReference<Schema> bqschema = new AtomicReference<>();
        Adapter adapter = builder.build(bqschema);
        Schema schema = bqschema.get();

        // Then
        Assertions.assertNotNull(adapter);
        Assertions.assertNotNull(schema);
        Assertions.assertEquals(Report.getClassSchema().getFields().size()-1, schema.getFields().size());
    }
}