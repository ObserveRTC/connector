package org.observertc.webrtc.connector.adapters.bigquery;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.observertc.webrtc.connector.ReportGenerator;
import org.observertc.webrtc.schemas.reports.InitiatedCall;
import org.observertc.webrtc.schemas.reports.Report;
import org.observertc.webrtc.schemas.reports.ReportType;

import java.util.Map;
import java.util.function.Function;

class AdapterTest {

    static ReportGenerator generator = new ReportGenerator();

    @Test
    public void shouldAdapt() {
        // Given
        Adapter adapter = new Adapter();
        adapter.add("version", Function.identity(), Function.identity());
        Report report = generator.emptyReportSupplier(ReportType.INITIATED_CALL).get();

        // When
        Map<String, Object> entry = adapter.apply(report);

        // Then
        Assertions.assertNotNull(entry);
        Assertions.assertEquals(1, entry.size());
        Assertions.assertEquals(report.getVersion(), entry.get("version"));
    }

    @Test
    public void shouldAdaptEmbedded() {
        // Given
        Adapter adapter = new Adapter();
        Adapter embeddedAdapter = new Adapter();
        adapter.add("payload", embeddedAdapter);
        embeddedAdapter.add("callName", Function.identity(), Function.identity());
        Report report = generator.initiatedCallReportSupplier().get();

        // When
        Map<String, Object> entry = adapter.apply(report);

        // Then
        Assertions.assertNotNull(entry);
        Assertions.assertEquals(1, entry.size());
        InitiatedCall initiatedCall = (InitiatedCall) report.getPayload();
        Assertions.assertEquals(initiatedCall.getCallName(), entry.get("callName"));
    }

    @Test
    public void shouldAdaptValueByCustomResolver() {
        // Given
        Adapter adapter = new Adapter();
        Function<Integer, Integer> incrementer = i -> i+1;
        adapter.add("version", Function.identity(), incrementer);
        Report report = generator.emptyReportSupplier(ReportType.INITIATED_CALL).get();

        // When
        Map<String, Object> entry = adapter.apply(report);


        // Then
        Assertions.assertNotNull(entry);
        Assertions.assertEquals(1, entry.size());
        Assertions.assertEquals(incrementer.apply(report.getVersion()), entry.get("version"));
    }

    @Test
    public void shouldAdaptKeyByCustomResolver() {
        // Given
        final String myKey = "ThisIsSparta";
        Function<String, String> keyMapper = str->myKey;
        Adapter adapter = new Adapter();
        adapter.add("version", keyMapper, Function.identity());
        Report report = generator.emptyReportSupplier(ReportType.INITIATED_CALL).get();

        // When
        Map<String, Object> entry = adapter.apply(report);

        // Then
        Assertions.assertNotNull(entry);
        Assertions.assertEquals(1, entry.size());
        Assertions.assertEquals(report.getVersion(), entry.get(myKey));
    }
}