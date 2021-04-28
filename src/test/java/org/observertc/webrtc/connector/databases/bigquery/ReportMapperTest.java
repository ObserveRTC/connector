package org.observertc.webrtc.connector.databases.bigquery;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.observertc.webrtc.connector.ReportGenerator;
import org.observertc.webrtc.connector.databases.ReportMapper;
import org.observertc.webrtc.schemas.reports.InitiatedCall;
import org.observertc.webrtc.schemas.reports.Report;
import org.observertc.webrtc.schemas.reports.ReportType;

import java.util.Map;
import java.util.function.Function;

class ReportMapperTest {

    static ReportGenerator generator = new ReportGenerator();

    @Test
    public void shouldAdapt() {
        // Given
        ReportMapper adapter = new ReportMapper();
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
        ReportMapper adapter = new ReportMapper();
        ReportMapper embeddedReportMapper = new ReportMapper();
        adapter.add("payload", embeddedReportMapper);
        embeddedReportMapper.add("callName", Function.identity(), Function.identity());
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
        ReportMapper adapter = new ReportMapper();
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
        ReportMapper adapter = new ReportMapper();
        adapter.add("version", keyMapper, Function.identity());
        Report report = generator.emptyReportSupplier(ReportType.INITIATED_CALL).get();

        // When
        Map<String, Object> entry = adapter.apply(report);

        // Then
        Assertions.assertNotNull(entry);
        Assertions.assertEquals(1, entry.size());
        Assertions.assertEquals(report.getVersion(), entry.get(myKey.toLowerCase()));
    }
}