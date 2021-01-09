package org.observertc.webrtc.connector.transformations;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.observertc.webrtc.connector.ReportGenerator;
import org.observertc.webrtc.schemas.reports.Report;
import org.observertc.webrtc.schemas.reports.ReportType;

import java.security.MessageDigest;

class ReportObfuscatorTest {

    static ReportGenerator generator = new ReportGenerator();

    @Test
    public void shouldObfuscateReportBaseFields() throws Throwable {
        // Given
        Report report = generator.emptyReportSupplier(ReportType.INITIATED_CALL).get();
        ReportObfuscator obfuscator = new ReportObfuscator(MessageDigest.getInstance("SHA-256"));

        // When
        Report newReport = obfuscator.transform(report).get();

        // Then
        Assertions.assertNotNull(newReport);
        Assertions.assertNotEquals(report.getServiceName(), newReport.getServiceName());
        Assertions.assertNotEquals(report.getServiceUUID(), newReport.getServiceUUID());
        Assertions.assertNotEquals(report.getMarker(), newReport.getMarker());
    }
}