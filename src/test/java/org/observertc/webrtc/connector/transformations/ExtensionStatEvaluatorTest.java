package org.observertc.webrtc.connector.transformations;

import io.reactivex.rxjava3.core.Observable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.observertc.webrtc.connector.ReportGenerator;
import org.observertc.webrtc.schemas.reports.ExtensionReport;
import org.observertc.webrtc.schemas.reports.Report;
import org.observertc.webrtc.schemas.reports.ReportType;

import java.util.LinkedList;
import java.util.List;

public class ExtensionStatEvaluatorTest {

    static ReportGenerator generator = new ReportGenerator();

    @Test
    public void shouldPassThroughExtensionStatEvaluator() {
        // Given
        List<Report> reports = new LinkedList<>();
        var config = this.makeConfig();
        ExtensionStatEvaluator filter = new ExtensionStatEvaluator().withConfig(config);

        // When
        Observable.fromArray(generator.extensionStatReportSupplier("type", "payload").get())
                .lift(filter)
                .subscribe(reports::add);

        // Then
        Assertions.assertEquals(1, reports.size());
        Report receivedReport = reports.get(0);
        Assertions.assertEquals(ReportType.EXTENSION, receivedReport.getType());
        ExtensionReport extensionReport = (ExtensionReport) receivedReport.getPayload();
        Assertions.assertEquals("Evaluated value in string", extensionReport.getPayload());
    }

    private ExtensionStatEvaluatorBuilder.Config makeConfig() {
        ExtensionStatEvaluatorBuilder.Config result = new ExtensionStatEvaluatorBuilder.Config();
        result.enabled = true;
        return result;
    }

}