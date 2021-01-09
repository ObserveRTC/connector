package org.observertc.webrtc.connector.transformations;

import io.reactivex.rxjava3.core.Observable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.observertc.webrtc.connector.ReportGenerator;
import org.observertc.webrtc.schemas.reports.Report;
import org.observertc.webrtc.schemas.reports.ReportType;

import java.util.LinkedList;
import java.util.List;

public class FilterTest {

    static ReportGenerator generator = new ReportGenerator();

    @Test
    public void shouldFilterReportTypeIncludes() {
        // Given
        final ReportType allowedType = ReportType.FINISHED_CALL;
        final ReportType notAllowedType = ReportType.INITIATED_CALL;
        List<Report> reports = new LinkedList<>();
        Filter filter = new Filter().addPredicate(report -> report.getType().equals(allowedType));

        // When
        Observable.fromArray(generator.emptyReportSupplier(allowedType).get(),
                generator.emptyReportSupplier(notAllowedType).get())
                .lift(filter)
                .subscribe(reports::add);

        // Then
        Assertions.assertEquals(1, reports.size());
        Assertions.assertEquals(allowedType, reports.get(0).getType());
    }

}