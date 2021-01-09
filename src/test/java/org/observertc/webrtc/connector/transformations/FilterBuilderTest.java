package org.observertc.webrtc.connector.transformations;

import io.reactivex.rxjava3.core.Observable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.observertc.webrtc.connector.ReportGenerator;
import org.observertc.webrtc.schemas.reports.Report;
import org.observertc.webrtc.schemas.reports.ReportType;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

class FilterBuilderTest {

    static ReportGenerator generator = new ReportGenerator();

    @Test
    public void shouldCreateFilterProceedInclusions() {
        // Given
        final ReportType allowedType = ReportType.FINISHED_CALL;
        final ReportType notAllowedType = ReportType.INITIATED_CALL;
        List<Report> reports = new LinkedList<>();
        FilterBuilder filterBuilder = new FilterBuilder();
        filterBuilder.withConfiguration(Map.of("reportType", Map.of("including", List.of(allowedType.name()))));
        Filter filter = filterBuilder.build();

        // When
        Observable.fromArray(generator.emptyReportSupplier(allowedType).get(),
                generator.emptyReportSupplier(notAllowedType).get())
                .lift(filter)
                .subscribe(reports::add);

        // Then
        Assertions.assertEquals(1, reports.size());
        Assertions.assertEquals(allowedType, reports.get(0).getType());
    }

    @Test
    public void shouldCreateFilterProceedExclusion() {
        // Given
        final ReportType allowedType = ReportType.FINISHED_CALL;
        final ReportType notAllowedType = ReportType.INITIATED_CALL;
        List<Report> reports = new LinkedList<>();
        FilterBuilder filterBuilder = new FilterBuilder();
        filterBuilder.withConfiguration(Map.of("reportType", Map.of("excluding", List.of(notAllowedType.name()))));
        Filter filter = filterBuilder.build();

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