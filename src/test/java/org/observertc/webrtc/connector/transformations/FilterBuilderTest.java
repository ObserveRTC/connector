package org.observertc.webrtc.connector.transformations;

import io.reactivex.rxjava3.core.Observable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.observertc.webrtc.connector.ReportGenerator;
import org.observertc.webrtc.schemas.reports.Report;
import org.observertc.webrtc.schemas.reports.ReportType;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

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

    @Test
    public void shouldCreateFilterProceedExcludedTimestamps() throws ParseException {
        // Given
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        df.setTimeZone(TimeZone.getTimeZone("GMT"));
        long excludedTimestamp = df.parse("2020-01-01").toInstant().toEpochMilli();
        long notExcludedTimestamp = df.parse("2020-01-03").toInstant().toEpochMilli();
        long excludedTimestamp2 = df.parse("2020-01-04").toInstant().toEpochMilli();
        long notExcludedTimestamp2 = df.parse("2020-01-06").toInstant().toEpochMilli();
        List<Report> reports = new LinkedList<>();
        FilterBuilder filterBuilder = new FilterBuilder();
        filterBuilder.withConfiguration(Map.of("timestamps", Map.of("excluding", List.of("from 2020-01-01 until 2020-01-02", "from 2020-01-04 until 2020-01-05"))));
        Filter filter = filterBuilder.build();

        // When
        Observable.fromArray(generator.emptyReportWithTimestamp(ReportType.INITIATED_CALL, excludedTimestamp).get(),
                generator.emptyReportWithTimestamp(ReportType.INITIATED_CALL, notExcludedTimestamp).get(),
                generator.emptyReportWithTimestamp(ReportType.INITIATED_CALL, excludedTimestamp2).get(),
                generator.emptyReportWithTimestamp(ReportType.INITIATED_CALL, notExcludedTimestamp2).get())
                .lift(filter)
                .subscribe(reports::add);

        // Then
        Assertions.assertEquals(2, reports.size());
        Assertions.assertEquals(notExcludedTimestamp, reports.get(0).getTimestamp());
        Assertions.assertEquals(notExcludedTimestamp2, reports.get(1).getTimestamp());
    }

    @Test
    public void shouldCreateFilterProceedIncludedTimestamps() throws ParseException {
        // Given
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        df.setTimeZone(TimeZone.getTimeZone("GMT"));
        long includedTimestamp = df.parse("2020-01-01").toInstant().toEpochMilli();
        long notIncludedTimestamp = df.parse("2020-01-03").toInstant().toEpochMilli();
        long includedTimestamp2 = df.parse("2020-01-04").toInstant().toEpochMilli();
        long notIncludedTimestamp2 = df.parse("2020-01-06").toInstant().toEpochMilli();
        List<Report> reports = new LinkedList<>();
        FilterBuilder filterBuilder = new FilterBuilder();
        filterBuilder.withConfiguration(Map.of("timestamps", Map.of("including", List.of("from 2020-01-01 until 2020-01-02", "from 2020-01-04 until 2020-01-05"))));
        Filter filter = filterBuilder.build();

        // When
        Observable.fromArray(generator.emptyReportWithTimestamp(ReportType.INITIATED_CALL, includedTimestamp).get(),
                generator.emptyReportWithTimestamp(ReportType.INITIATED_CALL, notIncludedTimestamp).get(),
                generator.emptyReportWithTimestamp(ReportType.INITIATED_CALL, includedTimestamp2).get(),
                generator.emptyReportWithTimestamp(ReportType.INITIATED_CALL, notIncludedTimestamp2).get())
                .lift(filter)
                .subscribe(reports::add);

        // Then
        Assertions.assertEquals(2, reports.size());
        Assertions.assertEquals(includedTimestamp, reports.get(0).getTimestamp());
        Assertions.assertEquals(includedTimestamp2, reports.get(1).getTimestamp());
    }
}