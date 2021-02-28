package org.observertc.webrtc.connector.sinks.jdbcsink;

import io.reactivex.rxjava3.annotations.NonNull;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertValuesStepN;
import org.jooq.Table;
import org.observertc.webrtc.connector.databases.ReportMapper;
import org.observertc.webrtc.connector.sinks.Sink;
import org.observertc.webrtc.schemas.reports.Report;
import org.observertc.webrtc.schemas.reports.ReportType;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class JDBCSink extends Sink {
    private final Supplier<DSLContext> contextSupplier;
    private final Map<ReportType, Route> routes = new HashMap<>();

    public JDBCSink(Supplier<DSLContext> contextSupplier) {
        this.contextSupplier = contextSupplier;
    }

    @Override
    public void onNext(@NonNull List<Report> reports) {
        var context = this.contextSupplier.get();
        Map<ReportType, InsertValuesStepN> batch = new HashMap<>();
        for (Report report : reports) {
            var reportType = report.getType();
            Route route = this.routes.get(reportType);
            var insertValuesStepN = batch.get(reportType);
            Map<String, Object> recordValues = route.mapper.apply(report);
            if (Objects.isNull(insertValuesStepN)) {
                var insertSetStep = context.insertInto(route.table);
                insertValuesStepN = insertSetStep.columns(route.fields);
                batch.put(reportType, insertValuesStepN);
            }
            var values = Arrays.stream(route.fields).map(f -> recordValues.get(f.getName())).collect(Collectors.toList());
            insertValuesStepN.values(values);
        }

        // flush batch
        for (Map.Entry<ReportType, InsertValuesStepN> entry : batch.entrySet()) {
            ReportType reportType = entry.getKey();
            InsertValuesStepN insertValuesStepN = entry.getValue();
//            logger.info("SQL: {}", insertValuesStepN.getSQL(true));
            int insertedRecords = insertValuesStepN.execute();
            logger.info("For report type {} inserted {} records", reportType, insertedRecords);

        }
    }

    JDBCSink withRoute(ReportType reportType, Table<?> table, ReportMapper adapter) {
        JDBCSink.Route route = new JDBCSink.Route(table, adapter, table.fields());
        this.routes.put(reportType, route);
        return this;
    }

    private class Route {
        public final Field[] fields;
        public final Table<?> table;
        public final ReportMapper mapper;

        private Route(Table<?> table, ReportMapper mapper, Field[] fields) {
            this.table = table;
            this.mapper = mapper;
            this.fields = fields;
        }
    }
}
