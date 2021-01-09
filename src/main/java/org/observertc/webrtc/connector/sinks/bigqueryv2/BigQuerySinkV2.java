package org.observertc.webrtc.connector.sinks.bigqueryv2;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import io.reactivex.rxjava3.annotations.NonNull;
import org.observertc.webrtc.connector.adapters.bigquery.Adapter;
import org.observertc.webrtc.connector.sinks.Sink;
import org.observertc.webrtc.schemas.reports.Report;
import org.observertc.webrtc.schemas.reports.ReportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class BigQuerySinkV2 extends Sink {
    private static final Logger logger = LoggerFactory.getLogger(BigQuerySinkV2.class);
    private final Map<ReportType, Route> routes;
    private final BigQueryService bigQueryService;

    public BigQuerySinkV2(BigQueryService bigQueryService) {
        this.routes = new HashMap<>();
        this.bigQueryService = bigQueryService;
    }

    @Override
    public void onNext(@NonNull List<Report> reports) {
        Map<ReportType, InsertAllRequest.Builder> requestbuilders = new HashMap<>();
        Map<ReportType, Integer> counts = new HashMap<>();
        for (Report report : reports) {
            ReportType reportType = report.getType();
            InsertAllRequest.Builder requestBuilder = requestbuilders.get(reportType);
            Route route = this.routes.get(reportType);
            Adapter adapter = route.adapter;
            if (Objects.isNull(requestBuilder)) {
                String tableName = route.tableName;
                if (Objects.isNull(tableName)) {
                    logger.warn("{}: There is no route defined for entry type {}",
                            this.getPipelineName(),
                            reportType);
                    continue;
                }
                TableId tableId = TableId.of(this.bigQueryService.getProjectId(),
                        this.bigQueryService.getDatasetId(),
                        tableName);
                requestBuilder = InsertAllRequest.newBuilder(tableId);
                requestbuilders.put(reportType, requestBuilder);
            }
            Map<String, Object> row = adapter.apply(report);
            requestBuilder.addRow(row);
            int count = counts.getOrDefault(reportType, 0);
            counts.put(reportType, ++count);
        }

        if (requestbuilders.size() < 1) {
            logger.info("{}: No entries to send", this.getPipelineName());
            return;
        }

        Iterator<Map.Entry<ReportType, InsertAllRequest.Builder>> it = requestbuilders.entrySet().iterator();
        for (; it.hasNext();) {
            Map.Entry<ReportType, InsertAllRequest.Builder> entry = it.next();
            ReportType entryType = entry.getKey();
            InsertAllRequest.Builder requestBuilder = entry.getValue();
            InsertAllResponse response =
                    this.bigQueryService.getBigQuery().insertAll(requestBuilder.build());

            if (response.hasErrors()) {
                // If any of the insertions failed, this lets you inspect the errors
                for (Map.Entry<Long, List<BigQueryError>> errorEntry : response.getInsertErrors().entrySet()) {
                    logger.error("{}: Table: {}, ErrorEntryKey: {} ErrorResponse: {}",
                            this.getPipelineName(),
                            entryType,
                            errorEntry.getKey(),
                            String.join(", \n", errorEntry.getValue().stream().map(Object::toString).collect(Collectors.toList()))
                    );
                    // inspect row error
                }
            } else {
                logger.info("{}: {} rows inserted to inserted {}.",
                        this.getPipelineName(),
                        counts.get(entryType),
                        this.routes.get(entryType).tableName
                );
            }
        }
    }

    BigQuerySinkV2 withRoute(ReportType reportType, String tableName, Adapter adapter) {
        Route route = new Route(tableName, adapter);
        this.routes.put(reportType, route);
        return this;
    }

    private class Route {
        public final String tableName;
        public final Adapter adapter;

        private Route(String tableName, Adapter adapter) {
            this.tableName = tableName;
            this.adapter = adapter;
        }
    }
}
