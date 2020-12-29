package org.observertc.webrtc.reportconnector.sinks.bigquery;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import io.reactivex.rxjava3.annotations.NonNull;
import org.observertc.webrtc.reportconnector.models.Entry;
import org.observertc.webrtc.reportconnector.models.EntryType;
import org.observertc.webrtc.reportconnector.sinks.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class BigQuerySink extends Sink {
    private static final Logger logger = LoggerFactory.getLogger(BigQuerySink.class);
    private final Map<EntryType, String> routes;
    private final BigQueryService bigQueryService;

    public BigQuerySink(BigQueryService bigQueryService) {
        this.routes = new HashMap<>();
        this.bigQueryService = bigQueryService;
    }

    @Override
    public void onNext(@NonNull List<Entry> entries) {
        Map<EntryType, InsertAllRequest.Builder> requestbuilders = new HashMap<>();
        Map<EntryType, Integer> counts = new HashMap<>();
        for (Entry entry : entries) {
            InsertAllRequest.Builder requestBuilder = requestbuilders.get(entry.getEntryType());
            if (Objects.isNull(requestBuilder)) {
                String tableName = this.routes.get(entry.getEntryType());
                if (Objects.isNull(tableName)) {
                    logger.warn("There is no route defined for entry type {}", entry.getEntryType());
                    continue;
                }
                TableId tableId = TableId.of(this.bigQueryService.getProjectId(),
                        this.bigQueryService.getDatasetId(),
                        tableName);
                requestBuilder = InsertAllRequest.newBuilder(tableId);
                requestbuilders.put(entry.getEntryType(), requestBuilder);
            }
            requestBuilder.addRow(entry.toMap());
            int count = counts.getOrDefault(entry.getEntryType(), 0);
            counts.put(entry.getEntryType(), ++count);
        }

        if (requestbuilders.size() < 1) {
            logger.info("No entries to send");
            return;
        }

        Iterator<Map.Entry<EntryType, InsertAllRequest.Builder>> it = requestbuilders.entrySet().iterator();
        for (; it.hasNext();) {
            Map.Entry<EntryType, InsertAllRequest.Builder> entry = it.next();
            EntryType entryType = entry.getKey();
            InsertAllRequest.Builder requestBuilder = entry.getValue();
            InsertAllResponse response =
                    this.bigQueryService.getBigQuery().insertAll(requestBuilder.build());

            if (response.hasErrors()) {
                // If any of the insertions failed, this lets you inspect the errors
                for (Map.Entry<Long, List<BigQueryError>> errorEntry : response.getInsertErrors().entrySet()) {
                    logger.error("{}: {}", errorEntry.getKey(), String.join(", \n",
                            errorEntry.getValue().stream().map(Object::toString).collect(Collectors.toList())));
                    // inspect row error
                }
            } else {
                logger.info("To {} inserted {} rows", this.routes.get(entryType), counts.get(entryType));
            }
        }
    }

    BigQuerySink withEntryRoute(EntryType entryType, String tableName) {
        this.routes.put(entryType, tableName);
        return this;
    }
}
