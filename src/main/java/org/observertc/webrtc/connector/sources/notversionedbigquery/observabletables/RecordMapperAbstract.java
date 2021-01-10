package org.observertc.webrtc.connector.sources.notversionedbigquery.observabletables;

import com.google.cloud.bigquery.*;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import org.observertc.webrtc.connector.common.BigQueryService;
import org.observertc.webrtc.connector.sources.notversionedbigquery.NotVersionedBigQuerySource;
import org.observertc.webrtc.schemas.reports.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public abstract class RecordMapperAbstract extends Observable<Report> {
    private static final Logger logger = LoggerFactory.getLogger(RecordMapperAbstract.class);
    public static final String MIGRATION_MARKER = NotVersionedBigQuerySource.class.getSimpleName();
    public static final String SERVICE_UUID_FIELD_NAME = "serviceUUID";
    public static final String SERVICE_NAME_FIELD_NAME = "serviceName";
    public static final String TIMESTAMP_FIELD_NAME = "timestamp";

    public static final int MIGRATED_REPORT_VERSION = 1;
    private final BigQueryService bigQueryService;
    private final String tableName;
    private final ReportType reportType;
    private final int limit = 100000;
    private final Map<String, Integer> fieldMap = new HashMap<>();

    public RecordMapperAbstract(BigQueryService bigQueryService, String tableName, ReportType reportType) {
        this.bigQueryService = bigQueryService;
        this.tableName = tableName;
        this.reportType = reportType;
    }

    @Override
    protected void subscribeActual(@NonNull Observer<? super Report> observer) {
        try {
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
            TableId tableId = TableId.of(this.bigQueryService.getDatasetId(), this.tableName);
            TableResult result = bigquery.listTableData(tableId, BigQuery.TableDataListOption.pageSize(this.limit));
            this.fieldMap.putAll(this.buildFieldMap(tableId));
            logger.info("Fetching records for {} has begun", this.tableName);
            for (FieldValueList row : result.iterateAll()) {
                Report report = this.makeReport(row);
                observer.onNext(report);
            }
            logger.info("Fetching records for {} has ended", this.tableName);

        } catch (Throwable ex) {
            logger.warn("Migration for {} is stoppped due to exception: {}", this.tableName, ex.getMessage());
        }
        observer.onComplete();
    }
    private List<String> getReportFieldNames() {
        List<String> result = new ArrayList<>();
        result.add(SERVICE_UUID_FIELD_NAME);
        result.add(SERVICE_NAME_FIELD_NAME);
        result.add(TIMESTAMP_FIELD_NAME);
        result.addAll(this.getPayloadFieldNames());
        return result;
    }

    private Map<String, Integer> buildFieldMap(TableId tableId){
        Map<String, Integer> result = new HashMap<>();
        FieldList fieldList = this.bigQueryService.getBigQuery().getTable(tableId).getDefinition().getSchema().getFields();
        List<String> fieldNames = this.getReportFieldNames();
        for (String fieldName : fieldNames) {
            int index;
            try {
                 index = fieldList.getIndex(fieldName);
            } catch (Exception ex) {
                logger.warn("Field name problem for field: {} at table {}. Message: {}", fieldName,tableName, ex.getMessage());
                continue;
            }
            result.put(fieldName, index);
        }
        return result;
    }

    protected<T> T getValue(FieldValueList row, String fieldName, Function<FieldValue, T> converter, T defaultValue) {
        Integer index = this.fieldMap.get(fieldName);
        if (Objects.isNull(index)) {
            return defaultValue;
        }
        FieldValue fieldValue = row.get(index);
        try {
            T result = converter.apply(fieldValue);
            if (Objects.isNull(result)) {
                return defaultValue;
            }
            return result;
        } catch (Throwable t) {
            return defaultValue;
        }
    }

    protected abstract List<String> getPayloadFieldNames();

    protected Report makeReport(FieldValueList row) {
        String serviceUUID = this.getValue(row, SERVICE_UUID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND");
        String serviceName = this.getValue(row, SERVICE_NAME_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND");
        Long timestamp = this.getValue(row, TIMESTAMP_FIELD_NAME, FieldValue::getLongValue, 0L);

        Object payload = this.makePayload(row);
        var result = Report.newBuilder()
                .setVersion(MIGRATED_REPORT_VERSION)
                .setType(this.reportType)
                .setServiceUUID(serviceUUID)
                .setServiceName(serviceName)
                .setTimestamp(timestamp)
                .setPayload(payload)
                .setMarker(MIGRATION_MARKER);

        return result.build();
    }

    protected abstract Object makePayload(FieldValueList row);

    protected Integer getInteger(FieldValue fieldValue) {
        Long value = fieldValue.getLongValue();
        return value.intValue();
    }

    protected Float getFloat(FieldValue fieldValue) {
        Double value = fieldValue.getDoubleValue();
        return value.floatValue();
    }

    protected ICEState getICEState(FieldValue fieldValue) {
        String value = fieldValue.getStringValue();
        try {
            return ICEState.valueOf( value);
        } catch (Exception ex) {
            logger.warn("Error during enum conversion", ex);
            return null;
        }
    }

    protected MediaType getMediaType(FieldValue fieldValue) {
        String value = fieldValue.getStringValue();
        try {
            return MediaType.valueOf( value);
        } catch (Exception ex) {
            logger.warn("Error during enum conversion", ex);
            return null;
        }
    }

    protected NetworkType getNetworkType(FieldValue fieldValue) {
        String value = fieldValue.getStringValue();
        try {
            return NetworkType.valueOf( value);
        } catch (Exception ex) {
            logger.warn("Error during enum conversion", ex);
            return null;
        }
    }

    protected CandidateType getCandidateType(FieldValue fieldValue) {
        String value = fieldValue.getStringValue();
        try {
            return CandidateType.valueOf( value);
        } catch (Exception ex) {
            logger.warn("Error during enum conversion", ex);
            return null;
        }
    }

    protected TransportProtocol getTransportProtocol(FieldValue fieldValue) {
        String value = fieldValue.getStringValue();
        try {
            return TransportProtocol.valueOf( value);
        } catch (Exception ex) {
            logger.warn("Error during enum conversion", ex);
            return null;
        }
    }

    protected RTCQualityLimitationReason getRTCQualityLimitationReason(FieldValue fieldValue) {
        String value = fieldValue.getStringValue();
        try {
            return RTCQualityLimitationReason.valueOf( value);
        } catch (Exception ex) {
            logger.warn("Error during enum conversion", ex);
            return null;
        }
    }
}
