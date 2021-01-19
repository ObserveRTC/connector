package org.observertc.webrtc.connector.sources.bigquerysources.observabletables;

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import org.observertc.webrtc.connector.common.BigQueryService;
import org.observertc.webrtc.schemas.reports.FinishedCall;
import org.observertc.webrtc.schemas.reports.ReportType;

import java.util.ArrayList;
import java.util.List;

public class FinishedCalls extends RecordMapperAbstract {

    public static final String CALL_UUID_FIELD_NAME = "callUUID";
    public static final String CALL_NAME_FIELD_NAME = "callName";

    public FinishedCalls(BigQueryService bigQueryService, String tableName) {
        super(bigQueryService, tableName, ReportType.FINISHED_CALL);
    }

    @Override
    protected Object makePayload(FieldValueList row) {
        // String type
        String callUUID = this.getValue(row, CALL_UUID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND");
        String callName = this.getValue(row, CALL_NAME_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND");

        var result = FinishedCall.newBuilder()
                .setCallUUID(callName)
                .setCallUUID(callUUID);
        return result.build();
    }

    @Override
    protected List<String> getPayloadFieldNames() {
        List<String> result = new ArrayList<>();
        result.add(CALL_UUID_FIELD_NAME);
        result.add(CALL_NAME_FIELD_NAME);
        return result;
    }
}
