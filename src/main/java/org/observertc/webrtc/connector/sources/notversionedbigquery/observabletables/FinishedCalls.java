package org.observertc.webrtc.connector.sources.notversionedbigquery.observabletables;

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
        String callUUID = row.get(CALL_UUID_FIELD_NAME).getStringValue();
        String callName = row.get(CALL_NAME_FIELD_NAME).getStringValue();

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
