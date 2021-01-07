package org.observertc.webrtc.connector.models;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FinishedCallEntry implements Entry{
    public static final String SERVICE_UUID_FIELD_NAME = "serviceUUID";
    public static final String SERVICE_NAME_FIELD_NAME = "serviceName";
    public static final String CALL_UUID_FIELD_NAME = "callUUID";
    public static final String CALL_NAME_FIELD_NAME = "callName";
    public static final String MARKER_FIELD_NAME = "marker";
    public static final String TIMESTAMP_FIELD_NAME = "timestamp";

    private final Map<String, Object> values;

    public FinishedCallEntry() {
        this.values = new HashMap<>();
    }

    public FinishedCallEntry withServiceUUID(String value) {
        this.values.put(SERVICE_UUID_FIELD_NAME, value);
        return this;
    }

    public String getServiceUUID() {
        String result = (String) this.values.get(SERVICE_UUID_FIELD_NAME);
        return result;
    }

    public FinishedCallEntry withCallUUID(String value) {
        this.values.put(CALL_UUID_FIELD_NAME, value.toString());
        return this;
    }

    public String getCallUUID() {
        String result = (String) this.values.get(CALL_UUID_FIELD_NAME);
        return result;
    }

    public FinishedCallEntry withServiceName(String value) {
        this.values.put(SERVICE_NAME_FIELD_NAME, value);
        return this;
    }

    public String getServiceName() {
        String result = (String) this.values.get(SERVICE_NAME_FIELD_NAME);
        return result;
    }

    public FinishedCallEntry withCallName(String value) {
        this.values.put(CALL_NAME_FIELD_NAME, value);
        return this;
    }

    public String getCallName() {
        String result = (String) this.values.get(CALL_NAME_FIELD_NAME);
        return result;
    }

    public FinishedCallEntry withMarker(String value) {
        this.values.put(MARKER_FIELD_NAME, value);
        return this;
    }

    public String getMarker() {
        String result = (String) this.values.get(MARKER_FIELD_NAME);
        return result;
    }

    public FinishedCallEntry withTimestamp(Long value) {
        this.values.put(TIMESTAMP_FIELD_NAME, value);
        return this;
    }

    public Long getTimestamp() {
        Long result = (Long) this.values.get(TIMESTAMP_FIELD_NAME);
        return result;
    }

    @Override
    public EntryType getEntryType() {
        return EntryType.FinishedCall;
    }

    public Map<String, Object> toMap() {
        return this.values;
    }
}
