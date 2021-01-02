package org.observertc.webrtc.connector.models;

import java.util.Map;

public interface Entry {

     EntryType getEntryType();

     Map<String, Object> toMap();
}
