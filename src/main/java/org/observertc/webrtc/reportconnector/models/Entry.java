package org.observertc.webrtc.reportconnector.models;

import java.util.Map;

public interface Entry {

     EntryType getEntryType();

     Map<String, Object> toMap();
}
