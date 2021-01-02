package org.observertc.webrtc.connector.sinks;

import java.util.Map;

public interface SinkTypeBuilder {
    void withConfiguration(Map<String, Object> configuration);
    Sink build();
}
