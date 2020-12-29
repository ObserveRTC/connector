package org.observertc.webrtc.reportconnector.sinks;

import org.observertc.webrtc.reportconnector.sources.Source;

import java.util.Map;

public interface SinkTypeBuilder {
    void withConfiguration(Map<String, Object> configuration);
    Sink build();
}
