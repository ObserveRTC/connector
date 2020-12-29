package org.observertc.webrtc.reportconnector.sources;

import java.util.Map;

public interface SourceTypeBuilder {
    void withConfiguration(Map<String, Object> configuration);
    Source build();
}
