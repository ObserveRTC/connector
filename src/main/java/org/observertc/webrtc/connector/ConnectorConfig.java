package org.observertc.webrtc.connector;

import io.micronaut.context.annotation.ConfigurationProperties;

import java.util.List;
import java.util.Map;

@ConfigurationProperties("connector")
public class ConnectorConfig {
    public List<Map<String, Object>> pipelines;
}
