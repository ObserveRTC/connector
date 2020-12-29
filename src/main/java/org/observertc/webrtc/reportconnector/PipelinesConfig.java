package org.observertc.webrtc.reportconnector;

import io.micronaut.context.annotation.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties("pipelines")
public class PipelinesConfig {

    public List<String> files = new ArrayList<>();
    public int corePoolSize = 10;
    public int maxPoolSize = 50;
}
