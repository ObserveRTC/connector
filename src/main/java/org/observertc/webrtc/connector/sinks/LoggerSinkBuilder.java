package org.observertc.webrtc.connector.sinks;

import org.observertc.webrtc.connector.configbuilders.AbstractBuilder;
import org.observertc.webrtc.connector.configbuilders.Builder;

public class LoggerSinkBuilder extends AbstractBuilder implements Builder<Sink> {

    @Override
    public Sink build() {
        Config config = this.convertAndValidate(Config.class);
        LoggerSink result = new LoggerSink();
        return result
                .withDetailedRow(config.detailedLogs);
    }

    public static class Config {
        public boolean detailedLogs = false;
    }
}
