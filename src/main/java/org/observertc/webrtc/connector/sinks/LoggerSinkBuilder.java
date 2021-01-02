package org.observertc.webrtc.connector.sinks;

import org.observertc.webrtc.connector.configbuilders.AbstractBuilder;

public class LoggerSinkBuilder extends AbstractBuilder implements SinkTypeBuilder{

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
