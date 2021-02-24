package org.observertc.webrtc.connector.sinks.file;

import io.micronaut.context.annotation.Prototype;
import org.observertc.webrtc.connector.configbuilders.AbstractBuilder;
import org.observertc.webrtc.connector.configbuilders.Builder;
import org.observertc.webrtc.connector.sinks.Sink;

import javax.validation.constraints.NotNull;

@Prototype
public class FileSinkBuilder extends AbstractBuilder implements Builder<Sink> {

    @Override
    public Sink build() {
        Config config = this.convertAndValidate(Config.class);
        FileSink result = new FileSink();
        return result
                .withPath(config.path)
                .withOverwriteExistingFile(config.overwriteExistingFile)
                .withOnlyOnePerReportType(config.onlyOnePerReportType)
                .withMaxFileNum(config.maxFileNum)
                ;
    }

    public static class Config {
        public boolean overwriteExistingFile = true;

        @NotNull
        public String path;

        public int maxFileNum = -1;

        public boolean onlyOnePerReportType = false;
    }
}
