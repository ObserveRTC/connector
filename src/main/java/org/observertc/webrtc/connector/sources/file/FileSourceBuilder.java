package org.observertc.webrtc.connector.sources.file;

import io.micronaut.context.annotation.Prototype;
import org.observertc.webrtc.connector.configbuilders.AbstractBuilder;
import org.observertc.webrtc.connector.configbuilders.Builder;
import org.observertc.webrtc.connector.sources.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.File;

@Prototype
public class FileSourceBuilder extends AbstractBuilder implements Builder<Source> {

    private final static Logger logger = LoggerFactory.getLogger(FileSourceBuilder.class);
    private String sourceName;

    public Source build() {
        Config config = this.convertAndValidate(Config.class);
        FileSource result = new FileSource();
        File path = new File(config.path);
        if (!path.exists() || !path.isDirectory()) {
            logger.error("The given path ({}) for a FileSource is not exists or it is not a directory", config.path);
            return null;
        }
        return result
                .setPath(config.path)
                ;
    }


    public static class Config {

        @NotNull
        public String path;
    }

}
