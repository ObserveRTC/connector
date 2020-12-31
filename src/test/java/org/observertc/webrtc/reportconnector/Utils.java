package org.observertc.webrtc.reportconnector;


import io.micrometer.core.instrument.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class Utils {
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    public static String getResourceFileAsString(String path) {
        ClassLoader classLoader = Utils.class.getClassLoader();
        InputStream inputStream;
        try {
             inputStream = new FileInputStream(classLoader.getResource(path).getFile());
        } catch (FileNotFoundException e) {
            logger.error( "Resource file not found", e);
            throw new RuntimeException(e);
        }
        String result = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        return result;
    }
}
