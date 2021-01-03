package org.observertc.webrtc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class ObjectToString {
    private static final ObjectWriter OBJECT_WRITER;

    static {
        OBJECT_WRITER = new ObjectMapper().writer().withDefaultPrettyPrinter();
    }


    public static String toString(Object subject) {
        try {
            String result = OBJECT_WRITER.writeValueAsString(subject);
            return result;
        } catch (JsonProcessingException e) {
            return e.getMessage();
        }
    }
}
