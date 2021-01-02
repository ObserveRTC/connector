package org.observertc.webrtc.connector.sinks;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.disposables.Disposable;
import org.observertc.webrtc.connector.models.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LoggerSink extends Sink {

    private static final Logger logger = LoggerFactory.getLogger(LoggerSink.class);
    private boolean detailedRow = true;

    @Override
    public void onSubscribe(@NonNull Disposable d) {
        super.onSubscribe(d);
    }

    @Override
    public void onNext(@NonNull List<Entry> entries) {
        logger.info("Number of entries are: {}", entries.size());
        if (this.detailedRow) {
            for (Entry entry : entries) {
                logger.info("EntryType {}. The row: \n {}",
                        entry.getEntryType(),
                        this.mapString(entry.toMap(), "\t"));
            }
        }
    }

    private String mapString(Map<String, Object> map, String prefix) {

        StringBuffer resultBuffer = new StringBuffer();
        Iterator<Map.Entry<String, Object>> mapIt = map.entrySet().iterator();
        for (; mapIt.hasNext(); ) {
            Map.Entry<String, Object> entry = mapIt.next();
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value == null) {
                resultBuffer.append(String.format("%s%s: null\n", prefix, key));
            } else if (value instanceof Map) {
                resultBuffer.append(String.format("%s%s: %s\n", prefix, key,
                        this.mapString((Map<String, Object>) value, prefix + "\t")));
            } else {
                resultBuffer.append(String.format("%s%s: %s\n", prefix, entry.getKey(), value.toString()));
            }
        }
        return resultBuffer.toString();
    }

    LoggerSink withDetailedRow(boolean value) {
        this.detailedRow = value;
        return this;
    }

}
