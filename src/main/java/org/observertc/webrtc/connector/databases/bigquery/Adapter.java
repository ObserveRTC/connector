package org.observertc.webrtc.connector.databases.bigquery;

import org.apache.avro.specific.SpecificRecordBase;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

public class Adapter implements Function<SpecificRecordBase, Map<String, Object>> {
    private Map<String, Mapping> mappings = new HashMap<>();
    private Map<String, Adapter> adapters = new HashMap<>();

    @Override
    public Map<String, Object> apply(SpecificRecordBase subject) {
        Map<String, Object> result = new HashMap<>();
        Iterator<Map.Entry<String, Mapping>> it = this.mappings.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry<String, Mapping> entry = it.next();
            String field = entry.getKey();
            Mapping mapping = entry.getValue();
            Object value = subject.get(field);
            result.put(
                    mapping.fieldAdapter.apply(field),
                    mapping.valueAdapter.apply(value)
            );
        }

        Iterator<Map.Entry<String, Adapter>> it2 = this.adapters.entrySet().iterator();
        while(it2.hasNext()) {
            Map.Entry<String, Adapter> entry = it2.next();
            String field = entry.getKey();
            Adapter adapter = entry.getValue();
            SpecificRecordBase value = (SpecificRecordBase) subject.get(field);
            Map<String, Object> fields = adapter.apply(value);
            result.putAll(fields);
        }

        return result;
    }

    Adapter add(String fieldName, Function<String, String> fieldAdapter, Function valueAdapter) {
        Mapping mapping = new Mapping(fieldAdapter, valueAdapter);
        this.mappings.put(fieldName, mapping);
        return this;
    }

    Adapter add(String fieldName, Adapter adapter) {
        this.adapters.put(fieldName, adapter);
        return this;
    }

    private class Mapping {
        final Function<String, String> fieldAdapter;
        final Function valueAdapter;

        private Mapping(Function<String, String> fieldAdapter, Function valueAdapter) {
            this.fieldAdapter = fieldAdapter;
            this.valueAdapter = valueAdapter;
        }
    }
}
