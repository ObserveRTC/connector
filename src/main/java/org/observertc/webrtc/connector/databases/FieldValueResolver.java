package org.observertc.webrtc.connector.databases;

import org.apache.avro.specific.SpecificRecordBase;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public class FieldValueResolver implements Function<SpecificRecordBase, Map.Entry<String, Optional<Object>>> {
    private final String fieldName;
    private final Function valueResolver;
    private final Function<String, String> fieldNameResolver;

    public FieldValueResolver(String fieldName, Function valueMapper) {
        this(fieldName, Function.identity(), valueMapper);
    }

    public FieldValueResolver(String fieldName, Function<String, String> fieldNameResolver, Function valueResolver) {
        this.fieldName = fieldName;
        this.fieldNameResolver = fieldNameResolver;
        this.valueResolver = valueResolver;
    }

    public Map.Entry<String, Optional<Object>> apply(SpecificRecordBase record) {
        Object fieldValue = record.get(this.fieldName);
        String newField = this.fieldNameResolver.apply(this.fieldName);
        Object newValue = this.valueResolver.apply(fieldValue);
        if (Objects.isNull(newValue)) {
            return Map.entry(newField, Optional.empty());
        } else {
            return Map.entry(newField, Optional.of(newValue));
        }
    }

}
