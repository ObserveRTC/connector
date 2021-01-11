package org.observertc.webrtc.connector.databases.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import javafx.util.Pair;
import org.apache.avro.Schema;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class AdapterBuilder {

    private Schema schema = null;
    private Set<String> excludedFields = new HashSet<>();
    private Map<String, Pair<Function<String, String>,Function>> fieldResolver = new HashMap<>();
    private Map<String, LegacySQLTypeName> explicitFieldTypes = new HashMap<>();
    private Map<String, AdapterBuilder> flatMaps = new HashMap<>();

    public AdapterBuilder excludeFields(String... fieldNames) {
        for (int i = 0; i < fieldNames.length; ++i) {
            String fieldName = fieldNames[i].toLowerCase();
            this.excludedFields.add(fieldName);
        }
        return this;
    }

    public AdapterBuilder explicitTypeMapping(String fieldName, LegacySQLTypeName legacySQLTypeName) {
        this.explicitFieldTypes.put(fieldName, legacySQLTypeName);
        return this;
    }

    public AdapterBuilder mapFieldBy(String fieldName,
                                     Function<String, String> fieldNameResolver,
                                     Function fieldValueResolver) {
        this.fieldResolver.put(fieldName, new Pair<>(fieldNameResolver, fieldValueResolver));
        return this;
    }

    public AdapterBuilder mapFieldBy(String fieldName, Function fieldValueResolver) {
        this.fieldResolver.put(fieldName, new Pair<>(Function.identity(), fieldValueResolver));
        return this;
    }


    public AdapterBuilder flatMap(String fieldName, AdapterBuilder adapterBuilder) {
        this.flatMaps.put(fieldName, adapterBuilder);
        return this;
    }

    public AdapterBuilder forSchema(Schema schema) {
        this.schema = schema;
        return this;
    }

    public Adapter build(AtomicReference<com.google.cloud.bigquery.Schema> schemaHolder) {
        Adapter result = new Adapter();
        List<Field> fields = new ArrayList<>();
        for (Schema.Field field : this.schema.getFields()) {
            String fieldName = field.name();
            if (this.excludedFields.contains(fieldName)) {
                continue;
            }
            AdapterBuilder adapterBuilder = this.flatMaps.get(fieldName);
            if (Objects.nonNull(adapterBuilder)) {
                AtomicReference<com.google.cloud.bigquery.Schema> subSchemaHolder = new AtomicReference<>();
                Adapter adapter = adapterBuilder.build(subSchemaHolder);
                result.add(fieldName, adapter);
                com.google.cloud.bigquery.Schema subSchema = subSchemaHolder.get();
                if (Objects.nonNull(subSchema)) {
                    subSchema.getFields().stream().forEach(fields::add);
                }
                continue;
            }
            AtomicReference<Function> convertHolder = new AtomicReference<>();
            Optional<Field> fieldHolder = this.mapField(field, convertHolder);
            if (fieldHolder.isPresent()) {
                fields.add(fieldHolder.get());
            }
            Pair<Function<String, String>, Function> mapper = this.fieldResolver.get(fieldName);
            if (Objects.nonNull(mapper)) {
                result.add(fieldName, mapper.getKey(), mapper.getValue());
                continue;
            } else {
                result.add(fieldName, Function.identity(), convertHolder.get());
            }

        }

        if (Objects.nonNull(schemaHolder)) {
            com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(fields);
            schemaHolder.set(schema);
        }
        return result;
    }

    private Optional<Field> mapField(Schema.Field field, AtomicReference<Function> convertHolder) {
        String fieldName = field.name();
        Schema fieldSchema = field.schema();
        Schema.Type fieldType = fieldSchema.getType();
        LegacySQLTypeName dbType;
        Function converter = Function.identity();
        if (this.explicitFieldTypes.containsKey(fieldName.toLowerCase())) {
            dbType = this.explicitFieldTypes.get(fieldName.toLowerCase());
        } else {
            if (fieldType.equals(Schema.Type.UNION) && fieldSchema.getTypes().size() == 2) {
                Schema.Type subType = fieldSchema.getTypes().get(0).getType();
                if (subType.equals(Schema.Type.NULL)) { // most likely nullable
                    subType = fieldSchema.getTypes().get(1).getType();
                }
                Pair<LegacySQLTypeName, Function> tuple= this.mapType(subType);
                dbType = tuple.getKey();
                converter = tuple.getValue();
            } else {
                Pair<LegacySQLTypeName, Function> tuple= this.mapType(fieldType);
                dbType = tuple.getKey();
                converter = tuple.getValue();
            }
        }
        convertHolder.set(converter);
        Field.Mode mode;
        if (fieldSchema.isNullable()) {
            mode = Field.Mode.NULLABLE;
        } else {
            mode = Field.Mode.REQUIRED;
        }
        Field result = Field.newBuilder(
                fieldName,
                dbType
        ).setMode(mode)
                .setDescription(fieldSchema.getDoc())
                .build();

        return Optional.of(result);
    }

    private Pair<LegacySQLTypeName, Function> mapType(Schema.Type source) {
        switch (source) {
            case LONG:
            case INT:
                return new Pair<>(LegacySQLTypeName.INTEGER, Function.identity());
            case BYTES:
                return new Pair<>(LegacySQLTypeName.BYTES, Function.identity());
            case BOOLEAN:
                return new Pair<>(LegacySQLTypeName.BOOLEAN, Function.identity());
            case STRING:
                return new Pair<>(LegacySQLTypeName.STRING, Function.identity());
            case ENUM:
                Function<Enum, String> enumConverter = e -> e.name();
                return new Pair<>(LegacySQLTypeName.STRING, enumConverter);
            case DOUBLE:
            case FLOAT:
                return new Pair<>(LegacySQLTypeName.FLOAT, Function.identity());
            default:
                throw new NoSuchElementException("No mapping exists from avro field type of " + source + " and to bigquery");
        }
    }
}
