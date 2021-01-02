package org.observertc.webrtc.connector.configbuilders;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.observertc.webrtc.connector.Utils;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@MicronautTest
class ObservableConfigTest {

    private static final String EXAMPLE_YAML_RESOURCE_PATH = "observable_config_example.yaml";
    private static final String EXAMPLE_JSON_RESOURCE_PATH = "observable_config_example.json";

    @Inject
    ObservableConfig observableConfig;

    @Test
    public void shouldLoadTheSame() {

        // Given
        final String yaml = Utils.getResourceFileAsString(EXAMPLE_YAML_RESOURCE_PATH);
        final String json = Utils.getResourceFileAsString(EXAMPLE_JSON_RESOURCE_PATH);
        AtomicReference<Map<String, Object>> yamlMapHolder = new AtomicReference<>(null);
        AtomicReference<Map<String, Object>> jsonMapHolder = new AtomicReference<>(null);

        // When
        this.observableConfig.fromYamlString(yaml).subscribe(yamlMapHolder::set);
        this.observableConfig.fromJsonString(json).subscribe(jsonMapHolder::set);

        // Then
        Assertions.assertNotNull(yamlMapHolder.get());
        Assertions.assertNotNull(jsonMapHolder.get());
        assertEquals(yamlMapHolder.get(), jsonMapHolder.get());
    }

    private void assertEquals(Map<String, Object> map1, Map<String, Object> map2) {
        assertMapContains(map1, map2);
        assertMapContains(map2, map1);
    }

    private void assertMapContains(Map<String, Object> source, Map<String, Object> subject) {
        Assertions.assertEquals(source.size(), subject.size(), "The sizes of the maps are different");
        Iterator<Map.Entry<String, Object>> it = source.entrySet().iterator();
        for (; it.hasNext(); ) {
            Map.Entry<String, Object> sourceEntry = it.next();
            String sourceKey = sourceEntry.getKey();
            Assertions.assertTrue(subject.containsKey(sourceKey), "The subject map does not contain the source map key: " + sourceKey);

            Object sourceValue = sourceEntry.getValue();
            Object subjectValue = subject.get(sourceKey);
            if (sourceValue instanceof Map) {
                assertMapContains((Map<String, Object>) sourceValue, (Map<String, Object>)subjectValue);
            } else {
                Assertions.assertEquals(sourceValue, subjectValue, "The source value " + sourceValue.toString() + " and the subject value " + subjectValue.toString() +" are different ");
            }
        }
    }
}