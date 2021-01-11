package org.observertc.webrtc.connector.pipelines;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipelineConfig {
    public String name;

    public MetaConfig meta = new MetaConfig();

    @NotNull
    public Map<String, Object> source;

    public Map<String, Object> decoder = new HashMap<>();

    public List<Map<String, Object>> transformations = new ArrayList<>();

    public BufferConfig buffer = new BufferConfig();

    @NotNull
    public Map<String, Object> sink;

}
