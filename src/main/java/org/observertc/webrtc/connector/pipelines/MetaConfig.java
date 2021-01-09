package org.observertc.webrtc.connector.pipelines;

import javax.validation.constraints.Min;

public class MetaConfig {

    public String description = "No description is given for this pipeline";

    @Min(1)
    public int replicas = 1;
}
