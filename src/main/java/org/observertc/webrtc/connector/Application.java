package org.observertc.webrtc.connector;

import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;
import org.jooq.tools.StringUtils;
import org.observertc.webrtc.connector.configbuilders.ObservableConfig;
import org.observertc.webrtc.connector.pipelines.Pipelines;
import org.observertc.webrtc.connector.pipelines.PipelinesConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);
    private static final String INITIAL_WAIT_IN_S = "INITIAL_WAITING_TIME_IN_S";
    public static ApplicationContext context;



    public static void main(String[] args) {
        Sleeper.makeFromSystemEnv(INITIAL_WAIT_IN_S, ChronoUnit.SECONDS).run();
        context = Micronaut.run(Application.class, args);
        List<String> configFiles = getPipelineConfigFiles();
        ConnectorConfig connectorConfig = context.getBean(ConnectorConfig.class);
        load(configFiles, connectorConfig.pipelines);
    }

    private static List<String> getPipelineConfigFiles() {
        List<String> result = new LinkedList<>();
        String pipelineConfigFiles = System.getenv("PIPELINE_CONFIG_FILES");
        if (pipelineConfigFiles != null) {
            logger.info("Loading files {}", pipelineConfigFiles);
            Arrays.asList(pipelineConfigFiles.split(",")).stream().forEach(result::add);
        }

        if (Objects.nonNull(context)) {
            PipelinesConfig pipelineSourceConfig = context.getBean(PipelinesConfig.class);
            if (Objects.nonNull(pipelineSourceConfig)) {
                pipelineSourceConfig.files.stream().forEach(result::add);
            } else {
                logger.warn("{} is null", PipelinesConfig.class.getSimpleName());
            }
        } else {
            logger.error("Context is null. Where did you called getPipelineConfigFiles?");
        }
        return result;
    }

    private static void load(List<String> paths, List<Map<String, Object>> pipelineConfigs) {
        ObservableConfig observableConfig = context.getBean(ObservableConfig.class);
        Pipelines pipelines = context.getBean(Pipelines.class);
        if (Objects.nonNull(pipelineConfigs)) {
            pipelineConfigs.forEach(pipelines::build);
        }
        AtomicReference<Throwable> error = new AtomicReference<>(null);
        if (Objects.nonNull(paths)) {
            for (String configPath : paths) {
                if (StringUtils.isBlank(configPath)) {
                    continue;
                }
                InputStream inputStream = null;
                try {
                    if (configPath.startsWith("classpath:")) {
                        configPath = configPath.substring(10);
                        inputStream = Application.class.getClassLoader()
                                .getResourceAsStream(configPath);
                    } else {
                        inputStream = new FileInputStream(configPath);
                    }
                    if (Objects.isNull(inputStream)) {
                        logger.warn("Cannot find {}", configPath);
                        continue;
                    }
                    observableConfig
                            .fromYamlInputStream(inputStream)
                            .subscribe(pipelines::build, error::set);
                    if (Objects.nonNull(error.get())) {
                        logger.error("During pipeline loading an error happened", error.get());
                        return;
                    }
                } catch (Exception e) {
                    logger.error("Error during piipeline configuration loading", e);
                    continue;
                }

            }
        }
        pipelines.startAll();
    }


}
