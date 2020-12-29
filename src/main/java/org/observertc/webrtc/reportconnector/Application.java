package org.observertc.webrtc.reportconnector;

import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;
import org.observertc.webrtc.reportconnector.configbuilders.ConfigurationSourceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);
    public static ApplicationContext context;

    public static void main(String[] args) {

        context = Micronaut.run(Application.class, args);
        PipelinesConfig pipelineSourceConfig = context.getBean(PipelinesConfig.class);
        load(pipelineSourceConfig);
    }

    private static void load(PipelinesConfig config) {
        if (Objects.isNull(config)) {
            logger.info("{} is null", PipelinesConfig.class.getSimpleName());
            return;
        }
        List<String> paths = config.files;
        ConfigurationSourceProvider configurationSourceProvider = context.getBean(ConfigurationSourceProvider.class);
        Pipelines pipelines = context.getBean(Pipelines.class);
        AtomicReference<Throwable> error = new AtomicReference<>(null);
        if (Objects.nonNull(paths)) {
            for (String configPath : paths) {
                InputStream inputStream = null;
                try {
                    if (configPath.startsWith("classpath:")) {
                        configPath = configPath.substring(10);
                        inputStream = Application.class.getClassLoader()
                                .getResourceAsStream(configPath);
                    } else {
                        inputStream = new FileInputStream(configPath);
                    }
                    configurationSourceProvider
                            .fromYamlInputStream(inputStream)
                            .subscribe(pipelines::add, error::set);
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
