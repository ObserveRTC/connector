package org.observertc.webrtc.connector.pipelines;

import org.observertc.webrtc.ObjectToString;
import org.observertc.webrtc.connector.configbuilders.ConfigConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

@Singleton
public class Pipelines {
    private static final Logger logger = LoggerFactory.getLogger(Pipelines.class);

    private final List<PipelineBuilder> builders = new ArrayList<>();
    private final Provider<PipelineBuilder> pipelineBuilderProvider;
    private final ThreadPoolExecutor executorService;
    private final Map<UUID, Pipeline> scheduled;


    public Pipelines(
                    PipelinesConfig config,
                    Provider<PipelineBuilder> pipelineBuilderProvider)
    {
        this.scheduled = new HashMap<>();
        this.executorService = new ThreadPoolExecutor(config.corePoolSize, config.maxPoolSize, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
        this.pipelineBuilderProvider = pipelineBuilderProvider;

    }

    public Runnable build(Map<String, Object> configuration) {
        PipelineConfig config = ConfigConverter.convert(PipelineConfig.class, configuration);
        List<Runnable> replicas = new ArrayList<>();
        String name = config.name;
        for (int instances = 1; instances <= config.meta.replicas; ++instances) {
            if (1 < config.meta.replicas) {
                config.name = name.concat(String.format("-%d", instances));
            }
            PipelineBuilder pipelineBuilder = this.pipelineBuilderProvider.get();
            pipelineBuilder.withConfiguration(config);
            Optional<Pipeline> pipelineHolder = pipelineBuilder.build();
            if (!pipelineHolder.isPresent()) {
                logger.warn("Cannot build pipeline for configuration: {}", ObjectToString.toString(configuration));
                return () -> {
                    logger.warn("A runnable trigger is called for a pipeline, " +
                                    "which was not built. configuration for that pipeline: {}",
                            ObjectToString.toString(configuration));
                };
            }
            Pipeline pipeline = pipelineHolder.get();
            this.builders.add(pipelineBuilder);
            UUID uuid = UUID.randomUUID();
            this.scheduled.put(uuid, pipeline);
            Pipelines lock = this;
            Runnable start = () -> {
                synchronized (lock) {
                    Pipeline scheduledPipeline = scheduled.remove(uuid);
                    if (Objects.isNull(scheduledPipeline)) {
                        logger.warn("There is no scheduled pipeline for uuid {}. Nothing will be started", uuid);
                        return;
                    }
                    String pipelineName = scheduledPipeline.getName();
                    scheduledPipeline.withClosingCallback(() -> {
                        logger.info("{} is ended. total number or running pipelines: {}", pipelineName,
                                executorService.getActiveCount() - 1);
                    });
                    executorService.submit(scheduledPipeline);
                    logger.info("{} is running. total number or running pipelines: {}", pipelineName,
                            executorService.getActiveCount());
                }
            };
            replicas.add(start);
        }

        return () -> {
            replicas.forEach(Runnable::run);
        };
    }

    public void remove(Predicate<PipelineBuilder> filter) {
        throw new NoSuchElementException();
    }

    public void removeAll() {
        this.executorService.shutdownNow();
    }

    public void startAll() {
        synchronized (this) {
            Iterator<Map.Entry<UUID, Pipeline>> it = this.scheduled.entrySet().iterator();
            for (; it.hasNext();) {
                Map.Entry<UUID, Pipeline> entry = it.next();
                Pipeline scheduledPipeline = entry.getValue();
                String pipelineName = scheduledPipeline.getName();
                scheduledPipeline.withClosingCallback(() -> {
                    logger.info("{} is ended. total number or running pipelines: {}", pipelineName,
                            executorService.getActiveCount());
                });

                executorService.submit(scheduledPipeline);
                logger.info("{} is running. total number or running pipelines: {}", pipelineName,
                        executorService.getActiveCount() - 1);
                it.remove();
            }
        }
    }

}
