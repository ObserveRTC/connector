package org.observertc.webrtc.connector.pipelines;

import org.jooq.Meta;
import org.observertc.webrtc.ObjectToString;
import org.observertc.webrtc.connector.configbuilders.ConfigConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Singleton
public class Pipelines {
    private static final Logger logger = LoggerFactory.getLogger(Pipelines.class);

    private final Provider<PipelineBuilder> pipelineBuilderProvider;
    private final ThreadPoolExecutor executorService;
//    private final Map<UUID, List<Pipeline>> pipelines;
//    private final Map<UUID, List<Future>> running;

    private final Map<UUID, ActivePipelines> pipelines;

    private class ActivePipelines {
        final UUID uuid;
        final Map<String, Object> config;
        List<Pipeline> prepared = new LinkedList<>();
        List<Future> started = new LinkedList<>();

        private ActivePipelines(UUID uuid, Map<String, Object> config) {
            this.uuid = uuid;
            this.config = config;
        }
    }


    public Pipelines(
                    PipelinesConfig config,
                    Provider<PipelineBuilder> pipelineBuilderProvider)
    {
        this.pipelines = new HashMap<>();
        this.executorService = new ThreadPoolExecutor(config.corePoolSize, config.maxPoolSize, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
        this.pipelineBuilderProvider = pipelineBuilderProvider;

    }

    public UUID build(Map<String, Object> config) {
        UUID uuid = UUID.randomUUID();
        ActivePipelines pipelines = new ActivePipelines(uuid, config);
        this.pipelines.put(uuid, pipelines);
        String name = (String) config.get("name");
        Map<String, Object> metaConfig = (Map<String, Object>) config.get("meta");
        if (Objects.isNull(name)) {
            return null;
        }
        MetaConfig meta = new MetaConfig();
        if (Objects.nonNull(metaConfig)) {
            meta = ConfigConverter.convert(MetaConfig.class, metaConfig);
        }
        for (int instances = 1; instances <= meta.replicas; ++instances) {
            if (1 < meta.replicas) {
                name = name.concat(String.format("-%d", instances));
            }
            PipelineBuilder pipelineBuilder = this.pipelineBuilderProvider.get();
            pipelineBuilder.withConfiguration(config);
            Optional<Pipeline> pipelineHolder = pipelineBuilder.build();
            if (!pipelineHolder.isPresent()) {
                String message = "Cannot build pipeline for configuration: " + ObjectToString.toString(config);
                throw new IllegalStateException(message);
            }
            Pipeline pipeline = pipelineHolder.get();
            pipelines.prepared.add(pipeline);
        }
        return uuid;
//        PipelineConfig config = ConfigConverter.convert(PipelineConfig.class, configuration);
//        return this.build(config);
    }

    public Map<UUID, Map<String, Object>> findAll() {
        Map<UUID, Map<String, Object>> result = this.pipelines.entrySet().stream()
                .map(e -> Map.entry(e.getKey(), e.getValue().config))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
        return result;
    }

    public void remove(UUID uuid) {
        ActivePipelines pipeline = this.pipelines.get(uuid);
        if (Objects.isNull(pipeline)) {
            return;
        }
        this.start(uuid);
        this.pipelines.remove(uuid);
    }

    public void stop(UUID uuid) {
        ActivePipelines pipelines = this.pipelines.get(uuid);
        var futures = pipelines.started;
        if (Objects.isNull(futures)) {
            return;
        }
        for (var future : futures) {
            if (future.isDone() || future.isCancelled()) {
                continue;
            }
            future.cancel(true);
        }
    }

    public void stopAll() {
        Set<UUID> futures = this.pipelines.keySet();
        futures.forEach(this::stop);
    }

    public String start(UUID pipelineUUID) {
        String result;
        boolean infoWritten = false;
        synchronized (this) {
            ActivePipelines activePipelines = this.pipelines.get(pipelineUUID);
            if (Objects.isNull(activePipelines)) {
                result = String.format("There is no pipeline exists for uuid %s. Nothing will be started", pipelineUUID.toString());
                return result;
            }
            List<Pipeline> pipelines = activePipelines.prepared;
            boolean allCreated = pipelines.stream().allMatch(p -> p.getState().equals(Pipeline.State.CREATED));
            if (!allCreated) {
                result = String.format("WIll not start %s, because the belonging pipelines are already either started or finished", pipelineUUID.toString());
                return result;
            }
            for (Pipeline pipeline : pipelines) {
                String pipelineName = pipeline.getName();
                if (!infoWritten) {
                    logger.info("Pipeline {} -> {} is started", pipelineUUID, pipelineName);
                    infoWritten = true;
                }
                pipeline.withClosingCallback(() -> {
                    logger.info("{} is ended. total number or running pipelines: {}", pipelineName,
                            executorService.getActiveCount() - 1);
                });
                var future = executorService.submit(pipeline);
                activePipelines.started.add(future);
                logger.info("{} is running. total number or running pipelines: {}", pipelineName,
                        executorService.getActiveCount());
            }
        }
        result = String.format("Pipeline %s has been started", pipelineUUID);
        return result;
    }

    public Map<UUID, String> startAll() {
        Map<UUID, String> result = new HashMap<>();
        Set<UUID> pipelinesUUIDs = this.pipelines.keySet();
        for (UUID pipelineUUID : pipelinesUUIDs) {
            String mesasge = this.start(pipelineUUID);
            result.put(pipelineUUID, mesasge);
        }
        return result;
    }
}
