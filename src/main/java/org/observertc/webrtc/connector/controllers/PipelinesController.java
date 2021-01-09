package org.observertc.webrtc.connector.controllers;

import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import org.observertc.webrtc.connector.pipelines.Pipelines;
import org.observertc.webrtc.connector.configbuilders.ObservableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Controller("/pipelines")
public class PipelinesController {
    private static final Logger logger = LoggerFactory.getLogger(PipelinesController.class);

    private final Pipelines pipelines;
    private final ObservableConfig observableConfig;
    public PipelinesController(
            Pipelines pipelines,
            ObservableConfig observableConfig) {
        this.pipelines = pipelines;
        this.observableConfig = observableConfig;
    }

    @Get("/")
    public HttpStatus getAll() {
        return HttpStatus.OK;
    }

    @Post("/")
    public HttpStatus add() {
        return HttpStatus.OK;
    }

    @Delete("/")
    public HttpStatus remove() {
        return HttpStatus.OK;
    }

    @Put("/")
    public HttpStatus update() {
        return HttpStatus.OK;
    }
}
