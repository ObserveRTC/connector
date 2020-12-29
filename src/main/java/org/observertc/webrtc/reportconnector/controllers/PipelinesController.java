package org.observertc.webrtc.reportconnector.controllers;

import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import org.observertc.webrtc.reportconnector.Pipelines;
import org.observertc.webrtc.reportconnector.configbuilders.ConfigurationSourceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Controller("/pipelines")
public class PipelinesController {
    private static final Logger logger = LoggerFactory.getLogger(PipelinesController.class);

    private final Pipelines pipelines;
    private final ConfigurationSourceProvider configurationSourceProvider;
    public PipelinesController(
            Pipelines pipelines,
            ConfigurationSourceProvider configurationSourceProvider) {
        this.pipelines = pipelines;
        this.configurationSourceProvider = configurationSourceProvider;
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
