package org.observertc.webrtc.reportconnector.controllers;

import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

@Controller("/")
public class IndexController {

    @Get("/")
    public HttpStatus index() {
        return HttpStatus.OK;
    }
}
