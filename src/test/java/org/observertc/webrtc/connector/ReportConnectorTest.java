package org.observertc.webrtc.connector;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.observertc.webrtc.schemas.reports.InitiatedCall;
import org.observertc.webrtc.schemas.reports.Report;

@MicronautTest
public class ReportConnectorTest {

    @Test
    public void something() {
        System.out.println(Report.getClassSchema().getName());
        Report.getClassSchema().getFields().forEach(field -> {
            System.out.println(field.name());
        });
        System.out.println(InitiatedCall.getClassSchema().getName());
        InitiatedCall.getClassSchema().getFields().forEach(field -> {
            System.out.println(field.name());
        });
        InitiatedCall initiatedCall;

    }


}
