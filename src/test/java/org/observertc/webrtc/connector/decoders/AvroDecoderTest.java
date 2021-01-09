package org.observertc.webrtc.connector.decoders;

import io.reactivex.rxjava3.core.Observable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.observertc.webrtc.schemas.reports.Report;
import org.observertc.webrtc.schemas.reports.ReportType;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

class AvroDecoderTest {

    @Test
    public void shouldDecodeValidInput() throws IOException {
        // Given
        AtomicReference<Report> decoded = new AtomicReference<>(null);
        Report report = Report.newBuilder()
                .setVersion(1)
                .setServiceUUID(UUID.randomUUID().toString())
                .setServiceName("serviceName")
                .setType(ReportType.INITIATED_CALL)
                .setTimestamp(1234L)
                .build();
        byte[] bytes = report.toByteBuffer().array();

        // When
        Observable.fromArray(bytes)
                .lift(new AvroDecoder())
                .subscribe(decoded::set);

        // Then
        Report subject = decoded.get();
        Assertions.assertNotNull(subject);
        Assertions.assertEquals(subject.getType(), report.getType());
        Assertions.assertEquals(subject.getTimestamp(), report.getTimestamp());
        Assertions.assertEquals(subject.getMarker(), report.getMarker());
        Assertions.assertEquals(subject.getServiceName(), report.getServiceName());
        Assertions.assertEquals(subject.getServiceUUID(), report.getServiceUUID());
    }

}