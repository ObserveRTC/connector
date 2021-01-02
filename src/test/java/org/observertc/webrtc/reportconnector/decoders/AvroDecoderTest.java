package org.observertc.webrtc.reportconnector.decoders;

import io.reactivex.rxjava3.core.Observable;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.observertc.webrtc.schemas.reports.Report;
import org.observertc.webrtc.schemas.reports.ReportType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

class AvroDecoderTest {

    @Test
    public void shouldDecodeValidInput() {
        // Given
        AtomicReference<Report> decoded = new AtomicReference<>(null);
        Report report = Report.newBuilder()
                .setServiceUUID(UUID.randomUUID().toString())
                .setServiceName("serviceName")
                .setType(ReportType.INITIATED_CALL)
                .setTimestamp(1234L)
                .build();
        byte[] bytes = convert(report);

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

    byte[] convert(Report report) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        SpecificDatumWriter datumWriter = new SpecificDatumWriter<>(Report.SCHEMA$);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        outputStream.reset();

        try {
            datumWriter.write(report, encoder);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        byte[] out;
        try {
            encoder.flush();
            out = outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out;
    }
}