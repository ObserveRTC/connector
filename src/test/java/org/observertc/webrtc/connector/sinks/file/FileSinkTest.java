package org.observertc.webrtc.connector.sinks.file;


import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.observertc.webrtc.connector.ReportGenerator;
import org.observertc.webrtc.connector.sinks.Sink;
import org.observertc.webrtc.schemas.reports.Report;

import java.io.File;
import java.io.IOException;
import java.util.List;

@MicronautTest
public class FileSinkTest {

    private static ReportGenerator reportGenerator = new ReportGenerator();

    @TempDir
    static File temporaryFolder;


//    @Rule
//    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void shouldWork() throws InterruptedException, IOException {
        // Given
        Sink sink = new FileSink().withPath(temporaryFolder.getPath());
        Report report = reportGenerator.joinedPeerConnectionReportSupplier("callName").get();

        // When
        sink.onNext(List.of(report));

        // Then
        Assert.assertEquals(1, temporaryFolder.listFiles().length);
    }



}