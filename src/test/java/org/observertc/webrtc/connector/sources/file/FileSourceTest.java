package org.observertc.webrtc.connector.sources.file;


import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.observertc.webrtc.connector.sources.Source;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

@MicronautTest
public class FileSourceTest {

    @TempDir
    static File temporaryFolder;
//    @Rule
//    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void shouldWork() throws InterruptedException, IOException {
        // Given
        String writtenText = this.writeToFile( "myText");
        Source source = new FileSource().setPath(temporaryFolder.getPath());
        AtomicReference<byte[]> lastReadBytes = new AtomicReference<>();

        // When
        source.subscribe(lastReadBytes::set);
        source.run();

        // Then
        Assert.assertEquals(writtenText, new String(lastReadBytes.get()));
    }

    private String writeToFile(String text) throws IOException {
        File tempFile = Path.of(temporaryFolder.getPath(), "tempFile").toFile();
        byte data[] = text.getBytes(StandardCharsets.UTF_8);
        FileOutputStream writer = new FileOutputStream(tempFile);
        try {
            writer.write(data);
        } finally {
            if (Objects.nonNull(writer)) {
                writer.close();
            }
            return text;
        }
    }


}