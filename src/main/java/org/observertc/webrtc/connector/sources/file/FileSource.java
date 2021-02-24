package org.observertc.webrtc.connector.sources.file;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import org.observertc.webrtc.connector.sources.Source;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.stream.Stream;

public class FileSource extends Source {
    private String path;

    public FileSource() {

    }

    @Override
    protected Observable<byte[]> makeObservable() {
        return new Observable<byte[]>() {
            @Override
            protected void subscribeActual(@NonNull Observer<? super byte[]> observer) {
                try (Stream<Path> paths = Files.walk(Paths.get(path))) {
                    paths
                            .filter(Files::isRegularFile)
                            .map(Path::toFile)
                            .map(FileSource.this::read)
                            .filter(Objects::nonNull)
                            .forEach(observer::onNext);
                } catch (IOException e) {
                    observer.onError(e);
                }
                observer.onComplete();
            }
        };
    }

    private byte[] read(File file) {
        byte[] result = null;
        try (InputStream inputStream = new FileInputStream(file)) {
            long fileSize = file.length();
            result = new byte[(int) fileSize];
            inputStream.read(result);
            return result;
        } catch (IOException ex) {
            logger.warn("Exception occurred while reading file {}", file, ex);
            return null;
        }
    }

    FileSource setPath(String path) {
        this.path = path;
        return this;
    }
}
