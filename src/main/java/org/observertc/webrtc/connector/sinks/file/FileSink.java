package org.observertc.webrtc.connector.sinks.file;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.disposables.Disposable;
import org.observertc.webrtc.connector.sinks.Sink;
import org.observertc.webrtc.schemas.reports.Report;
import org.observertc.webrtc.schemas.reports.ReportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class FileSink extends Sink {

    private static final Logger logger = LoggerFactory.getLogger(FileSink.class);
    private boolean overWriteExistingFile = true;
    private String path;
    private volatile int seqNum = 0;
    private String digitsFormat = "%05d";
    private int maxRetry = 1;
    private volatile int createdFileNum = 0;
    private boolean onlyOnePerReportType = false;

    private Set<ReportType> reportedTypes = new HashSet<>();
    private int maxFileNum;

    @Override
    public void onSubscribe(@NotNull Disposable d) {
        Objects.requireNonNull(this.path, "Path must be set for FileSink");
        super.onSubscribe(d);
    }

    @Override
    public void onNext(@NonNull List<Report> reports) {
        boolean maxFileReached = false;
        boolean reportsAdded = false;
        for (Report report : reports) {
            if (0 < this.maxFileNum && this.maxFileNum < this.createdFileNum) {
                maxFileReached = true;
                continue;
            }
            if (this.onlyOnePerReportType) {
                if (this.reportedTypes.contains(report.getType())) {
                    continue;
                }
                this.reportedTypes.add(report.getType());
            }
            this.write(report);
            reportsAdded = true;
        }

        if (maxFileReached) {
            logger.info("Maximum number of file has been reached to be written, yet reports are coming");
        }
        if (0 < reports.size() && !reportsAdded) {
            logger.info("There were reports arrived, yet none of them are written into file");
        }
    }

    FileSink withPath(String path) {
        this.path = path;
        return this;
    }

    FileSink withOverwriteExistingFile(boolean value) {
        this.overWriteExistingFile = value;
        return this;
    }

    private File getFile(Report report) {
        int tried = 0;
        do {
            String filePath = String.format("%s/" + this.digitsFormat + "-%s.dat", this.path, ++this.seqNum, report.getType().name());
            File file = new File(filePath);
            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    logger.warn("Exception occurred while creating file {}", file, e);
                    throw new RuntimeException(e);
                }
            } else if (!this.overWriteExistingFile){
                String message = String.format("File %s is already exists, and configuration prohibit to overwrite. Trying with the next sequence", file.toPath());
                continue;
            }
            return file;
        } while (++tried < this.maxRetry);
        String message = String.format("Cannot create file, because limit has been reached: " + tried);
        throw new RuntimeException(message);

    }

    private void write(Report report) {
        File file = this.getFile(report);
        OutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            logger.warn("File not found on location {}", file, e);
            return;
        }
        try {
            byte[] bytes = report.toByteBuffer().array();
            outputStream.write(bytes);
            ++this.createdFileNum;
        } catch (IOException e) {
            logger.warn("Exception occurred during serialization", e);
            return;
        } finally {
            if (Objects.isNull(outputStream)) {
                return;
            }
            try {
                outputStream.flush();
            } catch (Throwable t) {
                logger.error("Unable to flush to file {} due to exception", this.path, t);
            }
            try {
                outputStream.close();
            } catch (Throwable t) {
                logger.error("Cannot close file {} due to exception", this.path, t);
            }

        }
    }

    FileSink withOnlyOnePerReportType(boolean onlyOnePerReportType) {
        this.onlyOnePerReportType = onlyOnePerReportType;
        return this;
    }

    public FileSink withMaxFileNum(int maxFileNum) {
        this.maxFileNum = maxFileNum;
        return this;
    }
}
