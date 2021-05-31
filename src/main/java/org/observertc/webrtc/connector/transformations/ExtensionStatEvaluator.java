package org.observertc.webrtc.connector.transformations;

import org.observertc.webrtc.schemas.reports.ExtensionReport;
import org.observertc.webrtc.schemas.reports.Report;
import org.observertc.webrtc.schemas.reports.ReportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

public class ExtensionStatEvaluator extends Transformation {
    private static final Logger logger  = LoggerFactory.getLogger(ExtensionStatEvaluator.class);
    private ExtensionStatEvaluatorBuilder.Config config = null;

    ExtensionStatEvaluator withConfig(ExtensionStatEvaluatorBuilder.Config config) {
        if (Objects.nonNull(this.config)) {
            throw new RuntimeException("Extension stat evaluator has already been configured");
        }
        this.config = config;
        return this;
    }

    @Override
    protected Optional<Report> transform(Report report) throws Throwable {
        ReportType reportType = report.getType();
        if (Objects.isNull(this.config) || !this.config.enabled) {
            return Optional.of(report);
        }
        if (Objects.isNull(reportType)) {
            // do not process further
            return Optional.empty();
        }
        if (!reportType.equals(ReportType.EXTENSION)) {
            return Optional.of(report);
        }
        try {
            return this.makeExtensionReport(report);
        } catch (Exception ex) {
            logger.warn("Exception is thrown by evaluating extension stat", ex);
            return Optional.empty();
        }
    }

    private Optional<Report> makeExtensionReport(Report report) {
        ExtensionReport extensionReport = (ExtensionReport) report.getPayload();
        String extensionType = extensionReport.getExtensionType();
        String extensionPayload = extensionReport.getPayload();

        // evaluate the extension stat here.
        String evaluatedExtension = this.evaluate(extensionType, extensionPayload);

        ExtensionReport newExtensionReport = ExtensionReport.newBuilder()
                .setExtensionType(extensionType)
                .setPayload(evaluatedExtension)
                .setCallName(extensionReport.getCallName())
                .setMediaUnitId(extensionReport.getMediaUnitId())
                .setUserId(extensionReport.getUserId())
                .setBrowserId(extensionReport.getBrowserId())
                .setPeerConnectionUUID(extensionReport.getPeerConnectionUUID())
                .build()
                ;
        Report result = Report.newBuilder()
                .setPayload(newExtensionReport)
                .setVersion(report.getVersion())
                .setType(report.getType())
                .setServiceName(report.getServiceName())
                .setTimestamp(report.getTimestamp())
                .setMarker(report.getMarker())
                .setServiceUUID(report.getServiceUUID())
                .build()
                ;
        return Optional.of(result);
    }

    private String evaluate(String type, String payload) {
        // evaluate the extension string accordingly
        switch (type) {
            case "CUSTOM_TYPE_1":
                return "Result of evaluation extension payload has CUSTOM_TYPE_1";
            default:
                return "Evaluated value in string";
        }
    }
}
