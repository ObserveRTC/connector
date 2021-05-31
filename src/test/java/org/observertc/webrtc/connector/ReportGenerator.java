package org.observertc.webrtc.connector;

import org.observertc.webrtc.schemas.reports.*;

import javax.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;

@Singleton
public class ReportGenerator {
    private static final List<String> serviceNames = Arrays.asList(
            "myVideoServiceCompany",
            "anotherVideoServiceCompany",
            "andAnotherVideoServiceCompany"
    );

    private static final List<String> callNames = Arrays.asList(
            "myCall",
            "myOtherCall",
            "anotherCall"
    );

    private static String provideServiceName() {
        Random rand = new Random();
        String result = serviceNames.get(rand.nextInt(serviceNames.size()));
        return result;
    }

    public Supplier<Report> initiatedCallReportSupplier(String... callNames) {
        Random rand = new Random();

        return () -> {
            String callName = null;
            if (0 < callNames.length) {
                int index = rand.nextInt(serviceNames.size());
                callName = callNames[index];
            }
            InitiatedCall initiatedCall = InitiatedCall.newBuilder()
                    .setCallUUID(UUID.randomUUID().toString())
                    .setCallName(callName)
                    .build();
            return makeReport(ReportType.INITIATED_CALL, initiatedCall);
        };
    }

    public Supplier<Report> finishedCallReportSupplier(String... callNames) {
        Random rand = new Random();

        return () -> {
            String callName = null;
            if (0 < callNames.length) {
                int index = rand.nextInt(serviceNames.size());
                callName = callNames[index];
            }
            FinishedCall finishedCall = FinishedCall.newBuilder()
                    .setCallUUID(UUID.randomUUID().toString())
                    .setCallName(callName)
                    .build();
            return makeReport(ReportType.FINISHED_CALL, finishedCall);
        };
    }

    public Supplier<Report> joinedPeerConnectionReportSupplier(String... callNames) {
        Random rand = new Random();

        return () -> {
            String callName = null;
            if (0 < callNames.length) {
                int index = rand.nextInt(serviceNames.size());
                callName = callNames[Math.min(index, callNames.length - 1)];
            }
            JoinedPeerConnection finishedCall = JoinedPeerConnection.newBuilder()
                    .setCallUUID(UUID.randomUUID().toString())
                    .setCallName(callName)
                    .setPeerConnectionUUID(UUID.randomUUID().toString())
                    .build();
            return makeReport(ReportType.JOINED_PEER_CONNECTION, finishedCall);
        };
    }

    public Supplier<Report> extensionStatReportSupplier(String type, String payload) {
        Random rand = new Random();

        return () -> {
            ExtensionReport extensionReport = ExtensionReport.newBuilder()
                    .setCallName("callName")
                    .setPeerConnectionUUID(UUID.randomUUID().toString())
                    .setExtensionType(type)
                    .setPayload(payload)
                    .build();
            return makeReport(ReportType.EXTENSION, extensionReport);
        };
    }

    public Supplier<Report> emptyReportSupplier(ReportType reportType) {
        return () -> {
            return makeReport(reportType, new Object());
        };
    }

    public Supplier<Report> emptyReportWithTimestamp(ReportType reportType, Long timestamp) {
        return () -> {
            String serviceName = provideServiceName();
            Report result = Report.newBuilder()
                    .setServiceUUID(UUID.randomUUID().toString())
                    .setServiceName(serviceName)
                    .setType(reportType)
                    .setTimestamp(timestamp)
                    .setPayload(new Object())
                    .setVersion(1)
                    .setMarker("marker")
                    .build();
            return result;
        };
    }

    private Report makeReport(ReportType reportType, Object payload) {
        String serviceName = provideServiceName();
        Report result = Report.newBuilder()
                .setServiceUUID(UUID.randomUUID().toString())
                .setServiceName(serviceName)
                .setType(reportType)
                .setTimestamp(1234L)
                .setPayload(payload)
                .setVersion(1)
                .setMarker("marker")
                .build();
        return result;
    }
}
