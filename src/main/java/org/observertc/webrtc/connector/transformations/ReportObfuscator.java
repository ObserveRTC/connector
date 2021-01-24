package org.observertc.webrtc.connector.transformations;

import org.observertc.webrtc.connector.common.ReportVisitor;
import org.observertc.webrtc.schemas.reports.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;

public class ReportObfuscator extends Transformation {

    private static final Logger logger = LoggerFactory.getLogger(ReportObfuscator.class);
    private static final String ABC = "qwertyuiopasdfghjkxcvbnm1234567890QWERTYUIOPASDFGHJKLZXCVBNM.";

    private final MessageDigest messageDigest;
    private Function<String, String> serviceNameObfuscator;
    private Function<String, String> markerObfuscator;
    private final ReportVisitor<Object> payloadObfuscator;

    public ReportObfuscator(MessageDigest messageDigest) {
        this.messageDigest = messageDigest;
        this.serviceNameObfuscator = this::digestAZ;
        this.markerObfuscator = this::digest;
        this.payloadObfuscator = this.makePayloadObfuscator();
    }

    ReportObfuscator withServiceName(String newServiceName) {
        this.serviceNameObfuscator = in -> newServiceName;
        return this;
    }

    ReportObfuscator withMarker(String newMarkerName) {
        this.markerObfuscator = in -> newMarkerName;
        return this;
    }

    @Override
    protected Optional<Report> transform(Report original) throws Throwable {
        Report.Builder builder = Report.newBuilder();
        builder.setVersion(original.getVersion());
        builder.setTimestamp(original.getTimestamp());
        builder.setType(original.getType());
        this.baseChange(builder, original);
        Object payload = this.payloadObfuscator.apply(original);
        if (Objects.isNull(payload)) {
            return Optional.empty();
        }
        builder.setPayload(payload);
        return Optional.of(builder.build());
    }

    private void baseChange(Report.Builder builder, final Report original) {
        String newServiceUUID = obfuscateUUIDSource(original.getServiceUUID());
        builder.setServiceUUID(newServiceUUID);
        String newServiceName = this.serviceNameObfuscator.apply(original.getServiceName());
        builder.setServiceName(newServiceName);
        String newMarker = this.markerObfuscator.apply(original.getMarker());
        builder.setMarker(newMarker);
    }

    private String digest(String source) {
        if (Objects.isNull(source)) {
            return null;
        }
        messageDigest.reset();
        messageDigest.update(source.getBytes(StandardCharsets.UTF_8));
        return new String(messageDigest.digest());
    }


    private String digestAZ(String source) {
        if (Objects.isNull(source)) {
            return null;
        }
        messageDigest.reset();
        messageDigest.update(source.getBytes(StandardCharsets.UTF_8));
        StringBuilder result = new StringBuilder();
        byte[] bytes = messageDigest.digest();
        final int MOD = ABC.length() - 1;
        for (int i = 0; i < bytes.length; ++i) {
            int j = Math.abs(bytes[i]) % MOD;
            result.append(ABC.substring(j, j+1));
        }
        return result.toString();
    }

    private String obfuscateUUIDSource(String source) {
        if (Objects.isNull(source)) {
            return null;
        }
        String digestedStr = this.digest(source);
        UUID result = UUID.nameUUIDFromBytes(digestedStr.getBytes());
        return result.toString();
    }

    private ReportVisitor<Object> makePayloadObfuscator() {
        return new ReportVisitor<Object>() {
            @Override
            public Object visitTrackReport(Report report, Track payload) {
                String callName = payload.getCallName();
                if (Objects.nonNull(callName)) {
                    callName = digestAZ(callName);
                }
                String userId = payload.getUserId();
                if (Objects.nonNull(userId)) {
                    userId = digestAZ(userId);
                }
                String peerConnectionUUID = payload.getPeerConnectionUUID();
                if (Objects.nonNull(peerConnectionUUID)) {
                    peerConnectionUUID = obfuscateUUIDSource(peerConnectionUUID);
                }
                Track newPayload = Track.newBuilder(payload)
                        .setCallName(callName)
                        .setUserId(userId)
                        .setPeerConnectionUUID(peerConnectionUUID)
                        .build();
                return newPayload;
            }

            @Override
            public Object visitFinishedCallReport(Report report, FinishedCall payload) {
                String callName = payload.getCallName();
                if (Objects.nonNull(callName)) {
                    callName = digestAZ(callName);
                }
                FinishedCall newPayload = FinishedCall.newBuilder(payload)
                        .setCallName(callName)
                        .build();
                return newPayload;
            }

            @Override
            public Object visitInitiatedCallReport(Report report, InitiatedCall payload) {
                String callName = payload.getCallName();
                if (Objects.nonNull(callName)) {
                    callName = digestAZ(callName);
                }
                InitiatedCall newPayload = InitiatedCall.newBuilder(payload)
                        .setCallName(callName)
                        .build();
                return newPayload;
            }

            @Override
            public Object visitJoinedPeerConnectionReport(Report report, JoinedPeerConnection payload) {
                String callName = payload.getCallName();
                if (Objects.nonNull(callName)) {
                    callName = digestAZ(callName);
                }
                String userId = payload.getUserId();
                if (Objects.nonNull(userId)) {
                    userId = digestAZ(userId);
                }
                String peerConnectionUUID = payload.getPeerConnectionUUID();
                if (Objects.nonNull(peerConnectionUUID)) {
                    peerConnectionUUID = obfuscateUUIDSource(peerConnectionUUID);
                }
                JoinedPeerConnection newPayload = JoinedPeerConnection.newBuilder(payload)
                        .setCallName(callName)
                        .setPeerConnectionUUID(peerConnectionUUID)
                        .setUserId(userId)
                        .setPeerConnectionUUID(peerConnectionUUID)
                        .build();
                return newPayload;
            }

            @Override
            public Object visitDetachedPeerConnectionReport(Report report, DetachedPeerConnection payload) {
                String callName = payload.getCallName();
                if (Objects.nonNull(callName)) {
                    callName = digestAZ(callName);
                }
                String userId = payload.getUserId();
                if (Objects.nonNull(userId)) {
                    userId = digestAZ(userId);
                }
                String peerConnectionUUID = payload.getPeerConnectionUUID();
                if (Objects.nonNull(peerConnectionUUID)) {
                    peerConnectionUUID = obfuscateUUIDSource(peerConnectionUUID);
                }
                DetachedPeerConnection newPayload = DetachedPeerConnection.newBuilder(payload)
                        .setCallName(callName)
                        .setPeerConnectionUUID(peerConnectionUUID)
                        .setUserId(userId)
                        .build();
                return newPayload;
            }

            @Override
            public Object visitInboundRTPReport(Report report, InboundRTP payload) {
                String callName = payload.getCallName();
                if (Objects.nonNull(callName)) {
                    callName = digestAZ(callName);
                }
                String userId = payload.getUserId();
                if (Objects.nonNull(userId)) {
                    userId = digestAZ(userId);
                }
                String peerConnectionUUID = payload.getPeerConnectionUUID();
                if (Objects.nonNull(peerConnectionUUID)) {
                    peerConnectionUUID = obfuscateUUIDSource(peerConnectionUUID);
                }
                InboundRTP newPayload = InboundRTP.newBuilder(payload)
                        .setCallName(callName)
                        .setPeerConnectionUUID(peerConnectionUUID)
                        .setUserId(userId)
                        .build();
                return newPayload;
            }

            @Override
            public Object visitOutboundRTPReport(Report report, OutboundRTP payload) {
                String callName = payload.getCallName();
                if (Objects.nonNull(callName)) {
                    callName = digestAZ(callName);
                }
                String userId = payload.getUserId();
                if (Objects.nonNull(userId)) {
                    userId = digestAZ(userId);
                }
                String peerConnectionUUID = payload.getPeerConnectionUUID();
                if (Objects.nonNull(peerConnectionUUID)) {
                    peerConnectionUUID = obfuscateUUIDSource(peerConnectionUUID);
                }
                OutboundRTP newPayload = OutboundRTP.newBuilder(payload)
                        .setCallName(callName)
                        .setPeerConnectionUUID(peerConnectionUUID)
                        .setUserId(userId)
                        .build();
                return newPayload;
            }

            @Override
            public Object visitRemoteInboundRTPReport(Report report, RemoteInboundRTP payload) {
                String callName = payload.getCallName();
                if (Objects.nonNull(callName)) {
                    callName = digestAZ(callName);
                }
                String userId = payload.getUserId();
                if (Objects.nonNull(userId)) {
                    userId = digestAZ(userId);
                }
                String peerConnectionUUID = payload.getPeerConnectionUUID();
                if (Objects.nonNull(peerConnectionUUID)) {
                    peerConnectionUUID = obfuscateUUIDSource(peerConnectionUUID);
                }
                RemoteInboundRTP newPayload = RemoteInboundRTP.newBuilder(payload)
                        .setCallName(callName)
                        .setPeerConnectionUUID(peerConnectionUUID)
                        .setUserId(userId)
                        .build();
                return newPayload;
            }

            @Override
            public Object visitMediaSourceReport(Report report, MediaSource payload) {
                String callName = payload.getCallName();
                if (Objects.nonNull(callName)) {
                    callName = digestAZ(callName);
                }
                String userId = payload.getUserId();
                if (Objects.nonNull(userId)) {
                    userId = digestAZ(userId);
                }
                String peerConnectionUUID = payload.getPeerConnectionUUID();
                if (Objects.nonNull(peerConnectionUUID)) {
                    peerConnectionUUID = obfuscateUUIDSource(peerConnectionUUID);
                }
                MediaSource newPayload = MediaSource.newBuilder(payload)
                        .setCallName(callName)
                        .setPeerConnectionUUID(peerConnectionUUID)
                        .setUserId(userId)
                        .build();
                return newPayload;
            }

            @Override
            public Object visitObserverReport(Report report, ObserverEventReport payload) {
                String callName = payload.getCallName();
                if (Objects.nonNull(callName)) {
                    callName = digestAZ(callName);
                }
                String userId = payload.getUserId();
                if (Objects.nonNull(userId)) {
                    userId = digestAZ(userId);
                }
                String peerConnectionUUID = payload.getPeerConnectionUUID();
                if (Objects.nonNull(peerConnectionUUID)) {
                    peerConnectionUUID = obfuscateUUIDSource(peerConnectionUUID);
                }
                ObserverEventReport newPayload = ObserverEventReport.newBuilder(payload)
                        .setCallName(callName)
                        .setPeerConnectionUUID(peerConnectionUUID)
                        .setUserId(userId)
                        .build();
                return newPayload;
            }

            @Override
            public Object visitUserMediaErrorReport(Report report, UserMediaError payload) {
                String callName = payload.getCallName();
                if (Objects.nonNull(callName)) {
                    callName = digestAZ(callName);
                }
                String userId = payload.getUserId();
                if (Objects.nonNull(userId)) {
                    userId = digestAZ(userId);
                }
                String peerConnectionUUID = payload.getPeerConnectionUUID();
                if (Objects.nonNull(peerConnectionUUID)) {
                    peerConnectionUUID = obfuscateUUIDSource(peerConnectionUUID);
                }
                UserMediaError newPayload = UserMediaError.newBuilder(payload)
                        .setCallName(callName)
                        .setPeerConnectionUUID(peerConnectionUUID)
                        .setUserId(userId)
                        .build();
                return newPayload;
            }

            @Override
            public Object visitICECandidatePairReport(Report report, ICECandidatePair payload) {
                String callName = payload.getCallName();
                if (Objects.nonNull(callName)) {
                    callName = digestAZ(callName);
                }
                String userId = payload.getUserId();
                if (Objects.nonNull(userId)) {
                    userId = digestAZ(userId);
                }
                String peerConnectionUUID = payload.getPeerConnectionUUID();
                if (Objects.nonNull(peerConnectionUUID)) {
                    peerConnectionUUID = obfuscateUUIDSource(peerConnectionUUID);
                }
                ICECandidatePair newPayload = ICECandidatePair.newBuilder(payload)
                        .setCallName(callName)
                        .setPeerConnectionUUID(peerConnectionUUID)
                        .setUserId(userId)
                        .build();
                return newPayload;
            }

            @Override
            public Object visitICELocalCandidateReport(Report report, ICELocalCandidate payload) {
                String callName = payload.getCallName();
                if (Objects.nonNull(callName)) {
                    callName = digestAZ(callName);
                }
                String userId = payload.getUserId();
                if (Objects.nonNull(userId)) {
                    userId = digestAZ(userId);
                }
                String peerConnectionUUID = payload.getPeerConnectionUUID();
                if (Objects.nonNull(peerConnectionUUID)) {
                    peerConnectionUUID = obfuscateUUIDSource(peerConnectionUUID);
                }
                Integer port = payload.getPort();
                if (Objects.nonNull(port)) {
                    try {
                        port = 31 * port % ABC.length();
                    } catch (NumberFormatException e) {
                        port = new Random().nextInt();
                    }
                }
                String ipLSH = payload.getIpLSH();
                if (Objects.nonNull(ipLSH)) {
                    ipLSH = digestAZ(ipLSH);
                }
                ICELocalCandidate newPayload = ICELocalCandidate.newBuilder(payload)
                        .setCallName(callName)
                        .setPeerConnectionUUID(peerConnectionUUID)
                        .setUserId(userId)
                        .setIpLSH(ipLSH)
                        .setPort(port)
                        .build();
                return newPayload;
            }

            @Override
            public Object visitICERemoteCandidateReport(Report report, ICERemoteCandidate payload) {
                String callName = payload.getCallName();
                if (Objects.nonNull(callName)) {
                    callName = digestAZ(callName);
                }
                String userId = payload.getUserId();
                if (Objects.nonNull(userId)) {
                    userId = digestAZ(userId);
                }
                String peerConnectionUUID = payload.getPeerConnectionUUID();
                if (Objects.nonNull(peerConnectionUUID)) {
                    peerConnectionUUID = obfuscateUUIDSource(peerConnectionUUID);
                }
                Integer port = payload.getPort();
                if (Objects.nonNull(port)) {
                    try {
                        port = 31 * port % ABC.length();
                    } catch (NumberFormatException e) {
                        port = new Random().nextInt();
                    }
                }
                String ipLSH = payload.getIpLSH();
                if (Objects.nonNull(ipLSH)) {
                    ipLSH = digestAZ(ipLSH);
                }
                ICERemoteCandidate newPayload = ICERemoteCandidate.newBuilder(payload)
                        .setCallName(callName)
                        .setPeerConnectionUUID(peerConnectionUUID)
                        .setUserId(userId)
                        .setPort(port)
                        .setIpLSH(ipLSH)
                        .build();
                return newPayload;
            }

            @Override
            public Object visitUnrecognizedReport(Report report) {
                return report.getPayload();
            }

            @Override
            public Object visitExtensionReport(Report report, ExtensionReport payload) {
                String callName = payload.getCallName();
                if (Objects.nonNull(callName)) {
                    callName = digestAZ(callName);
                }
                String userId = payload.getUserId();
                if (Objects.nonNull(userId)) {
                    userId = digestAZ(userId);
                }
                String peerConnectionUUID = payload.getPeerConnectionUUID();
                if (Objects.nonNull(peerConnectionUUID)) {
                    peerConnectionUUID = obfuscateUUIDSource(peerConnectionUUID);
                }
                ExtensionReport newPayload = ExtensionReport.newBuilder(payload)
                        .setCallName(callName)
                        .setUserId(userId)
                        .setPeerConnectionUUID(peerConnectionUUID)
                        .build();
                return newPayload;
            }

            @Override
            public Object visitUnknownType(Report report) {
                return report.getPayload();
            }
        };
    }
}
