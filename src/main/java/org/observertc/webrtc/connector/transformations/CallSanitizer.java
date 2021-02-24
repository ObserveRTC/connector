package org.observertc.webrtc.connector.transformations;

import org.observertc.webrtc.connector.common.ReportVisitor;
import org.observertc.webrtc.schemas.reports.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class CallSanitizer extends Transformation {
    private long counter = 0;

    private static final Logger logger = LoggerFactory.getLogger(CallSanitizer.class);
    private Set<String> initiatedCalls = new HashSet<>();
    private Map<String, Set<String>> joinedPeerConnections = new HashMap<>();
    private Map<String, Set<String>> detachedPeerConnections = new HashMap<>();
    private Map<String, Long> finishedCalls = new HashMap<>();
    private final ReportVisitor<Boolean> processor;
    private AtomicBoolean spinLock = new AtomicBoolean(false);
    private long cleanupThreshold = 5000000;
    private long cleanupPeriodInCycle = 1000000;
    private int sleepTimeInMs = 1000;

    public CallSanitizer() {
        this.processor = this.makeProcess();
    }

    CallSanitizer withSpinLockSleepTime(int sleepTimeInMs) {
        this.sleepTimeInMs = sleepTimeInMs;
        return this;
    }

    CallSanitizer withCleanupPeriodInCycle(long cleanupPeriodInCycle) {
        this.cleanupPeriodInCycle = cleanupPeriodInCycle;
        return this;
    }

    CallSanitizer withCleanupThreshold(long cleanupThreshold) {
        this.cleanupThreshold = cleanupThreshold;
        return this;
    }

    protected void cleanMaps() {
        while (!this.spinLock.compareAndSet(false, true)) {
            try {
                logger.info("SpinLock is active, need to wait");
                Thread.sleep(this.sleepTimeInMs);
            } catch (InterruptedException e) {
                logger.warn("Unexpected exceptions", e);
            }
        }
        try {
            Iterator<Map.Entry<String, Long>> it = this.finishedCalls.entrySet().iterator();
            final long threshold = Math.max(0, this.counter - this.cleanupThreshold);
            if (threshold < 1) {
                return;
            }
            int removedCalls = 0;
            int removedJoinedPCs = 0;
            int removedDetachedPCs = 0;
            while (it.hasNext()) {
                Map.Entry<String, Long> entry = it.next();
                String callUUID = entry.getKey();
                Long detected = entry.getValue();
                if (threshold < detected) {
                    continue;
                }
//                logger.info("Call is finished earlier than {} count. (detected: {}, now: {}), so we it clean from our repo", threshold, detected, this.counter);
                this.initiatedCalls.remove(callUUID);
                Set<String> joinedPcs = this.joinedPeerConnections.remove(callUUID);
                Set<String> detachedPcs = this.detachedPeerConnections.remove(callUUID);
                if (Objects.nonNull(joinedPcs)) {
                    removedJoinedPCs += joinedPcs.size();
                }
                if (Objects.nonNull(detachedPcs)) {
                    removedDetachedPCs += detachedPcs.size();
                }
                ++removedCalls;
                it.remove();
            }
            if (0 < removedCalls) {
                logger.info("Removed number of calls {}, removed number of joined PCs: {}, removed number of detached PCs: {}",
                        removedCalls, removedJoinedPCs, removedDetachedPCs);
            }
        } catch (Throwable t) {
            logger.error("Unexpected error occurred at " + this.getClass().getSimpleName(), t);
        } finally {
            this.spinLock.set(false);
        }
    }


    @Override
    protected Optional<Report> transform(Report original) throws Throwable {
        Boolean isClean = this.processor.apply(original);
        if (++this.counter % this.cleanupPeriodInCycle == 0) {
            this.cleanMaps();
        }
        if (isClean) {
            return Optional.of(original);
        } else {
            return Optional.empty();
        }
    }


    private ReportVisitor<Boolean> makeProcess() {
        return new ReportVisitor<Boolean>() {
            @Override
            public Boolean visitTrackReport(Report report, Track payload) {
                return true;
            }

            @Override
            public Boolean visitFinishedCallReport(Report report, FinishedCall payload) {
                Long detected = finishedCalls.get(payload.getCallUUID());
                if (Objects.isNull(detected)) {
                    finishedCalls.put(payload.getCallUUID(), counter);
                    return true;
                }
                logger.info("Duplicated Entry is caught for {}. CallUUID is: {}, detected: {}, the counter now is {}",
                        payload.getClass().getSimpleName(),
                        payload.getCallUUID(),
                        detected,
                        counter);
                finishedCalls.put(payload.getCallUUID(), counter);
                return false;
            }

            @Override
            public Boolean visitInitiatedCallReport(Report report, InitiatedCall payload) {
                boolean detected = initiatedCalls.contains(payload.getCallUUID());
                if (!detected) {
                    initiatedCalls.add(payload.getCallUUID());
                    return true;
                }
                logger.info("Duplicated Entry is caught for {}. CallUUID: {}  " +
                        "the counter now is {}",
                        payload.getClass().getSimpleName(),
                        payload.getCallUUID(),
                        counter);
                return false;
            }

            @Override
            public Boolean visitJoinedPeerConnectionReport(Report report, JoinedPeerConnection payload) {
                Set<String> peerConnections = joinedPeerConnections.get(payload.getCallUUID());
                if (Objects.isNull(peerConnections)) {
                    peerConnections = new HashSet<>();
                    joinedPeerConnections.put(payload.getCallUUID(), peerConnections);
                }
                boolean detected = peerConnections.contains(payload.getPeerConnectionUUID());
                if (!detected) {
                    peerConnections.add(payload.getPeerConnectionUUID());
                    return true;
                }
                logger.info("Duplicated Entry is caught for {}. CallUUID: {}, peerConnectionUUID: {} " +
                        "the counter now is {}",
                        payload.getClass().getSimpleName(),
                        payload.getCallUUID(),
                        payload.getPeerConnectionUUID(),
                        counter);
                return false;
            }

            @Override
            public Boolean visitDetachedPeerConnectionReport(Report report, DetachedPeerConnection payload) {
                Set<String> peerConnections = detachedPeerConnections.get(payload.getCallUUID());
                if (Objects.isNull(peerConnections)) {
                    peerConnections = new HashSet<>();
                    detachedPeerConnections.put(payload.getCallUUID(), peerConnections);
                }
                boolean detected = peerConnections.contains(payload.getPeerConnectionUUID());
                if (!detected) {
                    peerConnections.add(payload.getPeerConnectionUUID());
                    return true;
                }
                logger.info("Duplicated Entry is caught for {}. CallUUID: {}, peerConnectionUUID: {} " +
                        "the counter now is {}",
                        payload.getClass().getSimpleName(),
                        payload.getCallUUID(),
                        payload.getPeerConnectionUUID(),
                        counter
                );
                return false;
            }

            @Override
            public Boolean visitInboundRTPReport(Report report, InboundRTP payload) {
                return true;
            }

            @Override
            public Boolean visitOutboundRTPReport(Report report, OutboundRTP payload) {
                return true;
            }

            @Override
            public Boolean visitRemoteInboundRTPReport(Report report, RemoteInboundRTP payload) {
                return true;
            }

            @Override
            public Boolean visitMediaSourceReport(Report report, MediaSource payload) {
                return true;
            }

            @Override
            public Boolean visitObserverReport(Report report, ObserverEventReport payload) {
                return true;
            }

            @Override
            public Boolean visitUserMediaErrorReport(Report report, UserMediaError payload) {
                return true;
            }

            @Override
            public Boolean visitICECandidatePairReport(Report report, ICECandidatePair payload) {
                return true;
            }

            @Override
            public Boolean visitICELocalCandidateReport(Report report, ICELocalCandidate payload) {
                return true;
            }

            @Override
            public Boolean visitICERemoteCandidateReport(Report report, ICERemoteCandidate payload) {
                return true;
            }

            @Override
            public Boolean visitUnrecognizedReport(Report report) {
                return true;
            }

            @Override
            public Boolean visitExtensionReport(Report report, ExtensionReport payload) {
                return true;
            }

            @Override
            public Boolean visitUnknownType(Report report) {
                return true;
            }

            @Override
            public Boolean visitClientDetailsReport(Report report, ClientDetails payload) {
                return true;
            }

            @Override
            public Boolean visitMediaDeviceReport(Report report, MediaDevice payload) {
                return true;
            }
        };
    }
}
