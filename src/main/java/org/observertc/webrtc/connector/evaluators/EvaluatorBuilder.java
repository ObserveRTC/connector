package org.observertc.webrtc.connector.evaluators;

import io.reactivex.rxjava3.functions.Function;
import org.observertc.webrtc.connector.configbuilders.AbstractBuilder;
import org.observertc.webrtc.connector.models.*;
import org.observertc.webrtc.schemas.reports.Report;

public class EvaluatorBuilder extends AbstractBuilder  {

    public Evaluator build() {
        Evaluator result = new Evaluator();
        Config config = this.convertAndValidate(Config.class);


        if (config.evaluateInitiatedCalls) {
            Function<Report, InitiatedCallEntry> mapper = new InitiatedCallMapper();
            result.withInitiatedCallMapper(mapper);
        }

        if (config.evaluateFinishedCalls) {
            Function<Report, FinishedCallEntry> mapper = new FinishedCallMapper();
            result.withFinishedCallMapper(mapper);
        }

        if (config.evaluateJoinedPeerConnections) {
            Function<Report, JoinedPeerConnectionEntry> mapper = new JoinedPeerConnectionMapper();
            result.withJoinedPeerConnectionMapper(mapper);
        }

        if (config.evaluateDetachedPeerConnections) {
            Function<Report, DetachedPeerConnectionEntry> mapper = new DetachedPeerConnectionMapper();
            result.withDetachedPeerConnectionMapper(mapper);
        }

        if (config.evaluateInboundRTPs) {
            Function<Report, InboundRTPEntry> mapper = new InboundRTPMapper();
            result.withInboundRTPMapper(mapper);
        }

        if (config.evaluateRemoteInboundRTPs) {
            Function<Report, RemoteInboundRTPEntry> mapper = new RemoteInboundRTPMapper();
            result.withRemoteInboundRTPMapper(mapper);
        }

        if (config.evaluateICECandidatePairs) {
            Function<Report, ICECandidatePairEntry> mapper = new ICECandidatePairMapper();
            result.withICECandidatePairMapper(mapper);
        }
        if (config.evaluateICELocalCandidates) {
            Function<Report, ICELocalCandidateEntry> mapper = new ICELocalCandidateMapper();
            result.withICELocalCandidateMapper(mapper);
        }
        if (config.evaluateICERemoteCandidates) {
            Function<Report, ICERemoteCandidateEntry> mapper = new ICERemoteCandidateMapper();
            result.withICERemoteCandidateMapper(mapper);
        }

        if (config.evaluateOutboundRTPs) {
            Function<Report, OutboundRTPEntry> mapper = new OutboundRTPMapper();
            result.withOutboundRTPMapper(mapper);
        }

        if (config.evaluateUserMediaError) {
            Function<Report, UserMediaErrorEntry> mapper = new UserMediaErrorMapper();
            result.withUserMediaError(mapper);
        }

        if (config.evaluateMediaSources) {
            Function<Report, MediaSourceEntry> mapper = new MediaSourceMapper();
            result.withMediaSource(mapper);
        }

        if (config.evaluateTracks) {
            Function<Report, TrackEntry> mapper = new TrackMapper();
            result.withTrackMapper(mapper);
        }

        if (config.evaluateObserverEvents) {
            Function<Report, ObserverEventEntry> mapper = new ObserverEventMapper();
            result.withObserverEventMapper(mapper);
        }

        return result
                .withBufferThresholdNum(config.bufferThresholdNum)
                .withBufferThresholdInS(config.bufferThresholdInS);
    }

    public static class Config {
        public int bufferThresholdNum = 10000;
        public int bufferThresholdInS = 30;
        public boolean evaluateInitiatedCalls = true;
        public boolean evaluateFinishedCalls = true;
        public boolean evaluateJoinedPeerConnections = true;
        public boolean evaluateDetachedPeerConnections = true;
        public boolean evaluateInboundRTPs = true;
        public boolean evaluateRemoteInboundRTPs = true;
        public boolean evaluateOutboundRTPs = true;
        public boolean evaluateICELocalCandidates = true;
        public boolean evaluateICERemoteCandidates = true;
        public boolean evaluateICECandidatePairs = true;
        public boolean evaluateUserMediaError = true;
        public boolean evaluateMediaSources = true;
        public boolean evaluateTracks = true;
        public boolean evaluateObserverEvents = true;
    }
}
