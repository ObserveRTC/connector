package org.observertc.webrtc.connector.transformations;

import io.micronaut.context.annotation.Prototype;
import io.reactivex.rxjava3.functions.Predicate;
import org.observertc.webrtc.connector.common.ReportVisitor;
import org.observertc.webrtc.connector.configbuilders.AbstractBuilder;
import org.observertc.webrtc.connector.configbuilders.Builder;
import org.observertc.webrtc.schemas.reports.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

@Prototype
public class FilterBuilder extends AbstractBuilder implements Builder<Transformation> {

    private static final Logger logger = LoggerFactory.getLogger(FilterBuilder.class);

    @Override
    public Filter build() {
        Config config = this.convertAndValidate(Config.class);
        Filter result = new Filter();

        Arrays.asList(
                this.makeAllowanceFilter(Report::getType, str -> ReportType.valueOf(str), config.reportType),
                this.makeAllowanceFilter(Report::getServiceName, Function.identity(), config.serviceName),
                this.makeAllowanceFilter(Report::getServiceUUID, Function.identity(), config.serviceUUIDs),
                this.makeAllowanceFilter(Report::getMarker, Function.identity(), config.marker)
        ).forEach(result::addPredicate);

        if (!config.nullableCallNames) {
            ReportVisitor<Boolean> reportVisitor = this.makeNonNullCallNameFilter();
            result.addPredicate(reportVisitor::apply);
        }
        return result;
    }

    private<T> Predicate<Report> makeAllowanceFilter(Function<Report, T> extractor, Function<String, T> converter, Config.AllowanceConfig config) {
        Set<T> includes = this.collect(converter, config.including);
        Set<T> excludes = this.collect(converter, config.excluding);
        if (includes.size() < 1 && excludes.size() < 1) {
            return reportType -> true;
        }
        Predicate<Report> checkExclusion = report -> !excludes.contains(extractor.apply(report));
        Predicate<Report> checkInclusion = report -> includes.contains(extractor.apply(report));
        if (includes.size() < 1){
            return checkExclusion;
        }

        if (excludes.size() < 1) {
            return checkInclusion;
        }
        return report -> {
            return checkInclusion.test(report) && checkExclusion.test(report);
        };
    }

    private<T> Set<T> collect(Function<String, T> converter, List<String> original) {
        if (Objects.isNull(original)) {
            return Collections.unmodifiableSet(new HashSet<>());
        }
        Set<T> result = new HashSet<>();
        for (String source : original) {
            T type;
            try {
                type = converter.apply(source);
            } catch (Exception ex) {
                logger.warn("converted value for " + source +" throws an exception. it will not be added to the allowancefilter", ex);
                continue;
            }
            result.add(type);
        }
        return Collections.unmodifiableSet(result);
    }

    private ReportVisitor<Boolean> makeNonNullCallNameFilter() {

        return new ReportVisitor<Boolean>() {
            @Override
            public Boolean visitTrackReport(Report report, Track payload) {
                return Objects.nonNull(payload.getCallName());
            }

            @Override
            public Boolean visitFinishedCallReport(Report report, FinishedCall payload) {
                return Objects.nonNull(payload.getCallName());
            }

            @Override
            public Boolean visitInitiatedCallReport(Report report, InitiatedCall payload) {
                return Objects.nonNull(payload.getCallName());
            }

            @Override
            public Boolean visitJoinedPeerConnectionReport(Report report, JoinedPeerConnection payload) {
                return Objects.nonNull(payload.getCallName());
            }

            @Override
            public Boolean visitDetachedPeerConnectionReport(Report report, DetachedPeerConnection payload) {
                return Objects.nonNull(payload.getCallName());
            }

            @Override
            public Boolean visitInboundRTPReport(Report report, InboundRTP payload) {
                return Objects.nonNull(payload.getCallName());
            }

            @Override
            public Boolean visitOutboundRTPReport(Report report, OutboundRTP payload) {
                return Objects.nonNull(payload.getCallName());
            }

            @Override
            public Boolean visitRemoteInboundRTPReport(Report report, RemoteInboundRTP payload) {
                return Objects.nonNull(payload.getCallName());
            }

            @Override
            public Boolean visitMediaSourceReport(Report report, MediaSource payload) {
                return Objects.nonNull(payload.getCallName());
            }

            @Override
            public Boolean visitObserverReport(Report report, ObserverEventReport payload) {
                return Objects.nonNull(payload.getCallName());
            }

            @Override
            public Boolean visitUserMediaErrorReport(Report report, UserMediaError payload) {
                return Objects.nonNull(payload.getCallName());
            }

            @Override
            public Boolean visitICECandidatePairReport(Report report, ICECandidatePair payload) {
                return Objects.nonNull(payload.getCallName());
            }

            @Override
            public Boolean visitICELocalCandidateReport(Report report, ICELocalCandidate payload) {
                return Objects.nonNull(payload.getCallName());
            }

            @Override
            public Boolean visitICERemoteCandidateReport(Report report, ICERemoteCandidate payload) {
                return Objects.nonNull(payload.getCallName());
            }

            @Override
            public Boolean visitUnrecognizedReport(Report report) {
                return true;
            }

            @Override
            public Boolean visitExtensionReport(Report report, ExtensionReport payload) {
                return Objects.nonNull(payload.getCallName());
            }

            @Override
            public Boolean visitUnknownType(Report report) {
                return true;
            }
        };
    }


    public static class Config {

        public boolean nullableCallNames = true;
        public AllowanceConfig reportType = new AllowanceConfig();
        public AllowanceConfig marker = new AllowanceConfig();
        public AllowanceConfig serviceName = new AllowanceConfig();
        public AllowanceConfig serviceUUIDs = new AllowanceConfig();

        public class AllowanceConfig {
            public List<String> including = new ArrayList<>();
            public List<String> excluding = new ArrayList<>();
        }

    }
}
