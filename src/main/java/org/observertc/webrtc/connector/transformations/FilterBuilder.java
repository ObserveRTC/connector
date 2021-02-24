package org.observertc.webrtc.connector.transformations;

import io.micronaut.context.annotation.Prototype;
import io.reactivex.rxjava3.functions.Predicate;
import org.observertc.webrtc.connector.common.ReportVisitor;
import org.observertc.webrtc.connector.configbuilders.AbstractBuilder;
import org.observertc.webrtc.connector.configbuilders.Builder;
import org.observertc.webrtc.schemas.reports.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Prototype
public class FilterBuilder extends AbstractBuilder implements Builder<Transformation> {
    private static final Pattern TIME_REGEX_PATTERN = Pattern.compile("^from (\\d{4}-\\d{2}-\\d{2}) until (\\d{4}-\\d{2}-\\d{2})$");
    private static final Logger logger = LoggerFactory.getLogger(FilterBuilder.class);

    @Override
    public Filter build() {
        Config config = this.convertAndValidate(Config.class);
        Filter result = new Filter();

        Arrays.asList(
                this.makeAllowanceFilter(Report::getType, str -> ReportType.valueOf(str), config.reportType),
                this.makeAllowanceFilter(Report::getServiceName, Function.identity(), config.serviceName),
                this.makeAllowanceFilter(Report::getServiceUUID, Function.identity(), config.serviceUUIDs),
                this.makeAllowanceFilter(Report::getMarker, Function.identity(), config.marker),
                this.makeTimeRangeAllowanceFilter(Report::getTimestamp, config.timestamps)
        ).forEach(result::addPredicate);

        if (!config.nullableCallNames) {
            ReportVisitor<Boolean> reportVisitor = this.makeNonNullCallNameFilter();
            result.addPredicate(reportVisitor::apply);
        }
        return result;
    }

    private Predicate<Report> makeTimeRangeAllowanceFilter(Function<Report, Long> extractor, Config.AllowanceConfig config) {
        Function<List<String>,  java.util.function.Predicate<Long>> setup = subjects -> {
            java.util.function.Predicate result = null;
            for (String subject : subjects) {
                Matcher mather = TIME_REGEX_PATTERN.matcher(subject);
                if (!mather.matches() || mather.groupCount() < 2) {
                    logger.warn("Cannot interpret time range interval {}", subject);
                    continue;
                }
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                long fromEpoch, untilEpoch;
                try {
                    String from = mather.group(1);
                    String until = mather.group(2);
                    fromEpoch = df.parse(from).toInstant().toEpochMilli();
                    untilEpoch = df.parse(until).toInstant().toEpochMilli();
                } catch (Exception ex) {
                    logger.warn("Cannot interpret time range interval {}", subject, ex);
                    continue;
                }
                java.util.function.Predicate<Long> predicate = timestamp -> {
//                        logger.info("Report with timestamp {} will be{}allowed",
//                                Instant.ofEpochMilli(timestamp).toString(),
//                                (fromEpoch <= timestamp && timestamp <= untilEpoch) ? " " : " not "
//                                );
                        return fromEpoch <= timestamp && timestamp <= untilEpoch;
                };
                if (Objects.isNull(result)) {
                    result = predicate;
                } else {
                    result = result.or(predicate);
                }
            }
            return result;
        };
        java.util.function.Predicate<Long> includes = setup.apply(config.including);
        java.util.function.Predicate<Long> excludes = setup.apply(config.excluding);
        if (Objects.isNull(includes) && Objects.isNull(excludes)) {
            return report -> true;
        }

        if (Objects.isNull(includes)) {
            return this.makeAllowanceFilter(extractor, excludes::test, null);
        }
        if (Objects.isNull(excludes)) {
            return this.makeAllowanceFilter(extractor, null,  includes::test);
        }
        return this.makeAllowanceFilter(extractor, includes::test, excludes::test);
    }

    private<T> Predicate<Report> makeAllowanceFilter(Function<Report, T> extractor, Function<String, T> converter, Config.AllowanceConfig config) {
        Set<T> includes = this.collect(converter, config.including);
        Set<T> excludes = this.collect(converter, config.excluding);
        if (includes.size() < 1 && excludes.size() < 1) {
            return report -> true;
        }
        if (includes.size() < 1){
            return this.makeAllowanceFilter(extractor, excludes::contains, null);
        }

        if (excludes.size() < 1) {
            return this.makeAllowanceFilter(extractor, null, includes::contains);
        }
        return this.makeAllowanceFilter(extractor, excludes::contains, includes::contains);
    }

    private<T> Predicate<Report> makeAllowanceFilter(Function<Report, T> extractor, Function<T, Boolean> excMatcher, Function<T, Boolean> incMatcher) {
        if (Objects.isNull(excMatcher) && Objects.isNull(incMatcher)){
            return report -> true;
        }
        if (Objects.isNull(excMatcher)){
            return report -> {
                T value = extractor.apply(report);
                return Objects.nonNull(value) && incMatcher.apply(value);
            };
        }
        if (Objects.isNull(incMatcher)){
            return report -> {
                T value = extractor.apply(report);
                return Objects.nonNull(value) && !excMatcher.apply(value);
            };
        }
        return report -> {
            T value = extractor.apply(report);
            return Objects.nonNull(value) && incMatcher.apply(value) && !excMatcher.apply(value);
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
            public Boolean visitClientDetailsReport(Report report, ClientDetails payload) {
                return Objects.nonNull(payload.getCallName());
            }

            @Override
            public Boolean visitMediaDeviceReport(Report report, MediaDevice payload) {
                return Objects.nonNull(payload.getCallName());
            }

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
        public AllowanceConfig timestamps = new AllowanceConfig();

        public static class AllowanceConfig {
            public List<String> including = new ArrayList<>();
            public List<String> excluding = new ArrayList<>();
        }

    }
}
