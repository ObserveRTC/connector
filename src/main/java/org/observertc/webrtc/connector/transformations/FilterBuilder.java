package org.observertc.webrtc.connector.transformations;

import io.micronaut.context.annotation.Prototype;
import io.reactivex.rxjava3.functions.Predicate;
import org.observertc.webrtc.connector.configbuilders.AbstractBuilder;
import org.observertc.webrtc.connector.configbuilders.Builder;
import org.observertc.webrtc.schemas.reports.Report;
import org.observertc.webrtc.schemas.reports.ReportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Prototype
public class FilterBuilder extends AbstractBuilder implements Builder<Transformation> {

    private static final Logger logger = LoggerFactory.getLogger(FilterBuilder.class);

    @Override
    public Filter build() {
        Config config = this.convertAndValidate(Config.class);
        Filter result = new Filter();

        Config.ReportTypeConfig reportTypeConfig = config.reportType;
        Set<ReportType> includes = this.collectTypes(reportTypeConfig.including);
        Set<ReportType> excludes = this.collectTypes(reportTypeConfig.excluding);
        Predicate<Report> typeFilter = this.makeTypeFilter(includes, excludes);
        result.addPredicate(typeFilter);

        return result;
    }

    public static class Config {

        public ReportTypeConfig reportType = new ReportTypeConfig();

        public class ReportTypeConfig {
            public List<String> including = new ArrayList<>();
            public List<String> excluding = new ArrayList<>();
        }


    }

    private Set<ReportType> collectTypes(List<String> types) {
        if (Objects.isNull(types)) {
            return Collections.unmodifiableSet(new HashSet<>());
        }
        Set<ReportType> result = new HashSet<>();
        for (String type : types) {
            ReportType reportType;
            try {
                reportType = ReportType.valueOf(type);
            } catch (Exception ex) {
                logger.warn("{} has not recognized as a {}", type, ReportType.class.getSimpleName());
                continue;
            }
            result.add(reportType);
        }
        return Collections.unmodifiableSet(result);
    }

    private Predicate<Report> makeTypeFilter(Set<ReportType> includes, Set<ReportType> excludes) {
        if (includes.size() < 1 && excludes.size() < 1) {
            return reportType -> true;
        }
        Predicate<Report> checkExclusion = report -> !excludes.contains(report.getType());
        Predicate<Report> checkInclusion = report -> includes.contains(report.getType());
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


}
