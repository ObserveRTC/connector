package org.observertc.webrtc.connector.databases;

import org.observertc.webrtc.connector.common.Task;
import org.observertc.webrtc.schemas.reports.ReportType;
import org.slf4j.Logger;

import java.util.Map;

public interface SchemaMapper extends Task {

    SchemaMapper withLogger(Logger logger);

    SchemaMapper createDatasetIfNotExists(boolean createDatasetIfNotExists) ;

    SchemaMapper createTableIfNotExists(boolean createTableIfNotExists);

    SchemaMapper withSchemaCheckEnabled(boolean value);

    ReportMapper getReportMapper(ReportType reportType);

    Map<ReportType, ReportMapper> getReportMappers();

}
