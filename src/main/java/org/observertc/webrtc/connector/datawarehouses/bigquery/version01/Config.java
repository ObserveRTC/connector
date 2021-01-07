package org.observertc.webrtc.connector.datawarehouses.bigquery.version01;

import com.google.cloud.bigquery.BigQuery;
import org.observertc.webrtc.connector.models.EntryType;

import java.util.Map;

public class Config {
    public final BigQuery bigQuery;
    public final String projectId;
    public final String datasetId;
    public final Map<EntryType, String> tableNames;
    public final boolean createDatasetIfNotExists;
    public final boolean createTableIfNotExists;
    public final String deleteTableIfExists;

    public Config(BigQuery bigQuery,
                  String projectId,
                  String datasetId,
                  Map<EntryType, String> tableNames,
                  boolean createDatasetIfNotExists,
                  boolean createTableIfNotExists,
                  String deleteTableIfExists) {
        this.bigQuery = bigQuery;
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.tableNames = tableNames;
        this.createDatasetIfNotExists = createDatasetIfNotExists;
        this.createTableIfNotExists = createTableIfNotExists;
        this.deleteTableIfExists = deleteTableIfExists;
    }
}
