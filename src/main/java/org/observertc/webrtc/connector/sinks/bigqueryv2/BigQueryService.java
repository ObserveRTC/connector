package org.observertc.webrtc.connector.sinks.bigqueryv2;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;

public class BigQueryService {
    private static final Logger logger = LoggerFactory.getLogger(BigQueryService.class);

    private final String projectId;
    private final String datasetId;
    private final BigQuery bigQuery;

    public BigQueryService(String projectId, String datasetId, String credentialFile) {
        this.projectId = projectId;
        this.datasetId = datasetId;
        if (Objects.isNull(credentialFile)) {
            logger.info("No crednetialfile has been set, the default (GOOGLE_APPLICATION_CREDENTIALS) will be used");
            this.bigQuery = BigQueryOptions.getDefaultInstance().getService();
        } else {
            this.bigQuery = this.getBigQuery(this.projectId, credentialFile);
        }
    }

    public BigQuery getBigQuery() {
        return this.bigQuery;
    }

    public String getProjectId() {
        return this.projectId;
    }

    public String getDatasetId() {
        return this.datasetId;
    }

    private BigQuery getBigQuery(String projectId, String filePath) {
        File credentialsPath = new File(filePath);

        // Load credentials from JSON key file. If you can't set the GOOGLE_APPLICATION_CREDENTIALS
        // environment variable, you can explicitly load the credentials file to construct the
        // credentials.
        GoogleCredentials credentials;
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (IOException ex) {
            logger.error("Exception occured during retrieving credentials", ex);
            return null;
        }

        // Instantiate a client.
        return
                BigQueryOptions.newBuilder()
                        .setCredentials(credentials)
                        .setProjectId(projectId)
                        .build()
                        .getService();
    }
}
