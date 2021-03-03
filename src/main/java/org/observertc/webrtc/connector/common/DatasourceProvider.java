package org.observertc.webrtc.connector.common;

import io.micronaut.inject.qualifiers.Qualifiers;
import org.observertc.webrtc.connector.Application;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Singleton;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

@Singleton
public class DatasourceProvider implements Function<String, DataSource> {

    private static final Logger logger = LoggerFactory.getLogger(DatasourceProvider.class);

    private final Map<String, DataSource> dataSources = new HashMap<>();

    DataSource defaultDataSource;

    @PostConstruct
    void setup() {
        try {
            this.defaultDataSource = Application.context.getBean(DataSource.class, Qualifiers.byName("default"));
        } catch (Throwable t) {
            logger.warn("No default datasource provider is defined.");
            this.defaultDataSource = null;
        }
    }

    @Override
    public DataSource apply(String datasourceName) {
        DataSource dataSource = dataSources.get(datasourceName);
        if (Objects.isNull(dataSource)) {
            dataSource = Application.context.getBean(DataSource.class, Qualifiers.byName(datasourceName));
            if (Objects.isNull(dataSource)) {
                logger.warn("No Datasource has found for name {}, the default will be used. ", datasourceName);
                dataSource = this.defaultDataSource;
            } else {
                logger.info("Datasource {} will be registered in DSLContextProvider.", datasourceName);
            }
            if (Objects.isNull(dataSource)) {
                logger.warn("No datasource was found for the provided name {} ", datasourceName);
                return null;
            }
            this.dataSources.put(datasourceName, dataSource);
        }
        return dataSource;
    }
}
