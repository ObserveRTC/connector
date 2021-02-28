package org.observertc.webrtc.connector.common;

import io.micronaut.inject.qualifiers.Qualifiers;
import io.reactivex.rxjava3.functions.BiFunction;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.observertc.webrtc.connector.Application;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Singleton
public class DSLContextProvider implements BiFunction<String, SQLDialect, DSLContext> {

    private static final Logger logger = LoggerFactory.getLogger(DSLContextProvider.class);

    private final Map<String, DataSource> dataSources = new HashMap<>();

    @Inject
    DataSource defaultDataSource;

    @Override
    public DSLContext apply(String datasourceName, SQLDialect dialect) {
        DataSource dataSource = dataSources.get(datasourceName);
        if (Objects.isNull(dataSource)) {
            dataSource = Application.context.getBean(DataSource.class, Qualifiers.byName(datasourceName));
            if (Objects.isNull(dataSource)) {
                logger.warn("No Datasource has found for name {}, the default will be used. " +
                        "If the default will points to database, which SQL dialect is not {}, then you have a problem.", datasourceName, dialect);
                dataSource = this.defaultDataSource;
            } else {
                logger.info("Datasource {} will be registered in DSLContextProvider.", datasourceName);
            }
            this.dataSources.put(datasourceName, dataSource);
        }
        return DSL.using(dataSource, dialect);
    }
}
