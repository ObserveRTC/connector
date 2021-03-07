package org.observertc.webrtc.connector.sinks.jdbc;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.util.Properties;

class HikariDataSourceBuilder {
    private Properties properties = new Properties();

    public HikariDataSourceBuilder withURL(String value) {
        this.properties.put("jdbcUrl", value);
        return this;
    }

    public HikariDataSourceBuilder withUsername(String value) {
        this.properties.put("username", value);
        return this;
    }

    public HikariDataSourceBuilder withPassword(String value) {
        this.properties.put("password", value);
        return this;
    }

    public HikariDataSourceBuilder withDriverClassName(String value) {
        this.properties.put("driverClassName", value);
        return this;
    }

    public HikariDataSource build() {
        var config = new HikariConfig(this.properties);
        return new HikariDataSource(config);
    }

}
