package org.observertc.webrtc.connector.databases.jdbc;

import javax.validation.constraints.NotNull;

public class TableInfoConfig {

    public static TableInfoConfig of(String tableName) {
        TableInfoConfig result = new TableInfoConfig();
        result.tableName = tableName;
        return result;
    }

    @NotNull
    public String tableName;

    public String autoIncrementPrimaryKeyName = "recordid";

    public String[] primaryKeyColumns = null;

}
