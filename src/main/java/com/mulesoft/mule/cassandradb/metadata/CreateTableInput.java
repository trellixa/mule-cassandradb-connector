package com.mulesoft.mule.cassandradb.metadata;

import java.util.List;

public class CreateTableInput {

    private String tableName;
    private String keyspaceName;
    List<ColumnInput> columns;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getKeyspaceName() {
        return keyspaceName;
    }

    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    public List<ColumnInput> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnInput> columns) {
        this.columns = columns;
    }
}
