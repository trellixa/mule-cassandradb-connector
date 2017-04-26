/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
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
