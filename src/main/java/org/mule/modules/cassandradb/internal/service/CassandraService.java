/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.internal.service;

import com.datastax.driver.core.DataType;
import org.mule.modules.cassandradb.api.AlterColumnInput;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.connectors.commons.template.service.ConnectorService;
import org.mule.runtime.extension.api.annotation.param.Content;

import java.util.List;
import java.util.Map;

public interface CassandraService extends ConnectorService {

    void createKeyspace(@Content CreateKeyspaceInput input);

    void dropKeyspace(@Content String keyspaceName);

    void createTable(@Content CreateTableInput input);

    void dropTable(String tableName, String keyspaceName);

    void addNewColumn(String tableName, String keyspaceName, String column, DataType dataType);

    void dropColumn(String tableName, String keyspaceName, String column);

    void renameColumn(String tableName, String keyspaceName, String oldColumnName, String newColumnName);

    void changeColumnType(String tableName, String keyspaceName, AlterColumnInput alterColumnInput);

    List<String> getTableNamesFromKeyspace(String keyspaceName);

    void insert(String keyspaceName, String table, Map<String, Object> entity);

    void update(String keySpace, String table, Map<String, Object> entityToUpdate);

    List<Map<String, Object>> executeCQLQuery(String cqlQuery, List<Object> params);

    List<Map<String, Object>> select(String query, List<Object> params);

    public void deleteWithoutEntity(String keySpace, String table, Map<String, Object> whereClause);

    void delete(String keySpace, String table, List<String> entity, Map<String, Object> whereClause);

    void deleteRow(String keySpace, String table, Map<String, Object> whereClause);
}
