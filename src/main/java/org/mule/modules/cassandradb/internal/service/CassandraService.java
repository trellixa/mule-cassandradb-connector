package org.mule.modules.cassandradb.internal.service;

import com.datastax.driver.core.DataType;
import org.mule.connectors.commons.template.service.ConnectorService;
import org.mule.modules.cassandradb.api.AlterColumnInput;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.api.CreateTableInput;
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

    void update(String keySpace, String table, Map<String, Object> entity, Map<String, Object> whereClause);

    List<Map<String, Object>> executeCQLQuery(String cqlQuery, List<Object> params);

    List<Map<String, Object>> select(String query, List<Object> params);

    void delete(String keySpace, String table, List<String> entity, Map<String, Object> whereClause);

    void deleteRow(String keySpace, String table, Map<String, Object> whereClause);
}
