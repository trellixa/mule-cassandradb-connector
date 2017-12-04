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

    boolean createKeyspace(@Content CreateKeyspaceInput input);

    boolean dropKeyspace(@Content String keyspaceName);

    boolean createTable(@Content CreateTableInput input);

    boolean dropTable(String tableName, String keyspaceName);

    boolean addNewColumn(String tableName, String keyspaceName, String column, DataType dataType);

    boolean dropColumn(String tableName, String keyspaceName, String column);

    boolean renameColumn(String tableName, String keyspaceName, String oldColumnName, String newColumnName);

    boolean changeColumnType(String tableName, String keyspaceName, AlterColumnInput alterColumnInput);

    List<String> getTableNamesFromKeyspace(String keyspaceName);

    void insert(String keyspaceName, String table, Map<String, Object> entity);
}
