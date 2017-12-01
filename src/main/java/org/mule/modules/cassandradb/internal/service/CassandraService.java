package org.mule.modules.cassandradb.internal.service;

import com.datastax.driver.core.DataType;
import org.mule.connectors.commons.template.service.ConnectorService;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.runtime.extension.api.annotation.param.Content;

public interface CassandraService extends ConnectorService {

    boolean createKeyspace(@Content CreateKeyspaceInput input);

    boolean dropKeyspace(@Content String keyspaceName);

    boolean createTable(@Content CreateTableInput input);

    boolean dropTable(String tableName, String keyspaceName);

    boolean addNewColumn(String tableName, String keyspaceName, String column, DataType dataType);

    boolean dropColumn(String tableName, String keyspaceName, String column);
}
