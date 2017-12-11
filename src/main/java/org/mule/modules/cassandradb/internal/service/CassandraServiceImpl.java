package org.mule.modules.cassandradb.internal.service;

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.mule.connectors.commons.template.service.DefaultConnectorService;
import org.mule.modules.cassandradb.api.AlterColumnInput;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.modules.cassandradb.internal.config.CassandraConfig;
import org.mule.modules.cassandradb.internal.connection.CassandraConnection;
import org.mule.modules.cassandradb.internal.exception.QueryErrorException;
import org.mule.modules.cassandradb.internal.util.builders.HelperStatements;
import static java.util.stream.Collectors.toList;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.mule.modules.cassandradb.internal.util.Constants.PARAM_HOLDER;
import static org.mule.modules.cassandradb.internal.util.Constants.SELECT;

public class CassandraServiceImpl extends DefaultConnectorService<CassandraConfig, CassandraConnection> implements CassandraService{

    public CassandraServiceImpl(CassandraConfig config, CassandraConnection connection) {
        super(config, connection);
    }

    @Override
    public boolean createKeyspace(CreateKeyspaceInput input) {
        String queryString = HelperStatements.createKeyspaceStatement(input).getQueryString();
        return getCassandraSession().execute(queryString).wasApplied();
    }

    @Override
    public boolean dropKeyspace(String keyspaceName) {
        String queryString = HelperStatements.dropKeyspaceStatement(keyspaceName).getQueryString();
        return getCassandraSession().execute(queryString).wasApplied();
    }

    @Override
    public boolean createTable(CreateTableInput input) {
        String keyspace = getKeyspaceNameToUse(input.getKeyspaceName());
        String queryString = HelperStatements.createTable(keyspace, input).getQueryString();
        return getCassandraSession().execute(queryString).wasApplied();
    }

    @Override
    public boolean dropTable(String tableName, String keyspaceName) {
        String keyspace = getKeyspaceNameToUse(keyspaceName);
        return getCassandraSession().execute(HelperStatements.dropTable(tableName, keyspace)).wasApplied();
    }

    @Override
    public boolean addNewColumn(String tableName, String keyspaceName, String columnName, DataType columnType) {
        String keyspace = getKeyspaceNameToUse(keyspaceName);
        SchemaStatement statement = HelperStatements.addNewColumn(tableName, keyspace, columnName, columnType);
        return getCassandraSession().execute(statement).wasApplied();
    }

    @Override
    public boolean dropColumn(String tableName, String keyspaceName, String column) {
        String keyspace = getKeyspaceNameToUse(keyspaceName);
        SchemaStatement statement = HelperStatements.dropColumn(tableName, keyspace, column);
        return getCassandraSession().execute(statement).wasApplied();
    }

    @Override
    public boolean renameColumn(String tableName, String keyspaceName, String oldColumnName, String newColumnName) {
        String keyspace = getKeyspaceNameToUse(keyspaceName);
        SchemaStatement statement = HelperStatements.renameColumn(tableName, keyspace, oldColumnName, newColumnName);
        return getCassandraSession().execute(statement).wasApplied();
    }

    public boolean changeColumnType(String tableName, String keyspaceName, AlterColumnInput input) {
        String keyspace = getKeyspaceNameToUse(keyspaceName);
        SchemaStatement statement = HelperStatements.changeColumnType(tableName, keyspace, input);
        return getCassandraSession().execute(statement).wasApplied();
    }

    @Override
    public List<String> getTableNamesFromKeyspace(String keyspaceName) {
        String keyspace = getKeyspaceNameToUse(keyspaceName);
        List<String> tableNames = Collections.emptyList();
        if (isNotBlank(keyspace)) {
            if (!(getCassandraSession().getCluster().getMetadata().getKeyspace(keyspace) == null)) {
                tableNames = getCassandraSession().getCluster()
                        .getMetadata().getKeyspace(keyspace)
                        .getTables()
                        .stream()
                        .map(AbstractTableMetadata::getName).collect(toList());
            }

        }
        return tableNames;
    }

    @Override
    public void insert(String keyspaceName, String table, Map<String, Object> entity) {
        String keyspace = getKeyspaceNameToUse(keyspaceName);

        Insert insertObject = QueryBuilder.insertInto(keyspace, table);

        for (Map.Entry<String, Object> entry : entity.entrySet()) {
            insertObject.value(entry.getKey(), entry.getValue());
        }

        getCassandraSession().execute(insertObject);
    }

    @Override
    public void update(String keySpace, String table, Map<String, Object> entity, Map<String, Object> whereClause) {
        if (entity == null || whereClause == null) {
            throw new QueryErrorException("Mismatched input. SET and WHERE clause must not be null.");
        }
        Update updateObject = QueryBuilder.update(keySpace, table);

        for (Map.Entry<String, Object> entry : entity.entrySet()) {
            updateObject.with().and(QueryBuilder.set(entry.getKey(), entry.getValue()));
        }
        // In where clause in cassandra, when using it with update command, can be used only eq and IN operators
        for (Map.Entry<String, Object> entry : whereClause.entrySet()) {
            if (entry.getValue() instanceof List) {
                updateObject.where(QueryBuilder.in(entry.getKey(), (List) entry.getValue()));
            } else {
                updateObject.where(QueryBuilder.eq(entry.getKey(), entry.getValue()));
            }
        }

        getCassandraSession().execute(updateObject);
    }

    @Override
    public List<Map<String, Object>> executeCQLQuery(String cqlQuery, List<Object> params) {
        ResultSet result = null;

        if (isNotBlank(cqlQuery)) {
            if (!CollectionUtils.isEmpty(params)) {
                result = executePreparedStatement(cqlQuery, params);
            } else {
                result = getCassandraSession().execute(cqlQuery);
            }
        }

        return getResponseFromResultSet(result);
    }

    @Override
    public List<Map<String, Object>> select(String query, List<Object> params) {
        validateSelectQuery(query, params);

        ResultSet result;

        if (!CollectionUtils.isEmpty(params)) {
            result = executePreparedStatement(query, params);
        } else {
            result = getCassandraSession().execute(query);
        }

        return getResponseFromResultSet(result);
    }

    @Override
    public void delete(String keySpace, String table, List<String> entity, Map<String, Object> whereClause) {
        if (whereClause == null) {
            throw new QueryErrorException("Mismatched input. WHERE clause must not be null.");
        }
        Delete.Selection selectionObject = QueryBuilder.delete();
        // if the entity list is empty, means that the entire row(s) is deleted
        if (entity != null && !entity.isEmpty()) {
            for (String entry : entity) {
                selectionObject.column(entry);
            }
        }

        Delete deleteObject = selectionObject.from(keySpace, table);

        // In where clause in cassandra, on delete command, can be used only eq and IN operators
        for (Map.Entry<String, Object> entry : whereClause.entrySet()) {
            if (entry.getValue() instanceof List) {
                deleteObject.where(QueryBuilder.in(entry.getKey(), (List) entry.getValue()));
            } else {
                deleteObject.where(QueryBuilder.eq(entry.getKey(), entry.getValue()));
            }
        }

        getCassandraSession().execute(deleteObject);
    }

    private void validateParams(String query, List<Object> params) throws QueryErrorException {
        int expectedParams = StringUtils.countMatches(query, PARAM_HOLDER);
        int parameterSize = (params == null) ? 0 : params.size();

        if (expectedParams != parameterSize) {
            throw new QueryErrorException("Expected query parameters is " + expectedParams + " but found " + parameterSize);
        }
    }

    private void validateSelectQuery(String query, List<Object> params) {
        if (!query.toUpperCase().startsWith(SELECT)) {
            throw new QueryErrorException("It must be a SELECT action.");
        }

        validateParams(query, params);
    }

    private ResultSet executePreparedStatement(String query, List<Object> params) {
        PreparedStatement ps = getCassandraSession().prepare(query);
        Object[] paramArray = params.toArray(new Object[params.size()]);
        return getCassandraSession().execute(ps.bind(paramArray));
    }

    private List<Map<String, Object>> getResponseFromResultSet(ResultSet result) {
        List<Map<String, Object>> responseList = new LinkedList<>();

        if (result != null) {
            responseList = result.all().stream()
                    .map(CassandraServiceImpl::mapProperties)
                    .collect(toList());
        }

        return responseList;
    }

    private static Map<String, Object> mapProperties(Row row){
        Map<String, Object> mappedRow = new HashMap<String, Object>();

        int columnsSize = row.getColumnDefinitions().size();
        for (int i = 0; i < columnsSize; i++) {
            String columnName = row.getColumnDefinitions().getName(i);
            Object columnValue = row.getObject(i);
            mappedRow.put(columnName, columnValue);
        }
        return mappedRow;
    }

    private Session getCassandraSession() {
        return getConnection().getCassandraSession();
    }

    private String getKeyspaceNameToUse(String keyspaceName) {
        return isNotBlank(keyspaceName) ? keyspaceName : getCassandraSession().getLoggedKeyspace();
    }
}
