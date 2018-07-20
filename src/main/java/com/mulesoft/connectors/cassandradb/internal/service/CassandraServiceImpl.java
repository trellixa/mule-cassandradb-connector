package com.mulesoft.connectors.cassandradb.internal.service;

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.mulesoft.connectors.cassandradb.api.AlterColumnInput;
import com.mulesoft.connectors.cassandradb.api.CreateKeyspaceInput;
import com.mulesoft.connectors.cassandradb.api.CreateTableInput;
import com.mulesoft.connectors.cassandradb.internal.connection.CassandraConnection;
import com.mulesoft.connectors.cassandradb.internal.exception.CassandraException;
import com.mulesoft.connectors.cassandradb.internal.util.builders.HelperStatements;
import org.apache.commons.lang3.StringUtils;
import org.mule.connectors.commons.template.service.DefaultConnectorService;
import com.mulesoft.connectors.cassandradb.internal.config.CassandraConfig;
import com.mulesoft.connectors.cassandradb.internal.exception.OperationNotAppliedException;
import com.mulesoft.connectors.cassandradb.internal.exception.QueryErrorException;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static org.apache.commons.collections.CollectionUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static com.mulesoft.connectors.cassandradb.internal.util.Constants.COLUMNS;
import static com.mulesoft.connectors.cassandradb.internal.util.Constants.PARAM_HOLDER;
import static com.mulesoft.connectors.cassandradb.internal.util.Constants.SELECT;
import static com.mulesoft.connectors.cassandradb.internal.util.Constants.WHERE;


public class CassandraServiceImpl extends DefaultConnectorService<CassandraConfig, CassandraConnection> implements CassandraService{

    public CassandraServiceImpl(CassandraConfig config, CassandraConnection connection) {
        super(config, connection);
    }

    @Override
    public void createKeyspace(CreateKeyspaceInput input) {
        validateOutput(getCassandraSession().execute(HelperStatements.createKeyspaceStatement(input).getQueryString()).wasApplied());
    }

    @Override
    public void dropKeyspace(String keyspaceName) {
        validateOutput(getCassandraSession().execute(HelperStatements.dropKeyspaceStatement(keyspaceName).getQueryString()).wasApplied());
    }

    @Override
    public void createTable(CreateTableInput input) {
        validateOutput(getCassandraSession().execute(HelperStatements.createTable(getKeyspaceNameToUse(input.getKeyspaceName()), input).getQueryString()).wasApplied());
    }

    @Override
    public void dropTable(String tableName, String keyspaceName) {
        validateOutput(getCassandraSession().execute(HelperStatements.dropTable(tableName, getKeyspaceNameToUse(keyspaceName))).wasApplied());
    }

    @Override
    public void addNewColumn(String tableName, String keyspaceName, String columnName, DataType columnType) {
        validateOutput(getCassandraSession().execute(HelperStatements.addNewColumn(tableName, getKeyspaceNameToUse(keyspaceName), columnName, columnType)).wasApplied());
    }

    @Override
    public void dropColumn(String tableName, String keyspaceName, String column) {
        validateOutput(getCassandraSession().execute(HelperStatements.dropColumn(tableName, getKeyspaceNameToUse(keyspaceName), column)).wasApplied());
    }

    @Override
    public void renameColumn(String tableName, String keyspaceName, String oldColumnName, String newColumnName) {
        validateOutput(getCassandraSession().execute(HelperStatements.renameColumn(tableName, getKeyspaceNameToUse(keyspaceName), oldColumnName, newColumnName)).wasApplied());
    }

    public void changeColumnType(String tableName, String keyspaceName, AlterColumnInput input) {
        validateOutput(getCassandraSession().execute(HelperStatements.changeColumnType(tableName, getKeyspaceNameToUse(keyspaceName), input)).wasApplied());
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
    public void update(String keySpace, String table, Map<String, Object> entityToUpdate) {
        Map<String,Object> entity = Map.class.cast(entityToUpdate.get(COLUMNS));
        Map<String,Object> whereClause = Map.class.cast(entityToUpdate.get(WHERE));

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
            if (!isEmpty(params)) {
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

        if (!isEmpty(params)) {
            result = executePreparedStatement(query, params);
        } else {
            result = getCassandraSession().execute(query);
        }

        return getResponseFromResultSet(result);
    }

    @Override
    public void deleteWithoutEntity(String keySpace, String table, Map<String, Object> whereClause) {
        delete(keySpace, table, null, whereClause);
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

    @Override
    public void deleteRow(String keySpace, String table, Map<String, Object> whereClause) {
        delete(keySpace, table, null, whereClause);
    }

    private void executeCommandAndThrowExceptionIfFails(Statement command) {
        boolean wasApplied = getCassandraSession().execute(command).wasApplied();
        if (!wasApplied) {
            throw new OperationNotAppliedException("Operation was not applied");
        }
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

    private void validateOutput(boolean result){
        if(!result){
            throw new CassandraException("Operation failed");
        }
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
