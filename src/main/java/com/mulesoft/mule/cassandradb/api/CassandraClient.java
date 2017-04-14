/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.api;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.*;
import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
import com.mulesoft.mule.cassandradb.utils.Constants;
import com.mulesoft.mule.cassandradb.utils.builders.HelperStatements;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.mule.api.ConnectionExceptionCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public final class CassandraClient {

    /**
     * Cassandra Cluster.
     */
    private Cluster cluster;
    /**
     * Cassandra Session.
     */
    private Session cassandraSession;

    final static Logger logger = LoggerFactory.getLogger(CassandraClient.class);

    /**
     * Connect to Cassandra Cluster specified by provided host IP
     * address and port number.
     *
     * @param host     Cluster host IP address.
     * @param port     Port of cluster host.
     * @param username the username to buildCassandraClient with
     * @param keyspace optional - keyspace to retrieve cluster session for
     */
    public static CassandraClient buildCassandraClient(final String host, final int port, final String username, final String password, final String keyspace) throws org.mule.api.ConnectionException {
        Cluster.Builder clusterBuilder = Cluster.builder()
                .addContactPoint(host)
                .withPort(port);

        if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
            clusterBuilder.withCredentials(username, password);
        }

        CassandraClient client = new CassandraClient();
        client.cluster = clusterBuilder.build();

        try {
            logger.info("Connecting to Cassandra Database: {} , port: {}", host, port);
            client.cassandraSession = StringUtils.isNotEmpty(keyspace) ? client.cluster.connect(keyspace) : client.cluster.connect();
            logger.info("Connected to Cassandra Cluster: {} !", client.cassandraSession.getCluster().getClusterName());
        } catch (Exception cassandraException) {
            logger.error("Error while connecting to Cassandra database!", cassandraException);
            throw new org.mule.api.ConnectionException(ConnectionExceptionCode.UNKNOWN, null, cassandraException.getMessage());
        }
        return client;
    }

    public boolean createKeyspace(String keyspaceName, Map<String, Object> replicationStrategy) {
        return cassandraSession.execute(HelperStatements.createKeyspaceStatement(keyspaceName, replicationStrategy).getQueryString()).wasApplied();
    }

    public boolean dropKeyspace(String keyspaceName) {
        return cassandraSession.execute(HelperStatements.dropKeyspaceStatement(keyspaceName).getQueryString()).wasApplied();
    }

    public boolean createTable(String tableName, String customKeyspaceName, Map<String, Object> partitionKey) {
        return cassandraSession.execute(HelperStatements.createTable(tableName,
                StringUtils.isNotBlank(customKeyspaceName) ? customKeyspaceName : cassandraSession.getLoggedKeyspace(), partitionKey)).wasApplied();
    }

    public boolean addColumnToTable(String tableName, String customKeyspaceName, String columnName, DataType columnType) {
        return cassandraSession.execute(
                HelperStatements.addColumnToTable(tableName, StringUtils.isNotBlank(customKeyspaceName) ? customKeyspaceName : getLoggedKeyspace(), columnName, columnType))
                .wasApplied();
    }

    public boolean dropTable(String tableName, String customKeyspaceName) {
        return cassandraSession.execute(HelperStatements.dropTable(tableName,
                StringUtils.isNotBlank(customKeyspaceName) ? customKeyspaceName : cassandraSession.getLoggedKeyspace())).wasApplied();
    }

    public ResultSet executeCQLQuery(String cqlQuery) {
        if (StringUtils.isNotBlank(cqlQuery)) {
            return cassandraSession.execute(cqlQuery);
        }
        return null;
    }

    public List<String> getTableNamesFromKeyspace(String keyspaceName) {
        if (StringUtils.isNotBlank((keyspaceName))) {
            logger.info("Retrieving table names from the keyspace: {} ...", keyspaceName);
            Collection<TableMetadata> tables = cluster
                    .getMetadata().getKeyspace(keyspaceName)
                    .getTables();
            ArrayList<String> tableNames = new ArrayList<String>();
            for (TableMetadata table : tables) {
                tableNames.add(table.getName());
            }
            return tableNames;
        }
        return null;
    }

    /**
     * Fetches table metadata using DataStax java driver, based on the keyspace provided
     *
     * @return the table metadata as returned by the driver.
     */
    public TableMetadata fetchTableMetadata(final String keyspaceUsed, final String tableName) {
        if (StringUtils.isNotBlank(tableName)) {
            logger.info("Retrieving table metadata for: {} ...", tableName);
            Metadata metadata = cluster.getMetadata();
            KeyspaceMetadata ksMetadata = metadata.getKeyspace(keyspaceUsed);
            if (ksMetadata != null) {
                return ksMetadata.getTable(tableName);
            }
        }
        return null;
    }
    
    public void insert(String keySpace, String table, Map<String, Object> entity) throws CassandraDBException {

        Insert insertObject = QueryBuilder.insertInto(keySpace, table);

        for (Map.Entry<String, Object> entry : entity.entrySet()) {
            insertObject.value(entry.getKey(), entry.getValue());
        }

        try {

            logger.debug("Insert Request: " + insertObject.toString());

            cassandraSession.execute(insertObject);            

        } catch (Exception e) {

            logger.error("Insert Request Failed: " + e.getMessage());
            throw new CassandraDBException(e.getMessage());
        }
    }

    /*
    * Update the table @param table using the keySpace @param keySpace
    * */
    public void update(String keySpace, String table, Map<String, Object> entity, Map<String, Object> whereClause) throws CassandraDBException {

        if (entity == null || whereClause == null) {
            throw new CassandraDBException("Mismatched input. SET and WHERE clause must not be null.");
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

        try {

            logger.debug("Update Request: " + updateObject.toString());

            cassandraSession.execute(updateObject);

        } catch (Exception e) {

            logger.error("Update Request Failed: " + e.getMessage());
            throw new CassandraDBException(e.getMessage());
        }
    }

    /*
* DELETE command can be used to:
* - remove one or more columns from one or more rows in a table;
* - remove the entire row (one or more);
* - if column_name refers to a collection (a list or map), the parameter in parentheses indicates the term in the collection
*    to be deleted
* */
    public void delete(String keySpace, String table, List<String> entity, Map<String, Object> whereClause) throws CassandraDBException {

        if (whereClause == null) {
            throw new CassandraDBException("Mismatched input. WHERE clause must not be null.");
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

        try {

            logger.debug("Delete Request: " + deleteObject.toString());

            cassandraSession.execute(deleteObject);

        } catch (Exception e) {

            logger.error("Delete Request Failed: " + e.getMessage());
            throw new CassandraDBException(e.getMessage());
        }
    }

    public List<Map<String, Object>> select(String query, List<Object> params) throws CassandraDBException {

        validateSelectQuery(query, params);

        ResultSet result = null;

        try {
            if (!CollectionUtils.isEmpty(params)) {
                result = executePreparedStatement(query, params);
            } else {
                result = executeCQLQuery(query);
            }
        } catch (Exception e) {
            logger.error("Select Request Failed: " + e.getMessage());
            throw new CassandraDBException(e.getMessage(), e);
        }

        return getResponseFromResultSet(result);
    }

    private void validateSelectQuery(String query, List<Object> params) throws CassandraDBException {
        
        if (!query.toUpperCase().startsWith(Constants.SELECT)) {
            throw new CassandraDBException("It must be a SELECT action.");
        }

        validateParams(query, params);

    }

    private List<Map<String, Object>> getResponseFromResultSet(ResultSet result) {
        List<Map<String, Object>> responseList = new LinkedList<>();

        for (Row row : result.all()) {

            int columnsSize = row.getColumnDefinitions().size();

            Map<String, Object> mappedRow = new HashMap<String, Object>();

            for (int i = 0; i < columnsSize; i++) {
                String columnName = row.getColumnDefinitions().getName(i);
                String columnValue = row.getString(i);
                mappedRow.put(columnName, columnValue);
            }

            responseList.add(mappedRow);
        }

        return responseList;
    }

    public String getLoggedKeyspace() {
        return cassandraSession.getLoggedKeyspace();
    }

    /**
     * Close cluster.
     */
    private void closeCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    private void closeSession() {
        if (cassandraSession != null) {
            cassandraSession.close();
        }
    }

    public void close() {
        closeSession();
        closeCluster();
    }

    private ResultSet executePreparedStatement(String query, List<Object> params) {
        PreparedStatement ps = cassandraSession.prepare(query);
        Object[] paramArray = params.toArray(new Object[params.size()]);
        return cassandraSession.execute(ps.bind(paramArray));
    }

    private void validateParams(String query, List<Object> params) throws CassandraDBException {

        int expectedParams = StringUtils.countMatches(query, Constants.PARAM_HOLDER);
        int parameterSize = (params == null) ? 0 : params.size();

        if (expectedParams != parameterSize) {
            throw new CassandraDBException("Expected query parameters is " + expectedParams + " but found " + parameterSize);
        }
    }
}
