/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.api;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.mule.api.ConnectionException;
import org.mule.api.ConnectionExceptionCode;
import org.mule.modules.cassandradb.configurations.AdvancedConnectionParameters;
import org.mule.modules.cassandradb.configurations.ConnectionParameters;
import org.mule.modules.cassandradb.metadata.AlterColumnInput;
import org.mule.modules.cassandradb.metadata.CreateKeyspaceInput;
import org.mule.modules.cassandradb.metadata.CreateTableInput;
import org.mule.modules.cassandradb.utils.CassandraDBException;
import org.mule.modules.cassandradb.utils.ConnectionUtil;
import org.mule.modules.cassandradb.utils.Constants;
import org.mule.modules.cassandradb.utils.builders.HelperStatements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

    private static final Logger logger = LoggerFactory.getLogger(CassandraClient.class);

    /**
     * Connect to Cassandra Cluster specified by provided host IP
     * address and port number.
     *
     * @param connectionParameters the connection parameters
     * @return CassandraClient created
     * @throws org.mule.api.ConnectionException if any error occurs when trying to connect
     */
    public static CassandraClient buildCassandraClient(ConnectionParameters connectionParameters) throws org.mule.api.ConnectionException {

        Cluster.Builder clusterBuilder = Cluster.builder();

        CassandraClient client = new CassandraClient();

        if (StringUtils.isNotBlank(connectionParameters.getAdvancedConnectionParameters().getClusterNodes())) {
            connectWithAdvancedParams(connectionParameters, clusterBuilder);
        } else {
            connectWithBasicParams(connectionParameters, clusterBuilder);
        }


        withCredentials(connectionParameters, clusterBuilder);

        addAdvancedConnectionParameters(connectionParameters, clusterBuilder);

        client.cluster = clusterBuilder.build();

        connect(connectionParameters, client);

        return client;
    }

    public static void connectWithBasicParams(ConnectionParameters connectionParameters, Cluster.Builder clusterBuilder) throws ConnectionException {
        validateBasicParams(connectionParameters);

        try {
            clusterBuilder.addContactPoint(connectionParameters.getHost()).withPort(Integer.parseInt(connectionParameters.getPort()));
        } catch (IllegalArgumentException connEx) {
            logger.error("Error while connecting to Cassandra database!", connEx);
            throw new org.mule.api.ConnectionException(ConnectionExceptionCode.CANNOT_REACH, null, connEx.getMessage());
        }
    }

    public static void connectWithAdvancedParams(ConnectionParameters connectionParameters, Cluster.Builder clusterBuilder) throws ConnectionException {
        try {
            validateAdvancedParams(connectionParameters);

            Map<String, String> nodes = ConnectionUtil.parseClusterNodesString(connectionParameters.getAdvancedConnectionParameters().getClusterNodes());

            for (Map.Entry<String, String> entry : nodes.entrySet()) {
                clusterBuilder.addContactPoint(entry.getKey()).withPort(Integer.parseInt(entry.getValue()));
            }

        } catch (IllegalArgumentException connEx) {
            logger.error("Error while connecting to Cassandra database!", connEx);
            throw new org.mule.api.ConnectionException(ConnectionExceptionCode.CANNOT_REACH, null, connEx.getMessage());
        }
    }

    private static void withCredentials(ConnectionParameters connectionParameters, Cluster.Builder clusterBuilder) {
        if (StringUtils.isNotEmpty(connectionParameters.getUsername()) && StringUtils.isNotEmpty(connectionParameters.getPassword())) {
            clusterBuilder.withCredentials(connectionParameters.getUsername(), connectionParameters.getPassword());
        }
    }

    public static void addAdvancedConnectionParameters(ConnectionParameters connectionParameters, Cluster.Builder clusterBuilder) {
        if (connectionParameters.getAdvancedConnectionParameters() != null) {
            addAdvancedConnectionParameters(clusterBuilder, connectionParameters.getAdvancedConnectionParameters());
        }
    }

    private static void connect(ConnectionParameters connectionParameters, CassandraClient client) throws ConnectionException {
        try {
            logger.info("Connecting to Cassandra Database: {} , port: {} with clusterName: {} , protocol version {} and compression type {} ",
                    connectionParameters.getHost(),
                    connectionParameters.getPort(),
                    connectionParameters.getAdvancedConnectionParameters() != null ? connectionParameters.getAdvancedConnectionParameters().getClusterName() : null,
                    connectionParameters.getAdvancedConnectionParameters() != null ? connectionParameters.getAdvancedConnectionParameters().getProtocolVersion() : null,
                    connectionParameters.getAdvancedConnectionParameters() != null ? connectionParameters.getAdvancedConnectionParameters().getCompression() : null);

            client.cassandraSession = StringUtils.isNotEmpty(connectionParameters.getKeyspace()) ? client.cluster.connect(connectionParameters.getKeyspace())
                    : client.cluster.connect();

            logger.info("Connected to Cassandra Cluster: {} !", client.cassandraSession.getCluster().getClusterName());
        } catch (Exception cassandraException) {
            logger.error("Error while connecting to Cassandra database!", cassandraException);
            throw new org.mule.api.ConnectionException(ConnectionExceptionCode.UNKNOWN, null, cassandraException.getMessage());
        }
    }

    private static void addAdvancedConnectionParameters(Cluster.Builder clusterBuilder, AdvancedConnectionParameters advancedConnectionParameters) {
        if (StringUtils.isNotEmpty(advancedConnectionParameters.getClusterName())) {
            clusterBuilder.withClusterName(advancedConnectionParameters.getClusterName());
        }

        if (advancedConnectionParameters.getMaxSchemaAgreementWaitSeconds() > 0) {
            clusterBuilder.withMaxSchemaAgreementWaitSeconds(advancedConnectionParameters.getMaxSchemaAgreementWaitSeconds());
        }

        if (advancedConnectionParameters.getProtocolVersion() != null) {
            clusterBuilder.withProtocolVersion(advancedConnectionParameters.getProtocolVersion());
        }

        if (advancedConnectionParameters.getCompression() != null) {
            clusterBuilder.withCompression(advancedConnectionParameters.getCompression());
        }

        if (advancedConnectionParameters.isSsl()) {
            clusterBuilder.withSSL();
        }
    }

    public boolean createKeyspace(CreateKeyspaceInput input) {
        return cassandraSession.execute(HelperStatements.createKeyspaceStatement(input).getQueryString()).wasApplied();
    }

    public boolean dropKeyspace(String keyspaceName) {
        return cassandraSession.execute(HelperStatements.dropKeyspaceStatement(keyspaceName).getQueryString()).wasApplied();
    }

    public boolean createTable(CreateTableInput input) throws CassandraDBException {
        return cassandraSession.execute(
                HelperStatements.createTable(StringUtils.isNotBlank(input.getKeyspaceName()) ? input.getKeyspaceName() : cassandraSession.getLoggedKeyspace(), input)).wasApplied();
    }

    public boolean changeColumnType(String tableName, String customKeyspaceName, AlterColumnInput input) {
        return cassandraSession.execute(
                HelperStatements.changeColumnType(tableName, StringUtils.isNotBlank(customKeyspaceName) ? customKeyspaceName : getLoggedKeyspace(), input))
                .wasApplied();
    }

    public boolean addNewColumn(String tableName, String customKeyspaceName, String columnName, DataType columnType) {
        return cassandraSession.execute(
                HelperStatements.addNewColumn(tableName, StringUtils.isNotBlank(customKeyspaceName) ? customKeyspaceName : getLoggedKeyspace(), columnName, columnType))
                .wasApplied();
    }

    public boolean dropColumn(String tableName, String customKeyspaceName, String columnName) {
        return cassandraSession.execute(
                HelperStatements.dropColumn(tableName, StringUtils.isNotBlank(customKeyspaceName) ? customKeyspaceName : getLoggedKeyspace(), columnName))
                .wasApplied();
    }

    public boolean renameColumn(String tableName, String customKeyspaceName, String oldColumnName, String newColumnName) {
        return cassandraSession.execute(
                HelperStatements.renameColumn(tableName, StringUtils.isNotBlank(customKeyspaceName) ? customKeyspaceName : getLoggedKeyspace(), oldColumnName, newColumnName)).wasApplied();
    }

    public boolean dropTable(String tableName, String customKeyspaceName) {
        return cassandraSession.execute(HelperStatements.dropTable(tableName,
                StringUtils.isNotBlank(customKeyspaceName) ? customKeyspaceName : cassandraSession.getLoggedKeyspace())).wasApplied();
    }

    public List<Map<String, Object>> executeCQLQuery(String cqlQuery, List<Object> params) throws CassandraDBException {
        ResultSet result = null;

        try {
            if (StringUtils.isNotBlank(cqlQuery)) {
                if (!CollectionUtils.isEmpty(params)) {
                    result = executePreparedStatement(cqlQuery, params);
                } else {
                    result = cassandraSession.execute(cqlQuery);
                }
            }
        } catch (Exception e) {
            logger.error("Execute cql query Request Failed: " + e.getMessage());
            throw new CassandraDBException(e.getMessage(), e);
        }

        return getResponseFromResultSet(result);
    }

    public List<String> getTableNamesFromKeyspace(String customKeyspaceName) {
        String keyspaceName = StringUtils.isNotBlank(customKeyspaceName) ? customKeyspaceName : cassandraSession.getLoggedKeyspace();
        if (StringUtils.isNotBlank(keyspaceName)) {
            logger.info("Retrieving table names from the keyspace: {} ...", keyspaceName);
            if (cluster.getMetadata().getKeyspace(keyspaceName) == null) {
                return Collections.emptyList();
            }
            Collection<TableMetadata> tables = cluster
                    .getMetadata().getKeyspace(keyspaceName)
                    .getTables();
            ArrayList<String> tableNames = new ArrayList<String>();
            for (TableMetadata table : tables) {
                tableNames.add(table.getName());
            }
            return tableNames;
        }
        return Collections.emptyList();
    }

    /**
     * Fetches table metadata using DataStax java driver, based on the keyspace provided
     *
     * @param keyspaceUsed the Keyspace to fetch from
     * @param tableName    the Table from keyspace
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
            throw new CassandraDBException(e.getMessage(), e);
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
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    /**
     * DELETE command can be used to:
     * - remove one or more columns from one or more rows in a table;
     * - remove the entire row (one or more);
     * - if column_name refers to a collection (a list or map), the parameter in parentheses indicates the term in the collection
     * to be deleted
     *
     * @param keySpace    Keyspace to delete from
     * @param table       Table to delete from
     * @param entity      Entity to be removed
     * @param whereClause WHERE clause condition
     * @throws CassandraDBException if something goes wrong
     */
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
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    public List<Map<String, Object>> select(String query, List<Object> params) throws CassandraDBException {

        validateSelectQuery(query, params);

        ResultSet result = null;

        try {
            if (!CollectionUtils.isEmpty(params)) {
                result = executePreparedStatement(query, params);
            } else {
                result = cassandraSession.execute(query);
            }
        } catch (Exception e) {
            logger.error("Select Request Failed: " + e.getMessage());
            throw new CassandraDBException(e.getMessage(), e);
        }

        return getResponseFromResultSet(result);
    }

    public List<KeyspaceMetadata> getKeyspaces() {
        return cassandraSession.getCluster().getMetadata().getKeyspaces();
    }

    private void validateSelectQuery(String query, List<Object> params) throws CassandraDBException {

        if (!query.toUpperCase().startsWith(Constants.SELECT)) {
            throw new CassandraDBException("It must be a SELECT action.");
        }

        validateParams(query, params);

    }

    private List<Map<String, Object>> getResponseFromResultSet(ResultSet result) {
        List<Map<String, Object>> responseList = new LinkedList<Map<String, Object>>();

        if (result != null) {
            for (Row row : result.all()) {

                int columnsSize = row.getColumnDefinitions().size();

                Map<String, Object> mappedRow = new HashMap<String, Object>();

                for (int i = 0; i < columnsSize; i++) {
                    String columnName = row.getColumnDefinitions().getName(i);
                    Object columnValue = row.getObject(i);
                    mappedRow.put(columnName, columnValue);
                }

                responseList.add(mappedRow);
            }
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

    private static void validateAdvancedParams(ConnectionParameters parameters) {
        if (StringUtils.isBlank(parameters.getAdvancedConnectionParameters().getClusterNodes())) {
            throw new IllegalArgumentException("Unable to connect! You must specify at least a node!");
        }
    }

    private static void validateBasicParams(ConnectionParameters parameters) {
        if (StringUtils.isBlank(parameters.getHost()) || StringUtils.isBlank(parameters.getPort())) {
            throw new IllegalArgumentException("Unable to connect! Missing HOST or PORT parameter!");
        }
    }
}