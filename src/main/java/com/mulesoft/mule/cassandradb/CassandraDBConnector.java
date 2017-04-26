/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb;

import com.mulesoft.mule.cassandradb.configurations.BasicAuthConnectionStrategy;
import com.mulesoft.mule.cassandradb.metadata.*;
import com.mulesoft.mule.cassandradb.utils.*;
import org.mule.api.annotations.*;
import org.mule.api.annotations.display.Placement;
import org.mule.api.annotations.licensing.RequiresEnterpriseLicense;
import org.mule.api.annotations.param.Default;
import org.mule.api.annotations.param.MetaDataKeyParam;
import org.mule.api.annotations.param.MetaDataKeyParamAffectsType;
import org.mule.api.annotations.param.Optional;
import org.mule.common.query.DsqlQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * The Apache Cassandra database is the right choice when you need scalability and high availability without compromising performance.
 * Cassandra's ColumnFamily data model offers the convenience of column indexes with the performance of log-structured updates, strong support for materialized views, and powerful built-in caching.
 *
 * @author MuleSoft, Inc.
 */
@Connector(name = "cassandradb", schemaVersion = "3.2", friendlyName = "CassandraDB", minMuleVersion = "3.5")
@RequiresEnterpriseLicense(allowEval = true)
public class CassandraDBConnector {

    private static final Logger logger = LoggerFactory.getLogger(CassandraDBConnector.class);
    
    private static final String PAYLOAD = "#[payload]";

    @Config
    private BasicAuthConnectionStrategy basicAuthConnectionStrategy;

    /**
     *
     * @param input operation input containing the keyspace name and the replication details
     * @return true if the operation succeeded, false otherwise
     * @throws CassandraDBException
     */
    @Processor
    public boolean createKeyspace(@Default(PAYLOAD) CreateKeyspaceInput input) throws CassandraDBException {
        if (logger.isDebugEnabled()) {
            logger.debug("Creating keyspace " + input.toString());
        }
        try {
            return basicAuthConnectionStrategy.getCassandraClient().createKeyspace(input);
        } catch (Exception e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    /**
     *
     * @param keyspaceName the name of the keyspace to be dropped
     * @return true if the operation succeeded, false otherwise
     * @throws CassandraDBException
     */
    @Processor
    public boolean dropKeyspace(String keyspaceName) throws CassandraDBException{
        if (logger.isDebugEnabled()) {
            logger.debug("Dropping keyspace " + keyspaceName);
        }
        try {
            return basicAuthConnectionStrategy.getCassandraClient().dropKeyspace(keyspaceName);
        } catch (Exception e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    /**
     * creates a table(column family) in a specific keyspace
     *
     * @param input operation input describing the table name, the keyspace name and the list of columns
     * @return true if the operation succeeded, false otherwise
     * @throws CassandraDBException
     */
    @Processor
    public boolean createTable(@Default(PAYLOAD) CreateTableInput input) throws CassandraDBException {
        if (logger.isDebugEnabled()) {
            logger.debug("Creating table " + input.toString());
        }
        try {
            return basicAuthConnectionStrategy.getCassandraClient().createTable(input);
        } catch (Exception e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    /**
     *
     * @param tableName the name of the table to be dropped
     * @param customKeyspaceName (optional) the keyspace which contains the table to be dropped
     * @return true if the operation succeeded, false otherwise
     * @throws CassandraDBException
     */
    @Processor
    public boolean dropTable(String tableName, @Optional String customKeyspaceName) throws CassandraDBException{
        if (logger.isDebugEnabled()) {
            logger.debug("Dropping table " + tableName);
        }
        try {
            return basicAuthConnectionStrategy.getCassandraClient().dropTable(tableName, customKeyspaceName);
        } catch (Exception e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    /**
     * method executes the raw input query provided
     * @MetaDataScope annotation required for Functional tests to pass;to be removed
     *
     * @param input CQLQueryInput describing the parametrized query to be executed along with the parameters
     * @return the result of the query execution
     * @throws CassandraDBException
     */
    @Processor(friendlyName="Execute CQL Query")
    @MetaDataScope(CassandraMetadataCategory.class)
    public List<Map<String, Object>> executeCQLQuery(@Placement(group = "Query") @Default(PAYLOAD) CQLQueryInput input) throws CassandraDBException {
        if (logger.isDebugEnabled()) {
            logger.debug("Executing query " + input);
        }
        return basicAuthConnectionStrategy.getCassandraClient().executeCQLQuery(input.getCqlQuery(), input.getParameters());
    }

    /**
     * executes the insert entity operation
     * @param table the table name in which the entity will be inserted
     * @param entity the entity to be inserted
     * @throws CassandraDBException
     */
    @Processor
    @MetaDataScope(CassandraMetadataCategory.class)
    public void insert(@MetaDataKeyParam(affects = MetaDataKeyParamAffectsType.INPUT) String table, @Default(PAYLOAD) Map<String, Object> entity) throws CassandraDBException {
        if (logger.isDebugEnabled()) {
            logger.debug("Inserting entity " + entity + " into the " + table + " table ");
        }
        String keySpace = basicAuthConnectionStrategy.getKeyspace();
        basicAuthConnectionStrategy.getCassandraClient().insert(keySpace,table,entity);
    }

    /**
     * executes the update entity operation
     * @param table the table name in which the entity will be updated
     * @param entity the entity to be updated
     * @throws CassandraDBException
     */
    @Processor
    @MetaDataScope(CassandraWithFiltersMetadataCategory.class)
    public void update(@MetaDataKeyParam(affects = MetaDataKeyParamAffectsType.INPUT) String table,
            @Default(PAYLOAD) Map<String, Object> entity) throws CassandraDBException {
        if (logger.isDebugEnabled()) {
            logger.debug("Updating  entity" + entity + " into the " + table + " table ");
        }
        String keySpace = basicAuthConnectionStrategy.getKeyspace();
        basicAuthConnectionStrategy.getCassandraClient().update(keySpace, table, (Map) entity.get(Constants.COLUMNS), (Map) entity.get(Constants.WHERE));
    }

    /**
     * deletes values from an object specified by the where clause
     * @param table the name of the table
     * @param payload operation input: columns to be deleted and where clause for the delete operation
     * @throws CassandraDBException
     */
    @Processor
    @MetaDataScope(CassandraWithFiltersMetadataCategory.class)
    public void deleteColumnsValue(@MetaDataKeyParam(affects = MetaDataKeyParamAffectsType.INPUT) String table,
            @Default(PAYLOAD) Map<String, Object> payload) throws CassandraDBException {
        String keySpace = basicAuthConnectionStrategy.getKeyspace();
        basicAuthConnectionStrategy.getCassandraClient().delete(keySpace, table, (List) payload.get(Constants.COLUMNS), (Map) payload.get(Constants.WHERE));
    }

    /**
     * deletes an entire record
     * @param table the name of the table
     * @param payload operation input: where clause for the delete operation
     * @throws CassandraDBException
     */
    @Processor
    @MetaDataScope(CassandraOnlyWithFiltersMetadataCategory.class)
    public void deleteRows(@MetaDataKeyParam(affects = MetaDataKeyParamAffectsType.INPUT) String table,
            @Default(PAYLOAD) Map<String, Object> payload) throws CassandraDBException {
        String keySpace = basicAuthConnectionStrategy.getKeyspace();
        basicAuthConnectionStrategy.getCassandraClient().delete(keySpace, table, null, (Map) payload.get(Constants.WHERE));
    }

    /**
     * executes a select query
     * @param query  the query to be executed
     * @param parameters the query parameters
     * @return list of entities returned by the select query
     * @throws CassandraDBException
     */
    @Processor
    @MetaDataScope(CassandraMetadataCategory.class)
    public List<Map<String, Object>>  select(@Default(PAYLOAD) @Query final String query, @Optional List<Object> parameters) throws CassandraDBException {
        if (logger.isDebugEnabled()) {
            logger.debug("Executing select query: " + query + " with the parameters: " + parameters);
        }
        return basicAuthConnectionStrategy.getCassandraClient().select(query, parameters);
    }

    /**
     *
     * @param keyspaceName the name of the keyspace to be used on the operation
     * @return a list of table names
     * @throws CassandraDBException
     */
    @Processor
    public List<String> getTableNamesFromKeyspace(String keyspaceName) throws CassandraDBException {
        try {
            return basicAuthConnectionStrategy.getCassandraClient().getTableNamesFromKeyspace(keyspaceName);
        } catch (Exception e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    @Processor(friendlyName="Change the type of a column")
    public boolean changeColumnType(String table, @Default(PAYLOAD) AlterColumnInput input){
        String keySpace = basicAuthConnectionStrategy.getKeyspace();
        return basicAuthConnectionStrategy.getCassandraClient().changeColumnType(table, keySpace, input);
    }

    @Processor(friendlyName="Add new column")
    public boolean addNewColumn(String table, @Default(PAYLOAD) AlterColumnInput input) {
        String keySpace = basicAuthConnectionStrategy.getKeyspace();
        return basicAuthConnectionStrategy.getCassandraClient().addNewColumn(table, keySpace, input.getColumn(), ColumnType.resolveDataType(input.getType()));
    }

    @Processor(friendlyName="Remove column")
    @MetaDataScope(CassandraMetadataCategory.class)
    public boolean dropColumn(String table, @Default(PAYLOAD) String columnName) {
        String keySpace = basicAuthConnectionStrategy.getKeyspace();
        return basicAuthConnectionStrategy.getCassandraClient().dropColumn(table, keySpace, columnName);
    }

    @Processor(friendlyName="Rename column")
    @MetaDataScope(CassandraPrimaryKeyMetadataCategory.class)
    public boolean renameColumn(@MetaDataKeyParam(affects = MetaDataKeyParamAffectsType.INPUT) String table, @Default(PAYLOAD) Map<String, String> input) {
        String keySpace = basicAuthConnectionStrategy.getKeyspace();
        return basicAuthConnectionStrategy.getCassandraClient().renameColumn(table, keySpace, input);
    }

    @QueryTranslator public String toNativeQuery(DsqlQuery query) {
        CassQueryVisitor visitor = new CassQueryVisitor();
        query.accept(visitor);
        return visitor.dsqlQuery();
    }

    public BasicAuthConnectionStrategy getBasicAuthConnectionStrategy() {
        return basicAuthConnectionStrategy;
    }

    public void setBasicAuthConnectionStrategy(BasicAuthConnectionStrategy basicAuthConnectionStrategy) {
        this.basicAuthConnectionStrategy = basicAuthConnectionStrategy;
    }
}
