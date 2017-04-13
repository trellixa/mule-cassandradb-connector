/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb;

import com.datastax.driver.core.ResultSet;
import com.mulesoft.mule.cassandradb.configurations.BasicAuthConnectionStrategy;
import com.mulesoft.mule.cassandradb.metadata.CassandraMetadataCategory;
import com.mulesoft.mule.cassandradb.metadata.CassQueryVisitor;
import com.mulesoft.mule.cassandradb.utils.*;
import org.mule.api.annotations.*;
import org.mule.api.annotations.display.Placement;
import org.mule.api.annotations.licensing.RequiresEnterpriseLicense;
import org.mule.api.annotations.param.Default;
import org.mule.api.annotations.param.MetaDataKeyParam;
import org.mule.api.annotations.param.MetaDataKeyParamAffectsType;
import org.mule.api.annotations.param.Optional;
import org.mule.common.query.DsqlQuery;

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
    
    private static final String PAYLOAD = "#[payload]";
    private static final String PARAMETERS = "#[flowVars.parameters]";

    @Config
    private BasicAuthConnectionStrategy basicAuthConnectionStrategy;

    @Processor
    public boolean createKeyspace(String keyspaceName, @Optional Map<String, Object> replicationStrategy) throws CassandraDBException {
        try {
            return basicAuthConnectionStrategy.getCassandraClient().createKeyspace(keyspaceName, replicationStrategy);
        } catch (Exception e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    @Processor
    public boolean dropKeyspace(String keyspaceName) throws CassandraDBException{
        try {
            return basicAuthConnectionStrategy.getCassandraClient().dropKeyspace(keyspaceName);
        } catch (Exception e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    /**
     * method creates a table(column family) in a specific keyspace
     * @param customKeyspaceName - if the parameter is present, we overwrite the keyspace used in the connection itself (if present)
     * @param partitionKey - mandatory field; if null, default partitionKey will be added
     */
    @Processor
    public boolean createTable(String tableName, @Optional String customKeyspaceName, @Optional Map<String, Object> partitionKey) throws CassandraDBException{
        try {
            return basicAuthConnectionStrategy.getCassandraClient().createTable(tableName, customKeyspaceName, partitionKey);
        } catch (Exception e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    @Processor
    public boolean dropTable(String tableName, @Optional String customKeyspaceName) throws CassandraDBException{
        try {
            return basicAuthConnectionStrategy.getCassandraClient().dropTable(tableName, customKeyspaceName);
        } catch (Exception e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }

    /**
     * method executes the raw input query provided
     * @MetaDataScope annotation required for Functional tests to pass;to be removed
     */
    @Processor(friendlyName="Execute CQL Query")
    @MetaDataScope(CassandraMetadataCategory.class)
    public ResultSet executeCQLQuery(@Query @Placement(group = "Query") final String cqlQuery) throws CassandraDBException {
        try {
            return basicAuthConnectionStrategy.getCassandraClient().executeCQLQuery(cqlQuery);
        } catch (Exception e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }
    
    @Processor
    @MetaDataScope(CassandraMetadataCategory.class)
    public void insert(@MetaDataKeyParam(affects = MetaDataKeyParamAffectsType.INPUT) String table, @Default(PAYLOAD) Map<String, Object> entity) throws CassandraDBException {
            String keySpace = basicAuthConnectionStrategy.getKeyspace();
            basicAuthConnectionStrategy.getCassandraClient().insert(keySpace,table,entity);
    }

    @Processor @MetaDataScope(CassandraMetadataCategory.class) public void update(@MetaDataKeyParam(affects = MetaDataKeyParamAffectsType.INPUT) String table,
            @Default(PAYLOAD) Map<String, Object> entity, @Default(PARAMETERS) Map<String, Object> whereClause) throws CassandraDBException {
        String keySpace = basicAuthConnectionStrategy.getKeyspace();
        basicAuthConnectionStrategy.getCassandraClient().update(keySpace, table, entity, whereClause);
    }
    
    @Processor
    @MetaDataScope(CassandraMetadataCategory.class)
    public List<Map<String, Object>>  select(@Default(PAYLOAD) @Query final String query, @Default(PARAMETERS) @Optional List<Object> parameters) throws CassandraDBException {
        return basicAuthConnectionStrategy.getCassandraClient().select(query, parameters);
    }

    @Processor
    public List<String> getTableNamesFromKeyspace(String keyspaceName) throws CassandraDBException {
        try {
            return basicAuthConnectionStrategy.getCassandraClient().getTableNamesFromKeyspace(keyspaceName);
        } catch (Exception e) {
            throw new CassandraDBException(e.getMessage(), e);
        }
    }


    @QueryTranslator
    public String toNativeQuery(DsqlQuery query) {
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
