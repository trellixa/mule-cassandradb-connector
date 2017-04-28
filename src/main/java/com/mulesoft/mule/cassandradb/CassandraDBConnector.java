/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb;

import com.datastax.driver.core.DataType;
import com.mulesoft.mule.cassandradb.configurations.BasicAuthConnectionStrategy;
import com.mulesoft.mule.cassandradb.metadata.*;
import com.mulesoft.mule.cassandradb.utils.*;
import com.mulesoft.mule.cassandradb.utils.builders.HelperStatements;
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
    public boolean createKeyspace(@Default(PAYLOAD) CreateKeyspaceInput input) throws CassandraDBException {
        try {
            return basicAuthConnectionStrategy.getCassandraClient().createKeyspace(input);
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
     */
    @Processor
    public boolean createTable(@Default(PAYLOAD) CreateTableInput input) throws CassandraDBException {
        try {
            return basicAuthConnectionStrategy.getCassandraClient().createTable(input);
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
    public List<Map<String, Object>> executeCQLQuery(@Placement(group = "Query") @Default(PAYLOAD) CreateCQLQueryInput input) throws CassandraDBException {
        return basicAuthConnectionStrategy.getCassandraClient().executeCQLQuery(input.getCqlQuery(), input.getParameters());
    }

    @Processor
    @MetaDataScope(CassandraMetadataCategory.class)
    public void insert(@MetaDataKeyParam(affects = MetaDataKeyParamAffectsType.INPUT) String table, @Default(PAYLOAD) Map<String, Object> entity) throws CassandraDBException {
            String keySpace = basicAuthConnectionStrategy.getKeyspace();
            basicAuthConnectionStrategy.getCassandraClient().insert(keySpace,table,entity);
    }

    @Processor
    @MetaDataScope(CassandraWithFiltersMetadataCategory.class)
    public void update(@MetaDataKeyParam(affects = MetaDataKeyParamAffectsType.INPUT) String table,
            @Default(PAYLOAD) Map<String, Object> entity) throws CassandraDBException {
        String keySpace = basicAuthConnectionStrategy.getKeyspace();
        basicAuthConnectionStrategy.getCassandraClient().update(keySpace, table, (Map) entity.get(Constants.COLUMNS), (Map) entity.get(Constants.WHERE));
    }

    @Processor
    @MetaDataScope(CassandraWithFiltersMetadataCategory.class)
    public void deleteColumnsValue(@MetaDataKeyParam(affects = MetaDataKeyParamAffectsType.INPUT) String table,
            @Default(PAYLOAD) Map<String, Object> payload) throws CassandraDBException {
        String keySpace = basicAuthConnectionStrategy.getKeyspace();
        basicAuthConnectionStrategy.getCassandraClient().delete(keySpace, table, (List) payload.get(Constants.COLUMNS), (Map) payload.get(Constants.WHERE));
    }

    @Processor
    @MetaDataScope(CassandraOnlyWithFiltersMetadataCategory.class)
    public void deleteRows(@MetaDataKeyParam(affects = MetaDataKeyParamAffectsType.INPUT) String table,
            @Default(PAYLOAD) Map<String, Object> payload) throws CassandraDBException {
        String keySpace = basicAuthConnectionStrategy.getKeyspace();
        basicAuthConnectionStrategy.getCassandraClient().delete(keySpace, table, null, (Map) payload.get(Constants.WHERE));
    }
    
    @Processor
    @MetaDataScope(CassandraMetadataCategory.class)
    public List<Map<String, Object>>  select(@Default(PAYLOAD) @Query final String query, @Optional List<Object> parameters) throws CassandraDBException {
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

    @Processor(friendlyName="Change the type of a column")
    public boolean changeColumnType(String table, @Default(PAYLOAD) ChangeColumnTypeInput input){
        String keySpace = basicAuthConnectionStrategy.getKeyspace();
        return basicAuthConnectionStrategy.getCassandraClient().changeColumnType(table, keySpace, input);
    }

    @Processor(friendlyName="Add new column")
    public boolean addNewColumn(String table, @Default(PAYLOAD) AddNewColumnInput input) {
        String keySpace = basicAuthConnectionStrategy.getKeyspace();
        DataType columnType;
        if (input.getType() instanceof Map) {
            columnType = HelperStatements.resolveDataTypeFromMap((Map) input.getType());
        } else {
            columnType = HelperStatements.resolveDataTypeFromString((String) input.getType());
        }

        return basicAuthConnectionStrategy.getCassandraClient().addNewColumn(table, keySpace, input.getColumn(), columnType);
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
