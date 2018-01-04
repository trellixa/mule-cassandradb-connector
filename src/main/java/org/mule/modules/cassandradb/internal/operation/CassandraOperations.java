package org.mule.modules.cassandradb.internal.operation;

import org.mule.modules.cassandradb.api.AlterColumnInput;
import org.mule.modules.cassandradb.api.CQLQueryInput;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.modules.cassandradb.internal.config.CassandraConfig;
import org.mule.modules.cassandradb.internal.connection.CassandraConnection;
import org.mule.modules.cassandradb.internal.exception.CassandraErrorTypeProvider;
import org.mule.modules.cassandradb.internal.metadata.CassandraMetadataResolver;
import org.mule.modules.cassandradb.internal.metadata.CassandraOnlyWithFiltersMetadataResolver;
import org.mule.modules.cassandradb.internal.metadata.CassandraWithFiltersMetadataCategory;
import org.mule.modules.cassandradb.internal.service.CassandraService;
import org.mule.modules.cassandradb.internal.util.DataTypeResolver;
import org.mule.modules.cassandradb.internal.util.DefaultDsqlQueryTranslator;
import org.mule.runtime.extension.api.annotation.error.Throws;
import org.mule.runtime.extension.api.annotation.metadata.MetadataKeyId;
import org.mule.runtime.extension.api.annotation.metadata.OutputResolver;
import org.mule.runtime.extension.api.annotation.metadata.TypeResolver;
import org.mule.runtime.extension.api.annotation.param.Config;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Content;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Query;
import org.mule.runtime.extension.api.annotation.param.display.Placement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.mule.modules.cassandradb.internal.util.Constants.COLUMNS;
import static org.mule.modules.cassandradb.internal.util.Constants.WHERE;


@Throws(CassandraErrorTypeProvider.class)
public class CassandraOperations extends CassandraBaseOperations {

    private static final Logger logger = LoggerFactory.getLogger(CassandraOperations.class);

    /**
     * Creates a new keyspace
     * @param input operation input containing the keyspace name and the replication details
     * @return true if the operation succeeded, false otherwise
     */
    // FIXME: Return type not required. If we fail, an exception should be thrown.
    public boolean createKeyspace(@Config CassandraConfig config,
                                  @Connection CassandraConnection connection,
                                  @Content CreateKeyspaceInput input) {
        // FIXME: Replace if block with 'logger.debug("Creating keyspace {}", input);'
        if (logger.isDebugEnabled()) {
            logger.debug("Creating keyspace " + input.toString());
        }
        return newExecutionBuilder(config, connection).execute(CassandraService::createKeyspace).withParam(input);
    }

    /**
     * Drops the entire keyspace
     * @param keyspaceName the name of the keyspace to be dropped
     * @return true if the operation succeeded, false otherwise
     */
    // FIXME: Return type not required. If we fail, an exception should be thrown.
    public boolean dropKeyspace(@Config CassandraConfig config,
                                @Connection CassandraConnection connection,
                                String keyspaceName) {
        // FIXME: Replace if block as above.
        if (logger.isDebugEnabled()) {
            logger.debug("Dropping keyspace " + keyspaceName);
        }
        return newExecutionBuilder(config, connection).execute(CassandraService::dropKeyspace).withParam(keyspaceName);
    }


    /**
     * Creates a table(column family) in a specific keyspace; If no keyspace is specified the keyspace used for login will be used
     *
     * @param createTableInput operation createTableInput describing the table name, the keyspace name and the list of columns
     * @return true if the operation succeeded, false otherwise
     */
    // FIXME: Return type not required. If we fail, an exception should be thrown.
    public boolean createTable(@Config CassandraConfig config,
                               @Connection CassandraConnection connection,
                               @Content CreateTableInput createTableInput) {
        // FIXME: Replace if block as above.
        if (logger.isDebugEnabled()) {
            logger.debug("Creating table " + createTableInput.toString());
        }
        return newExecutionBuilder(config, connection).execute(CassandraService::createTable).withParam(createTableInput);
    }

    /**
     * Drops an entire table form the specified keyspace or from the keyspace used for login if none is specified as an operation parameter
     * @param tableName the name of the table to be dropped
     * @param keyspaceName (optional) the keyspace which contains the table to be dropped
     * @return true if the operation succeeded, false otherwise
     */
    // FIXME: Return type not required. If we fail, an exception should be thrown.
    public boolean dropTable(@Config CassandraConfig config,
                             @Connection CassandraConnection connection,
                             String tableName,
                             @Optional String keyspaceName) {
        // FIXME: Replace if block as above.
        if (logger.isDebugEnabled()) {
            logger.debug("Dropping table " + tableName);
        }
        return newExecutionBuilder(config, connection).execute(CassandraService::dropTable)
                .withParam(tableName)
                .withParam(keyspaceName);
    }

    /**
     * Adds a new column
     * @param table the name of the table to be used for the operation
     * @param keyspaceName (optional) the keyspace which contains the table to be used
     * @param alterColumnInput POJO defining the name of the new column and its DataType
     * @return true if the operation succeeded or false if not
     */
    // FIXME: Return type not required. If we fail, an exception should be thrown.
    public boolean addNewColumn(@Config CassandraConfig config,
                                @Connection CassandraConnection connection,
                                String table,
                                @Optional String keyspaceName,
                                @Content AlterColumnInput alterColumnInput) {
        // FIXME: Replace if block as above.
        if (logger.isDebugEnabled()) {
            logger.debug("Adding new column " + alterColumnInput.toString());
        }
        return newExecutionBuilder(config, connection).execute(CassandraService::addNewColumn)
                .withParam(table)
                .withParam(keyspaceName)
                .withParam(alterColumnInput.getColumn())
                .withParam(DataTypeResolver.resolve(alterColumnInput.getType()));
        // FIXME: Replace last line with '.withParam(alterColumnInput.getType(), DataTypeResolver::resolve);'
    }

    /**
     * Removes a column
     * @param table the name of the table to be used for the operation
     * @param keyspaceName (optional) the keyspace which contains the table to be used
     * @param columnName the name of the column to be removed
     * @return true if the operation succeeded or false if not
     */
    // FIXME: Return type not required. If we fail, an exception should be thrown.
    public boolean dropColumn(@Config CassandraConfig config,
                              @Connection CassandraConnection connection,
                              String table,
                              @Optional String keyspaceName,
                              @Content String columnName) {
        return newExecutionBuilder(config, connection).execute(CassandraService::dropColumn)
                .withParam(table)
                .withParam(keyspaceName)
                .withParam(columnName);
    }

    /**
     * Renames a column
     * @param table the name of the table to be used for the operation
     * @param keyspaceName (optional) the keyspace which contains the table to be used
     * @param oldColumnName the name of the column to be changed
     * @param newColumnName the new value for the name of the column
     * @return true if the operation succeeded or false if not
     */
    // FIXME: Return type not required. If we fail, an exception should be thrown.
    public boolean renameColumn(@Config CassandraConfig config,
                                @Connection CassandraConnection connection,
                                String table,
                                @Optional String keyspaceName,
                                @Content String oldColumnName,
                                String newColumnName) {
        return newExecutionBuilder(config, connection).execute(CassandraService::renameColumn)
                .withParam(table)
                .withParam(keyspaceName)
                .withParam(oldColumnName)
                .withParam(newColumnName);
    }

    /**
     * Changes the type of a column - check compatibility here: <a href="http://docs.datastax.com/en/cql/3.1/cql/cql_reference/cql_data_types_c.html#concept_ds_wbk_zdt_xj__cql_data_type_compatibility">CQL type compatibility</a>
     * @param table the name of the table to be used for the operation
     * @param keyspaceName (optional) the keyspace which contains the table to be used
     * @param alterColumnInput POJO defining the name of the column to be changed and the new DataType
     * @return true if the operation succeeded or false if not
     */
    // FIXME: Return type not required. If we fail, an exception should be thrown.
    public boolean changeColumnType(@Config CassandraConfig config,
                                    @Connection CassandraConnection connection,
                                    String table,
                                    @Optional String keyspaceName,
                                    @Content AlterColumnInput alterColumnInput){
        return newExecutionBuilder(config, connection).execute(CassandraService::changeColumnType)
                .withParam(table)
                .withParam(keyspaceName)
                .withParam(alterColumnInput);
    }

    /**
     * Returns all the table names from the specified keyspace
     * @param keyspaceName the name of the keyspace to be used on the operation
     * @return a list of table names
     */
    public List<String> getTableNamesFromKeyspace(@Config CassandraConfig config,
                                                  @Connection CassandraConnection connection,
                                                  @Optional String keyspaceName) {
        return newExecutionBuilder(config, connection).execute(CassandraService::getTableNamesFromKeyspace)
                .withParam(keyspaceName);
    }

    /**
     * Executes the insert entity operation
     * @param table the table name in which the entity will be inserted
     * @param keyspaceName (optional) the keyspace which contains the table to be used
     * @param entity the entity to be inserted
     */
    public void insert(@Config CassandraConfig config,
                       @Connection CassandraConnection connection,
                       @MetadataKeyId(CassandraMetadataResolver.class) String table,
                       @Optional String keyspaceName,
                       @Content @TypeResolver(CassandraMetadataResolver.class) Map<String, Object> entity) {
        // FIXME: Replace if block as above.
        if (logger.isDebugEnabled()) {
            logger.debug("Inserting entity " + entity + " into the " + table + " table ");
        }
        newExecutionBuilder(config, connection).execute(CassandraService::insert)
                .withParam(keyspaceName)
                .withParam(table)
                .withParam(entity);
    }

    /**
     * Executes the update entity operation
     * @param table the table name in which the entity will be updated
     * @param keyspaceName (optional) the keyspace which contains the table to be dropped
     * @param entityToUpdate the entity to be updated
     */
    @SuppressWarnings("unchecked")  // FIXME: Remove /unchecked/
    public void update(@Config CassandraConfig config,
                       @Connection CassandraConnection connection,
                       @MetadataKeyId(CassandraWithFiltersMetadataCategory.class) String table,
                       @Optional String keyspaceName,
                       @Content @TypeResolver(CassandraWithFiltersMetadataCategory.class) Map<String, Object> entityToUpdate) {
        // FIXME: Replace if block as above.
        if (logger.isDebugEnabled()) {
            logger.debug("Updating  entity" + entityToUpdate + " into the " + table + " table ");
        }
        newExecutionBuilder(config, connection).execute(CassandraService::update)
                .withParam(keyspaceName)
                .withParam(table)
                .withParam((Map) entityToUpdate.get(COLUMNS))  // FIXME: Why have a single parameter for this? we should have it separated as a new parameter.
                .withParam((Map) entityToUpdate.get(WHERE)); // FIXME: Why have a single parameter for this? we should have it separated as a new parameter.
    }

    /**
     * Executes the raw input query provided
     *
     * @param cqlInput CQLQueryInput describing the parametrized query to be executed along with the parameters
     * @return the result of the query execution
     */
    @OutputResolver(output = CassandraMetadataResolver.class)
    public List<Map<String, Object>> executeCQLQuery(@Config CassandraConfig config,
                                                     @Connection CassandraConnection connection,
                                                     @Placement(tab = "Query") @Content CQLQueryInput cqlInput) {
        // FIXME: Replace if block as above.
        if (logger.isDebugEnabled()) {
            logger.debug("Executing query " + cqlInput.toString());
        }
        return newExecutionBuilder(config, connection).execute(CassandraService::executeCQLQuery)
                .withParam(cqlInput.getCqlQuery())
                .withParam(cqlInput.getParameters());
    }

    /**
     * Executes a select query
     * @param query  the query to be executed
     * @param parameters the query parameters
     * @return list of entities returned by the select query
     */
    @Query(translator = DefaultDsqlQueryTranslator.class, entityResolver = CassandraMetadataResolver.class, nativeOutputResolver = CassandraMetadataResolver.class)
    public List<Map<String, Object>>  select(@Config CassandraConfig config,
                                             @Connection CassandraConnection connection,
                                             @MetadataKeyId(CassandraMetadataResolver.class) final String query,
                                             @Optional List<Object> parameters)  {
        // FIXME: Replace if block as above.
        if (logger.isDebugEnabled()) {
            logger.debug("Executing select query: " + query + " with the parameters: " + parameters);
        }
        return newExecutionBuilder(config, connection).execute(CassandraService::select)
                .withParam(query)
                .withParam(parameters);
    }

    /**
     * Deletes an entire record
     * @param table the name of the table
     * @param keyspaceName (optional) the keyspace which contains the table to be used
     * @param payload operation input: where clause for the delete operation
     */
    public void deleteRows(@Config CassandraConfig config,
                           @Connection CassandraConnection connection,
                           @MetadataKeyId(CassandraOnlyWithFiltersMetadataResolver.class) String table,
                           @Optional String keyspaceName,
                           @Content @TypeResolver(CassandraOnlyWithFiltersMetadataResolver.class) Map<String, Object> payload) {
        newExecutionBuilder(config, connection).execute(CassandraService::delete)
                .withParam(keyspaceName)
                .withParam(table)
                .withParam(null) // FIXME: Remove hardcoded null.
                .withParam((Map) payload.get(WHERE)); // FIXME: Why are we getting the WHERE key?.
    }

    /**
     * Deletes values from an object specified by the where clause
     * @param table the name of the table
     * @param keyspaceName (optional) the keyspace which contains the table to be used
     * @param payload operation input: columns to be deleted and where clause for the delete operation
     */
    public void deleteColumnsValue(@Config CassandraConfig config,
                                   @Connection CassandraConnection connection,
                                   @MetadataKeyId(CassandraWithFiltersMetadataCategory.class) String table,
                                   @Optional String keyspaceName,
                                   @Content @TypeResolver(CassandraWithFiltersMetadataCategory.class) Map<String, Object> payload) {
        newExecutionBuilder(config, connection).execute(CassandraService::delete)
                .withParam(keyspaceName)
                .withParam(table)
                .withParam((List) payload.get(COLUMNS)) // FIXME: Why have a single parameter for this? we should have it separated as a new parameter.
                .withParam((Map) payload.get(WHERE)); // FIXME: Why have a single parameter for this? we should have it separated as a new parameter.
    }
}
