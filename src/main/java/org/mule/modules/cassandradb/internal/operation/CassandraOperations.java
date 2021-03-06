/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.internal.operation;

import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.BootstrappingException;
import com.datastax.driver.core.exceptions.BusyConnectionException;
import com.datastax.driver.core.exceptions.BusyPoolException;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.core.exceptions.ConnectionException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.FrameTooLongException;
import com.datastax.driver.core.exceptions.FunctionExecutionException;
import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.datastax.driver.core.exceptions.PagingStateException;
import com.datastax.driver.core.exceptions.ProtocolError;
import com.datastax.driver.core.exceptions.QueryConsistencyException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.exceptions.ReadFailureException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.ServerError;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.exceptions.TraceRetrievalException;
import com.datastax.driver.core.exceptions.TransportException;
import com.datastax.driver.core.exceptions.TruncateException;
import com.datastax.driver.core.exceptions.UnauthorizedException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.exceptions.UnpreparedException;
import com.datastax.driver.core.exceptions.UnresolvedUserTypeException;
import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import com.datastax.driver.core.exceptions.UnsupportedProtocolVersionException;
import com.datastax.driver.core.exceptions.WriteFailureException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import org.mule.modules.cassandradb.api.AlterColumnInput;
import org.mule.modules.cassandradb.api.CQLQueryInput;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.modules.cassandradb.internal.connection.CassandraConnection;
import org.mule.modules.cassandradb.internal.exception.CassandraException;
import org.mule.modules.cassandradb.internal.metadata.CassandraMetadataResolver;
import org.mule.modules.cassandradb.internal.metadata.CassandraOnlyWithFiltersMetadataResolver;
import org.mule.modules.cassandradb.internal.metadata.CassandraWithFiltersMetadataCategory;
import org.mule.modules.cassandradb.internal.service.CassandraServiceImpl;
import org.mule.modules.cassandradb.internal.util.DataTypeResolver;
import org.mule.modules.cassandradb.internal.util.DefaultDsqlQueryTranslator;
import org.mule.connectors.atlantic.commons.builder.config.exception.DefinedExceptionHandler;
import org.mule.connectors.atlantic.commons.builder.execution.ExecutionBuilder;
import org.mule.connectors.commons.template.operation.ConnectorOperations;
import org.mule.modules.cassandradb.internal.config.CassandraConfig;
import org.mule.modules.cassandradb.internal.exception.CassandraError;
import org.mule.modules.cassandradb.internal.exception.CassandraErrorTypeProvider;
import org.mule.modules.cassandradb.internal.service.CassandraService;
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
import org.mule.runtime.extension.api.exception.ModuleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

@Throws(CassandraErrorTypeProvider.class)
public class CassandraOperations extends ConnectorOperations<CassandraConfig, CassandraConnection, CassandraService> {

    private static final Logger logger = LoggerFactory.getLogger(CassandraOperations.class);

    public CassandraOperations() {
        super(CassandraServiceImpl::new);
    }

    /**
     * Creates a new keyspace
     *
     * @param config     The configuration of the connector.
     * @param connection The connection to CassandraDB.
     * @param input      operation input containing the keyspace name and the replication details
     */
    public void createKeyspace(@Config CassandraConfig config,
                               @Connection CassandraConnection connection,
                               @Content CreateKeyspaceInput input) {
        logger.debug("Creating keyspace {}", input);
        newExecutionBuilder(config, connection).execute(CassandraService::createKeyspace).withParam(input);
    }

    /**
     * Drops the entire keyspace
     *
     * @param config     The configuration of the connector.
     * @param connection The connection to CassandraDB.
     * @param keyspaceName the name of the keyspace to be dropped
     */
    public void dropKeyspace(@Config CassandraConfig config,
                             @Connection CassandraConnection connection,
                             @Content String keyspaceName) {
        logger.debug("Dropping keyspace {}", keyspaceName);
        newExecutionBuilder(config, connection).execute(CassandraService::dropKeyspace).withParam(keyspaceName);
    }


    /**
     * Creates a table(column family) in a specific keyspace; If no keyspace is specified the keyspace used for login will be used
     *
     * @param config     The configuration of the connector.
     * @param connection The connection to CassandraDB.
     * @param createTableInput operation createTableInput describing the table name, the keyspace name and the list of columns
     */
    public void createTable(@Config CassandraConfig config,
                            @Connection CassandraConnection connection,
                            @Content CreateTableInput createTableInput) {
        logger.debug("Creating table {}", createTableInput);
        newExecutionBuilder(config, connection).execute(CassandraService::createTable).withParam(createTableInput);
    }

    /**
     * Drops an entire table form the specified keyspace or from the keyspace used for login if none is specified as an operation parameter
     *
     * @param config     The configuration of the connector.
     * @param connection The connection to CassandraDB.
     * @param tableName    the name of the table to be dropped
     * @param keyspaceName (optional) the keyspace which contains the table to be dropped
     */
    public void dropTable(@Config CassandraConfig config,
                          @Connection CassandraConnection connection,
                          @Content String tableName,
                          @Optional String keyspaceName) {
        logger.debug("Dropping table {}", tableName);
        newExecutionBuilder(config, connection).execute(CassandraService::dropTable).withParam(tableName).withParam(keyspaceName);
    }

    /**
     * Adds a new column
     *
     * @param config     The configuration of the connector.
     * @param connection The connection to CassandraDB.
     * @param table            the name of the table to be used for the operation
     * @param keyspaceName     (optional) the keyspace which contains the table to be used
     * @param alterColumnInput POJO defining the name of the new column and its DataType
     */
    public void addNewColumn(@Config CassandraConfig config,
                             @Connection CassandraConnection connection,
                             String table,
                             @Optional String keyspaceName,
                             @Content AlterColumnInput alterColumnInput) {
        logger.debug("Adding new column {}", alterColumnInput);
        newExecutionBuilder(config, connection).execute(CassandraService::addNewColumn)
                .withParam(table)
                .withParam(keyspaceName)
                .withParam(alterColumnInput.getColumn())
                .withParam(alterColumnInput.getType(), DataTypeResolver::resolve);
    }

    /**
     * Removes a column
     *
     * @param config     The configuration of the connector.
     * @param connection The connection to CassandraDB.
     * @param table        the name of the table to be used for the operation
     * @param keyspaceName (optional) the keyspace which contains the table to be used
     * @param columnName   the name of the column to be removed
     */
    public void dropColumn(@Config CassandraConfig config,
                           @Connection CassandraConnection connection,
                           String table,
                           @Optional String keyspaceName,
                           @Content String columnName) {
        newExecutionBuilder(config, connection).execute(CassandraService::dropColumn)
                .withParam(table)
                .withParam(keyspaceName)
                .withParam(columnName);
    }

    /**
     * Renames a column
     *
     * @param config     The configuration of the connector.
     * @param connection The connection to CassandraDB.
     * @param table         the name of the table to be used for the operation
     * @param keyspaceName  (optional) the keyspace which contains the table to be used
     * @param oldColumnName the name of the column to be changed
     * @param newColumnName the new value for the name of the column
     */
    public void renameColumn(@Config CassandraConfig config,
                             @Connection CassandraConnection connection,
                             String table,
                             @Optional String keyspaceName,
                             @Content String oldColumnName,
                             String newColumnName) {
        newExecutionBuilder(config, connection).execute(CassandraService::renameColumn)
                .withParam(table)
                .withParam(keyspaceName)
                .withParam(oldColumnName)
                .withParam(newColumnName);
    }

    /**
     * Changes the type of a column - check compatibility here: <a href="http://docs.datastax.com/en/cql/3.1/cql/cql_reference/cql_data_types_c.html#concept_ds_wbk_zdt_xj__cql_data_type_compatibility">CQL type compatibility</a>
     *
     * @param config     The configuration of the connector.
     * @param connection The connection to CassandraDB.
     * @param table            the name of the table to be used for the operation
     * @param keyspaceName     (optional) the keyspace which contains the table to be used
     * @param alterColumnInput POJO defining the name of the column to be changed and the new DataType
     */
    public void changeColumnType(@Config CassandraConfig config,
                                 @Connection CassandraConnection connection,
                                 String table,
                                 @Optional String keyspaceName,
                                 @Content AlterColumnInput alterColumnInput) {
        newExecutionBuilder(config, connection).execute(CassandraService::changeColumnType)
                .withParam(table)
                .withParam(keyspaceName)
                .withParam(alterColumnInput);
    }

    /**
     * Returns all the table names from the specified keyspace
     *
     * @param config     The configuration of the connector.
     * @param connection The connection to CassandraDB.
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
     *
     * @param config     The configuration of the connector.
     * @param connection The connection to CassandraDB.
     * @param table          the table name in which the entity will be inserted
     * @param keyspaceName   (optional) the keyspace which contains the table to be used
     * @param entityToInsert the entity to be inserted
     */
    public void insert(@Config CassandraConfig config,
                       @Connection CassandraConnection connection,
                       @MetadataKeyId(CassandraMetadataResolver.class) String table,
                       @Optional String keyspaceName,
                       @Content @TypeResolver(CassandraMetadataResolver.class) Map<String, Object> entityToInsert) {
        logger.debug("Inserting entity {} into the {} table ", entityToInsert, table);
        newExecutionBuilder(config, connection).execute(CassandraService::insert)
                .withParam(keyspaceName)
                .withParam(table)
                .withParam(entityToInsert);
    }

    /**
     * Executes the update entity operation
     *
     * @param config     The configuration of the connector.
     * @param connection The connection to CassandraDB.
     * @param table          the table name in which the entity will be updated
     * @param keyspaceName   (optional) the keyspace which contains the table to be dropped
     * @param entityToUpdate the entity to be updated
     */
    public void update(@Config CassandraConfig config,
                       @Connection CassandraConnection connection,
                       @MetadataKeyId(CassandraWithFiltersMetadataCategory.class) String table,
                       @Optional String keyspaceName,
                       @Content @TypeResolver(CassandraWithFiltersMetadataCategory.class) Map<String, Object> entityToUpdate) {
        logger.debug("Updating entity {} into the {} table ", entityToUpdate, table);

        newExecutionBuilder(config, connection).execute(CassandraService::update)
                .withParam(keyspaceName)
                .withParam(table)
                .withParam(entityToUpdate);
    }

    /**
     * Executes the raw input query provided
     *
     * @param config     The configuration of the connector.
     * @param connection The connection to CassandraDB.
     * @param cqlInput CQLQueryInput describing the parametrized query to be executed along with the parameters
     * @return the result of the query execution
     */
    @OutputResolver(output = CassandraMetadataResolver.class)
    public List<Map<String, Object>> executeCQLQuery(@Config CassandraConfig config,
                                                     @Connection CassandraConnection connection,
                                                     @Placement(tab = "Query") @Content CQLQueryInput cqlInput) {
        logger.debug("Executing query  {}", cqlInput);

        return newExecutionBuilder(config, connection).execute(CassandraService::executeCQLQuery)
                .withParam(cqlInput.getCqlQuery())
                .withParam(cqlInput.getParameters());
    }

    /**
     * Executes a select query
     *
     * @param config     The configuration of the connector.
     * @param connection The connection to CassandraDB.
     * @param query      the query to be executed
     * @param parameters the query parameters
     * @return list of entities returned by the select query
     */
    @Query(translator = DefaultDsqlQueryTranslator.class, entityResolver = CassandraMetadataResolver.class, nativeOutputResolver = CassandraMetadataResolver.class)
    public List<Map<String, Object>> select(@Config CassandraConfig config,
                                            @Connection CassandraConnection connection,
                                            @Content @MetadataKeyId(CassandraMetadataResolver.class) final String query,
                                            @Optional List<Object> parameters) {
        logger.debug("Executing select query: {} with the parameters: {}", query, parameters);
        return newExecutionBuilder(config, connection).execute(CassandraService::select)
                .withParam(query)
                .withParam(parameters);
    }

    /**
     * Deletes an entire record
     *
     * @param config     The configuration of the connector.
     * @param connection The connection to CassandraDB.
     * @param table        the name of the table
     * @param keyspaceName (optional) the keyspace which contains the table to be used
     * @param whereClause  operation input: where clause for the delete operation
     */
    public void deleteRows(@Config CassandraConfig config,
                           @Connection CassandraConnection connection,
                           @MetadataKeyId(CassandraOnlyWithFiltersMetadataResolver.class) String table,
                           @Optional String keyspaceName,
                           @Content Map<String, Object> whereClause) {
        newExecutionBuilder(config, connection).execute(CassandraService::deleteWithoutEntity)
                .withParam(keyspaceName)
                .withParam(table)
                .withParam(whereClause);
    }

    /**
     * Deletes values from an object specified by the where clause
     *
     * @param config     The configuration of the connector.
     * @param connection The connection to CassandraDB.
     * @param table        the name of the table
     * @param keyspaceName (optional) the keyspace which contains the table to be used
     * @param entities     operation input: columns to be deleted
     * @param whereClause: where clause for the delete operation
     */
    public void deleteColumnsValue(@Config CassandraConfig config,
                                   @Connection CassandraConnection connection,
                                   @MetadataKeyId(CassandraWithFiltersMetadataCategory.class) String table,
                                   @Optional String keyspaceName,
                                   List<String> entities,
                                   @Content Map<String, Object> whereClause) {
        newExecutionBuilder(config, connection).execute(CassandraService::delete)
                .withParam(keyspaceName)
                .withParam(table)
                .withParam(entities)
                .withParam(whereClause);
    }

    @Override
    protected ExecutionBuilder<CassandraService> newExecutionBuilder(CassandraConfig config, CassandraConnection connection) {
        return super.newExecutionBuilder(config, connection)
                .withExceptionHandler(handle(Exception.class, CassandraError.UNKNOWN))
                .withExceptionHandler(handle(AlreadyExistsException.class, CassandraError.ALREADY_EXISTS))
                .withExceptionHandler(handle(AuthenticationException.class, CassandraError.AUTHENTICATION))
                .withExceptionHandler(handle(BootstrappingException.class, CassandraError.BOOTSTRAPPING))
                .withExceptionHandler(handle(BusyConnectionException.class, CassandraError.BUSY_CONNECTION))
                .withExceptionHandler(handle(BusyPoolException.class, CassandraError.BUSY_POOL))
                .withExceptionHandler(handle(CodecNotFoundException.class, CassandraError.CODEC_NOT_FOUND))
                .withExceptionHandler(handle(ConnectionException.class, CassandraError.CONNECTION))
                .withExceptionHandler(handle(DriverInternalError.class, CassandraError.DRIVER_INTERNAL_ERROR))
                .withExceptionHandler(handle(FrameTooLongException.class, CassandraError.FRAME_TOO_LONG))
                .withExceptionHandler(handle(FunctionExecutionException.class, CassandraError.FUNCTION_EXECUTION))
                .withExceptionHandler(handle(InvalidConfigurationInQueryException.class, CassandraError.INVALID_CONFIGURATION_IN_QUERY))
                .withExceptionHandler(handle(InvalidQueryException.class, CassandraError.INVALID_QUERY))
                .withExceptionHandler(handle(InvalidTypeException.class, CassandraError.INVALID_TYPE))
                .withExceptionHandler(handle(NoHostAvailableException.class, CassandraError.NO_HOST_AVAILABLE))
                .withExceptionHandler(handle(OperationTimedOutException.class, CassandraError.OPERATION_TIMED_OUT))
                .withExceptionHandler(handle(OverloadedException.class, CassandraError.OVERLOADED))
                .withExceptionHandler(handle(PagingStateException.class, CassandraError.PAGING_STATE))
                .withExceptionHandler(handle(ProtocolError.class, CassandraError.PROTOCOL_ERROR))
                .withExceptionHandler(handle(QueryConsistencyException.class, CassandraError.QUERY_CONSISTENCY))
                .withExceptionHandler(handle(QueryExecutionException.class, CassandraError.QUERY_EXECUTION))
                .withExceptionHandler(handle(QueryValidationException.class, CassandraError.QUERY_VALIDATION))
                .withExceptionHandler(handle(ReadFailureException.class, CassandraError.READ_FAILURE))
                .withExceptionHandler(handle(ReadTimeoutException.class, CassandraError.READ_TIMEOUT))
                .withExceptionHandler(handle(ServerError.class, CassandraError.SERVERE_RROR))
                .withExceptionHandler(handle(SyntaxError.class, CassandraError.SYNTAX_ERROR))
                .withExceptionHandler(handle(TraceRetrievalException.class, CassandraError.TRACE_RETRIEVAL))
                .withExceptionHandler(handle(TransportException.class, CassandraError.TRANSPORT))
                .withExceptionHandler(handle(TruncateException.class, CassandraError.TRUNCATE))
                .withExceptionHandler(handle(UnauthorizedException.class, CassandraError.UNAUTHORIZED))
                .withExceptionHandler(handle(UnavailableException.class, CassandraError.UNAVAILABLE))
                .withExceptionHandler(handle(UnpreparedException.class, CassandraError.UNPREPARED))
                .withExceptionHandler(handle(UnresolvedUserTypeException.class, CassandraError.UNRESOLVED_USER_TYPE))
                .withExceptionHandler(handle(UnsupportedFeatureException.class, CassandraError.UNSUPPORTED_FEATURE))
                .withExceptionHandler(handle(UnsupportedProtocolVersionException.class, CassandraError.UNSUPPORTED_PROTOCOL_VERSION))
                .withExceptionHandler(handle(WriteFailureException.class, CassandraError.WRITE_FAILURE))
                .withExceptionHandler(handle(WriteTimeoutException.class, CassandraError.WRITE_TIMEOUT))
                .withExceptionHandler(handleCassandraException());
    }

    private <T extends Throwable> DefinedExceptionHandler<T> handle(Class<T> exceptionClass, CassandraError errorCode) {
        return new DefinedExceptionHandler<>(exceptionClass, exception -> {
            throw new ModuleException(errorCode, exception);
        });
    }

    private DefinedExceptionHandler<CassandraException> handleCassandraException() {
        return new DefinedExceptionHandler<>(CassandraException.class, exception -> {
            throw new ModuleException(exception.getErrorCode(), exception);
        });
    }
}
