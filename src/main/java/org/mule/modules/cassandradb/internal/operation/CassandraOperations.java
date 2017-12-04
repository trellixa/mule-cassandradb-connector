package org.mule.modules.cassandradb.internal.operation;

import org.mule.connectors.atlantic.commons.builder.config.exception.DefinedExceptionHandler;
import org.mule.connectors.atlantic.commons.builder.execution.ExecutionBuilder;
import org.mule.connectors.commons.template.operation.ConnectorOperations;
import org.mule.modules.cassandradb.api.AlterColumnInput;
import org.mule.modules.cassandradb.api.ColumnType;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.modules.cassandradb.internal.config.CassandraConfig;
import org.mule.modules.cassandradb.internal.connection.CassandraConnection;
import org.mule.modules.cassandradb.internal.exception.CassandraError;
import org.mule.modules.cassandradb.internal.exception.CassandraException;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.internal.service.CassandraService;
import org.mule.modules.cassandradb.internal.service.CassandraServiceImpl;
import org.mule.runtime.extension.api.annotation.param.Config;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Content;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.exception.ModuleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.mule.modules.cassandradb.internal.exception.CassandraError.UNKNOWN;

public class CassandraOperations extends ConnectorOperations<CassandraConfig, CassandraConnection, CassandraService> {

    public CassandraOperations() {
        super(CassandraServiceImpl::new);
    }

    private static final Logger logger = LoggerFactory.getLogger(CassandraOperations.class);

    @Override
    protected ExecutionBuilder<CassandraService> newExecutionBuilder(CassandraConfig config, CassandraConnection connection) {
        return super.newExecutionBuilder(config, connection)
                .withExceptionHandler(handle(Exception.class, UNKNOWN))
                .withExceptionHandler(CassandraException.class, exception -> new ModuleException(exception.getErrorCode(), exception));
    }

    /**
     * Creates a new keyspace
     * @param input operation input containing the keyspace name and the replication details
     * @return true if the operation succeeded, false otherwise
     */
    public boolean createKeyspace(@Config CassandraConfig config,
                                  @Connection CassandraConnection connection,
                                  @Content CreateKeyspaceInput input) {
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
    public boolean dropKeyspace(@Config CassandraConfig config,
                                @Connection CassandraConnection connection,
                                String keyspaceName) {
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
    public boolean createTable(@Config CassandraConfig config,
                               @Connection CassandraConnection connection,
                               @Content CreateTableInput createTableInput) {
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
    public boolean dropTable(@Config CassandraConfig config,
                             @Connection CassandraConnection connection,
                             String tableName,
                             @Optional String keyspaceName) {
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
    public boolean addNewColumn(@Config CassandraConfig config,
                                @Connection CassandraConnection connection,
                                String table,
                                @Optional String keyspaceName,
                                @Content AlterColumnInput alterColumnInput) {
        if (logger.isDebugEnabled()) {
            logger.debug("Adding new column " + alterColumnInput.toString());
        }
        return newExecutionBuilder(config, connection).execute(CassandraService::addNewColumn)
                .withParam(table)
                .withParam(keyspaceName)
                .withParam(alterColumnInput.getColumn())
                .withParam(ColumnType.resolveDataType(alterColumnInput.getType()));
    }

    /**
     * Removes a column
     * @param table the name of the table to be used for the operation
     * @param keyspaceName (optional) the keyspace which contains the table to be used
     * @param columnName the name of the column to be removed
     * @return true if the operation succeeded or false if not
     */
    public boolean dropColumn(@Config CassandraConfig config,
                              @Connection CassandraConnection connection,
                              String table, @Optional String keyspaceName, @Content String columnName) {
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
    public boolean renameColumn(@Config CassandraConfig config,
                                @Connection CassandraConnection connection,
                                String table, @Optional String keyspaceName, @Content String oldColumnName, String newColumnName) {
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
    public boolean changeColumnType(@Config CassandraConfig config,
                                    @Connection CassandraConnection connection,
                                    String table, @Optional String keyspaceName, @Content AlterColumnInput alterColumnInput){
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
                       String table,
                       @Optional String keyspaceName,
                       @Content Map<String, Object> entity) {
        if (logger.isDebugEnabled()) {
            logger.debug("Inserting entity " + entity + " into the " + table + " table ");
        }
        newExecutionBuilder(config, connection).execute(CassandraService::insert)
                .withParam(keyspaceName)
                .withParam(table)
                .withParam(entity);
    }

    private <T extends Throwable> DefinedExceptionHandler<T> handle(Class<T> exceptionClass, CassandraError errorCode) {
        return new DefinedExceptionHandler<>(exceptionClass, exception -> {
            throw new ModuleException(errorCode, exception);
        });
    }
}
