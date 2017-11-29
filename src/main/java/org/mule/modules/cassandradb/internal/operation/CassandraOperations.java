package org.mule.modules.cassandradb.internal.operation;

import org.mule.connectors.atlantic.commons.builder.config.exception.DefinedExceptionHandler;
import org.mule.connectors.atlantic.commons.builder.execution.ExecutionBuilder;
import org.mule.connectors.atlantic.commons.builder.lambda.function.BiFunction;
import org.mule.connectors.commons.template.operation.ConnectorOperations;
import org.mule.modules.cassandradb.internal.config.CassandraConfig;
import org.mule.modules.cassandradb.internal.connection.CassandraConnection;
import org.mule.modules.cassandradb.internal.exception.CassandraError;
import org.mule.modules.cassandradb.internal.exception.CassandraException;
import org.mule.modules.cassandradb.internal.operation.parameters.CreateKeyspaceInput;
import org.mule.modules.cassandradb.internal.service.CassandraService;
import org.mule.modules.cassandradb.internal.service.CassandraServiceImpl;
import org.mule.runtime.extension.api.annotation.param.Config;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Content;
import org.mule.runtime.extension.api.exception.ModuleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private <T extends Throwable> DefinedExceptionHandler<T> handle(Class<T> exceptionClass, CassandraError errorCode) {
        return new DefinedExceptionHandler<>(exceptionClass, exception -> {
            throw new ModuleException(errorCode, exception);
        });
    }
}
