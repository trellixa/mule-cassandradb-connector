package org.mule.modules.cassandradb.internal.operation;

import org.mule.connectors.atlantic.commons.builder.config.exception.DefinedExceptionHandler;
import org.mule.connectors.atlantic.commons.builder.execution.ExecutionBuilder;
import org.mule.connectors.atlantic.commons.builder.lambda.function.BiFunction;
import org.mule.connectors.commons.template.operation.ConnectorOperations;
import org.mule.modules.cassandradb.internal.config.CassandraConfig;
import org.mule.modules.cassandradb.internal.connection.CassandraConnection;
import org.mule.modules.cassandradb.internal.exception.CassandraError;
import org.mule.modules.cassandradb.internal.exception.CassandraException;
import org.mule.modules.cassandradb.internal.service.CassandraService;
import org.mule.modules.cassandradb.internal.service.CassandraServiceImpl;
import org.mule.runtime.extension.api.exception.ModuleException;

import static org.mule.modules.cassandradb.internal.exception.CassandraError.UNKNOWN;

public class CassandraBaseOperarations extends ConnectorOperations<CassandraConfig, CassandraConnection, CassandraService> {

    public CassandraBaseOperarations() {
        super(CassandraServiceImpl::new);
    }

    @Override
    protected ExecutionBuilder<CassandraService> newExecutionBuilder(CassandraConfig config, CassandraConnection connection) {
        return super.newExecutionBuilder(config, connection)
                .withExceptionHandler(handle(Exception.class, UNKNOWN))
                .withExceptionHandler(handleCassandraException());
//                .withExceptionHandler(CassandraException.class, exception -> new ModuleException(exception.getErrorCode(), exception));
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
