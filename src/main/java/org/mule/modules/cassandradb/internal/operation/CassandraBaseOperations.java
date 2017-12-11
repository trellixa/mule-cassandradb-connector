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
import org.mule.connectors.atlantic.commons.builder.config.exception.DefinedExceptionHandler;
import org.mule.connectors.atlantic.commons.builder.execution.ExecutionBuilder;
import org.mule.connectors.commons.template.operation.ConnectorOperations;
import org.mule.modules.cassandradb.internal.config.CassandraConfig;
import org.mule.modules.cassandradb.internal.connection.CassandraConnection;
import org.mule.modules.cassandradb.internal.exception.CassandraError;
import org.mule.modules.cassandradb.internal.exception.CassandraException;
import org.mule.modules.cassandradb.internal.service.CassandraService;
import org.mule.modules.cassandradb.internal.service.CassandraServiceImpl;
import org.mule.runtime.extension.api.exception.ModuleException;

import static org.mule.modules.cassandradb.internal.exception.CassandraError.ALREADY_EXISTS;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.AUTHENTICATION;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.BOOTSTRAPPING;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.BUSY_CONNECTION;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.BUSY_POOL;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.CODEC_NOT_FOUND;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.CONNECTION;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.DRIVER_INTERNAL_ERROR;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.FRAME_TOO_LONG;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.FUNCTION_EXECUTION;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.INVALID_CONFIGURATION_IN_QUERY;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.INVALID_QUERY;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.INVALID_TYPE;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.NO_HOST_AVAILABLE;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.OPERATION_TIMED_OUT;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.OVERLOADED;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.PAGING_STATE;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.PROTOCOL_ERROR;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.QUERY_CONSISTENCY;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.QUERY_EXECUTION;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.QUERY_VALIDATION;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.READ_FAILURE;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.READ_TIMEOUT;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.SERVERE_RROR;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.SYNTAX_ERROR;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.TRACE_RETRIEVAL;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.TRANSPORT;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.TRUNCATE;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.UNAUTHORIZED;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.UNAVAILABLE;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.UNKNOWN;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.UNPREPARED;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.UNRESOLVED_USER_TYPE;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.UNSUPPORTED_FEATURE;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.UNSUPPORTED_PROTOCOL_VERSION;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.WRITE_FAILURE;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.WRITE_TIMEOUT;

public class CassandraBaseOperations extends ConnectorOperations<CassandraConfig, CassandraConnection, CassandraService> {

    public CassandraBaseOperations() {
        super(CassandraServiceImpl::new);
    }

    @Override
    protected ExecutionBuilder<CassandraService> newExecutionBuilder(CassandraConfig config, CassandraConnection connection) {
        return super.newExecutionBuilder(config, connection)
                .withExceptionHandler(handle(Exception.class, UNKNOWN))
                .withExceptionHandler(handle(AlreadyExistsException.class, ALREADY_EXISTS))
                .withExceptionHandler(handle(AuthenticationException.class, AUTHENTICATION))
                .withExceptionHandler(handle(BootstrappingException.class, BOOTSTRAPPING))
                .withExceptionHandler(handle(BusyConnectionException.class, BUSY_CONNECTION))
                .withExceptionHandler(handle(BusyPoolException.class, BUSY_POOL))
                .withExceptionHandler(handle(CodecNotFoundException.class, CODEC_NOT_FOUND))
                .withExceptionHandler(handle(ConnectionException.class, CONNECTION))
                .withExceptionHandler(handle(DriverInternalError.class, DRIVER_INTERNAL_ERROR))
                .withExceptionHandler(handle(FrameTooLongException.class, FRAME_TOO_LONG))
                .withExceptionHandler(handle(FunctionExecutionException.class, FUNCTION_EXECUTION))
                .withExceptionHandler(handle(InvalidConfigurationInQueryException.class, INVALID_CONFIGURATION_IN_QUERY))
                .withExceptionHandler(handle(InvalidQueryException.class, INVALID_QUERY))
                .withExceptionHandler(handle(InvalidTypeException.class, INVALID_TYPE))
                .withExceptionHandler(handle(NoHostAvailableException.class, NO_HOST_AVAILABLE))
                .withExceptionHandler(handle(OperationTimedOutException.class, OPERATION_TIMED_OUT))
                .withExceptionHandler(handle(OverloadedException.class, OVERLOADED))
                .withExceptionHandler(handle(PagingStateException.class, PAGING_STATE))
                .withExceptionHandler(handle(ProtocolError.class, PROTOCOL_ERROR))
                .withExceptionHandler(handle(QueryConsistencyException.class, QUERY_CONSISTENCY))
                .withExceptionHandler(handle(QueryExecutionException.class, QUERY_EXECUTION))
                .withExceptionHandler(handle(QueryValidationException.class, QUERY_VALIDATION))
                .withExceptionHandler(handle(ReadFailureException.class, READ_FAILURE))
                .withExceptionHandler(handle(ReadTimeoutException.class, READ_TIMEOUT))
                .withExceptionHandler(handle(ServerError.class, SERVERE_RROR))
                .withExceptionHandler(handle(SyntaxError.class, SYNTAX_ERROR))
                .withExceptionHandler(handle(TraceRetrievalException.class, TRACE_RETRIEVAL))
                .withExceptionHandler(handle(TransportException.class, TRANSPORT))
                .withExceptionHandler(handle(TruncateException.class, TRUNCATE))
                .withExceptionHandler(handle(UnauthorizedException.class, UNAUTHORIZED))
                .withExceptionHandler(handle(UnavailableException.class, UNAVAILABLE))
                .withExceptionHandler(handle(UnpreparedException.class, UNPREPARED))
                .withExceptionHandler(handle(UnresolvedUserTypeException.class, UNRESOLVED_USER_TYPE))
                .withExceptionHandler(handle(UnsupportedFeatureException.class, UNSUPPORTED_FEATURE))
                .withExceptionHandler(handle(UnsupportedProtocolVersionException.class, UNSUPPORTED_PROTOCOL_VERSION))
                .withExceptionHandler(handle(WriteFailureException.class, WRITE_FAILURE))
                .withExceptionHandler(handle(WriteTimeoutException.class, WRITE_TIMEOUT))
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
