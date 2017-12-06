package org.mule.modules.cassandradb.internal.exception;

import org.mule.runtime.extension.api.error.ErrorTypeDefinition;
import org.mule.runtime.extension.api.error.MuleErrors;

import java.util.Optional;

import static java.util.Optional.ofNullable;
import static org.mule.runtime.extension.api.error.MuleErrors.ANY;

public enum CassandraError implements ErrorTypeDefinition<CassandraError> {
    CassandraExecution(ANY),
    ALREADY_EXISTS(CassandraExecution),
    AUTHENTICATION(CassandraExecution),
    BOOTSTRAPPING(CassandraExecution),
    BUSY_CONNECTION(CassandraExecution),
    BUSY_POOL(CassandraExecution),
    CODEC_NOT_FOUND(CassandraExecution),
    CONNECTION(CassandraExecution),
    DRIVER_INTERNAL_ERROR(CassandraExecution),
    FRAME_TOO_LONG(CassandraExecution),
    FUNCTION_EXECUTION(CassandraExecution),
    INVALID_CONFIGURATION_IN_QUERY(CassandraExecution),
    INVALID_QUERY(CassandraExecution),
    INVALID_TYPE(CassandraExecution),
    NO_HOST_AVAILABLE(CassandraExecution),
    OPERATION_TIMED_OUT(CassandraExecution),
    OVERLOADED(CassandraExecution),
    PAGING_STATE(CassandraExecution),
    PROTOCOL_ERROR(CassandraExecution),
    QUERY_CONSISTENCY(CassandraExecution),
    QUERY_EXECUTION(CassandraExecution),
    QUERY_VALIDATION(CassandraExecution),
    READ_FAILURE(CassandraExecution),
    READ_TIMEOUT(CassandraExecution),
    SERVERE_RROR(CassandraExecution),
    SYNTAX_ERROR(CassandraExecution),
    TRACE_RETRIEVAL(CassandraExecution),
    TRANSPORT(CassandraExecution),
    TRUNCATE(CassandraExecution),
    UNAUTHORIZED(CassandraExecution),
    UNAVAILABLE(CassandraExecution),
    UNPREPARED(CassandraExecution),
    UNRESOLVED_USER_TYPE(CassandraExecution),
    UNSUPPORTED_FEATURE(CassandraExecution),
    UNSUPPORTED_PROTOCOL_VERSION(CassandraExecution),
    WRITE_FAILURE(CassandraExecution),
    WRITE_TIMEOUT(CassandraExecution),


    CassandraException(ANY),
    QUERY_ERROR(CassandraException),

    CONNECTIVITY(MuleErrors.CONNECTIVITY),
    UNKNOWN(CONNECTIVITY);

    private ErrorTypeDefinition<?> parentErrorType;

    CassandraError(final ErrorTypeDefinition parentErrorType) {
        this.parentErrorType = parentErrorType;
    }

    @Override
    public Optional<ErrorTypeDefinition<? extends Enum<?>>> getParent() {
        return ofNullable(parentErrorType);
    }
}
