package org.mule.modules.cassandradb.internal.exception;

import org.mule.runtime.extension.api.error.ErrorTypeDefinition;
import org.mule.runtime.extension.api.error.MuleErrors;

import java.util.Optional;

import static java.util.Optional.ofNullable;

public enum CassandraError implements ErrorTypeDefinition<CassandraError> {
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
