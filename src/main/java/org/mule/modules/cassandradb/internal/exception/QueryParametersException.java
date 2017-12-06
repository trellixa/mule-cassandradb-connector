package org.mule.modules.cassandradb.internal.exception;

public class QueryParametersException extends CassandraException{
    public QueryParametersException(String msg) {
        super(msg);
    }

    public CassandraError getErrorCode() {
        return CassandraError.QUERY_PARAMETERS_ERROR;
    }
}
