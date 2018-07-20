package com.mulesoft.connectors.cassandradb.internal.exception;

public class QueryErrorException extends CassandraException{
    public QueryErrorException(String msg) {
        super(msg);
    }

    public CassandraError getErrorCode() {
        return CassandraError.QUERY_ERROR;
    }
}
