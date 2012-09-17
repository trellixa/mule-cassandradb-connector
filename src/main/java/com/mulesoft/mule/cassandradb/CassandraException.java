package com.mulesoft.mule.cassandradb;


public class CassandraException extends RuntimeException {

    public CassandraException() {
    }

    public CassandraException(String s) {
        super(s);
    }

    public CassandraException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public CassandraException(Throwable throwable) {
        super(throwable);
    }
}
