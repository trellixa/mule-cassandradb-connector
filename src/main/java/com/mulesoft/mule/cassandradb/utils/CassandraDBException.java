/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.utils;

/**
 * Generic Exception wrapper class for Cassandra Exceptions.
 */
public class CassandraDBException extends Exception {

    /**
     * Constructs a new exception with null as its detail message.
     */
    public CassandraDBException() {
    }

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param msg the detail message.
     */
    public CassandraDBException(String msg) {
        super(msg);
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param msg   the detail message.
     * @param cause the cause.
     */
    public CassandraDBException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs a new exception with the specified cause and a detail message.
     *
     * @param cause the cause
     */
    public CassandraDBException(Throwable cause) {
        super(cause);
    }
}
