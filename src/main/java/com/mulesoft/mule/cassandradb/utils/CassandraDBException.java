/**
 * Mule Cassandra Connector
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.mulesoft.mule.cassandradb.utils;

/**
 * Generic Exception wrapper class for Thrift Exceptions.
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
