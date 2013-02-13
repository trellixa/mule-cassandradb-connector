package com.mulesoft.mule.cassandradb;
/**
 * Mule Cassandra Connector
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

/**
 * Cassandra Custom Exception
 */

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
