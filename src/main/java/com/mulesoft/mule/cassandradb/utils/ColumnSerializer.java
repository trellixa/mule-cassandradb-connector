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
 * Pair of values used to serialize columns
 */
public class ColumnSerializer {

    /**
     * The column name that needs to be serialized
     */
    String key;

    /**
     * The type of serializer that will be used to encode the data retrieved from Cassandra DB
     */
    String type;

    /**
     * Default Constructor
     */
    public ColumnSerializer() {
        key = null;
        type = null;
    }

    /**
     * Parametrized Constructor
     *
     * @param key  The column name that needs to be serialized
     * @param type The type of serializer that will be used to encode the data retrieved from Cassandra DB
     */
    public ColumnSerializer(String key, String type) {
        this.key = key;
        this.type = type;
    }

    /**
     * Retrieves the key
     *
     * @return the result is a key
     */
    public String getKey() {
        return key;
    }

    /**
     * Sets key
     *
     * @param key The column name as key.
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * Retrieves type
     *
     * @return the result is a type of serializer.
     */
    public String getType() {
        return type;
    }


    /**
     * Sets type
     *
     * @param type The type of serializer that will be used to encode the data
     */
    public void setType(String type) {
        this.type = type;
    }
}
