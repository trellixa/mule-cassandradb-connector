/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.utils;

public class Constants {

    public final static String REPLICATION_FACTOR = "replication_factor";

    //test specific constants
    public final static String SECOND_KEYSPACE_NAME = "second_dummy_keyspace";
    public final static String TABLE_NAME = "dummy_table";
    public static final String VALID_COLUMN = "dummy_column";
    public static final String DUMMY_PARTITION_KEY = "dummy_partitionKey";
    public static final String PARTITION_KEY_COLUMN_NAME = "partitionKeyColumnName";
    public static final String DATA_TYPE = "dataType";

    //configuration
    public final static String CASS_HOST = "config.host";
    public final static String CASS_PORT = "config.port";
    public final static String KEYSPACE_NAME = "config.keyspace";

    //query specific constants
    public static final String PARAM_HOLDER = "?";
    public static final String SELECT = "SELECT";

}
