/**
 * Mule Cassandra Connector
 * <p>
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * <p>
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.mulesoft.mule.cassandradb;

import com.datastax.driver.core.ResultSet;
import com.mulesoft.mule.cassandradb.configurations.BasicAuthConnectionStrategy;
import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
import com.mulesoft.mule.cassandradb.utils.Constants;
import org.junit.*;
import org.junit.runners.MethodSorters;
import org.mule.api.ConnectionException;
import org.mule.util.CollectionUtils;

import java.util.List;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CassandraDBConnectorMockTest {

    private static CassandraDBConnector connector = new CassandraDBConnector();
    private static BasicAuthConnectionStrategy strategy = new BasicAuthConnectionStrategy();

    @BeforeClass
    public static void init() throws ConnectionException, CassandraDBException {
        strategy.setHost("127.0.0.1");
        strategy.setPort(9042);
        strategy.connect(null, null);

        connector.setBasicAuthConnectionStrategy(strategy);

        //create a keyspace to be further used
        connector.createKeyspace(Constants.KEYSPACE_NAME, null);
    }

    @AfterClass
    public static void tearDown() throws CassandraDBException {
        connector.dropKeyspace(Constants.KEYSPACE_NAME);
        strategy.getCassandraClient().close();
    }

    @Test
    public void shouldCreateTable() throws CassandraDBException {
        //create a table
        connector.createTable(Constants.TABLE_NAME, Constants.KEYSPACE_NAME, null);
        Assert.assertTrue(verifyTableCreation(Constants.TABLE_NAME, Constants.KEYSPACE_NAME));
    }


    @Test
    public void shouldDropTable() throws CassandraDBException {
        connector.dropTable(Constants.TABLE_NAME, Constants.KEYSPACE_NAME);
        Assert.assertFalse(verifyTableCreation(Constants.TABLE_NAME, Constants.KEYSPACE_NAME));
    }

    @Test
    public void shouldDescribeKeyspaces() throws CassandraDBException {
        ResultSet resultSet = connector.executeCQLQuery("SELECT * FROM system_schema.keyspaces");
        Assert.assertTrue(resultSet.all().size() > 0);
    }

    /**
     * helper method used to verify is a table was created in a specific keyspace
     */
    private boolean verifyTableCreation(String tableToVerify, String keyspaceName) {
        List<String> tableNames = strategy.getCassandraClient().getTableNamesFromKeyspace(keyspaceName);
        return CollectionUtils.isNotEmpty(tableNames) && tableNames.contains(tableToVerify);
    }
}
