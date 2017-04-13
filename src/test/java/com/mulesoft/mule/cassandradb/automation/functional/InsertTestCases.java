/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.automation.functional;

import com.mulesoft.mule.cassandradb.api.CassandraClient;
import com.mulesoft.mule.cassandradb.utils.CassandraConfig;
import com.mulesoft.mule.cassandradb.utils.Constants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mulesoft.mule.cassandradb.utils.CassandraDBException;

public class InsertTestCases extends BaseTestCases {

    private static CassandraClient cassClient;
    private static CassandraConfig cassConfig;

    @BeforeClass public static void setup() throws Exception {
        cassConfig = getClientConfig();
        cassClient = configureClient(cassConfig);
    }

    @AfterClass public static void tearDown() throws Exception {
        cassClient.dropTable(Constants.TABLE_NAME, cassConfig.getKeyspace());
    }

    @Test public void testInsertWithSuccess() throws CassandraDBException {
        getConnector().insert(Constants.TABLE_NAME, TestDataBuilder.getValidEntity());
    }

    @Test(expected = CassandraDBException.class) public void testInsertWithInvalidInput() throws CassandraDBException {
        getConnector().insert(Constants.TABLE_NAME, TestDataBuilder.getInvalidEntity());
    }

}
