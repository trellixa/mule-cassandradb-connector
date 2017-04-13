/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.automation.functional;

import com.mulesoft.mule.cassandradb.api.CassandraClient;
import com.mulesoft.mule.cassandradb.util.PropertiesLoaderUtil;
import com.mulesoft.mule.cassandradb.utils.CassandraConfig;
import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
import com.mulesoft.mule.cassandradb.utils.Constants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mule.api.ConnectionException;
import org.mule.tools.devkit.ctf.exceptions.ConfigurationLoadingFailedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class UpdateTestCases extends BaseTestCases {

    private static CassandraClient cassClient;
    private static CassandraConfig cassConfig;

    @BeforeClass
    public static void setup() throws ConnectionException, CassandraDBException, IOException, ConfigurationLoadingFailedException {
        cassConfig = getClientConfig();
        cassClient = configureClient(cassConfig);
        cassClient.insert(cassConfig.getKeyspace(), Constants.TABLE_NAME, TestDataBuilder.getValidEntity());
    }

    @AfterClass
    public static void tearDown() throws CassandraDBException, ConnectionException {
        cassClient.dropTable(Constants.TABLE_NAME, cassConfig.getKeyspace());
    }


    @Test
    public void testUpdateUsingEqWithSuccess() throws CassandraDBException {
        getConnector().update(Constants.TABLE_NAME, TestDataBuilder.getValidEntityForUpdate(), TestDataBuilder.getValidWhereClauseWithEq());
    }

    @Test
    public void testUpdateUsingInWithSuccess() throws CassandraDBException {
        getConnector().update(Constants.TABLE_NAME, TestDataBuilder.getValidEntityForUpdate(), TestDataBuilder.getValidWhereClauseWithIN());
    }

    @Test(expected=CassandraDBException.class)
    public void testUpdateWithInvalidInput() throws CassandraDBException {
        getConnector().update(Constants.TABLE_NAME, TestDataBuilder.getInvalidEntity(), TestDataBuilder.getValidWhereClauseWithEq());
    }

    @Test(expected=CassandraDBException.class)
    public void testUpdateWithInvalidWhereClause() throws CassandraDBException {
        getConnector().update(Constants.TABLE_NAME, TestDataBuilder.getInvalidEntity(), TestDataBuilder.getInvalidWhereClause());
    }


}
