/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.automation.functional;

import com.mulesoft.mule.cassandradb.api.CassandraClient;
import com.mulesoft.mule.cassandradb.util.ConstantsTest;
import com.mulesoft.mule.cassandradb.utils.CassandraConfig;
import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class UpdateTestCases extends CassandraDBConnectorAbstractTestCase {

    private static CassandraClient cassClient;
    private static CassandraConfig cassConfig;

    @BeforeClass
    public static void setup() throws Exception {
        cassConfig = getClientConfig();
        cassClient = configureClient(cassConfig);
        cassClient.insert(cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME, TestDataBuilder.getValidEntity());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        cassClient.dropTable(ConstantsTest.TABLE_NAME, cassConfig.getKeyspace());
    }


    @Test
    public void testUpdateUsingEqWithSuccess() throws CassandraDBException {
        getConnector().update(ConstantsTest.TABLE_NAME, null, TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getValidEntityForUpdate(),
                TestDataBuilder.getValidWhereClauseWithEq()));
    }

    @Test
    public void testUpdateUsingInWithSuccess() throws CassandraDBException {
        getConnector().update(ConstantsTest.TABLE_NAME, null, TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getValidEntityForUpdate(),
                TestDataBuilder.getValidWhereClauseWithIN()));
    }

    @Test(expected=CassandraDBException.class)
    public void testUpdateWithInvalidInput() throws CassandraDBException {
        getConnector().update(ConstantsTest.TABLE_NAME, null, TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getInvalidEntity(), TestDataBuilder.getValidWhereClauseWithEq()));
    }

    @Test(expected=CassandraDBException.class)
    public void testUpdateWithInvalidWhereClause() throws CassandraDBException {
        getConnector().update(ConstantsTest.TABLE_NAME, null, TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getInvalidEntity(), TestDataBuilder.getInvalidWhereClause()));
    }


}
