///**
// * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
// */
//package com.mulesoft.mule.cassandradb.automation.functional.processors;
//
//import com.datastax.driver.core.exceptions.InvalidQueryException;
//import com.mulesoft.mule.cassandradb.api.CassandraClient;
//import com.mulesoft.mule.cassandradb.automation.functional.CassandraDBConnectorAbstractTestCase;
//import com.mulesoft.mule.cassandradb.automation.functional.TestDataBuilder;
//import com.mulesoft.mule.cassandradb.util.ConstantsTest;
//import com.mulesoft.mule.cassandradb.utils.CassandraConfig;
//import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
//import org.junit.AfterClass;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//public class RenameColumnTestCases extends CassandraDBConnectorAbstractTestCase {
//
//    private static CassandraClient cassClient;
//    private static CassandraConfig cassConfig;
//
//    @BeforeClass
//    public static void setup() throws Exception {
//        cassConfig = getClientConfig();
//        cassClient = initialSetup(cassConfig);
//        cassClient.insert(cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME_1, TestDataBuilder.getValidEntity());
//    }
//
//    @AfterClass
//    public static void tearDown() throws Exception {
//        cassClient.dropTable(ConstantsTest.TABLE_NAME_1, cassConfig.getKeyspace());
//    }
//
//    @Test
//    public void testRenameColumnWithSuccess() throws CassandraDBException {
//        getConnector().renameColumn(ConstantsTest.TABLE_NAME_1, ConstantsTest.VALID_COLUMN_1, "renamed");
//    }
//
//
//    @Test(expected = InvalidQueryException.class)
//    public void testRenameColumnWithInvalidName() throws CassandraDBException {
//        getConnector().dropColumn(ConstantsTest.TABLE_NAME_1, ConstantsTest.COLUMN);
//    }
//
//}
