///**
// * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
// */
//package com.mulesoft.mule.cassandradb.automation.functional.processors;
//
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//
//import com.mulesoft.mule.cassandradb.api.CassandraClient;
//import com.mulesoft.mule.cassandradb.automation.functional.CassandraDBConnectorAbstractTestCase;
//import com.mulesoft.mule.cassandradb.automation.functional.TestDataBuilder;
//import com.mulesoft.mule.cassandradb.util.ConstantsTest;
//import com.mulesoft.mule.cassandradb.utils.CassandraConfig;
//import org.junit.AfterClass;
//import org.junit.Assert;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
//
//import static org.hamcrest.Matchers.*;
//
//public class SelectTestCases extends CassandraDBConnectorAbstractTestCase {
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
//    public void testSelectNativeQueryWithParameters() throws CassandraDBException {
//        List<Map<String, Object>> result = getConnector().select(TestDataBuilder.VALID_PARAMETERIZED_QUERY, TestDataBuilder.getValidParmList());
//        Assert.assertThat(Integer.valueOf(result.size()),greaterThan(0));
//    }
//
//    @Test(expected=CassandraDBException.class)
//    public void testSelectNativeQueryWithInvalidParameters() throws CassandraDBException {
//        List<Map<String, Object>> result = getConnector().select(TestDataBuilder.VALID_PARAMETERIZED_QUERY, new LinkedList<Object>());
//        Assert.assertThat(Integer.valueOf(result.size()),greaterThan(0));
//    }
//
//    @Test
//    public void testSelectDSQLQuery() throws CassandraDBException {
//        List<Map<String, Object>> result = getConnector().select(TestDataBuilder.VALID_DSQL_QUERY, null);
//        Assert.assertThat(Integer.valueOf(result.size()),greaterThan(0));
//    }
//
//}
