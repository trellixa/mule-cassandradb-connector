///**
// * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
// */
//package com.mulesoft.mule.cassandradb.automation.functional.processors;
//
//import com.mulesoft.mule.cassandradb.api.CassandraClient;
//import com.mulesoft.mule.cassandradb.automation.functional.CassandraDBConnectorAbstractTestCase;
//import com.mulesoft.mule.cassandradb.automation.functional.TestDataBuilder;
//import com.mulesoft.mule.cassandradb.metadata.CreateKeyspaceInput;
//import com.mulesoft.mule.cassandradb.util.ConstantsTest;
//import com.mulesoft.mule.cassandradb.utils.CassandraConfig;
//import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
//import org.junit.AfterClass;
//import org.junit.Assert;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import java.util.List;
//import java.util.Map;
//
//import static org.hamcrest.Matchers.greaterThan;
//
//public class ExecuteCqlQueryTestCases extends CassandraDBConnectorAbstractTestCase {
//
//    @BeforeClass
//    public static void setup() throws Exception {
//        cassConfig = getClientConfig();
//        cassClient = initialSetup(cassConfig);
//
//        //setup db env
//        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
//        keyspaceInput.setKeyspaceName(cassConfig.getKeyspace());
//        cassClient.createKeyspace(keyspaceInput);
//        cassClient.createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME_1));
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
////    @Test
////    public void shouldThrow
//
//}
