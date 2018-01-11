///**
// * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
// */
//package org.mule.modules.cassandradb.automation.functional;
//
//import com.datastax.driver.core.DataType;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//import org.mule.modules.cassandradb.api.CreateTableInput;
//import org.mule.modules.cassandradb.internal.exception.CassandraError;
//
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//
//import static org.hamcrest.Matchers.greaterThan;
//import static org.junit.Assert.assertThat;
//import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.TABLE_NAME_1;
//import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.VALID_COLUMN_2;
//import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getBasicCreateTableInput;
//import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidEntity;
//import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidParmList;
//import static org.mule.modules.cassandradb.internal.exception.CassandraError.QUERY_ERROR;
//import static org.mule.tck.junit4.matcher.ErrorTypeMatcher.errorType;
//
//public class SelectTestCase extends AbstractTestCases {
//
//    public static final String VALID_PARAMETERIZED_QUERY =
//            "SELECT " + VALID_COLUMN_2 +
//            " FROM " + TABLE_NAME_1 +
//            " WHERE " + TestDataBuilder.DUMMY_PARTITION_KEY + " IN (?, ?)";
//    public static final String VALID_DSQL_QUERY = "dsql:" +
//            "SELECT " + VALID_COLUMN_2 +
//            " FROM " + TABLE_NAME_1;
//
//    @Before
//    public void setup() throws Exception {
//        CreateTableInput basicCreateTableInput = getBasicCreateTableInput(TestDataBuilder.getPrimaryKey(), getKeyspaceFromProperties(), TABLE_NAME_1);
//        getCassandraService().createTable(basicCreateTableInput);
//        getCassandraService().addNewColumn(TABLE_NAME_1, getKeyspaceFromProperties(), VALID_COLUMN_2, DataType.text());
//        getCassandraService().insert(getKeyspaceFromProperties(), TABLE_NAME_1, getValidEntity());
//    }
//
//    @After
//    public void tearDown() {
//        getCassandraService().dropTable(TABLE_NAME_1, getKeyspaceFromProperties());
//    }
//
//    @Test
//    public void testSelectNativeQueryWithParameters() throws Exception {
//        List<Map<String, Object>> result = select(VALID_PARAMETERIZED_QUERY, getValidParmList());
//        assertThat(Integer.valueOf(result.size()),greaterThan(0));
//    }
//
//    @Test
//    public void testSelectNativeQueryWithInvalidParameters() throws Exception {
//        selectExpException(VALID_PARAMETERIZED_QUERY, new LinkedList<>(), QUERY_ERROR);
//    }
//
//    @Test
//    public void testSelectDSQLQuery() throws Exception {
//        List<Map<String, Object>> result = select(VALID_DSQL_QUERY, null);
//        assertThat(Integer.valueOf(result.size()),greaterThan(0));
//    }
//
//    protected List<Map<String, Object>> select(String validParameterizedQuery, List<Object> validParmList) throws Exception {
//        return (List<Map<String, Object>>) flowRunner("select-flow")
//                .withPayload(validParameterizedQuery)
//                .withVariable("parameters", validParmList)
//                .run()
//                .getMessage()
//                .getPayload()
//                .getValue();
//    }
//
//
//    protected void selectExpException(String validParameterizedQuery, List<Object> validParmList, CassandraError error) throws Exception {
//        flowRunner("select-flow")
//                .withPayload(validParameterizedQuery)
//                .withVariable("parameters", validParmList)
//                .runExpectingException(errorType(error));
//    }
//}
