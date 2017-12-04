/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import com.datastax.driver.core.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.cassandradb.api.CQLQueryInput;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.modules.cassandradb.automation.util.TestsConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertNotNull;

public class ExecuteCqlQueryTestCases extends AbstractTestCases {

    private static String QUERY_PREFIX = "SELECT * FROM ";

    @Before
    public void setup() throws Exception {
        CreateTableInput basicCreateTableInput = TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getPrimaryKey(), getCassandraProperties().getKeyspace(), TestsConstants.TABLE_NAME_2);
        getCassandraService().createTable(basicCreateTableInput);
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_2, getCassandraProperties().getKeyspace(), TestsConstants.VALID_COLUMN_2, DataType.ascii());
        getCassandraService().insert(getCassandraProperties().getKeyspace(), TestsConstants.TABLE_NAME_2, TestDataBuilder.getValidEntity());
    }

    @After
    public void tearDown()  {
        getCassandraService().dropTable(TestsConstants.TABLE_NAME_2, getCassandraProperties().getKeyspace());
    }

    @Test
    public void shouldExecute_ValidParametrizedQuery() throws Exception {
        //Select * from dummy_table_name_2 WHERE dummy_partition_key = 'value1'
        List<Object> params = new ArrayList<Object>();
        params.add("value1");
        String whereClause = " WHERE " + TestsConstants.DUMMY_PARTITION_KEY + " = ?";
        CQLQueryInput query = new CQLQueryInput(QUERY_PREFIX + TestsConstants.TABLE_NAME_2 + whereClause, params);

        List<Map<String, Object>> queryResult = executeCQLQuery(query);
        assertNotNull(queryResult.get(0));
    }

    @Test
    public void shouldExecute_NonParametrizedQuery() throws Exception {
        CQLQueryInput query = new CQLQueryInput(QUERY_PREFIX + TestsConstants.TABLE_NAME_2, new ArrayList<Object>());
        List<Map<String, Object>> queryResult = executeCQLQuery(query);
        assertNotNull(queryResult.get(0));
    }

    @Test
    public void shouldThrowException_When_InsufficientAmountOfBindVariables() throws Exception {
        //Select * from dummy_table_name_2 WHERE dummy_partition_key = ?
        String whereClause = " WHERE " + TestsConstants.DUMMY_PARTITION_KEY + " = ?";
        CQLQueryInput query = new CQLQueryInput(QUERY_PREFIX + TestsConstants.TABLE_NAME_2 + whereClause, null);

        executeCQLQueryExpException(query);
    }

    @Test
    public void shouldThrowException_When_InsufficientAmountOfParameters() throws Exception {
        //Select * from dummy_table_name_2
        List<Object> params = new ArrayList<Object>();
        params.add("value1");
        CQLQueryInput query = new CQLQueryInput(QUERY_PREFIX + TestsConstants.TABLE_NAME_2, params);

        executeCQLQueryExpException(query);
    }
}
