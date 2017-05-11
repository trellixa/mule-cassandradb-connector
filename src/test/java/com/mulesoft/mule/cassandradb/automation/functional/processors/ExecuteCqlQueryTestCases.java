/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.automation.functional.processors;

import com.datastax.driver.core.DataType;
import com.mulesoft.mule.cassandradb.automation.functional.CassandraDBConnectorAbstractTestCase;
import com.mulesoft.mule.cassandradb.automation.functional.TestDataBuilder;
import com.mulesoft.mule.cassandradb.metadata.CQLQueryInput;
import com.mulesoft.mule.cassandradb.util.ConstantsTest;
import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.IsNull.notNullValue;

public class ExecuteCqlQueryTestCases extends CassandraDBConnectorAbstractTestCase {

    private static String QUERY_PREFIX = "SELECT * FROM ";

    @BeforeClass
    public static void setup() throws Exception {
        cassClient.createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getPrimaryKey(), cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME_2));
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME_2, cassConfig.getKeyspace(), ConstantsTest.VALID_COLUMN_2, DataType.ascii());
        cassClient.insert(cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME_2, TestDataBuilder.getValidEntity());
    }

    @AfterClass
    public static void tearDown()  {
        cassClient.dropTable(ConstantsTest.TABLE_NAME_2, cassConfig.getKeyspace());
    }

    @Test
    public void shouldAllowValidParametrizedQuery() throws CassandraDBException {
        //Select * from dummy_table_name_2 WHERE dummy_partition_key = 'value1'
        List<Object> params = new ArrayList<Object>();
        params.add("value1");
        String whereClause = " WHERE " + ConstantsTest.DUMMY_PARTITION_KEY + " = ?";
        CQLQueryInput query = new CQLQueryInput(QUERY_PREFIX + ConstantsTest.TABLE_NAME_2 + whereClause, params);

        List<Map<String, Object>> queryResult = getConnector().executeCQLQuery(query);
        Assert.assertThat(queryResult.get(0), notNullValue());
    }

    @Test
    public void shouldAllowNonParametrizedQuery() throws CassandraDBException {
        List<Map<String, Object>> queryResult = getConnector().executeCQLQuery(new CQLQueryInput(QUERY_PREFIX + ConstantsTest.TABLE_NAME_2, new ArrayList<Object>()));
        Assert.assertThat(queryResult.get(0), notNullValue());
    }

    @Test(expected = CassandraDBException.class)
    public void shouldThrowExceptionForInsufficientAmountOfBindVariables() throws CassandraDBException {
        //Select * from dummy_table_name_2 WHERE dummy_partition_key = ?
        String whereClause = " WHERE " + ConstantsTest.DUMMY_PARTITION_KEY + " = ?";
        CQLQueryInput query = new CQLQueryInput(QUERY_PREFIX + ConstantsTest.TABLE_NAME_2 + whereClause, null);

        getConnector().executeCQLQuery(query);
    }

    @Test(expected = CassandraDBException.class)
    public void shouldThrowExceptionForInsufficientAmountOfParameters() throws CassandraDBException {
        //Select * from dummy_table_name_2
        List<Object> params = new ArrayList<Object>();
        params.add("value1");
        CQLQueryInput query = new CQLQueryInput(QUERY_PREFIX + ConstantsTest.TABLE_NAME_2, params);

        getConnector().executeCQLQuery(query);
    }
}
