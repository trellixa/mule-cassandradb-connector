/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional.processors;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.DataType;
import org.mule.modules.cassandradb.automation.functional.TestDataBuilder;
import org.mule.modules.cassandradb.automation.util.TestsConstants;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.mule.modules.cassandradb.utils.CassandraDBException;

import static org.hamcrest.Matchers.*;

public class SelectTestCases extends CassandraDBConnectorAbstractTestCases {

    public static final String VALID_PARAMETERIZED_QUERY =
            "SELECT " + TestsConstants.VALID_COLUMN_2 +
            " FROM " + TestsConstants.TABLE_NAME_1 +
            " WHERE " + TestsConstants.DUMMY_PARTITION_KEY + " IN (?, ?)";
    public static final String VALID_DSQL_QUERY = "dsql:" +
            "SELECT " + TestsConstants.VALID_COLUMN_2 +
            " FROM " + TestsConstants.TABLE_NAME_1;

    @BeforeClass
    public static void setup() throws Exception {
        cassClient.createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getPrimaryKey(), cassConfig.getKeyspace(), TestsConstants.TABLE_NAME_1));
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), TestsConstants.VALID_COLUMN_2, DataType.text());
        cassClient.insert(cassConfig.getKeyspace(), TestsConstants.TABLE_NAME_1, TestDataBuilder.getValidEntity());
    }

    @AfterClass
    public static void tearDown() {
        cassClient.dropTable(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace());
    }

    @Test
    public void testSelectNativeQueryWithParameters() throws CassandraDBException {
        List<Map<String, Object>> result = getConnector().select(VALID_PARAMETERIZED_QUERY, TestDataBuilder.getValidParmList());
        Assert.assertThat(Integer.valueOf(result.size()),greaterThan(0));
    }

    @Test(expected=CassandraDBException.class)
    public void testSelectNativeQueryWithInvalidParameters() throws CassandraDBException {
        List<Map<String, Object>> result = getConnector().select(VALID_PARAMETERIZED_QUERY, new LinkedList<Object>());
        Assert.assertThat(Integer.valueOf(result.size()),greaterThan(0));
    }

    @Test
    public void testSelectDSQLQuery() throws CassandraDBException {
        List<Map<String, Object>> result = getConnector().select(VALID_DSQL_QUERY, null);
        Assert.assertThat(Integer.valueOf(result.size()),greaterThan(0));
    }

}
