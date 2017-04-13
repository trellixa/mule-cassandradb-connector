/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.automation.functional;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.mulesoft.mule.cassandradb.api.CassandraClient;
import com.mulesoft.mule.cassandradb.utils.CassandraConfig;
import com.mulesoft.mule.cassandradb.utils.Constants;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mulesoft.mule.cassandradb.utils.CassandraDBException;

import static org.hamcrest.Matchers.*;

public class SelectTestCases extends BaseTestCases {

    private static CassandraClient cassClient;
    private static CassandraConfig cassConfig;

    @BeforeClass
    public static void setup() throws Exception {
        cassConfig = getClientConfig();
        cassClient = configureClient(cassConfig);
        cassClient.insert(cassConfig.getKeyspace(), Constants.TABLE_NAME, TestDataBuilder.getValidEntity());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        cassClient.dropTable(Constants.TABLE_NAME, cassConfig.getKeyspace());
    }

    @Test
    public void testSelectNativeQueryWithParameters() throws CassandraDBException {
        List<Map<String, Object>> result = getConnector().select(TestDataBuilder.VALID_PARAMETERIZED_QUERY, TestDataBuilder.getValidParmList());
        Assert.assertThat(Integer.valueOf(result.size()),greaterThan(0));
    }
    
    @Test(expected=CassandraDBException.class)
    public void testSelectNativeQueryWithInvalidParameters() throws CassandraDBException {
        List<Map<String, Object>> result = getConnector().select(TestDataBuilder.VALID_PARAMETERIZED_QUERY, new LinkedList<>());
        Assert.assertThat(Integer.valueOf(result.size()),greaterThan(0));
    }
    
    @Test
    public void testSelectDSQLQuery() throws CassandraDBException {
        List<Map<String, Object>> result = getConnector().select(TestDataBuilder.VALID_DSQL_QUERY, null);
        Assert.assertThat(Integer.valueOf(result.size()),greaterThan(0));
    }

}
