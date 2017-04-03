package com.mulesoft.mule.cassandradb.automation.testcases;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import com.mulesoft.mule.cassandradb.automation.AutomationTestParent;
import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
import static org.hamcrest.Matchers.*;

public class SelectTestCases extends AutomationTestParent {
    
    private static final String VALID_PARAMETERIZED_QUERY = "SELECT dummy_partitionkey FROM dummy_table WHERE dummy_partitionkey IN (?, ?)";
    private static final String VALID_DSQL_QUERY = "dsql:SELECT dummy_partitionkey FROM dummy_table";
    
    @Test
    public void testSelectNativeQueryWithParameters() throws CassandraDBException {
        List<Map<String, Object>> result = connector.select(VALID_PARAMETERIZED_QUERY, getValidParmList());
        Assert.assertThat(Integer.valueOf(result.size()),greaterThan(0));
    }
    
    @Test(expected=CassandraDBException.class)
    public void testSelectNativeQueryWithInvalidParameters() throws CassandraDBException {
        List<Map<String, Object>> result = connector.select(VALID_PARAMETERIZED_QUERY, new LinkedList<>());
        Assert.assertThat(Integer.valueOf(result.size()),greaterThan(0));
    }
    
    @Test
    public void testSelectDSQLQuery() throws CassandraDBException {
        List<Map<String, Object>> result = connector.select(VALID_DSQL_QUERY, null);
        Assert.assertThat(Integer.valueOf(result.size()),greaterThan(0));
    }

    private List<Object> getValidParmList() {
        List<Object> parameters = new LinkedList<>();
        parameters.add("value2");
        parameters.add("value1");
        return parameters;
    }
    
}
