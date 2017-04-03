package com.mulesoft.mule.cassandradb.automation.testcases;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;


import com.mulesoft.mule.cassandradb.automation.AutomationTestParent;
import com.mulesoft.mule.cassandradb.utils.CassandraDBException;

public class InsertTestCases extends AutomationTestParent {

    private static final String DUMMY_TABLE = "dummy_table";
    private static final String VALID_COLUMN = "dummy_partitionkey";
    
    @Test
    public void testInsertWithSuccess() throws CassandraDBException {
        connector.insert(DUMMY_TABLE, getValidEntity());
    }
    
    @Test(expected=CassandraDBException.class)
    public void testInsertWithInvalidInput() throws CassandraDBException {
        connector.insert(DUMMY_TABLE, getInvalidEntity());
    }
    
    public Map<String, Object> getInvalidEntity(){
        Map<String, Object> entity = new HashMap<String,Object>();
        entity.put("invalid_column", "someValue");
        return entity;
    }   
    
    public Map<String, Object> getValidEntity(){
        Map<String, Object> entity = new HashMap<String,Object>();
        entity.put(VALID_COLUMN, "someValue");
        return entity;
    }
    
}
