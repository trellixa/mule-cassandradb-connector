package com.mulesoft.mule.cassandradb.automation.functional;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.mulesoft.mule.cassandradb.utils.CassandraDBException;


public class InsertTestCases extends BaseTestCases {
    
    private static final String DUMMY_TABLE = "dummy_table";
    private static final String VALID_COLUMN = "dummy_partitionkey";
    
      
    @Test
    public void testInsertWithSuccess() throws CassandraDBException {
        getConnector().insert(DUMMY_TABLE, getValidEntity());
    }
    
    @Test(expected=CassandraDBException.class)
    public void testInsertWithInvalidInput() throws CassandraDBException {
        getConnector().insert(DUMMY_TABLE, getInvalidEntity());
    }
    
    //TODO
    //@After delete inserted entry;
    
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
