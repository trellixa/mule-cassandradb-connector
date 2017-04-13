package com.mulesoft.mule.cassandradb.automation.functional;

import com.mulesoft.mule.cassandradb.utils.Constants;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class TestDataBuilder {

    protected static final String VALID_PARAMETERIZED_QUERY = "SELECT dummy_column FROM dummy_table WHERE dummy_partitionKey IN (?, ?)";
    protected static final String VALID_DSQL_QUERY = "dsql:SELECT dummy_column FROM dummy_table";

    private TestDataBuilder() {

    }

    @NotNull
    public static Map<String, Object> getInvalidEntity() {
        Map<String, Object> entity = new HashMap<String, Object>();
        entity.put("invalid_column", "someValue");
        return entity;
    }

    public static Map<String, Object> getValidEntity() {
        Map<String, Object> entity = new HashMap<String, Object>();
        entity.put(Constants.DUMMY_PARTITION_KEY, "value1");
        entity.put(Constants.VALID_COLUMN, "someValue" + System.currentTimeMillis());
        return entity;
    }

    public static Map<String, Object> getValidEntityForUpdate() {
        Map<String, Object> entity = new HashMap<String, Object>();
        entity.put(Constants.VALID_COLUMN, "someValue" + System.currentTimeMillis());
        return entity;
    }

    public static Map<String, Object> getValidWhereClauseWithEq() {
        Map<String, Object> entity = new HashMap<String, Object>();
        entity.put(Constants.DUMMY_PARTITION_KEY, "value1");
        return entity;
    }

    public static Map<String, Object> getValidWhereClauseWithIN() {
        Map<String, Object> entity = new HashMap<String, Object>();
        List list = new ArrayList();
        list.add("value1");
        list.add("value2");
        entity.put(Constants.DUMMY_PARTITION_KEY, list);
        return entity;
    }

    /* WHERE clause restrictions for the UPDATE are
    *   - the single-column EQ on any partition key or clustering columns
        - the single-column IN restriction on the last partition key column
    * */
    public static Map<String, Object> getInvalidWhereClause() {
        Map<String, Object> entity = new HashMap<String, Object>();
        entity.put(Constants.VALID_COLUMN, "someValue");
        return entity;
    }

    public static List<Object> getValidParmList() {
        List<Object> parameters = new LinkedList<>();
        parameters.add("value1");
        parameters.add("value2");
        return parameters;
    }
}
