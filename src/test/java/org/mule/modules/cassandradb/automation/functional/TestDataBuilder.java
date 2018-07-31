/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import com.google.common.collect.ImmutableList;
import org.mule.modules.cassandradb.api.AlterColumnInput;
import org.mule.modules.cassandradb.api.ColumnInput;
import org.mule.modules.cassandradb.api.ColumnType;
import org.mule.modules.cassandradb.api.CreateTableInput;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.getProperty;
import static java.util.Optional.ofNullable;

public class TestDataBuilder {

    public final static String KEYSPACE_DUMMY = "dummy_keyspace";
    public final static String KEYSPACE_NAME_1 = "keyspaceName1";
    public final static String  KEYSPACE_NAME_2 = "keyspaceName2";
    public final static String KEYSPACE_NAME_3 = "keyspaceName3";
    public final static String DATA_CENTER_NAME = "datacenter1";
    public final static String TABLE_NAME_1 = "dummy_table_name_1";
    public final static String TABLE_NAME_2 = "dummy_table_name_2";
    public final static String TABLE_FAKE = "fakeTable";
    public static final String VALID_COLUMN_1 = "dummy_column_1";
    public static final String VALID_COLUMN_2 = "dummy_column_2";
    public static final String VALID_LIST_COLUMN = "dummy_list_column";
    public static final String VALID_MAP_COLUMN = "dummy_map_column";
    public static final String VALID_SET_COLUMN = "dummy_set_column";
    public static final String DUMMY_PARTITION_KEY = "dummy_partitionKey";
    public static final String COLUMNS = "columns";
    public static final String WHERE = "where";
    public static final String COLUMN = "column";
    public static final String CLUSTER_NAME = "newClusterName";
    public static final int MAX_WAIT = 10;
    public static List<String> cassandraCategoryMetadataTestKeys = new LinkedList<String>();
    public static final String metadataKeyName = "dummy_table_name_2";
    public static final String insertFlowName = "insert-flow";
    public static final String deleteRowsFlowName = "deleteRows-flow";
    public static final String updateFlowName = "update-flow";
    public static final String DELETE_QUERY = "SELECT * FROM %s.%s";
    public static final String QUERY_PREFIX = "SELECT * FROM ";
    public static final String VALID_PARAMETERIZED_QUERY =
            "SELECT " + VALID_COLUMN_2 +
                    " FROM " + KEYSPACE_DUMMY +"."+ TABLE_NAME_1 +
                    " WHERE " + TestDataBuilder.DUMMY_PARTITION_KEY + " IN (?, ?)";
    public static final String VALID_DSQL_QUERY = "dsql:" +
            "SELECT " + VALID_COLUMN_2 +
            " FROM " + KEYSPACE_DUMMY +"."+ TABLE_NAME_1;

    public static final String UPDATED_VALUE = "updatedValue";

    public static final String INVALID_COLUMN_MESSAGE_ERROR = "Undefined column name invalid_column.";

    static {
        cassandraCategoryMetadataTestKeys.add(TABLE_NAME_2);
    }

    private TestDataBuilder() {

    }

    public static String getRandomColumnName(){
        return format("TEXT%d",currentTimeMillis());
    }

    public static Map<String, Object> getInvalidEntity() {
        Map<String, Object> entity = new HashMap<>();
        entity.put("invalid_column", "someValue");
        return entity;
    }

    public static Map<String, Object> getValidEntity() {
        Map<String, Object> entity = new HashMap<>();
        entity.put(DUMMY_PARTITION_KEY, "value1");
        entity.put(VALID_COLUMN_2, "someValue" + System.currentTimeMillis());
        return entity;
    }

    public static Map<String, Object> getValidEntityForUpdate(String newValue) {
        Map<String, Object> entity = new HashMap<>();
        entity.put(VALID_COLUMN_2, newValue);
        return entity;
    }

    public static Map<String, Object> getValidWhereClauseWithEq() {
        Map<String, Object> entity = new HashMap<>();
        entity.put(DUMMY_PARTITION_KEY, "value1");
        return entity;
    }

    public static Map<String, Object> getValidWhereClauseWithIN() {
        Map<String, Object> entity = new HashMap<>();
        List list = new ArrayList();
        list.add("value1");
        list.add("value2");
        entity.put(DUMMY_PARTITION_KEY, list);
        return entity;
    }

    /* WHERE clause restrictions for the UPDATE are
    *   - the single-column EQ on any partition key or clustering columns
        - the single-column IN restriction on the last partition key column
    * */
    public static Map<String, Object> getInvalidWhereClause() {
        Map<String, Object> entity = new HashMap<>();
        entity.put(VALID_COLUMN_1, "someValue");
        return entity;
    }

    public static List<Object> getValidParmList() {
        List<Object> parameters = new LinkedList<>();
        parameters.add("value1");
        parameters.add("value2");
        return parameters;
    }

    public static List<String> getValidColumnsListForDelete() {
        List<String> parameters = new LinkedList<>();
        parameters.add(VALID_COLUMN_1);
        parameters.add(VALID_COLUMN_2);
        return parameters;
    }

    public static List<String> getInvalidEntityForDelete() {
        List<String> entity = new ArrayList<>();
        entity.add("invalid_column");
        return entity;
    }

    public static Map<String, Object> getValidEntityWithList() {
        Map<String, Object> entity = new HashMap<>();
        List<String> list = new ArrayList<>();
        list.add("firstValue");
        list.add("secondValue");
        entity.put(DUMMY_PARTITION_KEY, "value1");
        entity.put(VALID_LIST_COLUMN, list);
        entity.put(VALID_COLUMN_1, 1);
        return entity;
    }

    public static List<String> getValidListItem() {
        List<String> entity = new ArrayList<>();
        entity.add(VALID_LIST_COLUMN + "[0]");
        return entity;
    }

    public static Map<String, Object> getValidEntityWithMap() {
        Map<String, Object> entity = new HashMap<>();
        Map<String, Object> item = new HashMap<>();
        item.put("1", "firstValue");
        item.put("2", "secondValue");
        entity.put(DUMMY_PARTITION_KEY, "value1");
        entity.put(VALID_MAP_COLUMN, item);
        entity.put(VALID_COLUMN_1, 1);
        return entity;
    }

    public static List<String> getValidMapItem() {
        List<String> entity = new ArrayList<>();
        entity.add(VALID_MAP_COLUMN + "['firstValue']");
        return entity;
    }

    public static Map<String, Object> getValidEntityWithSet() {
        Map<String, Object> entity = new HashMap<>();
        Set<String> item = new HashSet<String>();
        item.add("firstValue");
        item.add("secondValue");
        entity.put(DUMMY_PARTITION_KEY, "value1");
        entity.put(VALID_SET_COLUMN, item);
        entity.put(VALID_COLUMN_1, 1);
        return entity;
    }

    public static List<String> getValidSet() {
        List<String> entity = new ArrayList<>();
        entity.add(VALID_SET_COLUMN);
        return entity;
    }

    public static Map<String, Object> getPayloadColumnsAndFilters(Object entity, Map<String, Object> whereClause){
        Map<String, Object> payload = new HashMap<>();
        payload.put(COLUMNS, entity);
        payload.put(WHERE, whereClause);
        return payload;
    }

    public static List<ColumnInput> getPrimaryKey(){
        List<ColumnInput> columns = new ArrayList<ColumnInput>();
        columns.add(createColumn(true, DUMMY_PARTITION_KEY, ColumnType.TEXT));
        return columns;
    }

    public static List<ColumnInput> getMetadataColumns(){
        List<ColumnInput> columns = new ArrayList<ColumnInput>();
        columns.add(createColumn(true, "NUMBER", ColumnType.INT));
        columns.add(createColumn(false, "TEXT", ColumnType.TEXT));
        columns.add(createColumn(false, "BOOL", ColumnType.BOOLEAN));
        return columns;
    }

    public static ColumnInput createColumn(boolean isPrimary, String name, ColumnType type){
        ColumnInput column = new ColumnInput();
        column.setPrimaryKey(isPrimary);
        column.setName(name);
        column.setType(type);
        return column;
    }

    public static List<ColumnInput> getCompositePrimaryKey(){
        List<ColumnInput> columns = new ArrayList<>();

        ColumnInput column = new ColumnInput();
        column.setPrimaryKey(true);
        column.setName(DUMMY_PARTITION_KEY);
        column.setType(ColumnType.TEXT);

        ColumnInput column2 = new ColumnInput();
        column2.setPrimaryKey(true);
        column2.setName(VALID_COLUMN_1);
        column2.setType(ColumnType.INT);

        columns.add(column);
        columns.add(column2);

        return columns;
    }

    public static List<ColumnInput> getColumns(){
        List<ColumnInput> columns = new ArrayList<ColumnInput>();

        ColumnInput column1 = new ColumnInput();
        column1.setPrimaryKey(true);
        column1.setName(DUMMY_PARTITION_KEY);
        column1.setType(ColumnType.TEXT);

        ColumnInput column2 = new ColumnInput();
        column2.setName(VALID_COLUMN_1);
        column2.setType(ColumnType.INT);

        ColumnInput column3 = new ColumnInput();
        column3.setName(VALID_COLUMN_2);
        column3.setType(ColumnType.TEXT);

        columns.add(column1);
        columns.add(column2);
        columns.add(column3);

        return columns;
    }

    public static CreateTableInput getBasicCreateTableInput(List<ColumnInput> columns, String keyspaceName, String tableName){
        CreateTableInput input = new CreateTableInput();
        input.setColumns(columns);
        input.setKeyspaceName(keyspaceName);
        input.setTableName(tableName);
        return input;
    }

    public static AlterColumnInput getAlterColumnInput(String columnName, ColumnType type){
        AlterColumnInput result = new AlterColumnInput();
        result.setColumn(columnName);
        result.setType(type);
        return result;
    }

    public static Map<String, String> getRenameColumnInput(String oldName, String newName){
        Map<String, String> result = new HashMap<String, String>();
        result.put(oldName, newName);
        return result;
    }

    protected static Properties getAutomationCredentialsProperties() throws IOException {
        Properties properties = new Properties();
        String automationFile = format("%s/src/test/resources/%s", getProperty("user.dir"),
                ofNullable(getProperty("automation-credentials.properties")).orElse("automation-credentials.properties"));
        try (InputStream inputStream = new FileInputStream(automationFile)) {
            properties.load(inputStream);
        } catch (IOException e) {
            throw new FileNotFoundException(format("property file '%s' not found in the classpath", automationFile));
        }
        return properties;
    }

    protected static List<ColumnType[]> getTestTypes(){
        return ImmutableList.<ColumnType[]>builder()
                .add(getColumnTypesTuple(ColumnType.BLOB, ColumnType.TEXT))
                .add(getColumnTypesTuple(ColumnType.BLOB, ColumnType.ASCII))
                .add(getColumnTypesTuple(ColumnType.BLOB, ColumnType.INT))
                .add(getColumnTypesTuple(ColumnType.BLOB, ColumnType.TIMESTAMP))
                .add(getColumnTypesTuple(ColumnType.BLOB, ColumnType.TIMEUUID))
                .add(getColumnTypesTuple(ColumnType.BLOB, ColumnType.VARCHAR))
                .add(getColumnTypesTuple(ColumnType.TEXT, ColumnType.INT))
                .add(getColumnTypesTuple(ColumnType.TEXT, ColumnType.TIMESTAMP))
                .add(getColumnTypesTuple(ColumnType.TEXT, ColumnType.TIMEUUID))
                .add(getColumnTypesTuple(ColumnType.TEXT, ColumnType.BIGINT))
                .add(getColumnTypesTuple(ColumnType.TEXT, ColumnType.VARINT))
                .add(getColumnTypesTuple(ColumnType.TEXT, ColumnType.UUID))
                .add(getColumnTypesTuple(ColumnType.VARCHAR, ColumnType.INT))
                .add(getColumnTypesTuple(ColumnType.VARCHAR, ColumnType.TIMESTAMP))
                .add(getColumnTypesTuple(ColumnType.VARCHAR, ColumnType.ASCII))
                .add(getColumnTypesTuple(ColumnType.TIMESTAMP, ColumnType.ASCII))
                .add(getColumnTypesTuple(ColumnType.TIMESTAMP, ColumnType.TEXT))
                .add(getColumnTypesTuple(ColumnType.TIMESTAMP, ColumnType.INT))
                .add()
                .build();
    }

    private static ColumnType[] getColumnTypesTuple(ColumnType first, ColumnType second){
        return new ColumnType[]{ first, second };
    }
}
