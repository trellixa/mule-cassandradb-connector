/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import com.datastax.driver.core.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.modules.cassandradb.internal.exception.CassandraError;

import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.TABLE_NAME_1;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.VALID_COLUMN_2;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.VALID_LIST_COLUMN;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.VALID_MAP_COLUMN;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.VALID_SET_COLUMN;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getBasicCreateTableInput;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getColumns;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getInvalidEntityForDelete;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getInvalidWhereClause;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getPayloadColumnsAndFilters;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getValidColumnsListForDelete;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getValidEntity;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getValidEntityWithList;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getValidEntityWithMap;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getValidEntityWithSet;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getValidListItem;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getValidMapItem;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getValidSet;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getValidWhereClauseWithEq;
import static org.mule.modules.cassandradb.automation.util.TestDataBuilder.getValidWhereClauseWithIN;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.QUERY_VALIDATION;
import static org.mule.tck.junit4.matcher.ErrorTypeMatcher.errorType;

public class DeleteTestCase extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        CreateTableInput basicCreateTableInput = getBasicCreateTableInput(getColumns(), getKeyspaceFromProperties(), TABLE_NAME_1);
        getCassandraService().createTable(basicCreateTableInput);
    }

    @After
    public void tearDown() {
        getCassandraService().dropTable(TABLE_NAME_1, getKeyspaceFromProperties());
    }

    @Test
    public void testDeleteColumnUsingEqWithSuccess() throws Exception {
        getCassandraService().insert(getKeyspaceFromProperties(), TABLE_NAME_1, getValidEntity());

        Map<String, Object> payloadColumnsAndFilters = getPayloadColumnsAndFilters(getValidColumnsListForDelete(), getValidWhereClauseWithEq());
        deleteColumnsValue(TABLE_NAME_1, null, payloadColumnsAndFilters);

        Thread.sleep(SLEEP_DURATION);
        String query = format("SELECT %s FROM %s.%s", VALID_COLUMN_2, getKeyspaceFromProperties(), TABLE_NAME_1);
        String dummy_column_2 = (String) getCassandraService().select(query, null).get(0).get(VALID_COLUMN_2);
        assertNull(dummy_column_2);
    }

    @Test
    public void testDeleteRowUsingEqWithSuccess() throws Exception {
        getCassandraService().insert(getKeyspaceFromProperties(), TABLE_NAME_1, getValidEntity());

        Map<String, Object> payloadColumnsAndFilters = getPayloadColumnsAndFilters(null, getValidWhereClauseWithEq());
        deleteRows(TABLE_NAME_1, null, payloadColumnsAndFilters);

        Thread.sleep(SLEEP_DURATION);
        String query = format("SELECT * FROM %s.%s", getKeyspaceFromProperties(), TABLE_NAME_1);
        List<Map<String, Object>> select = getCassandraService().select(query, null);
        assertEquals(0, select.size());
    }

    @Test
    public void testDeleteColumnUsingINWithSuccess() throws Exception {
        getCassandraService().insert(getKeyspaceFromProperties(), TABLE_NAME_1, getValidEntity());

        Map<String, Object> payloadColumnsAndFilters = getPayloadColumnsAndFilters(getValidColumnsListForDelete(), getValidWhereClauseWithIN());
        deleteColumnsValue(TABLE_NAME_1, null, payloadColumnsAndFilters);
    }

    @Test
    public void testDeleteRowUsingINWithSuccess() throws Exception {
        getCassandraService().insert(getKeyspaceFromProperties(), TABLE_NAME_1, getValidEntity());

        Map<String, Object> payloadColumnsAndFilters = getPayloadColumnsAndFilters(null, getValidWhereClauseWithIN());
        deleteRows(TABLE_NAME_1, null, payloadColumnsAndFilters);
    }

    @Test
    public void testDeleteItemFromListWithSuccess() throws Exception {
        getCassandraService().addNewColumn(TABLE_NAME_1, getKeyspaceFromProperties(), VALID_LIST_COLUMN, DataType.list(DataType.text()));
        getCassandraService().insert(getKeyspaceFromProperties(), TABLE_NAME_1, getValidEntityWithList());

        Map<String, Object> payloadColumnsAndFilters = getPayloadColumnsAndFilters(getValidListItem(), getValidWhereClauseWithEq());
        deleteColumnsValue(TABLE_NAME_1, null, payloadColumnsAndFilters);
    }

    @Test
    public void testDeleteItemFromMapWithSuccess() throws Exception {
        getCassandraService().addNewColumn(TABLE_NAME_1, getKeyspaceFromProperties(), VALID_MAP_COLUMN, DataType.map(DataType.text(), DataType.text()));
        getCassandraService().insert(getKeyspaceFromProperties(), TABLE_NAME_1, getValidEntityWithMap());

        Map<String, Object> payloadColumnsAndFilters = getPayloadColumnsAndFilters(getValidMapItem(), getValidWhereClauseWithEq());
        deleteColumnsValue(TABLE_NAME_1, null, payloadColumnsAndFilters);
    }

    @Test
    public void testDeleteSetColumnWithSuccess() throws Exception {
        getCassandraService().addNewColumn(TABLE_NAME_1, getKeyspaceFromProperties(), VALID_SET_COLUMN, DataType.set(DataType.text()));
        getCassandraService().insert(getKeyspaceFromProperties(), TABLE_NAME_1, getValidEntityWithSet());

        Map<String, Object> payloadColumnsAndFilters = getPayloadColumnsAndFilters(getValidSet(), getValidWhereClauseWithEq());
        deleteColumnsValue(TABLE_NAME_1, null, payloadColumnsAndFilters);
    }

    @Test
    public void testDeleteWithInvalidInput() throws Exception {
        getCassandraService().insert(getKeyspaceFromProperties(), TABLE_NAME_1, getValidEntity());

        Map<String, Object> payloadColumnsAndFilters = getPayloadColumnsAndFilters(getInvalidEntityForDelete(), getValidWhereClauseWithEq());
        deleteColumnsValueExpException(TABLE_NAME_1, null, payloadColumnsAndFilters, QUERY_VALIDATION);
    }

    @Test
    public void testDeleteWithInvalidWhereClause() throws Exception {
        getCassandraService().insert(getKeyspaceFromProperties(), TABLE_NAME_1, getValidEntity());

        Map<String, Object> payloadColumnsAndFilters = getPayloadColumnsAndFilters(getValidColumnsListForDelete(), getInvalidWhereClause());
        deleteColumnsValueExpException(TABLE_NAME_1, null, payloadColumnsAndFilters, QUERY_VALIDATION);
    }

    protected void deleteColumnsValue(String tableName, String keyspaceName, Map<String, Object> payloadColumnsAndFilters) throws Exception {
        flowRunner("deleteColumnsValue-flow")
                .withPayload(payloadColumnsAndFilters)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    protected void deleteColumnsValueExpException(String tableName, String keyspaceName, Map<String, Object> payloadColumnsAndFilters, CassandraError error) throws Exception {
        flowRunner("deleteColumnsValue-flow")
                .withPayload(payloadColumnsAndFilters)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .runExpectingException(errorType(error));
    }

    protected void deleteRows(String tableName, String keyspaceName, Map<String, Object> payloadColumnsAndFilters) throws Exception {
        flowRunner("deleteRows-flow")
                .withPayload(payloadColumnsAndFilters)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }
}
