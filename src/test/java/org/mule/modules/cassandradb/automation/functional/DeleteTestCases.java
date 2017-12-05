/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import com.datastax.driver.core.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.modules.cassandradb.automation.util.TestsConstants;

import java.util.Map;

import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getColumns;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getInvalidEntityForDelete;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getInvalidWhereClause;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getPayloadColumnsAndFilters;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidColumnsListForDelete;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidEntity;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidEntityWithSet;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidListItem;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidMapItem;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidSet;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidWhereClauseWithEq;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidWhereClauseWithIN;
import static org.mule.modules.cassandradb.automation.util.TestsConstants.TABLE_NAME_1;

public class DeleteTestCases extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        CreateTableInput basicCreateTableInput = TestDataBuilder.getBasicCreateTableInput(getColumns(), getCassandraProperties().getKeyspace(), TABLE_NAME_1);
        getCassandraService().createTable(basicCreateTableInput);
    }

    @After
    public void tearDown() {
        getCassandraService().dropTable(TABLE_NAME_1, getCassandraProperties().getKeyspace());
    }

    @Test
    public void testDeleteColumnUsingEqWithSuccess() throws Exception {
        getCassandraService().insert(getCassandraProperties().getKeyspace(), TABLE_NAME_1, getValidEntity());

        Map<String, Object> payloadColumnsAndFilters = getPayloadColumnsAndFilters(getValidColumnsListForDelete(), getValidWhereClauseWithEq());
        deleteColumnsValue(TABLE_NAME_1, null, payloadColumnsAndFilters);
    }

    @Test
    public void testDeleteRowUsingEqWithSuccess() throws Exception {
        getCassandraService().insert(getCassandraProperties().getKeyspace(), TABLE_NAME_1, getValidEntity());

        Map<String, Object> payloadColumnsAndFilters = getPayloadColumnsAndFilters(null, getValidWhereClauseWithEq());
        deleteRows(TABLE_NAME_1, null, payloadColumnsAndFilters);
    }

    @Test
    public void testDeleteColumnUsingINWithSuccess() throws Exception {
        getCassandraService().insert(getCassandraProperties().getKeyspace(), TABLE_NAME_1, getValidEntity());

        Map<String, Object> payloadColumnsAndFilters = getPayloadColumnsAndFilters(getValidColumnsListForDelete(), getValidWhereClauseWithIN());
        deleteColumnsValue(TABLE_NAME_1, null, payloadColumnsAndFilters);
    }

    @Test
    public void testDeleteRowUsingINWithSuccess() throws Exception {
        getCassandraService().insert(getCassandraProperties().getKeyspace(), TABLE_NAME_1, getValidEntity());

        Map<String, Object> payloadColumnsAndFilters = getPayloadColumnsAndFilters(null, getValidWhereClauseWithIN());
        deleteRows(TABLE_NAME_1, null, payloadColumnsAndFilters);
    }

    @Test
    public void testDeleteItemFromListWithSuccess() throws Exception {
        getCassandraService().addNewColumn(TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestsConstants.VALID_LIST_COLUMN, DataType.list(DataType.text()));
        getCassandraService().insert(getCassandraProperties().getKeyspace(), TABLE_NAME_1, TestDataBuilder.getValidEntityWithList());

        Map<String, Object> payloadColumnsAndFilters = getPayloadColumnsAndFilters(getValidListItem(), getValidWhereClauseWithEq());
        deleteColumnsValue(TABLE_NAME_1, null, payloadColumnsAndFilters);
    }

    @Test
    public void testDeleteItemFromMapWithSuccess() throws Exception {
        getCassandraService().addNewColumn(TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestsConstants.VALID_MAP_COLUMN, DataType.map(DataType.text(), DataType.text()));
        getCassandraService().insert(getCassandraProperties().getKeyspace(), TABLE_NAME_1, TestDataBuilder.getValidEntityWithMap());

        Map<String, Object> payloadColumnsAndFilters = getPayloadColumnsAndFilters(getValidMapItem(), getValidWhereClauseWithEq());
        deleteColumnsValue(TABLE_NAME_1, null, payloadColumnsAndFilters);
    }

    @Test
    public void testDeleteSetColumnWithSuccess() throws Exception {
        getCassandraService().addNewColumn(TABLE_NAME_1, getCassandraProperties().getKeyspace(), TestsConstants.VALID_SET_COLUMN, DataType.set(DataType.text()));
        getCassandraService().insert(getCassandraProperties().getKeyspace(), TABLE_NAME_1, getValidEntityWithSet());

        Map<String, Object> payloadColumnsAndFilters = getPayloadColumnsAndFilters(getValidSet(), getValidWhereClauseWithEq());
        deleteColumnsValue(TABLE_NAME_1, null, payloadColumnsAndFilters);
    }

    @Test
    public void testDeleteWithInvalidInput() throws Exception {
        getCassandraService().insert(getCassandraProperties().getKeyspace(), TABLE_NAME_1, getValidEntity());

        Map<String, Object> payloadColumnsAndFilters = getPayloadColumnsAndFilters(getInvalidEntityForDelete(), getValidWhereClauseWithEq());
        deleteColumnsValueExpException(TABLE_NAME_1, null, payloadColumnsAndFilters);
    }

    @Test
    public void testDeleteWithInvalidWhereClause() throws Exception {
        getCassandraService().insert(getCassandraProperties().getKeyspace(), TABLE_NAME_1, getValidEntity());

        Map<String, Object> payloadColumnsAndFilters = getPayloadColumnsAndFilters(getValidColumnsListForDelete(), getInvalidWhereClause());
        deleteColumnsValueExpException(TABLE_NAME_1, null, payloadColumnsAndFilters);
    }
}
