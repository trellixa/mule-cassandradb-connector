/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.automation.functional.processors;

import com.datastax.driver.core.DataType;
import com.mulesoft.mule.cassandradb.automation.functional.CassandraDBConnectorAbstractTestCase;
import com.mulesoft.mule.cassandradb.automation.functional.TestDataBuilder;
import com.mulesoft.mule.cassandradb.util.ConstantsTest;
import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DeleteTestCases extends CassandraDBConnectorAbstractTestCase {

    @BeforeClass
    public static void setup() throws Exception {
        cassClient.createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME_1));
    }

    @AfterClass
    public static void tearDown() {
        cassClient.dropTable(ConstantsTest.TABLE_NAME_1, cassConfig.getKeyspace());
    }

    @Test
    public void testDeleteColumnUsingEqWithSuccess() throws CassandraDBException {
        // set up the data
        cassClient.insert(cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME_1, TestDataBuilder.getValidEntity());

        getConnector().deleteColumnsValue(ConstantsTest.TABLE_NAME_1, null,
                TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getValidColumnsListForDelete(), TestDataBuilder.getValidWhereClauseWithEq()));
    }

    @Test
    public void testDeleteRowUsingEqWithSuccess() throws CassandraDBException {
        // set up the data
        cassClient.insert(cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME_1, TestDataBuilder.getValidEntity());

        getConnector().deleteRows(ConstantsTest.TABLE_NAME_1, null, TestDataBuilder.getPayloadColumnsAndFilters(null, TestDataBuilder.getValidWhereClauseWithEq()));
    }

    @Test
    public void testDeleteColumnUsingINWithSuccess() throws CassandraDBException {
        // set up the data
        cassClient.insert(cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME_1, TestDataBuilder.getValidEntity());

        getConnector().deleteColumnsValue(ConstantsTest.TABLE_NAME_1, null,
                TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getValidColumnsListForDelete(), TestDataBuilder.getValidWhereClauseWithIN()));
    }

    @Test
    public void testDeleteRowUsingINWithSuccess() throws CassandraDBException {
        // set up the data
        cassClient.insert(cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME_1, TestDataBuilder.getValidEntity());

        getConnector().deleteRows(ConstantsTest.TABLE_NAME_1, null, TestDataBuilder.getPayloadColumnsAndFilters(null, TestDataBuilder.getValidWhereClauseWithIN()));
    }

    @Test
    public void testDeleteItemFromListWithSuccess() throws CassandraDBException {
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME_1, cassConfig.getKeyspace(), ConstantsTest.VALID_LIST_COLUMN, DataType.list(DataType.text()));
        // set up the data
        cassClient.insert(cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME_1, TestDataBuilder.getValidEntityWithList());

        getConnector().deleteColumnsValue(ConstantsTest.TABLE_NAME_1, null,
                TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getValidListItem(), TestDataBuilder.getValidWhereClauseWithEq()));
    }

    @Test
    public void testDeleteItemFromMapWithSuccess() throws CassandraDBException {
        // set up the data
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME_1, cassConfig.getKeyspace(), ConstantsTest.VALID_MAP_COLUMN, DataType.map(DataType.text(), DataType.text()));
        cassClient.insert(cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME_1, TestDataBuilder.getValidEntityWithMap());

        getConnector().deleteColumnsValue(ConstantsTest.TABLE_NAME_1, null,
                TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getValidMapItem(), TestDataBuilder.getValidWhereClauseWithEq()));
    }

    @Test
    public void testDeleteSetColumnWithSuccess() throws CassandraDBException {
        // set up the data
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME_1, cassConfig.getKeyspace(), ConstantsTest.VALID_SET_COLUMN, DataType.set(DataType.text()));
        cassClient.insert(cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME_1, TestDataBuilder.getValidEntityWithSet());

        getConnector().deleteColumnsValue(ConstantsTest.TABLE_NAME_1, null,
                TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getValidSet(), TestDataBuilder.getValidWhereClauseWithEq()));
    }

    @Test(expected = CassandraDBException.class)
    public void testDeleteWithInvalidInput() throws CassandraDBException {
        // set up the data
        cassClient.insert(cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME_1, TestDataBuilder.getValidEntity());

        getConnector().deleteColumnsValue(ConstantsTest.TABLE_NAME_1, null,
                TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getInvalidEntityForDelete(), TestDataBuilder.getValidWhereClauseWithEq()));
    }

    @Test(expected = CassandraDBException.class)
    public void testDeleteWithInvalidWhereClause() throws CassandraDBException {
        // set up the data
        cassClient.insert(cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME_1, TestDataBuilder.getValidEntity());

        getConnector().deleteColumnsValue(ConstantsTest.TABLE_NAME_1, null,
                TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getValidColumnsListForDelete(), TestDataBuilder.getInvalidWhereClause()));
    }
}
