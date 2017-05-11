/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.automation.functional.processors;

import com.datastax.driver.core.DataType;
import com.mulesoft.mule.cassandradb.automation.functional.CassandraDBConnectorAbstractTestCase;
import com.mulesoft.mule.cassandradb.automation.functional.TestDataBuilder;
import com.mulesoft.mule.cassandradb.util.ConstantsTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mulesoft.mule.cassandradb.utils.CassandraDBException;

public class InsertTestCases extends CassandraDBConnectorAbstractTestCase {

    @BeforeClass
    public static void setup() throws Exception {
        cassClient.createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME_1));
    }

    @AfterClass
    public static void tearDown()  {
        cassClient.dropTable(ConstantsTest.TABLE_NAME_1, cassConfig.getKeyspace());
    }

    @Test
    public void testInsertWithSuccess() throws CassandraDBException {
        getConnector().insert(ConstantsTest.TABLE_NAME_1, TestDataBuilder.getValidEntity());
    }

    @Test
    public void testInsertInListColumnWithSuccess() throws CassandraDBException {
        // set up the data
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME_1, cassConfig.getKeyspace(), ConstantsTest.VALID_LIST_COLUMN, DataType.list(DataType.text()));

        getConnector().insert(ConstantsTest.TABLE_NAME_1, TestDataBuilder.getValidEntityWithList());
    }

    @Test
    public void testInsertInMapColumnWithSuccess() throws CassandraDBException {
        // set up the data
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME_1, cassConfig.getKeyspace(), ConstantsTest.VALID_MAP_COLUMN, DataType.map(DataType.text(), DataType.text()));

        getConnector().insert(ConstantsTest.TABLE_NAME_1, TestDataBuilder.getValidEntityWithMap());
    }

    @Test
    public void testInsertISetColumnWithSuccess() throws CassandraDBException {
        // set up the data
        cassClient.addNewColumn(ConstantsTest.TABLE_NAME_1, cassConfig.getKeyspace(), ConstantsTest.VALID_SET_COLUMN, DataType.set(DataType.text()));

        getConnector().insert(ConstantsTest.TABLE_NAME_1, TestDataBuilder.getValidEntityWithSet());
    }

    @Test(expected = CassandraDBException.class)
    public void testInsertWithInvalidInput() throws CassandraDBException {
        getConnector().insert(ConstantsTest.TABLE_NAME_1, TestDataBuilder.getInvalidEntity());
    }

}
