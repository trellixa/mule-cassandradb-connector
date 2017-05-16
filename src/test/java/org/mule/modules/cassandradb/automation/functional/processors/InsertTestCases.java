/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional.processors;

import com.datastax.driver.core.DataType;
import org.mule.modules.cassandradb.automation.functional.TestDataBuilder;
import org.mule.modules.cassandradb.automation.util.TestsConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.mule.modules.cassandradb.utils.CassandraDBException;

public class InsertTestCases extends CassandraDBConnectorAbstractTestCases {

    @BeforeClass
    public static void setup() throws Exception {
        cassClient.createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), cassConfig.getKeyspace(), TestsConstants.TABLE_NAME_1));
    }

    @AfterClass
    public static void tearDown()  {
        cassClient.dropTable(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace());
    }

    @Test
    public void testInsertWithSuccess() throws CassandraDBException {
        getConnector().insert(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getValidEntity());
    }

    @Test
    public void testInsertInListColumnWithSuccess() throws CassandraDBException {
        // set up the data
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), TestsConstants.VALID_LIST_COLUMN, DataType.list(DataType.text()));

        getConnector().insert(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getValidEntityWithList());
    }

    @Test
    public void testInsertInMapColumnWithSuccess() throws CassandraDBException {
        // set up the data
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), TestsConstants.VALID_MAP_COLUMN, DataType.map(DataType.text(), DataType.text()));

        getConnector().insert(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getValidEntityWithMap());
    }

    @Test
    public void testInsertISetColumnWithSuccess() throws CassandraDBException {
        // set up the data
        cassClient.addNewColumn(TestsConstants.TABLE_NAME_1, cassConfig.getKeyspace(), TestsConstants.VALID_SET_COLUMN, DataType.set(DataType.text()));

        getConnector().insert(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getValidEntityWithSet());
    }

    @Test(expected = CassandraDBException.class)
    public void testInsertWithInvalidInput() throws CassandraDBException {
        getConnector().insert(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getInvalidEntity());
    }

}
