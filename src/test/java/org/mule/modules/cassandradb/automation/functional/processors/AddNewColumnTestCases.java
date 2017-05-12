/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional.processors;

import com.datastax.driver.core.DataType;
import org.mule.modules.cassandradb.automation.functional.TestDataBuilder;
import org.mule.modules.cassandradb.metadata.ColumnType;
import org.mule.modules.cassandradb.automation.util.ConstantsTest;
import org.mule.modules.cassandradb.utils.CassandraDBException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AddNewColumnTestCases extends CassandraDBConnectorAbstractTestCases {

    @BeforeClass
    public static void setup() throws CassandraDBException {
        cassClient.createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME_1));
    }

    @AfterClass
    public static void tearDown() {
        cassClient.dropTable(ConstantsTest.TABLE_NAME_1, cassConfig.getKeyspace());
    }

    @Test
    public void testAddNewColumnOfPrimitiveTypeWithSuccess() throws CassandraDBException {
        getConnector().addNewColumn(ConstantsTest.TABLE_NAME_1,
                TestDataBuilder.getAlterColumnInput(DataType.text().toString() + System.currentTimeMillis(), ColumnType.TEXT));
    }

    @Test(expected = CassandraDBException.class)
    public void testAddNewColumnWithSameName() throws CassandraDBException {
        getConnector().addNewColumn(ConstantsTest.TABLE_NAME_1, TestDataBuilder.getAlterColumnInput(ConstantsTest.VALID_COLUMN_1, ColumnType.TEXT));
    }
}
