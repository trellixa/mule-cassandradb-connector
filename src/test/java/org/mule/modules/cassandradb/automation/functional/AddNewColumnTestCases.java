/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import com.datastax.driver.core.DataType;
import org.junit.*;
import org.mule.modules.cassandradb.api.AlterColumnInput;
import org.mule.modules.cassandradb.api.ColumnType;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.modules.cassandradb.automation.util.TestsConstants;

import static org.junit.Assert.assertTrue;


public class AddNewColumnTestCases extends AbstractTestCases {

    @Before
    public void setup() {
        CreateTableInput basicCreateTableInput = TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), getCassandraProperties().getKeyspace(), TestsConstants.TABLE_NAME_1);
        getCassandraService().createTable(basicCreateTableInput);
    }

    @After
    public void tearDown() {
        getCassandraService().dropTable(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace());
    }

    @Test
    public void testAddNewColumnOfPrimitiveTypeWithSuccess() throws Exception {
        AlterColumnInput alterColumnInput = TestDataBuilder.getAlterColumnInput(DataType.text().toString() + System.currentTimeMillis(), ColumnType.TEXT);
        assertTrue(addNewColumn(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), alterColumnInput));
    }

    @Test
    public void testAddNewColumnWithSameName() throws Exception {
        AlterColumnInput alterColumnInput = TestDataBuilder.getAlterColumnInput(TestsConstants.VALID_COLUMN_1, ColumnType.TEXT);
        addNewColumnExpException(TestsConstants.TABLE_NAME_1, getCassandraProperties().getKeyspace(), alterColumnInput);
    }
}
