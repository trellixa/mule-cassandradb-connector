/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import com.datastax.driver.core.DataType;
import org.junit.*;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.modules.cassandradb.automation.util.TestsConstants;

public class InsertTestCase extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        CreateTableInput basicCreateTableInput = TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), getKeyspaceFromProperties(), TestsConstants.TABLE_NAME_1);
        getCassandraService().createTable(basicCreateTableInput);
    }

    @After
    public void tearDown()  {
        getCassandraService().dropTable(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties());
    }

    @Test
    public void testInsertWithSuccess() throws Exception {
        insert(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getValidEntity());
        //TODO: add validation that the objects are created.
    }

    @Test
    public void testInsertInListColumnWithSuccess() throws Exception {
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), TestsConstants.VALID_LIST_COLUMN, DataType.list(DataType.text()));

        insert(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getValidEntityWithList());
    }

    @Test
    public void testInsertInMapColumnWithSuccess() throws Exception {
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), TestsConstants.VALID_MAP_COLUMN, DataType.map(DataType.text(), DataType.text()));

        insert(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getValidEntityWithMap());
    }

    @Test
    public void testInsertISetColumnWithSuccess() throws Exception {
        getCassandraService().addNewColumn(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties(), TestsConstants.VALID_SET_COLUMN, DataType.set(DataType.text()));

        insert(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getValidEntityWithSet());
    }

    @Test
    public void testInsertWithInvalidInput() throws Exception {
        insertExpError(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getInvalidEntity());
    }

}
