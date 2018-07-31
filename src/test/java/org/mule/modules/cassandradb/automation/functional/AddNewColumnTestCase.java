/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.cassandradb.api.ColumnType;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class AddNewColumnTestCase extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), testKeyspace, TestDataBuilder.TABLE_NAME_1));
    }

    @After
    public void tearDown() throws Exception {
        dropTable(TestDataBuilder.TABLE_NAME_1, testKeyspace);
    }

    @Test
    public void testAddNewColumnOfPrimitiveTypeWithSuccess() {
        try {
            addNewColumn(TestDataBuilder.TABLE_NAME_1, testKeyspace, TestDataBuilder.getAlterColumnInput(TestDataBuilder.getRandomColumnName(), ColumnType.TEXT));
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testAddNewColumnWithSameName() {
        try{
            addNewColumn(TestDataBuilder.TABLE_NAME_1, testKeyspace, TestDataBuilder.getAlterColumnInput(TestDataBuilder.VALID_COLUMN_1, ColumnType.TEXT));
        } catch (Exception e){
            assertThat(e.getMessage(), startsWith("Invalid column name dummy_column_1 because it conflicts with an existing column."));
        }
    }
}
