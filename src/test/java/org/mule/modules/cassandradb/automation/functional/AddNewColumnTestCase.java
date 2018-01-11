/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mule.modules.cassandradb.api.ColumnType.TEXT;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.TABLE_NAME_1;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.VALID_COLUMN_1;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getAlterColumnInput;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getBasicCreateTableInput;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getColumns;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getRandomColumnName;

public class AddNewColumnTestCase extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        createTable(getBasicCreateTableInput(getColumns(), testKeyspace, TABLE_NAME_1));
    }

    @After
    public void tearDown() throws Exception {
        dropTable(TABLE_NAME_1, testKeyspace);
    }

    @Test
    public void testAddNewColumnOfPrimitiveTypeWithSuccess() {
        try {
            addNewColumn(TABLE_NAME_1, testKeyspace, getAlterColumnInput(getRandomColumnName(), TEXT));
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testAddNewColumnWithSameName() {
        try{
            addNewColumn(TABLE_NAME_1, testKeyspace, getAlterColumnInput(VALID_COLUMN_1, TEXT));
        } catch (Exception e){
            assertThat(e.getMessage(), startsWith("An exception occurred while converting from TEXT."));
        }
    }
}
