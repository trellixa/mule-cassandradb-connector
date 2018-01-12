/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.COLUMN;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.TABLE_NAME_1;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.VALID_COLUMN_1;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.VALID_COLUMN_2;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getBasicCreateTableInput;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getColumns;


public class DropColumnTestCase extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        createTable(getBasicCreateTableInput(getColumns(), testKeyspace, TABLE_NAME_1));
    }

    @After
    public void tearDown() throws Exception {
        dropTable(TABLE_NAME_1, testKeyspace);
    }

    @Test
    public void testRemoveColumnWithSuccess() throws Exception {
        try{
            dropColumn(TABLE_NAME_1, testKeyspace, VALID_COLUMN_1);
            dropColumn(TABLE_NAME_1, testKeyspace, VALID_COLUMN_2);
        } catch (Exception e){
            fail();
        }
    }

    @Test
    public void testRemoveColumnWithInvalidName() throws Exception {
        try{
            dropColumn(TABLE_NAME_1, testKeyspace, COLUMN);
        } catch (Exception e){
            assertThat(e.getMessage(), is("Column column was not found in table dummy_table_name_1."));
        }
    }
}
