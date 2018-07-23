/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.connectors.cassandradb.automation.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.TABLE_NAME_1;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.VALID_COLUMN_1;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getBasicCreateTableInput;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getCompositePrimaryKey;

public class RenameColumnTestCase extends AbstractTestCases {
    @Before
    public  void setup() throws Exception {
        createTable(getBasicCreateTableInput(getCompositePrimaryKey(), testKeyspace, TABLE_NAME_1));
    }

    @After
    public void tearDown() throws Exception {
        dropTable(TABLE_NAME_1, testKeyspace);
    }

    @Test
    public void shouldRenamePKColumnWithSuccess() throws Exception {
        try{
            renameColumn(TABLE_NAME_1, testKeyspace, VALID_COLUMN_1, "renamed");
        } catch (Exception e){
            fail();
        }
    }
}
