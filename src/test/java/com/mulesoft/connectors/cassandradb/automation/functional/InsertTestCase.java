/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.connectors.cassandradb.automation.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.INVALID_COLUMN_MESSAGE_ERROR;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.TABLE_NAME_1;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getBasicCreateTableInput;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getColumns;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getInvalidEntity;

public class InsertTestCase extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        createTable(getBasicCreateTableInput(getColumns(), testKeyspace, TABLE_NAME_1));
    }

    @After
    public void tearDown() throws Exception {
        dropTable(testKeyspace, TABLE_NAME_1);
    }

    @Test
    public void testInsertWithSuccess() throws Exception {
        insert(testKeyspace, TABLE_NAME_1, TestDataBuilder.getValidEntity());
        sleep(SLEEP_DURATION);
        assertEquals(1, select(format("SELECT * FROM %s.%s", testKeyspace, TABLE_NAME_1), null).size());
    }

    @Test
    public void testInsertWithInvalidInput() throws Exception {
        try{
            insert(testKeyspace, TABLE_NAME_1, getInvalidEntity());
        } catch (Exception e){
            assertThat(e.getMessage(), is(INVALID_COLUMN_MESSAGE_ERROR));
        }
    }
}
