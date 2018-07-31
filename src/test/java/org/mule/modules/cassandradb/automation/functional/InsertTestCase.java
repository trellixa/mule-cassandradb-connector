/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class InsertTestCase extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), testKeyspace, TestDataBuilder.TABLE_NAME_1));
    }

    @After
    public void tearDown() throws Exception {
        dropTable(testKeyspace, TestDataBuilder.TABLE_NAME_1);
    }

    @Test
    public void testInsertWithSuccess() throws Exception {
        insert(testKeyspace, TestDataBuilder.TABLE_NAME_1, TestDataBuilder.getValidEntity());
        sleep(SLEEP_DURATION);
        assertEquals(1, select(String.format("SELECT * FROM %s.%s", testKeyspace, TestDataBuilder.TABLE_NAME_1), null).size());
    }

    @Test
    public void testInsertWithInvalidInput() throws Exception {
        try{
            insert(testKeyspace, TestDataBuilder.TABLE_NAME_1, TestDataBuilder.getInvalidEntity());
        } catch (Exception e){
            assertThat(e.getMessage(), CoreMatchers.is(TestDataBuilder.INVALID_COLUMN_MESSAGE_ERROR));
        }
    }
}
