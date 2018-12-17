/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.hamcrest.CoreMatchers;
import org.junit.*;
import org.mule.modules.cassandradb.api.CreateTableInput;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
@Ignore

public class UpdateTestCase extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        CreateTableInput basicCreateTableInput = TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), testKeyspace, TestDataBuilder.TABLE_NAME_1);
        createTable(basicCreateTableInput);
        insert(testKeyspace, TestDataBuilder.TABLE_NAME_1, TestDataBuilder.getValidEntity());
    }

    @After
    public void tearDown() throws Exception {
        dropTable(TestDataBuilder.TABLE_NAME_1, testKeyspace);
    }

    @Test
    public void testUpdateUsingEqWithSuccess() throws Exception {
        update(TestDataBuilder.TABLE_NAME_1, testKeyspace, TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getValidEntityForUpdate(TestDataBuilder.UPDATED_VALUE), TestDataBuilder.getValidWhereClauseWithEq()));
        Thread.sleep(SLEEP_DURATION);
        Assert.assertEquals(select(String.format("SELECT * FROM %s.%s", testKeyspace, TestDataBuilder.TABLE_NAME_1), null).get(0).get(TestDataBuilder.VALID_COLUMN_2), TestDataBuilder.UPDATED_VALUE);
    }

    @Test
    public void testUpdateUsingInWithSuccess() throws Exception {
        try{
            update(TestDataBuilder.TABLE_NAME_1, testKeyspace, TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getValidEntityForUpdate("newValue"), TestDataBuilder.getValidWhereClauseWithIN()));
        } catch (Exception e){
            fail();
        }
    }

    @Test
    public void testUpdateWithInvalidInput() throws Exception {
        try{
            update(TestDataBuilder.TABLE_NAME_1, testKeyspace, TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getInvalidEntity(), TestDataBuilder.getValidWhereClauseWithEq()));
        } catch (Exception e){
            assertThat(e.getMessage(), CoreMatchers.is(TestDataBuilder.INVALID_COLUMN_MESSAGE_ERROR));
        }
    }

    @Test
    public void testUpdateWithInvalidWhereClause() throws Exception {
        try{
            update(TestDataBuilder.TABLE_NAME_1, testKeyspace, TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getValidEntity(), TestDataBuilder.getInvalidWhereClause()));
        } catch (Exception e){
            assertThat(e.getMessage(), is("PRIMARY KEY part dummy_partitionkey found in SET part."));
        }
    }
}
