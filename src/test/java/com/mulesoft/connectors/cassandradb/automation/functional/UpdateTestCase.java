/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.connectors.cassandradb.automation.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.mulesoft.connectors.cassandradb.api.CreateTableInput;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.INVALID_COLUMN_MESSAGE_ERROR;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.TABLE_NAME_1;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.UPDATED_VALUE;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.VALID_COLUMN_2;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getBasicCreateTableInput;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getColumns;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getInvalidEntity;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getInvalidWhereClause;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getPayloadColumnsAndFilters;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getValidEntity;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getValidEntityForUpdate;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getValidWhereClauseWithEq;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getValidWhereClauseWithIN;

public class UpdateTestCase extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        CreateTableInput basicCreateTableInput = getBasicCreateTableInput(getColumns(), testKeyspace, TABLE_NAME_1);
        createTable(basicCreateTableInput);
        insert(testKeyspace, TABLE_NAME_1, getValidEntity());
    }

    @After
    public void tearDown() throws Exception {
        dropTable(TABLE_NAME_1, testKeyspace);
    }

    @Test
    public void testUpdateUsingEqWithSuccess() throws Exception {
        update(TABLE_NAME_1, testKeyspace, getPayloadColumnsAndFilters(getValidEntityForUpdate(UPDATED_VALUE), getValidWhereClauseWithEq()));
        Thread.sleep(SLEEP_DURATION);
        assertEquals(select(format("SELECT * FROM %s.%s", testKeyspace, TABLE_NAME_1), null).get(0).get(VALID_COLUMN_2), UPDATED_VALUE);
    }

    @Test
    public void testUpdateUsingInWithSuccess() throws Exception {
        try{
            update(TABLE_NAME_1, testKeyspace, getPayloadColumnsAndFilters(getValidEntityForUpdate("newValue"), getValidWhereClauseWithIN()));
        } catch (Exception e){
            fail();
        }
    }

    @Test
    public void testUpdateWithInvalidInput() throws Exception {
        try{
            update(TABLE_NAME_1, testKeyspace, getPayloadColumnsAndFilters(getInvalidEntity(), getValidWhereClauseWithEq()));
        } catch (Exception e){
            assertThat(e.getMessage(), is(INVALID_COLUMN_MESSAGE_ERROR));
        }
    }

    @Test
    public void testUpdateWithInvalidWhereClause() throws Exception {
        try{
            update(TABLE_NAME_1, testKeyspace, getPayloadColumnsAndFilters(getValidEntity(), getInvalidWhereClause()));
        } catch (Exception e){
            assertThat(e.getMessage(), is("PRIMARY KEY part dummy_partitionkey found in SET part."));
        }
    }
}
