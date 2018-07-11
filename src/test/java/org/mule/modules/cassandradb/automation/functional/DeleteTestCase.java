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
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.INVALID_COLUMN_MESSAGE_ERROR;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.TABLE_NAME_1;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getBasicCreateTableInput;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getColumns;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getInvalidEntityForDelete;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getInvalidWhereClause;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidColumnsListForDelete;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidEntity;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidWhereClauseWithEq;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidWhereClauseWithIN;

public class DeleteTestCase extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        createTable(getBasicCreateTableInput(getColumns(), testKeyspace, TABLE_NAME_1));
    }

    @After
    public void tearDown() throws Exception {
        dropTable(TABLE_NAME_1, testKeyspace);
    }

    @Test
    public void testDeleteColumnUsingEqWithSuccess() throws Exception {
        try{
            insert(testKeyspace, TABLE_NAME_1, getValidEntity());
            deleteColumnsValue(TABLE_NAME_1, testKeyspace, getValidColumnsListForDelete(), getValidWhereClauseWithEq());
        } catch (Exception e){
            fail();
        }
    }

    @Test
    public void testDeleteRowUsingEqWithSuccess() throws Exception {
        try{
            insert(testKeyspace, TABLE_NAME_1, getValidEntity());
            deleteRows(TABLE_NAME_1, testKeyspace, getValidWhereClauseWithEq());
        } catch (Exception e){
            fail();
        }
    }

    @Test
    public void testDeleteColumnUsingINWithSuccess() {
        try{
            insert(testKeyspace, TABLE_NAME_1, getValidEntity());
            deleteColumnsValue(TABLE_NAME_1, testKeyspace, getValidColumnsListForDelete(), getValidWhereClauseWithIN());
        } catch (Exception e){
            fail();
        }
    }

    @Test
    public void testDeleteRowUsingINWithSuccess() {
        try{
            insert(testKeyspace, TABLE_NAME_1, getValidEntity());
            deleteRows(TABLE_NAME_1, testKeyspace, getValidWhereClauseWithIN());
        } catch (Exception e){
            fail();
        }
    }

    @Test
    public void testDeleteWithInvalidInput() {
        try{
            insert(testKeyspace, TABLE_NAME_1, getValidEntity());
            deleteColumnsValue(TABLE_NAME_1, testKeyspace, getInvalidEntityForDelete(), getValidWhereClauseWithEq());
        } catch( Exception e){
            assertThat(e.getMessage(), is(INVALID_COLUMN_MESSAGE_ERROR));
        }
    }

    @Test
    public void testDeleteWithInvalidWhereClause() throws Exception {
        try{
            insert(testKeyspace, TABLE_NAME_1, getValidEntity());
            deleteColumnsValue(TABLE_NAME_1, testKeyspace, getValidColumnsListForDelete(), getInvalidWhereClause());
        } catch (Exception e){
            assertThat(e.getMessage(), is("Some partition key parts are missing: dummy_partitionkey."));
        }
    }
}
