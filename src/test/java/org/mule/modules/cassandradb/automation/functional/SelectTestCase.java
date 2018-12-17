/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mule.modules.cassandradb.api.ColumnType;

import java.util.LinkedList;

import static java.lang.Integer.valueOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;
@Ignore

public class SelectTestCase extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getPrimaryKey(), testKeyspace, TestDataBuilder.TABLE_NAME_1));
        addNewColumn(TestDataBuilder.TABLE_NAME_1, testKeyspace, TestDataBuilder.getAlterColumnInput(TestDataBuilder.VALID_COLUMN_2, ColumnType.TEXT));
        insert(testKeyspace, TestDataBuilder.TABLE_NAME_1, TestDataBuilder.getValidEntity());
    }

    @After
    public void tearDown() throws Exception {
        dropTable(TestDataBuilder.TABLE_NAME_1, testKeyspace);
    }

    @Test
    public void testSelectNativeQueryWithParameters() throws Exception {
        assertThat(valueOf(select(TestDataBuilder.VALID_PARAMETERIZED_QUERY, TestDataBuilder.getValidParmList()).size()),greaterThan(0));
    }

    @Test
    public void testSelectNativeQueryWithInvalidParameters() throws Exception {
        try{
            select(TestDataBuilder.VALID_PARAMETERIZED_QUERY, new LinkedList<>());
        } catch (Exception e){
            assertThat(e.getMessage(), is("Expected query parameters is 2 but found 0."));
        }
    }

    @Test
    public void testSelectDSQLQuery() throws Exception {
        assertThat(valueOf(select(TestDataBuilder.VALID_DSQL_QUERY, null).size()),greaterThan(0));
    }
}
