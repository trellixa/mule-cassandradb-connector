/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.connectors.cassandradb.automation.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.mulesoft.connectors.cassandradb.api.ColumnType;

import java.util.LinkedList;

import static java.lang.Integer.valueOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.TABLE_NAME_1;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.VALID_COLUMN_2;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.VALID_DSQL_QUERY;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.VALID_PARAMETERIZED_QUERY;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getAlterColumnInput;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getBasicCreateTableInput;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getValidEntity;
import static com.mulesoft.connectors.cassandradb.automation.functional.TestDataBuilder.getValidParmList;

public class SelectTestCase extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        createTable(getBasicCreateTableInput(TestDataBuilder.getPrimaryKey(), testKeyspace, TABLE_NAME_1));
        addNewColumn(TABLE_NAME_1, testKeyspace, getAlterColumnInput(VALID_COLUMN_2, ColumnType.TEXT));
        insert(testKeyspace, TABLE_NAME_1, getValidEntity());
    }

    @After
    public void tearDown() throws Exception {
        dropTable(TABLE_NAME_1, testKeyspace);
    }

    @Test
    public void testSelectNativeQueryWithParameters() throws Exception {
        assertThat(valueOf(select(VALID_PARAMETERIZED_QUERY, getValidParmList()).size()),greaterThan(0));
    }

    @Test
    public void testSelectNativeQueryWithInvalidParameters() throws Exception {
        try{
            select(VALID_PARAMETERIZED_QUERY, new LinkedList<>());
        } catch (Exception e){
            assertThat(e.getMessage(), is("Expected query parameters is 2 but found 0."));
        }
    }

    @Test
    public void testSelectDSQLQuery() throws Exception {
        assertThat(valueOf(select(VALID_DSQL_QUERY, null).size()),greaterThan(0));
    }
}
