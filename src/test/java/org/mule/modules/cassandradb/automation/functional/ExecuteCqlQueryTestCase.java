/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mule.modules.cassandradb.api.CQLQueryInput;
import org.mule.modules.cassandradb.api.ColumnType;
import org.mule.modules.cassandradb.api.CreateTableInput;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.DUMMY_PARTITION_KEY;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.QUERY_PREFIX;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.TABLE_NAME_2;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.VALID_COLUMN_2;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getAlterColumnInput;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getPrimaryKey;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidEntity;
@Ignore

public class ExecuteCqlQueryTestCase extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        try{
            CreateTableInput basicCreateTableInput = TestDataBuilder.getBasicCreateTableInput(getPrimaryKey(), testKeyspace, TABLE_NAME_2);
            createTable(basicCreateTableInput);
            addNewColumn(TABLE_NAME_2, testKeyspace, getAlterColumnInput(VALID_COLUMN_2, ColumnType.TEXT));
            insert(testKeyspace, TABLE_NAME_2, getValidEntity());
        } catch (Exception e){
            fail();
        }
    }

    @After
    public void tearDown() throws Exception {
        dropTable(TABLE_NAME_2, testKeyspace);
    }

    @Test
    public void shouldExecute_ValidParametrizedQuery() throws Exception {
        //Select * from dummy_table_name_2 WHERE dummy_partition_key = 'value1'
        List<Object> params = new ArrayList<Object>();
        params.add("value1");
        String whereClause = " WHERE " + DUMMY_PARTITION_KEY + " = ?";
        CQLQueryInput query = new CQLQueryInput();
        query.setCqlQuery(QUERY_PREFIX + testKeyspace + "." + TABLE_NAME_2 + whereClause);
        query.setParameters(params);
        assertNotNull(executeCQLQuery(query).get(0));
    }

    @Test
    public void shouldExecute_NonParametrizedQuery() throws Exception {
        CQLQueryInput query = new CQLQueryInput();
        query.setCqlQuery(QUERY_PREFIX + testKeyspace + "." + TABLE_NAME_2);
        query.setParameters(new ArrayList<>());
        assertNotNull(executeCQLQuery(query).get(0));
    }

    @Test
    public void shouldThrowException_When_InsufficientAmountOfBindVariables() {
        //Select * from dummy_table_name_2 WHERE dummy_partition_key = ?
        try{
            String whereClause = " WHERE " + DUMMY_PARTITION_KEY + " = ?";
            CQLQueryInput query = new CQLQueryInput();
            query.setCqlQuery(QUERY_PREFIX + testKeyspace + "." + TABLE_NAME_2 + whereClause);
            executeCQLQuery(query);
        } catch (Exception e){
            assertThat(e.getMessage(), is("Invalid amount of bind variables."));
        }
    }

    @Test
    public void shouldThrowException_When_InsufficientAmountOfParameters() {
        //Select * from dummy_table_name_2
        try{
            List<Object> params = new ArrayList<Object>();
            params.add("value1");
            CQLQueryInput query = new CQLQueryInput();
            query.setCqlQuery(QUERY_PREFIX + testKeyspace + "." + TABLE_NAME_2);
            query.setParameters(params);
            executeCQLQuery(query);
        } catch (Exception e){
            assertThat(e.getMessage(), is("Prepared statement has only 0 variables, 1 values provided."));
        }
    }
}
