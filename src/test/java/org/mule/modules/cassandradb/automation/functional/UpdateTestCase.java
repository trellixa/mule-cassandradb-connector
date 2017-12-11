/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.cassandradb.api.CreateTableInput;
import org.mule.modules.cassandradb.automation.util.TestsConstants;
import org.mule.modules.cassandradb.internal.exception.CassandraError;
import org.mule.tck.junit4.matcher.ErrorTypeMatcher;

import java.util.Map;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getBasicCreateTableInput;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getColumns;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getInvalidEntity;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getInvalidWhereClause;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getPayloadColumnsAndFilters;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidEntity;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidEntityForUpdate;
import static org.mule.modules.cassandradb.automation.functional.TestDataBuilder.getValidWhereClauseWithEq;
import static org.mule.modules.cassandradb.automation.util.TestsConstants.TABLE_NAME_1;
import static org.mule.modules.cassandradb.automation.util.TestsConstants.VALID_COLUMN_2;
import static org.mule.modules.cassandradb.internal.exception.CassandraError.QUERY_VALIDATION;
import static org.mule.tck.junit4.matcher.ErrorTypeMatcher.errorType;

public class UpdateTestCase extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        CreateTableInput basicCreateTableInput = getBasicCreateTableInput(getColumns(), getKeyspaceFromProperties(), TABLE_NAME_1);
        getCassandraService().createTable(basicCreateTableInput);
        getCassandraService().insert(getKeyspaceFromProperties(), TABLE_NAME_1, getValidEntity());
    }

    @After
    public void tearDown() {
        getCassandraService().dropTable(TABLE_NAME_1, getKeyspaceFromProperties());
    }

    @Test
    public void testUpdateUsingEqWithSuccess() throws Exception {
        String updatedValue = "updatedValue";
        update(TABLE_NAME_1, null, getPayloadColumnsAndFilters(getValidEntityForUpdate(updatedValue), getValidWhereClauseWithEq()));

        Thread.sleep(SLEEP_DURATION);
        String query = format("SELECT * FROM %s.%s", getKeyspaceFromProperties(), TABLE_NAME_1);
        String newValue = (String) getCassandraService().select(query, null).get(0).get(VALID_COLUMN_2);
        assertEquals(newValue, updatedValue);
    }

    @Test
    public void testUpdateUsingInWithSuccess() throws Exception {
        update(TABLE_NAME_1, null, getPayloadColumnsAndFilters(getValidEntityForUpdate("newValue"),
                TestDataBuilder.getValidWhereClauseWithIN()));
    }

    @Test
    public void testUpdateWithInvalidInput() throws Exception {
        Map<String, Object> payloadColumnsAndFilters = getPayloadColumnsAndFilters(getInvalidEntity(), getValidWhereClauseWithEq());
        updateExpException(TABLE_NAME_1, null, payloadColumnsAndFilters, QUERY_VALIDATION);
    }

    @Test
    public void testUpdateWithInvalidWhereClause() throws Exception {
        updateExpException(TABLE_NAME_1, null, getPayloadColumnsAndFilters(getInvalidEntity(), getInvalidWhereClause()), QUERY_VALIDATION);
    }

    protected void update(String tableName, String keyspaceName, Map<String, Object> entityToUpdate) throws Exception {
        flowRunner("update-flow")
                .withPayload(entityToUpdate)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .run()
                .getMessage()
                .getPayload()
                .getValue();
    }

    protected void updateExpException(String tableName, String keyspaceName, Map<String, Object> entityToUpdate, CassandraError error) throws Exception {
        flowRunner("update-flow")
                .withPayload(entityToUpdate)
                .withVariable("tableName", tableName)
                .withVariable("keyspaceName", keyspaceName)
                .runExpectingException(errorType(error));
    }
}
