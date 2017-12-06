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

import static org.mule.modules.cassandradb.internal.exception.CassandraError.QUERY_VALIDATION;

public class UpdateTestCase extends AbstractTestCases {

    @Before
    public void setup() throws Exception {
        CreateTableInput basicCreateTableInput = TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getColumns(), getKeyspaceFromProperties(), TestsConstants.TABLE_NAME_1);
        getCassandraService().createTable(basicCreateTableInput);
        getCassandraService().insert(getKeyspaceFromProperties(), TestsConstants.TABLE_NAME_1, TestDataBuilder.getValidEntity());
    }

    @After
    public void tearDown() {
        getCassandraService().dropTable(TestsConstants.TABLE_NAME_1, getKeyspaceFromProperties());
    }

    @Test
    public void testUpdateUsingEqWithSuccess() throws Exception {
        update(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getValidEntityForUpdate(), TestDataBuilder.getValidWhereClauseWithEq()));
    }

    @Test
    public void testUpdateUsingInWithSuccess() throws Exception {
        update(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getValidEntityForUpdate(),
                TestDataBuilder.getValidWhereClauseWithIN()));
    }

    @Test
    public void testUpdateWithInvalidInput() throws Exception {
        Map<String, Object> payloadColumnsAndFilters = TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getInvalidEntity(), TestDataBuilder.getValidWhereClauseWithEq());
        updateExpException(TestsConstants.TABLE_NAME_1, null, payloadColumnsAndFilters, QUERY_VALIDATION);
    }

    @Test
    public void testUpdateWithInvalidWhereClause() throws Exception {
        updateExpException(TestsConstants.TABLE_NAME_1, null, TestDataBuilder.getPayloadColumnsAndFilters(TestDataBuilder.getInvalidEntity(), TestDataBuilder.getInvalidWhereClause()), QUERY_VALIDATION);
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
                .runExpectingException(ErrorTypeMatcher.errorType(error));
    }
}
